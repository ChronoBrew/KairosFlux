# 迭代复盘：BanNet 健壮性审计——一个坏帧/一个业务 bug 不该打崩整个服务

日期：2026-08-20　范围：`bannet/`（含 server 侧接线）、`proto/scan.go`、
`client/conn.go`、`config`、`internal/metrics`

## 背景

作者反馈"bannet 有很多问题，其实会 panic"。这轮是对 `bannet/` 的全量审计：
逐类排查显式 panic、数组/切片越界、nil 解引用、不带 ok 的类型断言、TLV 解析器
的经典漏洞（长度字段攻击者可控）、并发泄漏/竞态、资源上限是否真的被强制。

**方法论**：审计不是"读代码猜风险"，每一条发现都先写一个最小复现脚本实测触发，
确认是真实可复现的 panic/漏洞而不是理论推测，再动手修——这条纪律直接决定了
下面哪些是"确认的 bug"、哪些只是"看起来可疑但没实证"。

## 发现清单（修复前 → 修复后）

### 1（最高优先级）：业务 Handler 里任意一个 panic 打崩整个进程

**修复前**：`bannet/` 里没有任何一处 `recover()`——用一个故意 `panic()` 的
`Handler.Handle` 实现接入真实服务端验证，一次请求触发的 panic 直接终止了
整个测试进程（连同其它所有正常连接、其它测试用例一起死掉，退出码非 0）。

这不是边缘情形：`MsgHandle.DoMsgHandle` 是**唯一**执行业务代码
（`PreHandle`/`Handle`/`PostHandle`）的地方，被两条路径共用——无 worker 池时
`go DoMsgHandle(req)` 每帧一个新 goroutine，有 worker 池时是常驻 worker 的
`for range taskQueue { DoMsgHandle(request) }`。两条路径都没有任何防护，
业务代码里一个 nil 解引用、一次越界、一个不小心的类型断言，就是整个服务下线。

**修复后**：`DoMsgHandle` 顶部加 `recover()`，记录 `msgID`+panic 值+堆栈到
`slog.Error`，并计入新增的 `metrics.PanicsRecovered` 计数器（预期恒为 0，非 0
就是有真实 bug 需要跟进，不是"正常代价"）。同样的 `recoverConnGoroutine` 兜底
也加到了 `Connection.StartReader`/`StartWriter`/`Start` 与
`Server.CallConnStartFunc`/`CallConnStopFunc`——覆盖除业务分派外，帧解析本身、
连接生命周期回调本身可能出现的未知 panic，多一层纵深防御。

回归测试：`bannet/panic_recovery_test.go` 的 `TestHandlerPanicRecovered`——用
同样"故意 panic 的 Handler"接真实服务端，断言进程存活、`PanicsRecovered` 计数
增加、服务端在 panic 之后仍能接受新连接。

### 2：workerPoolSize==0 时的整数除零 panic

**修复前**：`SendMsgToTaskQueue` 里 `request.Conn().ID() % m.workerPoolSize`，
`m.workerPoolSize==0` 时直接 `panic: integer divide by zero`（已用最小复现验证）。
触发条件：`Connection.useWorkerPool`（连接构造时快照的
`config.G.WorkerPoolSize>0`）与 `MsgHandle.workerPoolSize`（`MsgHandle` 构造时
快照的同一全局配置）是两次独立读取，一旦两次快照之间配置被改过就会不一致——
本仓库测试普遍会临时改写 `config.G` 的字段，这不是纯理论场景。

**修复后**：`workerPoolSize==0` 时直接同步调用 `DoMsgHandle`，不做取模。
回归测试：`bannet/msghandle_internal_test.go` 的
`TestSendMsgToTaskQueue_ZeroWorkerPoolSizeNoPanic`（同包内测试，直接构造
`workerPoolSize:0` 的 `MsgHandle`）。

### 3：MaxPackageSize=0 时的内存放大 DoS

**修复前**：帧长上限检查是 `c.maxPackageSize > 0 && msg.MsgLen() > c.maxPackageSize`
——配置为 0（运维直觉是"不限制"）时整个检查被跳过，攻击者发一个 6 字节头部
声称 `dataLen=0xFFFFFFFF`（约 4.29GiB），服务端会在读到任何负载字节之前先
`make([]byte, 4.29GiB)`。这是 TLV 解析器最经典的内存放大攻击，配置的"0=不限"
语义本身就是这个漏洞的入口——默认配置下 `MaxPackageSize=16MiB` 不受影响，但
"运维手动设成 0 以为是关闭限制"是一个真实存在的误用路径。

**修复后**：新增 `hardMaxPackageSize`（256MiB）常量与 `effectiveMaxPackageSize()`
方法，配置为 0 时退回硬上限而不是"不限制"。回归测试：
`bannet/malformed_frame_test.go` 的
`TestMalformedFrame_OversizedWithZeroConfiguredLimitStillCapped`。

### 4：SCAN 响应解码的同款内存放大（proto 包，客户端侧）

**修复前**：`proto.DecodeScanResponse` 里 `count`（响应负载里攻击者/故障对端
可控的 u32）被直接用作 `make([]ScanEntry, 0, count)` 的预分配容量，在验证
实际负载是否真有这么多字节**之前**。`ScanEntry` 在 64 位平台是 48 字节
（两个切片头），`count=0xFFFFFFFF` 时这一行会尝试预留约 206GiB 容量——用一个
只有 7 字节的负载即可触发（已用测试验证："成功"分配巨大虚拟容量而不报错，
是虚拟内存机制掩盖了问题，不代表这不是漏洞）。这是与 #3 完全同类的漏洞，
只是换了一个方向（客户端解析"服务端"发来的字节）与一个包（`proto` 而非
`bannet`）。

**修复后**：预分配容量夹在 `min(count, (len(payload)-off)/8)`——每个条目最少
占 8 字节（keyLen+valueLen 头），这是不管 `count` 声明多大都成立的真实上界。
用 `go test -fuzz` 对 `DecodeScanRequest`/`DecodeScanResponse` 各跑 90 秒
（合计 2700 万次执行）复核，未发现其它同类问题。

### 5：客户端响应读取的同款问题（client 包）

**修复前**：`client/conn.go` 的 `roundTrip` 在校验 `dataLen` 之前就
`make([]byte, int(idLen)+int(dataLen))`——一个恶意或被攻陷的服务端只需回一个
6 字节头部就能让客户端尝试分配数 GiB，是 #3/#4 在协议另一端的镜像攻击面。

**修复后**：新增 `maxResponseFrameSize`（64MiB）常量，校验放在分配之前，超限
直接返回 `ErrProtocol` 并标记连接不可信（不放回连接池）。

### 6、7：无读/写超时——慢客户端/慢消费者永久占用资源

**修复前**：`bannet.Connection` 从未调用过 `SetReadDeadline`/`SetWriteDeadline`。
一个发送半个帧后彻底沉默的客户端（不断连，只是不再发送任何字节），会让
`io.ReadFull` 永久阻塞——这个连接永久占用一个 goroutine 与一个 `MaxConn` 名额，
不会因为任何机制而被回收。写方向同理：对端不读、TCP 发送缓冲区堆满时 `Write`
可能永久阻塞。

**修复后**：新增 `config.G.ConnReadTimeoutMs`（默认 30000ms）；每个逻辑读取
单元（头部/msgID/负载）开始前调用 `resetReadDeadline()`，一个正常但慢的大帧
不会被腰斩，只有"某一步完全不发了"才会触发超时；`write()` 复用同一超时值做
写超时。回归测试：`TestMalformedFrame_SlowClientSilentHalfHeader_TimesOut`
（短超时验证行为，不等默认的 30s）。

### 8：Accept 循环在持续错误下的自我资源耗尽

**修复前**：`acceptLoop` 遇到瞬时 `Accept` 错误（如 EMFILE：句柄数耗尽）时
`continue` 重试，没有任何退避——一旦进入持续错误状态，循环会用尽一个 CPU
核心空转打错误日志，这是服务端自己造成的资源耗尽，不需要攻击者配合。

**修复后**：借鉴 `net/http.Server` 的标准处理方式，加指数退避（5ms→1s，
一次成功 Accept 即重置）。

## 畸形帧测试集（`bannet/malformed_frame_test.go`）

对一个真实运行的服务端逐个打以下场景，断言优雅拒绝、进程不死、其它连接不
受影响（用一条全新连接跑完整 PUT 往返作为"服务仍健康"的直接证据）：

| 场景 | 断言 |
|---|---|
| 截断帧（只发半个头部即断连） | 服务端从 `io.ReadFull` 拿到 EOF，优雅清理，其它连接不受影响 |
| 超长声明 + `MaxPackageSize=0` | 硬上限（256MiB）兜底拒绝，不去分配 |
| 零长帧（dataLen=0, idLen=0） | 不是错误，只是空 msgID 查不到路由，连接保持可用 |
| 非法 msgID（非 UTF-8 乱码） | "unregistered msgID" 日志，不 panic，连接保持可用 |
| 乱码 payload（合法 msgID + 随机二进制负载） | 原样交给 Handler（bannet 不解析业务负载），正常收到响应 |
| 半帧后断连（头部+msgID 发完，负载只发一半） | `io.ReadFull` 拿到 `ErrUnexpectedEOF`，优雅清理 |
| 慢客户端沉默不断连 | 读超时窗口内被服务端主动关闭 |

## Fuzz 结果

`go test -fuzz` 对 4 个帧解析函数合计跑 300 秒（5 分钟），共约 3770 万次执行，
**零崩溃**：

| 目标 | 时长 | 执行次数 |
|---|---|---|
| `bannet.FuzzUnPack` | 60s | 369,985 |
| `proto.FuzzDecodeScanRequest` | 90s | 15,108,651 |
| `proto.FuzzDecodeScanResponse`（修复后复核） | 90s | 12,065,019 |
| `service/ingesthook.FuzzParsePut` | 60s | 10,203,443 |

`FuzzDecodeScanResponse` 是刻意安排了更长时间的目标——它对应的是刚修复的
真实漏洞（发现 #4），额外的 fuzz 时间是为了确认修复没有引入新问题、也没有
遗漏同一函数里的其它同类问题。

## 验证

- `go vet ./...`、`go build ./...`、`go build -tags experimental ./...` 全干净。
- `go test ./... -race` 全绿（含新增的 malformed frame / panic recovery /
  divide-by-zero 回归测试）。
- 详见上表的 fuzz 结果，零崩溃。

## 遗留

- `SendMsgToTaskQueue` work-stealing 全部队列满时"退化为阻塞等待"
  （`m.taskQueues[workerID] <- request`）仍然可能在极端背压下阻塞调用方
  goroutine——本轮没有改这部分行为，只处理了明确的 panic/资源放大问题。
- `proto/scan.go` 的长度字段求和（`off+startLen+endLen+fieldLen`）用 `int`
  运算，在 32 位平台上理论上有溢出风险（64 位平台上 `int` 是 64 位，三个
  u32 求和不会溢出）；当前部署假设是 64 位服务器，未做额外处理。
- 客户端连接池（`client.Client`）层面还没有对"响应体过大"之外的其它对端
  异常行为做更全面的防护（如响应帧无限延迟但未超时的资源占用），复用现有
  `RequestTimeout`/`context` 机制，未新增机制。
