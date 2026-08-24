# RFC：时态内核 M0 —— 版本化写入、as-of 查询、自校验指纹

状态：**接线完成**（`internal/temporal` 纯语义层 + storage/service/v2 opcode/
ban-cli 全链路接线，见下方"接线（2026-08-24）"一节）。

## 目标

让 BanDB 具备时态内核的三个地基能力：

1. **写不覆盖**：同一逻辑记录的每次写入都是不可变版本；
2. **读按 as-of**：查询可以问“在某个时刻，系统当时知道什么”；
3. **状态可重放校验**：任何状态都能从版本集合重放出来，指纹一致才算数。

这是“AI 原生时序数据引擎”的数据模型地基：AI/Agent 只能读可验证的时点真相，
写入永远留下审计轨迹。

## 关键决策

### 1. 键布局

```
逻辑键（业务不变）      quote:2026-08-17:600000
版本存储键              quote:2026-08-17:600000:v0000000000000003
当前指针键              quote:2026-08-17:600000:current
```

- `seq` 是同一逻辑键内的严格递增写入序号（总序）；
- 版本号定宽 20 位十进制，保证**字典序 = 数值序**（LSM 扫描天然按版本升序）；
- `:current` 指针存 `(seq, payload_hash)`，供快速 GET 与对账；
- 崩溃安全：先写版本键，再写指针键；指针永远指向已落盘的版本，孤儿版本可回收。

### 2. as-of 语义

`AsOf(versions, as_of)` 返回“写入时间 <= as_of”的版本中 `seq` 最大的那一条；
同写入时刻按 `seq` 决胜。**绝不返回未来写入**——这是 PIT 红线的数据层保证。

### 3. 自校验指纹

`Fingerprint(entries)` 对 `(LogicalKey, Seq, Payload)` 做确定性 sha256：

- 按 `(LogicalKey, Seq)` 排序，与输入顺序无关；
- 每条记录带 payload 长度前缀，消除键/负载边界歧义；
- 用途：重放全量版本后对最新状态做指纹，与 `:current` 指针比对；跨进程验证
  “同一份账本产生同一状态”。

## 兼容性

- 不改现有 v1/v2 PUT/GET/SCAN/DELETE 帧格式与存储格式；
- 不改现有 `quote:` schema 校验规则本身（`ValidateVersioned` 仍会跑它，只是
  跳过与版本化写入冲突的单调性启发式，见接线一节第 6 条）；
- 新增四个 v2 opcode（`0x09`-`0x0C`），不复用/不改写任何既有 opcode 的语义；
- `cmd/ban-server` 生产入口的接线（`service.NewRouterV2` 参数类型从
  `KVStore` 收紧为 `TemporalRawStore`）对生产/测试的现有调用点均透明——三处
  调用点传入的都已是 `*service.KVServer`，该类型接线后自动满足新接口，无需
  改动调用点本身。

## 验收

- `go test ./internal/temporal/ -v` 全绿：版本键 round-trip、定宽字典序=数值
  序、`Latest`/`AsOf` 语义（含未来写入不可见、同刻决胜）、指纹确定性/顺序
  无关/长度前缀无歧义、新增的 value/current 编解码 round-trip；
- `go build ./...` / `go vet ./...` / `go test ./...` / `go test -race
  ./internal/temporal/... ./service/... ./proto/... ./bannet/...` 四关独立
  退出码均为 0；
- 同一逻辑键写 3 个版本：`GET`（v1 与 v2）透明返回最新、`GET_AS_OF` 只返回
  当时可见版本（`TestRouterV2_PutVersionedThenGetAsOfAndListVersions`、
  `TestRouterV2_GetTransparentlyResolvesVersionedKeyOverWire`）；
- SCAN 内部键隔离：`TestKVServer_ScanHidesInternalTemporalKeysButKeepsPlainKeys`、
  `TestRouterV2_V1ScanUnaffectedByVersionedWrites`；
- 重启/跨调用重放指纹一致：`TestTemporalStore_ReplayFingerprintNoMismatchAfterNormalWrites`
  （同一账本两次重放指纹相同）、`TestRouterV2_ReplayFingerprintReportsZeroMismatchAfterNormalWrites`；
  指纹对账能检出真实不一致：`TestTemporalStore_ReplayFingerprintDetectsCorruptedCurrentPointer`；
- v1 零回归：既有 `TestRouterV2_V1ClientUnaffected` 与全部 crosslang 向量
  测试（`client/python/crosslang_test.go`）改动前后行为不变，本次改动未触碰
  这些测试文件本身（除新增一行 SCAN 路由注册，纯新增能力不改变已有断言）；
- `ban-cli put-versioned/get-as-of/list-versions/fingerprint` 四条新命令
  对一个真实运行的 standalone 服务端手动跑通（含 GET 透明解析、as_of 早于/
  晚于全部写入、fingerprint 两次重放结果一致）。

## 接线（2026-08-24）

### 1. PUT 语义分叉：v1/v2 `PUT`(opcode 0x01) 保留覆盖写，版本化只在新 opcode

`OpcodePut`/`OpcodeDel` 的行为一字不改（老客户端零影响）。版本化写入是四个
新增的 v2 opcode，紧接已有编号顺延分配（响应沿用既有 `OpcodeOK`/`OpcodeErr`,
`[statusCode][reason][payload]` 结构）：

| opcode | 值 | 请求负载 | OK 响应负载 |
|---|---|---|---|
| `OpcodePutVersioned` | `0x09` | 同 PUT：`proto.EncodePutFrame`（key=逻辑键, value=本次版本负载） | `[seq u64 LE]` 8 字节 |
| `OpcodeGetAsOf` | `0x0A` | `proto.EncodeAsOfFrame`：`[keyLen u32 LE][key][asOfNanos u64 LE]` | `proto.EncodeVersionEntry`：`[seq u64][writeNanos u64][payloadLen u32][payload]` |
| `OpcodeListVersions` | `0x0B` | 同 GET/DEL：`proto.EncodeKeyOnlyFrame`（key=逻辑键） | `proto.EncodeListVersionsResponse`：`[count u32][entry...]`（entry 同上），count=0 是合法结果 |
| `OpcodeReplayFingerprint` | `0x0C` | 同上，但 key 的含义是**逻辑键前缀**，不是某一个具体逻辑键 | `proto.EncodeReplayFingerprintResponse`：`[keyCount u32][mismatchCount u32][fingerprintLen u16][fingerprint][mismatchKey...]` |

四者均不参与 §11.2.2 的 ack 三档窗口/累计记账，统一按"请求-即时响应"处理
（与 GET/SCAN/STAT 同类）——它们是数据模型层的新增能力，不是既有写路径的
批处理优化，M0 不引入交叉耦合。

### 2. 存储落点：不改 storage.Engine，靠两次既有 Write() 叠出版本化语义

`PUT_VERSIONED` 落两条独立的 `Command{Type:CommandPut}`：先写版本键
`logical:vSEQ`（value=`temporal.EncodeVersionValue(writeNanos, payload)`），
再写 `:current` 指针（value=`temporal.EncodeCurrentValue({seq, payloadHash})`）。
两次都走 `KVStore.Write`，standalone 模式下即为"先 fsync WAL 再落 memtable"，
天然满足 RFC 原文"先版本键、后指针键"的崩溃安全顺序——不需要改
`storage.Engine` 任何一行。

seq 分配：`service.TemporalStore` 按逻辑键分别加锁（`sync.Map` 存
`*sync.Mutex`），临界区内先查进程内缓存的"上一个 seq"，未命中才回退读
`:current` 指针再 +1。这不只是优化：Raft 模式下 `Write()` 在日志提交后即
返回，`storage.Put` 是异步 apply 循环里才真正落盘的，"写前读 `:current`"
在提交与落盘之间的窗口期会读到旧值；进程内维护的单调计数器绕开了这个窗口，
在单 leader 模型下仍然正确。代价是冷启动（进程重启/leader 切换后首次触碰
某逻辑键）要退回读 `:current`，这依赖 Raft 既有的"已提交日志在成为可服务
leader 之前已经 apply 完"这条不变量，不是本次改动新引入的假设。

### 3. GET 透明解析：`KVServer.Get` 是唯一的回退落点

`PUT_VERSIONED` 从不写字面量 `logical` key。`KVServer.Get`（v1 Router 与 v2
RouterV2 的 GET 都经它）字面量未命中时，回退读 `:current` 指针 → 取指针指向
的版本键 → 拆出 payload。零回归：纯 v1 PUT 的 key 字面量直接命中，不触达
回退路径。指针/版本键损坏（理论上不该出现）按 NotFound 处理，不向上抛错——
那类不一致的诊断走 `REPLAY_FINGERPRINT`，不是让 GET 报错。

### 4. SCAN 可见性：隐藏内部键，M0 不做"解析出当前值当一行"

`KVServer.Scan`（业务 SCAN，v1/v2 共用）在 `ScanRange` 回调内过滤掉版本键
（`temporal.ParseVersionStorageKey` 命中）与 `:current` 指针键
（`temporal.IsCurrentStorageKey`），过滤发生在 `limit` 截断判断之前，避免
内部键挤占真实业务条目的名额。**选择"隐藏"而非"解析出当前值当作这一行"**：
后者需要把扫描上界外扩到指针键后缀范围之外才能不漏掉边界上的 `:current`，
这一段边界分析比看起来复杂（`prefix+0xFF` 之类的 hack 在业务 key 恰好用满
0xFF 字节时不成立），留给 M1+ 重新设计 SCAN 的双时态语义时一并做。M0 先保
证"零内部键泄漏 + 零回归"这个更容易验证正确性的立场。

时态内核自身读取版本键/`:current`（`LIST_VERSIONS`/`GET_AS_OF`/
`REPLAY_FINGERPRINT`）走新增的 `KVServer.ScanRaw`（不过滤，见
`service/temporal_store.go` 的 `TemporalRawStore` 接口）——与业务 SCAN 分成
两个方法，不是一个方法加个开关参数：调用方是谁、该不该看见内部键，类型
签名上就是两件事。

### 5. `REPLAY_FINGERPRINT` 做成服务端 opcode，`ban-cli` 是瘦客户端

没有做成"离线直接开库工具"（本仓库确有这类先例，见 `cmd/ban-ingest` 直接
`storage.NewEngine`）：那要求没有其它进程同时持有同一份 LSM 数据目录，而
这个自检的典型场景恰恰是"服务端正在跑，现在核对一下账本"，两个进程各自打
开同一组 SSTable/WAL 是不安全的。服务端算、CLI 只转发展示，避免这个问题。

指纹范围是"重放出的每个逻辑键的最新版本"这一集合（不是全历史版本），与
`:current` 的对账（seq、`Version.PayloadHash()` 逐一比较）分开报告——前者
是跨进程/重复运行的确定性摘要，后者是验收三问第 2 问的可执行入口，一致时
`mismatchCount=0`。前缀枚举用 `[prefix, prefix+0xFF]` 扫描 `:current` 键；
已知边界：若 prefix 后紧跟的业务字节本身用到 `0xFF`，这个上界会漏枚举，
M0 视为可接受（管理/自检工具，非热路径），不解决属 M1+。

### 6. 发现并修复的一处存量启发式冲突（`ingesthook.Filter`）

联调服务端时（不是纸面推演）复现：`ingesthook.Filter.Validate` 里"设备:
时间戳"单调性启发式（为无类型 IMU 场景写的，`dropBackward` 开启时生效）
把 `PUT_VERSIONED` 对同一逻辑键的第二次写判定为"非单调"直接拒绝——这个
启发式的前提"同一 key 反复写=时钟异常"与版本化写入的正常工作方式（反复
写同一逻辑键）直接冲突,与已知的 quote: 误杀事件（见 `parseKey` 调用点上方
注释）是同一类问题的第三次出现。修复：新增 `Filter.ValidateVersioned`
（`service/ingesthook/filter.go`），与 `Validate` 共享 value 长度/schema
校验/脱敏三项检查，只跳过单调性启发式；`RouterV2.handlePutVersioned` 调用
这个新方法,不调用 `Validate`。

## 下一步（M0 之外）

1. schema registry 数据化（机器可读契约，含版本）；
2. QuantScout 版本化写入迁移（快照/财报/筛选/推荐）；
3. QuantBrew 侧 as-of 数据适配器；
4. Python 客户端 v2 版本化写入 + as-of 读（本次只补了 Go 侧 `ban-cli` 瘦
   客户端，`client/python` 未改动）；
5. SCAN 的双时态语义重新设计（解析出当前值当作一行、bitemporal 查询）。
