# BANLV 协议规范 v1

> 本文档正式命名 BanDB 的自研 TLV 协议为 **BANLV**（原代码里的包名 `bannet` 是历史命名，
> 本轮改造只立宪、不修宪——不改代码包名/标识符，避免与在途改动冲突；本文档统一用
> BANLV 指代协议本身，用 `bannet`/`client` 指代其 Go 实现所在的代码包）。
>
> 权威来源：本规范的每一条字段布局、状态码、长度限制，均以 `bannet/`（服务端编解码）
> 与 `client/`（Go SDK）的现有实现为准逐条对照写出，不是先设计规范再让代码符合它——
> 如果本文档与代码出现分歧，以代码为准并视为文档 bug。生成方式见附录「测试向量」。
>
> 定位：BANLV 是 BanDB 的**生产摄入主协议**。仓库内另有一条 gRPC 传输
> （`internal/kvgrpc`），仅用于基准测试/协议对照，不是生产入口——据作者压测评估，
> BANLV 比 gRPC 约快 26%，这是自研协议存在的理由。见 `internal/kvgrpc` 包注释。

## 1. 帧格式（字节布局）

```
偏移   长度      字段        说明
0      4 字节    dataLen     u32，小端；data 字段的字节长度（不含 msgID）
4      2 字节    idLen       u16，小端；msgID 字段的字节长度
6      idLen     msgID       ASCII 字符串，非数字操作码
6+idLen  dataLen data        负载，格式随 msgID 而定（见第 2、3 节）
```

固定头部 6 字节（`dataLen` + `idLen`），之后是变长的 `msgID` 与 `data`。多字节整数
一律小端（Little Endian）。

```
┌─────────────┬─────────────┬───────────────┬─────────────────┐
│ dataLen(u32)│  idLen(u16) │ msgID(idLen B)│  data(dataLen B) │
│   4 bytes   │   2 bytes   │   变长         │      变长         │
└─────────────┴─────────────┴───────────────┴─────────────────┘
        ← 固定头部 6 字节 →
```

权威实现：编码见 `bannet/datapack.go` 的 `DataPack.Pack`（服务端/响应侧）与
`client/conn.go` 的 `encodeFrame`（客户端/请求侧，两者独立实现、由
`client/wire_compat_test.go` 交叉校验一致）；解码见 `DataPack.UnPack`（只解头部，
调用方按 `idLen`/`dataLen` 续读 msgID 与 data，见 `bannet/connection.go` 的
`Connection.StartReader`）。

## 2. 消息码表

| msgID    | 方向      | 语义                     | data 负载格式（第 3 节） |
|----------|----------|--------------------------|------------------------|
| `PUT`    | 请求      | 写入一条键值               | keyLen+valueLen+key+value |
| `GET`    | 请求      | 读取一条键值               | keyLen+key |
| `DEL`    | 请求      | 删除一条键值（幂等盲写）     | keyLen+key（与 GET 同形状） |
| `SCAN`   | 请求      | 服务端谓词下推的范围查询      | 见 2.1 |
| `OK`     | 响应      | 成功（PUT/DEL/GET/SCAN 通用）| statusLen+status[+GET/SCAN 专属尾段] |
| `ERR`    | 响应      | 失败/拒绝                  | statusLen+status |
| `HELLO`  | 保留      | **已声明未使用**：无任何生产路径引用，也无握手实现（见 3.3 版本协商） | — |
| `BYE`    | 保留      | **已声明未使用**：无任何生产路径引用 | — |

权威实现：`proto/codes.go`。`HELLO`/`BYE` 是声明但零调用方的保留常量——本规范如实
记录这一现状，不假装它们已有握手/优雅断连语义（见 4.2 连接生命周期）。

### 2.1 SCAN 请求负载

```
[startLen u32][endLen u32][fieldLen u32][op u8][start][end][field][operand=剩余]
```

`op` 是谓词算子（`predicate.Op`，见下表）；`operand` 没有显式长度字段——它是负载
剩余的全部字节，故 SCAN 请求里 `field`/`operand` 之后不能再附加其它字段，这是当前
格式的隐含约束。权威实现：`proto/scan.go` 的 `EncodeScanRequest`/`DecodeScanRequest`。

| op 值 | 算子 | 含义 |
|---|---|---|
| 0 | `OpNone` | 无谓词，全匹配 |
| 1 | `OpGT`   | `>`，按 float64 比较 |
| 2 | `OpGTE`  | `>=`，按 float64 比较 |
| 3 | `OpLT`   | `<`，按 float64 比较 |
| 4 | `OpLTE`  | `<=`，按 float64 比较 |
| 5 | `OpEQ`   | `==`，按字符串比较 |
| 6 | `OpNE`   | `!=`，按字符串比较 |

## 3. 响应负载格式

### 3.1 通用状态段

```
[statusLen u8][status: ASCII 字符串]
```

`PUT`/`DEL` 的成功与失败响应、以及 `GET`/`SCAN` 的失败响应，负载就是这一段本身。
权威实现：`service/router.go` 的 `statusPayload`；解析见 `client/conn.go` 的 `parseStatus`。

### 3.2 GET 成功响应

```
[statusLen u8][status="ok"][valueLen u32][value]
```

即通用状态段之后再跟 `valueLen`+`value`。权威实现：`service/router.go` 的
`handleGet`；解析见 `client/client.go` 的 `Get`。

### 3.3 SCAN 成功响应

```
[statusLen u8][status="ok"][count u32]{ [keyLen u32][key][valueLen u32][value] }×count
```

权威实现：`proto/scan.go` 的 `EncodeScanResponse`/`DecodeScanResponse`。

### 3.4 dropped 响应的丢弃原因（协议扩展，向后兼容）

```
[statusLen u8][status="dropped"][reasonLen u16 LE][reason: UTF-8 字符串]
```

即通用状态段之后再跟 `reasonLen`+`reason`。`reason` 是 `ingesthook.Filter` 判定
丢弃时给出的具体原因（如 `"quote: non-positive price: open=-1"`、
`"non_monotonic_timestamp"`、`"oversized_value"`），供客户端展示/日志使用。

这是在原有 `[statusLen][status]` 之后追加的字段，不是替换：**向后兼容**——老
客户端的 `parseStatus` 只读 `statusLen` 声明的字节数，`reasonLen`/`reason` 落在
它认为的"该操作特有的其余字节"（`rest`）里；旧版 Go SDK 的 `Put`/`Delete` 从不
读取 `rest`，新增这段不会让老客户端解析失败，只是拿不到 `reason`。`reason` 为
空时 `reasonLen=0`，字节序列退化为与「无 reason」版本完全相同。

背景：此前 `dropped` 响应不携带任何原因，客户端只知道"被拒绝了"，不知道具体
为什么——曾迫使调用方在本地重新实现一遍校验规则去猜测（QuantScout 全量实测
反馈的真实问题）。权威实现：`service/router.go` 的 `droppedPayload`；解析见
`client/conn.go` 的 `parseDropReason`（Go）与 `client/python/bandb_client.py`
的 `parse_drop_reason`（Python）。

gRPC 传输（`internal/kvgrpc`）**不携带这个字段**——`PutResponse` 只有
`Success bool`，没有为 reason 预留空间；这与 gRPC 是基准测试/协议对照用途
（不是生产入口）的定位一致，本轮不为它扩展 protobuf 消息。

### 3.5 状态码表

| status       | 含义 | 客户端应对（Go SDK 语义，见 `client/errors.go`） |
|---|---|---|
| `ok`         | 成功 | 正常返回 |
| `error`      | 服务端内部错误 | 可重试（`ErrServer`） |
| `dropped`    | 被 `ingesthook.Filter`（PreHandle 钩子）按策略拒绝：畸形负载、value 超限、时间戳非单调、schema 校验不通过 | 确定性拒绝，不应重试（`ErrDropped`） |
| `overloaded` | 网关自适应准入（`internal/admission`）过载 shed | 应退避重试（`ErrOverloaded`） |
| `notfound`   | key 不存在，或其最新版本是删除墓碑 | 正常查询结果，非故障（`ErrKeyNotFound`） |

权威实现：`proto/codes.go` 的状态常量；映射见 `client/conn.go` 的 `statusError`。
新版本服务端引入的未知状态，客户端一律按服务端错误处理（不静默当成功），故新增
状态不破坏旧客户端。

## 4. 长度限制与连接生命周期

### 4.1 长度限制

| 项 | 限制 | 配置项 | 默认值 |
|---|---|---|---|
| msgID 长度 | ≤ 65535（`uint16` 上限） | — | 硬限制，超出 `Pack` 直接返回错误 |
| 单帧总长度（`dataLen`） | 可配 | `config.MaxPackageSize` | 16 MiB（容纳多模态大值与多条 SCAN 响应） |
| 帧长上限校验时机 | 读完头部、读负载之前拒绝超限帧 | — | 避免按对端声称的长度分配内存，见 `bannet/connection.go` |
| PUT value 长度（应用层，非协议层） | 可配、按 Filter 实例 | `ingesthook.NewFilter(_, maxValueLen, _)` | 不限（`<=0`） |

### 4.2 连接生命周期

BANLV 是**严格请求-响应协议**：一条连接上必须「发一帧、收一帧」之后才能发下一帧，
不支持流水线（pipelining），也不支持服务端主动推送——连接建立/关闭时服务端不下发
任何未经请求的消息（`OnConnStart`/`OnConnStop` 均为空实现），因为那会让客户端把
它误读为下一个请求的响应，造成整条连接的响应错位。并发只能由多条连接提供，Go SDK
用连接池实现（`client.Client`，见 `client/client.go` 头部文档）。

单帧内「畸形负载」（如 `PUT` 的 `keyLen`/`valueLen` 与实际字节数不符）与「单帧本身
不完整」是两个不同层面：前者在应用层 `ingesthook.Filter` 判定为 `dropped` 并正常
回一个响应；后者（帧头声明的 `dataLen` 超出实际可读字节，或连接中途断开）会导致
读取阻塞/连接关闭，不产生响应——这是 TCP 流协议的固有行为，不是 BANLV 的特殊设计。

### 4.3 版本协商（当前无）

**当前帧头没有 magic number 或版本字段**——6 字节头部全部是 `dataLen`+`idLen`，
没有为协议版本预留任何比特。这是 v1 的已知限制，明确记录如下：

- 新老客户端/服务端之间没有握手协商机制，协议演进只能靠新增 msgID/status（**加法
  兼容**）或走外部约定（部署时保证客户端/服务端版本匹配）。
- `HELLO`/`BYE` 消息码已声明但从未实现握手/挥手语义（见第 2 节），说明协议设计者
  最初可能预留了握手的位置，但至今没有落地。
- 本规范**不在本轮修改帧格式**——协议改动影响所有已部署客户端，是大事，本轮的
  Python 客户端与跨语言测试向量都以"现状"为准逐字段对照实现，不夹带协议变更。

**v2 演进预留节**：若未来要引入版本协商，候选方案（仅记录方向，不展开设计）：
- 在 6 字节头部之后、`msgID` 之前插入 1 字节版本号，v1 隐式版本号视为 `0`；
  旧服务端会把该字节误读进 `idLen` 的一部分，故这**不是向后兼容**的改法，需要
  新老节点整体切换或双协议监听端口过渡。
- 或复用现有加法兼容路径：新增一个 `HELLO` 请求真正承载版本信息，服务端按
  「收到 HELLO 走新逻辑、未收到走 v1 默认」分支，代价是每连接多一次往返。
- 决策留给协议实际需要演进时再做，本轮只标注限制、不选型。

## 5. 与 gRPC 传输的关系

BANLV 是生产摄入的权威协议；`internal/kvgrpc` 提供的 gRPC 传输是基准测试/协议
对照用途，两者不是并列的生产候选（详见该包的包注释）。gRPC 的消息定义见
`internal/kvgrpc/kv.proto`，与 BANLV 的 msgID/status 概念上对应但线格式完全不同
（gRPC 用 protobuf 序列化，BANLV 用本文档描述的定长头+TLV），两套编解码相互独立、
不共享代码。

## 6. 跨语言实现与测试向量

BANLV 目前有两份独立实现：

- **Go**：`bannet/`（服务端编解码）+ `client/`（Go SDK，请求/响应负载编码）。是本
  规范的权威来源。
- **Python**：`client/python/bandb_client.py`（纯标准库 socket+struct，供 QuantScout
  等 Python 上游直接写入行情快照，不依赖 protobuf 工具链）。逐字段对照 Go 实现，
  每个函数的文档字符串标注对应的 Go 源码位置。

### 6.1 测试向量：防止两个实现分叉的锚点

`docs/banlv/vectors.json` 冻结了一组「语义 ↔ 十六进制字节」的标准向量，由 Go 权威
实现生成（生成脚本为一次性工具，未进仓库；生成方法见文件历史/本文档记录：用
`bannet.NewDataPack().Pack` 对每条 `(msgID, data)` 编码，`data` 按第 2、3 节的负载
格式手工构造）。**Go 与 Python 两侧的单元测试都必须加载并验证同一份 vectors.json**：

- Go 侧：`bannet/vectors_test.go`（`TestVectors_*`）。
- Python 侧：`client/python/test_bandb_client.py`（`VectorTests`）。

以后任何第三语言客户端接入 BANLV，也应以这份向量为验收标准——只要新实现能重新生成
与向量完全一致的字节，就说明它与 Go 权威实现语义一致。

### 6.2 向量样例（完整清单见 `docs/banlv/vectors.json`）

| name | msg_id | 语义 | frame_hex（截断） |
|---|---|---|---|
| `put_request_basic` | PUT | key="k1" value="v1" | `0c000000030050555402...` |
| `get_request_basic` | GET | key="k1" | `060000000300474554020...` |
| `resp_ok_get_with_value` | OK | GET 成功响应，value="v1" | `0900000002004f4b026f6b...` |
| `resp_err_dropped` | ERR | status="dropped" | `0800000003004552520764...` |
| `resp_err_overloaded` | ERR | status="overloaded" | `0b00000003004552520a6f...` |
| `put_request_quote` | PUT | 行情快照，key="quote:2026-08-17:600000" | `960000000300505554170...` |
| `put_request_malformed_lengths` | PUT | keyLen 声明 100、实际 2 字节数据（畸形负载） | `0a000000030050555464000000000000006162` |

（完整 11 条向量、未截断的 `data_hex`/`frame_hex`，以及每条的详细中文说明，见
`docs/banlv/vectors.json`。）

## 7. 行情快照的 key 布局（量化数据适配约定）

BanDB 承接量化数据流后，行情快照类记录的 key 布局裁决为：

```
quote:<YYYY-MM-DD>:<标的代码>
```

日期在前是刻意选择：同一天的全市场快照在 key 空间连续，投递按位点批量拉取、
retention 按已投递位点回收都天然按「日」成批，不需要为行情数据改动现有投递/回收
机制。对应的 schema 校验见 `service/ingesthook/schema/quote.go`（`QuoteSnapshot`），
按 key 前缀 `quote:` 分派，见 `service/ingesthook/schema/registry.go`。

## 8. v2 候选方向（占位，不展开）

作者计划研究 dubbo-go 的 **Triple 协议**（多路复用/流式/标准元数据）作为 BANLV v2
的演进输入。本节仅记录这一方向存在，不在本轮展开设计——是否采纳、采纳到什么程度，
留待协议实际需要演进（如需要突破当前严格请求-响应的单路复用限制）时再评估。
