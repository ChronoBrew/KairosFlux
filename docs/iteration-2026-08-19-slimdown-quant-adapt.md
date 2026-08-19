# 迭代复盘：架构体检后的瘦身第一轮 + 量化数据适配起步

日期：2026-08-19　范围：`internal/kvgrpc`、`service/ingesthook`（新增
`service/ingesthook/schema`）、`service/shardkv`、`service/delivery/governance`、
`client/python`（新增）、`docs/BANLV-协议规范.md`（新增）

## 背景

一次架构体检（诊断，未改代码）发现三件事：

1. `.claude/worktrees/` 下有两份过期 git worktree，把真实文件数从 154 撑成了
   "看起来 344 个文件"的错觉——体检报告先做了这个校正。
2. 全代码库的落盘前清洗逻辑只有一处：`service/ingesthook.Filter.Handle`，且与
   `bannet.Request`（裸帧接口）强耦合。它只挂在 bannet 入口的 `Router.PreHandle`
   上；gRPC 入口（`internal/kvgrpc.GRPCServer.Put`）直接 `kv.Write`，完全绕过
   畸形值/单调性/schema 校验——这是一个正确性缺口，不只是代码整洁度问题。
3. `service/shardkv`（Multi-Raft 分片 KV，v1）与 `service/delivery/governance`
   （借鉴 dubbo-go 治理模型的投递治理层）都是零调用方——测试齐全但未被任何
   `cmd/` 入口接线，是自洽但游离在主干构建之外的原型。

同时，项目计划承接一个量化系统的数据流（每日全市场股票快照起步，未来可能分钟级
百万行/天），需要为行情数据定义 schema 与清洗规则（价格>0、涨跌幅物理极限±20%等）。

## 一个需要先澄清的定位：BANLV 是主协议，gRPC 是基准对照

体检和最初几轮改造一度把"给 gRPC 入口补清洗"当作主线，但作者澄清：**bannet 自研
TLV 协议（本轮正式命名为 BANLV）是生产摄入的权威入口，比 gRPC 约快 26%，这是它
存在的理由，不是历史包袱**；gRPC（`internal/kvgrpc`、`cmd/ban-grpc-server`、
`cmd/ban-bench-grpc`）是基准测试/协议对照用途。

这个澄清改变了工作的**定性**、不改变已经做的**动作**：Filter.Validate 的拆分本来
就是 schema 注册的地基，照做；gRPC 入口接入清洗也照做——不是因为 gRPC 是生产入口，
而是纵深防御（防止将来有人把这条裸写路径误用为生产入口时完全跳过校验）。相应地，
`internal/kvgrpc` 补测试的优先级降为「基础覆盖即可」，不必对齐 bannet 入口的测试
深度；两个包的包注释都已更新，明确标注这一定位，防止后人误判。

## 做了什么

### 1. 清理过期 worktree，确认基线

`git worktree remove` 清掉两份过期快照，确认真实文件数 154、总行数 19,874（非测试
12,811、测试 7,063）。

### 2. Filter.Validate：拆出与传输层无关的清洗核心

`service/ingesthook/filter.go` 原来的 `Handle(req bannet.Request)` 把「解帧」与
「清洗」耦合在一起。拆分后：

- `Handle` 只做 bannet 特有的一步——从裸帧解出 key/value（畸形帧在这一步就地丢弃，
  这是「帧是否完整」的传输层问题）。
- 新增 `Validate(key, value []byte) (newValue []byte, changed bool, result Result, reason string)`：
  value 长度限制 + 时间戳单调性校验 + schema 校验（新增）+ 字段脱敏，只认字节、
  不认 `bannet.Request`，bannet 与 gRPC 均可调用。

`internal/kvgrpc.GRPCServer.Put` 现在会先调 `Filter.Validate` 再 `kv.Write`，与
bannet 入口复用同一套规则。新增对照测试锁定两端行为一致（见下）；`kvgrpc` 包测试
从 6.5% 覆盖提到约 11%（含 protobuf 生成代码拉低的分母；手写的 `Put`/`Get`/`Delete`
本身覆盖到 75%~100%），按澄清后的定位这已经够用，不再往深处补。

**已知局限（如实记录，未解决）**：`parseKey` 假设「设备:时间戳」的 key 约定是为
IMU 场景写的启发式；行情快照 key（`quote:<日期>:<代码>`）的末段是股票代码，可能被
误当时间戳做单调性校验。因此挂载行情数据的 `Filter` 必须传 `dropBackward=false`
（`cmd/ban-grpc-server/main.go` 与联调测试的服务端构造都已这样做），按数据类型
分派单调校验规则留待后续。

### 3. schema 注册表 + QuoteSnapshot 校验器

新包 `service/ingesthook/schema/`：

- `registry.go`：`Validator` 接口 + `Register`/`Unregister`/`Lookup`（按 key 前缀
  最长匹配分派；无匹配前缀视为「未纳管类型」放行，不误伤旧数据）。
- `quote.go`：`QuoteSnapshot`，按 `quote:` 前缀自注册（`init()`）。规则：
  - 必填字段 `code/date/open/high/low/close/volume`；
  - 价格字段（open/high/low/close）必须 `> 0`，volume 必须 `>= 0`；
  - OHLC 逻辑一致：`low <= open <= high` 且 `low <= close <= high`；
  - 涨跌幅物理极限 ±20%：用可选字段 `prev_close` 与 `close` 比对；缺
    `prev_close`（或为 0）时跳过该项检查并计数（`metrics.SchemaChecksSkipped`），
    不视为失败——首个交易日/复牌首日没有可比昨收。

行情快照 key 布局裁决为 `quote:<YYYY-MM-DD>:<代码>`（日期在前）——同一天的全市场
快照在 key 空间连续，现有投递（按位点批量拉取）与 retention（按已投递位点回收）
机制不需要为此改动。

`internal/metrics` 新增两个计数器：`FramesDroppedSchema`（被 schema 拒绝）、
`SchemaChecksSkipped`（因缺可选比对字段跳过某项检查）。

### 4. Python 最小 TLV 客户端（BANLV 协议实现随协议同仓）

`client/python/bandb_client.py`：纯标准库（socket+struct），逐字段对照 `bannet/`
与 `client/` 的 Go 实现，供 QuantScout 这类 Python 上游直接写入行情快照，不依赖
protobuf 工具链。最小可用集：连接、PUT（单条 + 批量循环）、GET/DELETE、错误响应
解析（`KeyNotFoundError`/`OverloadedError`/`DroppedError`/`ServerError`/`ProtocolError`，
与 Go SDK 的哨兵错误一一对应）。

`client/python/examples/write_quote.py`：命令行示例，写入一条行情快照并读回验证；
`client/python/examples/crosslang_probe.py`：跨语言联调测试的 Python 侧驱动腿。

### 5. BANLV 协议正式命名 + 协议规范文档

`docs/BANLV-协议规范.md`：以 `bannet/` 权威 Go 实现为准，写出帧格式字节布局图、
字段语义、消息码表、状态码表、长度限制、连接生命周期。代码包名 `bannet` 本轮不改，
文档统一称 BANLV。记录了一个此前没写清楚的限制：**当前帧头没有 magic/版本字段**，
标注为 v1 已知限制，预留 v2 演进节（不改现有线上格式）。末尾放了 v2 候选方向占位：
作者计划研究 dubbo-go Triple 协议（多路复用/流式/标准元数据）作为未来输入，本轮不展开。

### 6. 跨语言测试向量：防止 Go/Python 两个实现分叉的锚点

`docs/banlv/vectors.json`：11 条「语义 ↔ 十六进制字节」向量，由 `bannet.DataPack`
（权威 Go 实现）生成，覆盖 PUT/GET/DEL 请求、OK/ERR 各类响应、行情快照 PUT、以及
一条畸形 PUT 负载。Go 侧（`bannet/vectors_test.go`）与 Python 侧
（`client/python/test_bandb_client.py`）的单测都加载并验证同一份向量文件——以后
任何第三语言客户端接入 BANLV，也应以它为验收标准。

### 7. 跨语言联调测试：验收核心

`client/python/crosslang_test.go`：起一个真实的 ban-server（与 `cmd/ban-server`
同样接线：`KVServer` + `Router` + `ingesthook.Filter`），对合法行情/非法价格/超限
value/畸形负载四个场景，分别用 Go 客户端与 Python 客户端（子进程调用
`crosslang_probe.py`）各写一份（不同 key），断言服务端对两端的判定状态完全一致；
另有一条「Python 写入的字节经 Go 客户端读回、内容逐字节相同」的交叉核验，证明的
不只是状态码相同，而是 Python 编码的整帧被服务端按与 Go 完全相同的语义正确解析、
落盘。环境无 `python3` 时优雅跳过，不阻塞 `go test ./...`。

### 8. build tag 隔离 shardkv/governance；删除两个 sink 桩

`service/shardkv/` 与 `service/delivery/governance/` 全部文件加
`//go:build experimental`——默认 `go build ./...`/`go test ./...` 不再编译它们
（真实构建行为，非注释声明），需要时 `go build -tags experimental ./...`。**不
物理删除**：两者测试齐全（76.9%、66.3% 覆盖）且体现 Multi-Raft 分片/服务治理的
设计能力，未来若真有横向扩展或多下游治理需求，是现成的起点。

`service/delivery/clickhouse_sink.go`、`doris_sink.go`（各 ~28 行、`Send` 恒返回
"not implemented" 的桩）直接删除；`Sink` 接口（`Name`/`Send`/`Health` 三方法）
保留在 `service/delivery/sink.go`——真正要做的是**实现**一个可用的 ClickHouseSink，
不是保留一个空壳，触发条件是分析仓实例就位时再做。

## 验证

- `go vet ./...`、`go build ./...` 全干净。
- `go build -tags experimental ./...`、`go vet -tags experimental ./...` 全干净——
  隔离的 shardkv/governance 仍可独立编译验证，不是被悄悄弄坏后才发现。
- `go test ./...`（默认，不含 experimental）与 `go test -tags experimental
  ./service/shardkv/... ./service/delivery/governance/...` 均全绿；bannet 入口
  既有 8 个测试（`service/ingesthook` 包）未改一行断言、全部保持绿色，锁定本轮
  改造对既有行为零回归。
- Python 侧 `python3 -m unittest test_bandb_client -v`：11/11 通过。
- 跨语言联调 `go test ./client/python/... -run TestCrosslang -v`：5/5 子测试通过
  （valid_quote / invalid_price / oversized / malformed_lengths / 交叉读回核验）。
- 用编译出的真实 `ban-server` 二进制手工跑 `write_quote.py` 示例脚本：合法记录
  写入+读回成功；非正价格、涨跌幅超±20%两种非法记录均被拒绝（`dropped`）；缺
  `prev_close` 时正常放行（按设计跳过涨跌幅检查，不算失败）。

## 遗留（有意不在本轮做）

- `internal/kvgrpc` 测试覆盖按澄清后的定位（基准对照用途）没有往深处补，只覆盖
  `Put`/`Get`/`Delete` 主路径与清洗拒绝路径。
- `ClickHouseSink` 真实实现：等分析仓实例就位再做，`Sink` 接口已就位、改动范围
  可控（预估 150~300 行 + 测试）。
- `parseKey` 的「设备:时间戳」启发式尚未泛化为按数据类型分派单调校验规则——行情
  数据当前的解法是关闭 `dropBackward`，不是解决根因。
- BANLV v2（版本协商、dubbo-go Triple 协议输入）只在协议规范文档里占位，未设计。
