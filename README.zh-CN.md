# KairosFlux

[English](README.md) | [中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)
![Protocol](https://img.shields.io/badge/protocol-BANLV-orange)
![Performance](https://img.shields.io/badge/BANLV%20vs%20gRPC-2.7x%20faster%20(GET)-brightgreen)
<!-- CI 徽章：接入 GitHub Actions 后再补 -->

KairosFlux（原名 BanDB）是 **ChronoBrew** 组织下的时态数据流引擎，围绕一个
定位展开：AI 原生时序数据引擎——所有数据版本化写入、可按"当时知道什么"做
时点查询、任何状态都能从账本重放出来并跟自己的历史指纹核对。时序是内核，
确定性是独特性，AI 是方向。QuantBrew（确定性 A 股回测内核）与 ChronoScout
（全市场侦察爬虫）是它的头两个真实租户——三件套如何咬合见 QuantBrew 仓库的
[架构总览](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/架构总览-ChronoBrew.md)。

今天在生产环境里，它先把一个更窄但已经有用的问题解决了：高频写入要进数仓，
上游突发、下游怕打爆——KairosFlux 坐在中间：吸收突发流量、校验并清洗每条
记录、稳定落盘缓冲，再以下游扛得住的节奏投递出去。

单二进制，除 gRPC/Protobuf 外零第三方依赖（gRPC 仅用于一个可选的基准测试
入口，见下文）。

> 本仓库与协议正在改名过程中：产品名 `BanDB` → `KairosFlux`，协议品牌
> `BANLV` → `Kair`。改名只发生在品牌/文档层——帧格式、opcode、跨语言测试
> 向量字节全部不变，今天已部署的任何东西不会因此破坏。代码层路径
> （`cmd/ban-server`、`bandb_client.py`、`BANDB_ADDR`）尚未迁移，下面每条
> 命令都是今天能直接跑通的真实命令。

## 特性

- *原生协议* —— BANLV，一个为摄入场景设计的零依赖二进制 TCP TLV 协议。
- *落盘前清洗* —— 按记录类型可插拔的 schema 校验；不合格的数据不会进入缓冲。
- *持久化缓冲* —— WAL 支撑的 LSM 引擎，崩溃安全，重启后自动恢复。
- *可靠投递* —— 至少一次投递，熔断器 + 健康感知路由在多个 sink 间切换。
- *多语言* —— Go SDK，外加一个零依赖的 Python 客户端，不需要 protobuf 工具链。
- gRPC 入口仅用于基准测试——见「性能」一节。

## 快速开始

需要 Go 1.26+。

启动服务端：

```bash
cd cmd/ban-server && go run .
```

用 Go 客户端写读（另开终端）：

```console
$ go run ./cmd/ban-cli -addr 127.0.0.1:8080 put order:1001 '{"amount":128,"ts":1754380800}'
已写入: order:1001 = {"amount":128,"ts":1754380800}

$ go run ./cmd/ban-cli -addr 127.0.0.1:8080 get order:1001
{"amount":128,"ts":1754380800}
```

用 Python 客户端写一条行情快照（`client/python/`，纯标准库）：

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600000 --date 2026-08-17 \
    --open 10.0 --high 10.5 --low 9.8 --close 10.2 --volume 1000000 --prev-close 10.0
写入成功: key=quote:2026-08-17:600000
读回内容: {"code": "600000", "date": "2026-08-17", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 1000000.0, "prev_close": 10.0}
```

非正价格在落盘前就被 schema 校验拒绝：

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600001 --date 2026-08-17 --open -1 --high 10.5 --low 9.8 --close 10.2 --volume 1000000
写入被拒绝（清洗/schema 校验未通过）: dropped
```

## 部署须知

在当前的生产形态下——单机、单一写入方（QuantScout）、每日批量写入——
`config/config.json` 里的 `AdmissionEnabled` 与 `ShardRoutingEnabled` 应保持
关闭（二者默认即为 `false`，见 `config/global.go`）：

- `AdmissionEnabled` 是网关自适应准入，用延迟反馈防并发写过载、过载 shed。
  单一写入方的每日批量写不会产生它要防的那种并发突发，这里不存在它要
  防护的过载问题域，开启只会带来额外的延迟探测开销。
- `ShardRoutingEnabled` 是把不属本节点的 key 经 BanNet 转发到多节点分片
  集群里的属主节点。单节点拥有 100% 的 key 空间，没有需要转发的路由决策。

只有当部署形态真的长成这两项功能所对应的样子（真实的多写入方并发负载，
或真实的多节点分片集群）时再打开——在当前形态下默认打开，只会带来开销
和一套没有实际防护/路由对象的探测/转发面。

## 数据清洗

每条写入落盘前都要过一道清洗钩子：帧与长度校验、可选的时间戳单调性校验、
以及按记录类型分派的 schema 注册表（`service/ingesthook/schema`）。被拒绝的
记录返回 `dropped`，不会进入缓冲。内置的行情校验器要求必填字段齐全、价格
为正、OHLC 逻辑一致、日涨跌幅在 ±20% 的合理区间内。新增一种记录类型只需
实现一个 `Validator` 并注册——详见包内文档。

## 架构

`摄入(BANLV，即将改名为 Kair) → 清洗(schema) → 缓冲(LSM) → 投递(Sink)`

当前生产投递目标是本地文件；配置项 `DeliverySinkType` 也支持一个带健康感知
故障切换的 ClickHouse sink。另有两个子系统——Multi-Raft 分片 KV 与借鉴
dubbo-go 的投递治理层——已完整实现且测试齐全，但默认不参与编译
（`//go:build experimental`），需要时用 `-tags experimental` 构建。详见
[docs/BANLV-协议规范.md](docs/BANLV-协议规范.md)。

## 时态内核（已实现，尚未接入写入路径）

`internal/temporal` 实现了一个"AI 原生"时序引擎需要的语义：写入永不覆盖
（每次写产生一个不可变新版本）、`as_of(t)` 返回写入时间 ≤ t 的最新版本、
绝不返回未来写入，`Fingerprint(entries)` 对 `(LogicalKey, Seq, Payload)`
做确定性 sha256，让任何重放出来的状态都能跟自己的历史核对指纹。这部分
今天是**已单测覆盖的纯语义，还没有接入 router 或存储引擎**——生产写入路径
仍是上面说的"同 key 覆盖"。把它接线进去（连同下面的版本化 opcode）是下一个
路线图里程碑，不是已交付的能力，完整的四里程碑方案见 QuantBrew 仓库的
[`方案-BanDB-时态内核与AI数据平面.md`](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/方案-BanDB-时态内核与AI数据平面.md)。

协议这一侧（RFC 阶段，零代码落地——见
[`docs/rfc/BANLV-2.md`](docs/rfc/BANLV-2.md)，文档开头原话就是"纯设计文档，
零代码改动"）梳理了生产场景真正需要的形状：**写多读少**。真实负载是
QuantScout 每日批量导出约 5000 行，不是双向交互式请求-响应，所以 v2 设计了
按连接可选的三档 ack——`every`（现状，逐条写都等一个响应）、`window`（每 N
条或收到 `FLUSH` 才批量确认一次）、`none`（完全 fire-and-forget）。
`none` 档拿掉了逐条 ack，也就拿掉了"连接断了能知道丢了什么"这个保证——
所以设计上把**对账做成强制项**：选 `ack=none` 的客户端必须有能力把自己
发送过的内容跟服务端实际落盘的内容做重放/比对，做不到就不该用这一档。
这些目前都还没有代码落地，v1 的 `ack=every` 仍是唯一已上线的行为。

## 性能

实测中，BANLV 协议在读路径上明显快于内置的 gRPC 入口（50 并发下 GET 吞吐约
为 2.7 倍）；写路径两者基本持平，因为两条入口最终共用同一条受 fsync 约束的
持久化路径。复现：`bash scripts/bench.sh`，具体命令见
[docs/BANLV-协议规范.md](docs/BANLV-协议规范.md)。

## 健壮性（Fuzz 测试）

`go test -fuzz` 对 4 个帧解析入口合计跑了 300 秒（5 分钟），**约 3770 万次
执行、零崩溃**（`bannet.FuzzUnPack` 369,985 次 / `proto.FuzzDecodeScanRequest`
15,108,651 次 / `proto.FuzzDecodeScanResponse` 12,065,019 次 /
`ingesthook.FuzzParsePut` 10,203,443 次）。完整记录（包括它脱胎于的畸形帧
测试矩阵：截断帧、超长声明、非法 msgID、慢客户端半帧沉默等场景）见
[`docs/iteration-2026-08-20-bannet-robustness-audit.md`](docs/iteration-2026-08-20-bannet-robustness-audit.md)。

## 真实使用者

QuantScout（即将改名 ChronoScout，一个 Python 行情爬虫）把全市场日线快照
写入 KairosFlux，是第一个真实生产租户——用法见上方 Python 客户端示例。一次
真实的 5241 行全市场导出：5222 行写入成功、19 行被服务端拒绝（拒因 100%
可解释：17 行是停牌/退市/风险警示当日无成交，2 行是合规的创业板涨停被通用
±20% 合理性阈值误伤）——用 Go 客户端逐字段读回核对，跟 Python 侧源数据完全
一致。

## 文档

- [docs/BANLV-协议规范.md](docs/BANLV-协议规范.md) —— BANLV 协议规范。
- [docs/banlv/vectors.json](docs/banlv/vectors.json) —— 跨语言测试向量（Go + Python）。
- `docs/iteration-*.md` —— 按改动主题记录的工程笔记。

## 许可证

本项目采用 [Apache License, Version 2.0](LICENSE-APACHE) 或
[MIT license](LICENSE-MIT) 双重许可，使用者可任选其一。

除非你另有明确声明，否则你有意提交、意图纳入本项目的任何贡献，按
Apache-2.0 许可证的定义，均视为按上述方式双重许可，不附加任何其他条款。
