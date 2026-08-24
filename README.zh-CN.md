# KairosFlux

[English](README.md) | [中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)
![Protocol](https://img.shields.io/badge/protocol-Kair-orange)
![Performance](https://img.shields.io/badge/Kair%20vs%20gRPC-2.7x%20faster%20(GET)-brightgreen)
<!-- CI 徽章：接入 GitHub Actions 后再补 -->

KairosFlux（原名 **BanDB**）是一个 AI 原生时态数据引擎：所有写入都版本化、
不可变，任何读取都可以问"在某个时刻，系统当时知道什么"，任何状态都能从
版本账本重放出来并跟自己的确定性指纹核对。它是 **ChronoBrew** 组织下的
时态数据流引擎——时序是内核，确定性是独特性，AI 是方向。QuantBrew（确定性
A 股回测内核）与 ChronoScout（全市场侦察爬虫）是它的头两个真实租户——三件
套如何咬合见 QuantBrew 仓库的
[架构总览](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/架构总览-ChronoBrew.md)。

在时态语义之下，是一层更窄但已经跑在生产上的基础设施：高频写入要进数仓，
上游突发、下游怕打爆——KairosFlux 吸收突发流量、按机器可读的契约校验并
清洗每条记录、稳定落盘缓冲，再（可选地）以下游扛得住的节奏投递出去。

单二进制，除 gRPC/Protobuf 外零第三方依赖（gRPC 仅用于一个可选的基准测试
入口，见[性能](#性能)）。

> 本仓库的产品/协议改名已完成：产品名 `BanDB` → `KairosFlux`，协议品牌
> `BANLV` → `Kair`。模块路径、包名（`bannet` → `kairnet`）、`cmd/*` 二进制
> 与文档均已迁移。帧格式、opcode、跨语言测试向量字节全程不变，今天已部署
> 的任何东西不会因此破坏。有一个文件刻意仍叫 `client/python/bandb_client.py`
> ——它计划在后续清理里被整体下线，而不是原地改名，因此它的旧标识符
> （`BanDBClient`、`BanDBError` 等）暂时保留不动。本文档里的每条命令都是
> 撰写时对一个真实运行的服务端实测过的——见[快速开始](#快速开始)。

## 为什么是"AI 原生"

时点正确性是自治 Agent 需要从存储层拿到、而普通键值存储给不了的东西：

- **写永不覆盖。** `PUT_VERSIONED` 永远是给逻辑键追加一个不可变新版本，
  没有任何原地修改。
- **`as_of(t)` 查询是时点查询。** `GET_AS_OF(key, t)` 返回写入时间 `<= t`
  的最新版本，**绝不**返回写入时间晚于 `t` 的版本——Agent 重放一个历史
  决策时，看到的是那个时刻真实已知的东西，不是"现在"的东西。
- **状态可重放、可自检。** `REPLAY_FINGERPRINT` 从版本账本重建某个键范围
  的最新状态，对 `(LogicalKey, Seq, Payload)` 做确定性 sha256——两个各自
  重放同一份账本的进程，不需要逐字节比较就能证明它们落在了同一个状态上。
- **每次写都带着"谁/何时"。** 每条版本记录都携带一个操作元数据信封
  （`seq`、`write_ts`、`source`、`schema_ver`、`payload_hash`），可用审计
  命令查询——"这个键被谁写过几次、在这个时间窗口里"是一个能被回答的问题，
  不是翻日志才能知道的事。
- **让 Agent 直接调用的数据平面是方向，还不是已交付的能力。** 见
  [Roadmap](#roadmap)——面向 Agent 的 `Context`（读）/`Proposal`（写）接口
  已有设计方向，今天没有代码。

## 快速开始

需要 Go 1.26+。下面每条命令都是撰写本文档时对一个真实运行的
`kairosflux-server` 端到端跑通的；关键处标注了退出码。

构建：

```console
$ go build ./...
```

启动服务端（另开终端）：

```console
$ cd cmd/kairosflux-server && go run .
...
2026/08/24 23:50:47 INFO kairnet server starting name=KairosFlux addr=127.0.0.1:8080
```

给同一个逻辑键写三个版本——每次调用都是一个新的不可变版本，不是覆盖：

```console
$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 put-versioned order:1001 '{"amount":128}' scout
已写入版本 seq=1: order:1001 = {"amount":128}

$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 put-versioned order:1001 '{"amount":150}' scout
已写入版本 seq=2: order:1001 = {"amount":150}

$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 put-versioned order:1001 '{"amount":175}' scout
已写入版本 seq=3: order:1001 = {"amount":175}
```

（末尾的 `scout` 参数是操作元数据信封的 `source` 字段——"谁写的"，见
[Roadmap / M2](#roadmap)）

列出完整版本历史，然后按一个严格落在 v2 与 v3 写入时刻之间的时间戳做
`as_of` 读：

```console
$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 list-versions order:1001
seq=1 write_nanos=1787586686757950000 payload={"amount":128}
seq=2 write_nanos=1787586686829263000 payload={"amount":150}
seq=3 write_nanos=1787586686890053000 payload={"amount":175}

$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 get-as-of order:1001 1787586686850000000
seq=2 write_nanos=1787586686829263000 payload={"amount":150}
```

`as_of` 返回的是 **v2**，不是 v3——即便 v3 此刻已经存在于存储里，查询时刻
早于它的写入时刻，所以对这次读取不可见。这是时点保证本身，不是读取时序
凑巧造成的现象。

重放账本并核对自身指纹：

```console
$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 fingerprint order:
逻辑键数=1 不一致数=0 指纹=2c4e10cc1ab683b5dbcec51920641b4765d737d809e57d623018c79d8aa56788
```

`逻辑键数=1`（1 个逻辑键）、`不一致数=0`（与 `:current` 指针比对 0 处不
一致）,后面跟着重放状态的 64 位十六进制 sha256 指纹。同一份账本的两次
独立重放——包括跨一次服务端重启——会产出完全相同的 64 个字符。

<details>
<summary>用 Python 客户端写一条行情快照（v1 协议，无版本历史）</summary>

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600000 --date 2026-08-17 \
    --open 10.0 --high 10.5 --low 9.8 --close 10.2 --volume 1000000 --prev-close 10.0
写入成功: key=quote:2026-08-17:600000
读回内容: {"code": "600000", "date": "2026-08-17", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 1000000.0, "prev_close": 10.0}
```

非正价格在落盘前就被契约校验拒绝：

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600001 --date 2026-08-17 --open -1 --high 10.5 --low 9.8 --close 10.2 --volume 1000000
写入被拒绝（清洗/schema 校验未通过）: dropped
```

</details>

## 架构

```
   写入方              摄入            清洗                时态存储
 (Go / Python  ─Kair─▶ (kairnet TLV) ─▶ (contracts/  ─▶  (WAL + LSM,版本化)
   客户端)                              schema, M1)
                                                           │
                                          PUT_VERSIONED ───┤──▶ 新不可变版本
                                          GET_AS_OF(k,t) ──┤──▶ 最新版本,write_ts<=t,绝不来自未来
                                          LIST_VERSIONS ───┤──▶ 完整版本历史
                                          REPLAY_FINGERPRINT┤──▶ 确定性 sha256 对账:current  (M0)
                                          LIST_WRITES /    ─┤──▶ 审计:谁在何时写了什么       (M2,
                                          export-writes     │                              待合并)
                                                           │
                                    ┌──────────────────────┴───────────────────────┐
                                    ▼                                              ▼
                        投递(本地文件 / ClickHouse sink)                    AI Agent 数据平面
                        —— 既有 v1 摄入管道,独立于时态 opcode              Context(读)/Proposal(写)
                                                                          —— roadmap(M4),无代码
```

另有两个子系统——Multi-Raft 分片 KV 与借鉴 dubbo-go 的投递治理层——已完整
实现且测试齐全，但默认不参与编译（`//go:build experimental`），需要时用
`-tags experimental` 构建。线格式细节见 [docs/Kair-协议规范.md](docs/Kair-协议规范.md)；
时态键空间与语义见 [docs/架构与语义总览.md](docs/架构与语义总览.md)。

## 特性

| 状态 | 能力 |
|---|---|
| **已实现 —— M0** | 写永不覆盖；`PUT_VERSIONED`/`GET_AS_OF`/`LIST_VERSIONS`/`REPLAY_FINGERPRINT` 四个 opcode 已在服务端与 CLI 全链路接线（2026-08-24 交付）。 |
| **已实现 —— M1** | 按记录类型的机器可读契约（`contracts/*.schema.json`：键布局、PIT 语义、幂等键、校验规则）、契约加载 fail-fast、结构化校验子码（`0x3001`–`0x3004`）、时间戳单调性校验按声明的 time-kind 分派（而不是猜 key 字符串里冒号的位置）。 |
| **已合并 —— M2** | `LIST_WRITES` 审计查询（opcode 0x0D）+ 每条版本记录的操作元数据信封（`seq`、`write_ts`、`source`、`schema_ver`、`payload_hash`），带信封版本标记与对 M2 之前旧记录的懒迁移读兼容；按来源的 `COUNT` 聚合；确定性排序、append-only 的 JSONL 审计导出；`REPLAY_FINGERPRINT` 升级为按数据集/as-of 上界的可调用服务。全套测试与 race 全绿（2026-08-25 合并）。 |
| **已合并 —— M3** | 声明式 Job 控制面（`job:spec:{name}`/`job:status:{name}`/`job:events:{name}:v{seq}`，全部走既有版本化 opcode）、单进程 reconcile 循环（`internal/jobctl` + `cmd/kairosflux-jobctl`）、显式策略生命周期状态机（Hypothesis → Gate → Candidate → Paper → Live/Retired）。一万次重跑幂等对真实服务端验证通过。 |
| **Roadmap —— M4** | AI 原生数据平面：给 Agent 用的 `Context`（时点读）与 `Proposal`（走同一条版本化/可审计路径的写）接口。目前只有方向，未详细设计，无代码。 |

## 部署须知

在当前的生产形态下——单机、单一写入方（QuantScout）、每日批量写入——
`config/config.json` 里的 `AdmissionEnabled` 与 `ShardRoutingEnabled` 应保持
关闭（二者默认即为 `false`，见 `config/global.go`）：

- `AdmissionEnabled` 是网关自适应准入，用延迟反馈防并发写过载、过载 shed。
  单一写入方的每日批量写不会产生它要防的那种并发突发，这里不存在它要
  防护的过载问题域，开启只会带来额外的延迟探测开销。
- `ShardRoutingEnabled` 是把不属本节点的 key 经 KairNet 转发到多节点分片
  集群里的属主节点。单节点拥有 100% 的 key 空间，没有需要转发的路由决策。

只有当部署形态真的长成这两项功能所对应的样子（真实的多写入方并发负载，
或真实的多节点分片集群）时再打开——在当前形态下默认打开，只会带来开销
和一套没有实际防护/路由对象的探测/转发面。

## 数据清洗

每条写入落盘前都要过一道清洗钩子：帧与长度校验、可选的时间戳单调性校验
（M1 起按写入声明的 time-kind 分派，不是猜 key 字符串本身），以及按记录
类型分派的契约驱动 schema 注册表（`service/ingesthook/schema`、
`contracts/*.schema.json`）。被拒绝的记录返回 `dropped`，不会进入缓冲。
内置的行情契约要求必填字段齐全、价格为正、OHLC 逻辑一致、日涨跌幅在
±21% 的合理区间内（是 21%，不是 20%——创业板/科创板涨跌停线是相对前收盘
±20%，但涨停价本身经四舍五入后可能达到前收盘的约 20.02%，20% 的阈值会
把合规的涨停打印卡在门槛外；见 `service/ingesthook/schema/quote.go`）。
新增一种记录类型只需一份契约文件加一个 `Validator` 实现——详见包内文档。

## 时态内核——语义

完整写法见 [docs/架构与语义总览.md](docs/架构与语义总览.md)：时态键空间
（逻辑键/版本键/`:current` 指针）、`as_of(t)` 契约与它的时点保证、指纹
定义、双时态演进路线（M0 把有效时间与写入时间统一处理；把两者分离是
M2+ 的范围）。接线决策与理由的完整记录见
[`docs/rfc/时态内核-M0-版本化与as-of.md`](docs/rfc/时态内核-M0-版本化与as-of.md)。

协议这一侧（RFC 阶段，零代码落地——见
[`docs/rfc/Kair-2.md`](docs/rfc/Kair-2.md)，文档开头原话就是"纯设计文档，
零代码改动"）梳理了生产场景真正需要的形状：**写多读少**。真实负载是
QuantScout 每日批量导出约 5000 行，不是双向交互式请求-响应，所以 v2 设计了
按连接可选的三档 ack——`every`（现状，逐条写都等一个响应）、`window`（每 N
条或收到 `FLUSH` 才批量确认一次）、`none`（完全 fire-and-forget）。
`none` 档拿掉了逐条 ack，也就拿掉了"连接断了能知道丢了什么"这个保证——
所以设计上把**对账做成强制项**：选 `ack=none` 的客户端必须有能力把自己
发送过的内容跟服务端实际落盘的内容做重放/比对，做不到就不该用这一档。
这些目前都还没有代码落地，v1 的 `ack=every` 仍是唯一已上线的行为。

## Roadmap

完整的四里程碑方案（M0–M4）在 QuantBrew 仓库的
[`方案-BanDB-时态内核与AI数据平面.md`](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/方案-BanDB-时态内核与AI数据平面.md)。
简述：M0/M1 已交付（见[特性](#特性)）；M2（重放服务化、操作审计信封、
`LIST_WRITES`）与 M3（声明式 Job 控制面）均已合并；
M4（`Context`/`Proposal` AI 数据平面）是方向，还不是设计。

## 性能

实测中，Kair 协议在读路径上明显快于内置的 gRPC 入口（50 并发下 GET 吞吐约
为 2.7 倍）；写路径两者基本持平，因为两条入口最终共用同一条受 fsync 约束的
持久化路径。复现：`bash scripts/bench.sh`，具体命令见
[docs/Kair-协议规范.md](docs/Kair-协议规范.md)。

## 健壮性（Fuzz 测试）

`go test -fuzz` 对 4 个帧解析入口合计跑了 300 秒（5 分钟），**约 3770 万次
执行、零崩溃**（`kairnet.FuzzUnPack` 369,985 次 / `proto.FuzzDecodeScanRequest`
15,108,651 次 / `proto.FuzzDecodeScanResponse` 12,065,019 次 /
`ingesthook.FuzzParsePut` 10,203,443 次）。完整记录（包括它脱胎于的畸形帧
测试矩阵：截断帧、超长声明、非法 msgID、慢客户端半帧沉默等场景）见
[`docs/iteration-2026-08-20-bannet-robustness-audit.md`](docs/iteration-2026-08-20-bannet-robustness-audit.md)。

## 真实使用者

QuantScout（即将改名 ChronoScout，一个 Python 行情爬虫）把全市场日线快照
写入 KairosFlux，是第一个真实生产租户——用法见上方 Python 客户端示例。一次
真实的 5241 行全市场导出：5222 行写入成功、19 行被服务端拒绝（拒因 100%
可解释：17 行是停牌/退市/风险警示当日无成交，2 行是合规的创业板涨停被
±21% 合理性阈值误伤）——用 Go 客户端逐字段读回核对，跟 Python 侧源数据完全
一致。

## 文档

- [docs/架构与语义总览.md](docs/架构与语义总览.md) —— 时态键空间、版本
  语义、`as_of` 契约、指纹定义、双时态演进路线（贡献者地图，中文）。
- [docs/Kair-协议规范.md](docs/Kair-协议规范.md) —— Kair 协议规范。
- [docs/rfc/时态内核-M0-版本化与as-of.md](docs/rfc/时态内核-M0-版本化与as-of.md) —— 时态内核 RFC 与接线记录。
- [docs/kair/vectors-v2.json](docs/kair/vectors-v2.json) / [docs/kair/vectors.json](docs/kair/vectors.json) —— 跨语言测试向量（Go + Python），v2 与 v1。
- `docs/iteration-*.md` —— 按改动主题记录的工程笔记。

## 许可证

本项目采用 [Apache License, Version 2.0](LICENSE-APACHE) 或
[MIT license](LICENSE-MIT) 双重许可，使用者可任选其一。

除非你另有明确声明，否则你有意提交、意图纳入本项目的任何贡献，按
Apache-2.0 许可证的定义，均视为按上述方式双重许可，不附加任何其他条款。
