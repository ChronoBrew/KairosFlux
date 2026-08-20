# BanDB

[English](README.md) | [中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)
![Protocol](https://img.shields.io/badge/protocol-BANLV-orange)
![Performance](https://img.shields.io/badge/BANLV%20vs%20gRPC-2.7x%20faster%20(GET)-brightgreen)
<!-- CI 徽章：接入 GitHub Actions 后再补 -->

高频写入要进数仓，上游突发、下游怕打爆——BanDB 坐在中间：吸收突发流量、
校验并清洗每条记录、稳定落盘缓冲，再以下游扛得住的节奏投递出去。

单二进制，除 gRPC/Protobuf 外零第三方依赖（gRPC 仅用于一个可选的基准测试
入口，见下文）。

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

`摄入(BANLV) → 清洗(schema) → 缓冲(LSM) → 投递(Sink)`

当前生产投递目标是本地文件；配置项 `DeliverySinkType` 也支持一个带健康感知
故障切换的 ClickHouse sink。另有两个子系统——Multi-Raft 分片 KV 与借鉴
dubbo-go 的投递治理层——已完整实现且测试齐全，但默认不参与编译
（`//go:build experimental`），需要时用 `-tags experimental` 构建。详见
[docs/BANLV-协议规范.md](docs/BANLV-协议规范.md)。

## 性能

实测中，BANLV 协议在读路径上明显快于内置的 gRPC 入口（50 并发下 GET 吞吐约
为 2.7 倍）；写路径两者基本持平，因为两条入口最终共用同一条受 fsync 约束的
持久化路径。复现：`bash scripts/bench.sh`，具体命令见
[docs/BANLV-协议规范.md](docs/BANLV-协议规范.md)。

## 真实使用者

QuantScout（一个 Python 行情爬虫）把全市场日线快照写入 BanDB，是第一个真实
生产租户——用法见上方 Python 客户端示例。

## 文档

- [docs/BANLV-协议规范.md](docs/BANLV-协议规范.md) —— BANLV 协议规范。
- [docs/banlv/vectors.json](docs/banlv/vectors.json) —— 跨语言测试向量（Go + Python）。
- `docs/iteration-*.md` —— 按改动主题记录的工程笔记。

## 许可证

本项目采用 [Apache License, Version 2.0](LICENSE-APACHE) 或
[MIT license](LICENSE-MIT) 双重许可，使用者可任选其一。

除非你另有明确声明，否则你有意提交、意图纳入本项目的任何贡献，按
Apache-2.0 许可证的定义，均视为按上述方式双重许可，不附加任何其他条款。
