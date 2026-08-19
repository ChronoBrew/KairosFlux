# 迭代复盘：QuantScout 全量实测（5241 行真实行情）暴露的四个实战问题

日期：2026-08-20　范围：`service/ingesthook`、`service/ingesthook/schema`、
`service/router.go`、`client`（Go SDK）、`client/python`、`docs/BANLV-协议规范.md`

## 背景

QuantScout 用真实的 5241 行全市场日线快照对接 BanDB 做全量实测（不是抽样/合成
数据），暴露了四个此前单元测试没覆盖到的真实问题——全部是"规则本身没错，但
和真实数据/真实使用场景碰撞后才暴露的边界"，与合成测试数据检测不出来的那类
问题同源（真实数据的价值正在这里）。

## D1：±20% 阈值误伤真实涨停

**现象**：300069（+20.01%）、301106（+20.02%）两个真实创业板涨停日被 schema
校验拒收。创业板/科创板涨跌停线是 ±20%，但涨停价按当日基准价四舍五入到分后，
折算涨跌幅可能略超 20.00%——这不是数据错误，是四舍五入的舍入误差，卡在 20%
整数阈值上必然误伤。

**处理**：`service/ingesthook/schema/quote.go` 的 `maxPctChange` 从 0.20 改为
0.21，与 QuantBrew 数据质检的 `max_daily_move=0.21` 口径对齐——两套系统对同一
份行情数据的合理性判断必须一致，否则会出现"同一天数据在两个系统里一个收一个
拒"的诡异状态。新增测试 `TestQuoteSnapshot_RealCreationBoardLimitUpNotFalselyRejected`
直接用能复现 20.01%/20.02% 的价格组合回归锁定；原有边界测试
`TestQuoteSnapshot_PctChangeAtBoundaryPasses` 同步改为验证 ±21% 边界。

## D2：BANLV 拒绝不回传原因

**现象**：`ingesthook.Filter.Handle()` 把 `Validate()` 算出的 `reason` 直接丢弃，
客户端只收到 `dropped`，不知道具体原因——QuantScout 被迫在本地重新实现一遍
校验规则去猜测，这不该是调用方要做的事。

**处理**：
- `Filter.Handle` 签名从 `(bannet.Request) bannet.HookAction` 改为
  `(bannet.Request) (bannet.HookAction, string)`，把 `Validate` 已经算出的
  `reason` 带出来（此前只用于 metrics 计数）。
- `service/router.go`：`preHandleFunc`/`SetPreHandle` 同步改签名；`PreHandle`
  捕获 `reason` 传给新的 `sendDropped(request, reason)`；新增
  `droppedPayload(reason)`，在原有 `[statusLen][status]` 之后追加
  `[reasonLen u16 LE][reason]`——**向后兼容**：老客户端的 `parseStatus` 只读
  `statusLen` 声明的字节，新增字段落在它从不读取的 `rest` 里，不会解析失败。
- Go SDK（`client/conn.go`）：`statusError` 新增 `rest []byte` 参数，`dropped`
  状态用 `parseDropReason` 解出原因并包进 `fmt.Errorf("%w: %s", ErrDropped, reason)`
  ——`errors.Is(err, client.ErrDropped)` 仍然成立，只是错误信息更具体。
- Python 客户端：`DroppedError` 新增 `reason` 属性，但**不改动** `str(e)`/
  `e.args[0]`（仍恒为 `"dropped"`）——`crosslang_probe.py` 等按 status 精确
  比对的调用方不受影响，需要细节的调用方显式读 `e.reason`。
- gRPC 传输**不携带这个字段**（`PutResponse` 只有 `Success bool`），与 gRPC
  是基准测试/协议对照定位一致，本轮不为它扩展 protobuf 消息。
- `docs/BANLV-协议规范.md` 新增 3.4 节记录这个协议扩展；
  `docs/banlv/vectors.json` 追加第 12 条向量 `resp_err_dropped_with_reason`，
  Go（`bannet/vectors_test.go`，通用循环自动覆盖新向量）与 Python
  （新增 `test_resp_err_dropped_with_reason`）两侧都验证。

**测试**：`service/router_reason_test.go`（`droppedPayload` 编解码、
`Router.PreHandle` 端到端把 reason 送进 `Conn.SendBuffMsg`）；
`client/dropped_reason_test.go`（真实服务端+真实 Go 客户端，schema 拒绝/畸形
帧拒绝两种场景都验证 reason 能读到）；`service/ingesthook/filter_test.go` 补
`TestHandle_SchemaDropCarriesReason`；Python 侧补 3 个测试
（`test_parse_drop_reason_roundtrip`/`_missing_field_returns_empty`/
`test_dropped_error_exposes_reason_without_changing_message`）。

## D3：dropBackward 与 quote key 冲突

**现象**：`quote:<日期>:<代码>` 按代码乱序发送时被时间戳单调性检查误判为
"回退"——`parseKey` 假设 key 末段是数字时间戳（为 `imu:设备:时间戳` 这类无类型
key 设计的启发式），行情快照 key 的末段恰好也是数字（股票代码），QuantScout
全量实测里热身后按全量顺序跑，5 行被误杀。

**裁决**（两个选项里选了"schema 感知"，理由如下）：
- 选项 A（已采纳）：单调性检查对**已注册 schema 的 key 前缀无条件跳过**，
  不看 `dropBackward` 开关。理由——已注册 schema 的类型有自己明确的 key 语义
  约定，这个通用启发式压根不是为它设计的；schema 校验器自己如果真的需要
  单调性保证，应该在校验规则里显式实现（比如行情场景其实不需要單调性保证：
  每日全量快照允许乱序、允许重复投递同一天数据，靠 ClickHouse 的
  `ReplacingMergeTree` 去重即可）。
- 选项 B（未采纳）：按前缀可配置是否跳过。放弃理由——多一个配置项、多一种
  组合状态，而"已注册 schema 就跳过"已经能通过注册行为本身表达意图，不需要
  额外的开关来重复表达同一件事。

**实现**：`Filter.Validate` 里先 `schema.Lookup(key)` 一次，结果同时决定「是否
跳过单调性检查」与「是否要跑 schema 校验」——`hasSchema` 为真时，不管
`dropBackward` 是否开启，都不进 `parseKey`/`lastTS` 那段逻辑。

**测试**：`TestHandle_SchemaRegisteredKeysSkipMonotonicCheck` 直接复现"600000
之后写 000001"的乱序场景（`parseKey` 会把 "000001" 解析成比 "600000" 小的
数值时间戳），验证 `dropBackward=true` 时已注册 schema 的 key 仍正常放行；
原有 `TestHandle_MonotonicDrop` 等针对 `imu:` 前缀（未注册 schema）的用例不变，
证明这不是把单调性检查整体关掉，只是对已知类型跳过。

## D4：volume 量纲定契约

**现象**：QuantScout 按"手"（A 股惯例，1 手=100 股）写入 `volume` 字段，但这个
单位此前只存在于双方的隐含默契里，代码/文档都没有写死。量纲不显式约定，
将来只要有一处误按"股"消费，就是系统性的 100 倍误差，且不会报任何错——比
拒绝一条明显非法记录危险得多，因为它不触发任何校验失败。

**处理**：在权威定义处（`service/ingesthook/schema/quote.go` 的
`quoteRecord.Volume` 字段注释）显式写死"手"这个单位契约；下游
`docs/clickhouse-schema.md` 的 `quote_snapshot` 建表 DDL 同步在 `volume` 列
加同样的注释。两处都指向同一句话，不是各自表述一遍容易漂移的说法。

## 验证

- `go vet ./...`、`go build ./...`、`go build -tags experimental ./...` 全干净。
- `go test ./...`（默认）与 Python 侧 `python3 -m unittest test_bandb_client -v`
  全绿；跨语言联调 `go test ./client/python/... -run TestCrosslang -v` 5/5 通过
  （Python 客户端改动后重新验证过一次，未破坏跨语言一致性）。
- 新增/修改测试清单：
  - `service/ingesthook/schema`：`TestQuoteSnapshot_RealCreationBoardLimitUpNotFalselyRejected`
    (300069/301106 真实场景)、边界测试改为 ±21%。
  - `service/ingesthook`：`TestHandle_SchemaDropCarriesReason`、
    `TestHandle_SchemaRegisteredKeysSkipMonotonicCheck`、四个既有 Handle 测试
    改为接收 `(action, reason)` 两个返回值。
  - `service`：新增 `router_reason_test.go`（3 个测试）。
  - `client`：新增 `dropped_reason_test.go`（2 个测试，真实服务端+真实 SDK）。
  - `client/python`：新增 3 个 reason 相关测试 + 1 个新向量测试，共 15/15 通过。

## 遗留

- gRPC 传输的 `PutResponse` 不携带丢弃原因（`Success bool` 之外无扩展字段）；
  按当前"gRPC 仅基准测试用途"的定位判断，暂不为此改动 `.proto`。
- `dropBackward` 的"设备:时间戳"启发式仍未泛化为按数据类型显式声明单调性
  语义——本轮只是让已注册 schema 的类型跳过一个不适用它的检查，没有反过来
  给 schema 校验器提供"如果你需要单调性，在这里声明"的机制；行情场景当前
  确实不需要，但下一个新数据类型如果需要单调性保证，还得再走一遍类似的
  特例判断。
