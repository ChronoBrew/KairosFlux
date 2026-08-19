# 迭代复盘：ClickHouseSink 真实实现 + governance 治理层转正

日期：2026-08-20　范围：`service/delivery`（新增 `clickhouse_sink.go`）、
`service/delivery/governance`（去实验标签、新增优先级路由）、`config`、
`service/delivery_bootstrap.go`

## 背景

体检报告第 4 步（ClickHouseSink 真实实现）与 governance 的隔离说明都写明了
触发条件：分析仓实例就位、且需要治理多个下游 sink 时再做。这两个条件本轮
同时满足——作者已购服务器、ClickHouse 实例即将存在。

## 做了什么

### 1. ClickHouseSink：真实 HTTP 批量插入

`service/delivery/clickhouse_sink.go`：把一批 `Record` 编码为 JSONEachRow（原始
`Value` 字段 + 注入的 `_key` 字段，供审计）POST 到
`INSERT INTO <database>.<table> FORMAT JSONEachRow`。DSN/表名/超时/重试全部
配置化（`config.G.ClickHouse*`）。幂等不在这层做——`Sink` 接口只承诺「重复投递
不破坏下游正确性」，这里靠 ClickHouse 表本身的 `ReplacingMergeTree`（按业务
字段排序，重复插入在合并时折叠）达成，配合既有 offset 机制的 at-least-once
语义，效果是「最终去重」。

### 2. governance 转正：去掉 experimental 标签，新增优先级路由

`service/delivery/governance` 的 6 个文件全部去掉 `//go:build experimental`——
默认 `go build`/`go test ./...` 就能编译到它，不再需要 `-tags experimental`。

`Router` 原本只有 round-robin 语义（`NewRouter`），新增 `NewPriorityRouter`：
`Send` 总从 `sinks[0]` 起尝试而非轮转游标——`sinks[0]` 是真正的主选择，只有它
不健康/被熔断/发送失败时才降级到后面的兜底 sink。这不是可有可无的补充：
round-robin 在两个 sink 都健康时会把流量分流到"兜底"上，这不是「ClickHouse
主 + FileSink 兜底」场景要的语义——兜底不该分走本该走主链路的流量。新增两个
测试锁定：主健康时恒选主、主故障时同一次 Send 内即可降级、主恢复后立即切回
（不必等下一次轮转）。

### 3. delivery_bootstrap.go 接线

新增 `config.G.DeliverySinkType`（默认 `"file"`，行为不变；`"clickhouse"` 时
接 `governance.NewPriorityRouter([ClickHouseSink, FileSink], ...)` 作为投递
目标）。`newDeliverySink()` 的返回类型从 `delivery.Sink` 改为本地声明的
`deliveryTarget`（Name+Send 两方法）——因为 `governance.Router` 故意不实现
`Health()`（它的健康状态由内部的每 sink Breaker 与各 sink 自身 `Health()`
共同决定，不该被当作"另一层 Router 的一个 sink"来做健康探测）。

### 4. 故障注入测试挖出一个真实 bug：Health() 与 Router 熔断门槛互锁死

`ClickHouseSink.Health()` 最初实现是"跟随最近一次 `Send` 结果翻转"，写完
6 个单测（mock HTTP 服务器测批量编码/重试/错误映射）后全部通过，但补写
`clickhouse_router_test.go`（真实 ClickHouseSink + 真实 FileSink + 真实
Router，mock ClickHouse 先 5xx 后恢复 200）时，"恢复后应切回主"这一步断言
失败——数据仍在往文件里落，即便 mock 已经恢复健康。

根因：`Router.Send` 的门槛是 `!sink.Health().Healthy || !breaker.Allow()`。
一旦 `Send` 失败一次，`Health().Healthy` 变 `false`，Router 从此不再对该 sink
调用 `Send`——包括熔断器已经转入 half-open、本该放行一次探测的时候。探测
请求根本发不出去，`Healthy` 永远没有机会被下一次成功的 `Send` 翻回 `true`，
sink 永久卡在"兜底"，即使下游早已恢复。

这不是测试写错了断言——是 `ClickHouseSink.Health()` 的实现方式与
`governance.Breaker` 的职责重叠又冲突：`Breaker` 已经有完整的
closed/open/half-open 状态机专门负责"现在该不该再试一次"，`Sink` 不该自己
再实现一套会跟它打架的影子熔断。修复：`Health()` 改为只反映结构性可用性
（`addr`/`database`/`table` 是否配置），不再随 `Send` 成败波动——与既有
`FileSink.Health()`（只查文件句柄是否打开）的模式对齐。`Send` 失败的详情仍
记进 `Reason` 供观测，但不再驱动 `Healthy`。

**这正是"故障注入测试要断言恢复路径"这一要求本身的价值**：只测"CH 挂了会
降级"不够，必须测"CH 恢复后真的切得回来"，否则这类"一次故障后永久兜底"的
bug 不会被发现——它在生产上会表现为"重启进程才能恢复投递 ClickHouse"，
而不会有任何报错。

## 验证

- `go vet ./...`、`go build ./...` 全干净；`governance` 现默认参与构建
  （`go list ./...` 可见，无需 `-tags experimental`）；`shardkv` 仍隔离
  （只在 `-tags experimental` 下出现）。
- `service/delivery` 全部测试通过，含新增 7 个 ClickHouseSink 单测 + 1 个
  端到端故障注入测试（`TestClickHouseRouter_FaultInjection_FallsBackToFileThenRecovers`）。
- `service/delivery/governance` 全部测试通过，含新增 2 个优先级路由测试。
- 全仓 `go test ./...` 绿。

## 遗留

- ClickHouse 端的去重完全依赖 `ReplacingMergeTree` 的后台合并；查询侧在合并
  发生前看到的可能是重复行，需要业务方按需 `FINAL` 或 `argMax` 去重，详见
  `docs/clickhouse-schema.md`。
- 真实 ClickHouse 集成验证留给服务器实际部署时做——本轮测试全部基于
  `httptest` mock，覆盖协议交互与治理逻辑，不覆盖 ClickHouse 自身行为
  （如 `ReplacingMergeTree` 合并时机、`JSONEachRow` 类型转换边界）。
