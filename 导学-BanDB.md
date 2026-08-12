# 导学：BanDB

## 已确认输入

- 简称：BanDB
- 技术定位：Go 后端 / 基础设施 / 边缘采集 KV 数据引擎
- 分析依据：仓库 README、核心源码、迭代文档、测试文件与 benchmark 文档

## 1. 前置知识（面试高频标注）

| 知识点 | 为何需要 | 在本项目中的位置 | 高频度 |
|---|---|---|---|
| LSM-Tree 写优化 | 理解为什么高频写先进入内存结构，再顺序刷盘成不可变文件 | `storage/zstorage/memtable.go`、`storage/zstorage/SSTable.go` | 高 |
| 跳表 | 理解 MemTable 如何提供有序写入、点查和范围扫描 | `storage/zstorage/memtable.go` | 高 |
| WAL 与崩溃恢复 | 理解写确认、fsync、重放、撕裂尾写处理和持久化契约 | `storage/wal.go`、`service/fsm.go` | 高 |
| group commit | 理解如何把多次 fsync 合并成一次，提高并发写吞吐 | `storage/wal.go`、`Raft/raft_wal.go`、`docs/iteration-2026-07-19-standalone-write-durability.md` | 高 |
| SSTable 块索引与 Bloom Filter | 理解读路径如何先过滤文件，再定位块，减少磁盘扫描 | `storage/zstorage/SSTable.go`、`storage/zstorage/bloom.go`、`storage/zstorage/partitioned_bloom.go` | 高 |
| 墓碑删除 | 理解 LSM 中删除不能简单物理移除，否则旧 SSTable 数据会复活 | `storage/zstorage/memtable.go`、`storage/zstorage/SSTable.go` | 高 |
| 背压 | 理解高频写入场景下如何限制未刷盘内存，避免 OOM | `pkg/credit/credit.go`、`storage/zstorage/memtable.go`、`docs-step/M2-backpressure-result.md` | 高 |
| Raft 基础 | 理解 leader 选举、日志复制、提交索引、快照和 apply 循环 | `Raft/raft.go`、`Raft/rpc.go`、`service/fsm.go` | 中 |
| 二进制协议 | 理解自研 TCP 入口如何编码请求、路由请求和返回响应 | `network/banNet/DataPack.go`、`service/router.go`、`pkg/proto/scan.go` | 中 |
| 谓词下推 | 理解边缘侧先筛选再回传，降低弱网传输成本 | `pkg/predicate/predicate.go`、`service/fsm.go`、`service/router.go` | 中 |
| 可观测性 | 理解用计数器和仪表证明系统行为，而不是只说优化有效 | `pkg/metrics/metrics.go`、`Server/server.go` | 中 |
| Go 并发原语 | 理解 channel、goroutine、RWMutex、atomic、sync.Cond 在系统中的角色 | `storage/wal.go`、`network/banNet/msgHandle.go`、`Raft/raft.go` | 高 |

## 2. 重点亮点与学习顺序（先看这个）

1. 持久化写路径与 group commit
   - 为什么重要：这是项目最像真实后端系统的部分，能从现象、瓶颈、修复、验证完整讲出工程闭环。
   - 通用技术关键词：WAL、fsync、group commit、崩溃恢复、持久化契约。
   - 先看哪些文件：`service/fsm.go`、`storage/wal.go`、`docs/iteration-2026-07-19-standalone-write-durability.md`。
   - 建议学习顺序：先读迭代文档理解问题，再读 `KVServer.Write` 到 `WAL.Append` 的调用链，最后读 WAL 重写和 checkpoint。

2. LSM 存储引擎读写路径
   - 为什么重要：它支撑了高频写入、延迟刷盘、读路径多层查找、删除语义和重启恢复。
   - 通用技术关键词：MemTable、SkipList、SSTable、tombstone、block index、Bloom Filter、compaction。
   - 先看哪些文件：`storage/zstorage/memtable.go`、`storage/zstorage/SSTable.go`、`storage/zstorage/merge_iterator.go`。
   - 建议学习顺序：先看 Put/Get/Delete，再看 Flush，再看 SSTable 写入格式、索引、Bloom 和归并。

3. 高频摄入下的背压与内存边界
   - 为什么重要：面试官很容易追问“高频写撑爆内存怎么办”，这里有真实 benchmark 和改进记录。
   - 通用技术关键词：backpressure、byte budget、credit pool、bounded memory、producer-consumer。
   - 先看哪些文件：`pkg/credit/credit.go`、`storage/zstorage/memtable.go`、`docs-step/M1-ingest-benchmark-result.md`、`docs-step/M2-backpressure-result.md`。
   - 建议学习顺序：先看 M1 如何发现内存无界，再看 M2 如何用字节信用池封顶，最后看仍未解决的 SSTable 元数据增长边界。

4. 边缘侧可编程采集入口
   - 为什么重要：这是项目区别于普通 KV 的业务定位，能讲“为什么不是 Redis clone”。
   - 通用技术关键词：ingest hook、validation、redaction、drop policy、request-response invariant。
   - 先看哪些文件：`Server/server.go`、`service/router.go`、`service/ingesthook/filter.go`。
   - 建议学习顺序：先看服务启动如何挂钩子，再看 PreHandle 如何决定丢弃或改写，最后看丢弃响应如何避免协议错位。

5. 边缘范围查询与谓词下推
   - 为什么重要：它体现“只回传命中切片”的数据移动优化思路，但也有当前仅覆盖热数据的诚实边界。
   - 通用技术关键词：range scan、predicate pushdown、server-side filtering、result cap。
   - 先看哪些文件：`pkg/proto/scan.go`、`pkg/predicate/predicate.go`、`service/fsm.go`、`storage/engine.go`。
   - 建议学习顺序：先看协议编码，再看谓词 Eval，再看服务层 Scan 如何限制结果数量并复制返回。

6. Raft 写复制与状态机 apply
   - 为什么重要：它能展示分布式系统基础，但要按“教学型/项目型实现”讲清边界。
   - 通用技术关键词：leader election、heartbeat、log replication、commit index、snapshot、FSM。
   - 先看哪些文件：`Raft/raft.go`、`Raft/rpc.go`、`Raft/raft_wal.go`、`service/fsm.go`。
   - 建议学习顺序：先理解状态转换，再看 AppendEntry 和 WaitForCommit，最后看快照如何交给存储层应用。

## 3. 必备知识点

- 能画出写路径：客户端请求 -> 路由 -> 服务层 -> standalone WAL 或 Raft 日志 -> MemTable -> Flush -> SSTable。
- 能讲清为什么 WAL 要先于 MemTable 写入，以及 `Append` 返回代表什么持久化承诺。
- 能说明 group commit 为什么能提升 QPS，以及它没有改变持久化语义。
- 能解释 MemTable 的 active/dirty 双表模型，为什么刷盘时仍可继续接收新写入。
- 能说明删除为什么用墓碑，而不是从跳表或文件里直接删掉。
- 能解释 SSTable 文件尾部的块索引、Bloom trailer 和 footer 如何参与读路径。
- 能说出 checkpoint 为什么要和写路径互斥，以及为什么 WAL 重写要使用临时文件加原子 rename。
- 能讲清背压解决的是“未刷盘热数据无界”，不是所有内存增长。
- 能诚实说明 SCAN 当前只覆盖 MemTable 热数据，SSTable 历史范围扫描仍待扩展。
- 能区分 standalone 模式和 raft 模式的耐久性来源。

## 4. 推荐阅读（结合仓库）

| 主题 | 通用技术点 | 建议阅读位置 | 预计时间 | 读完能回答什么 |
|---|---|---|---|---|
| 项目全局定位 | 边缘采集、KV 引擎、LSM、Raft | `README.md` | 20 分钟 | 这个项目解决什么问题，为什么不是普通 KV demo？ |
| 写路径主链路 | WAL、group commit、checkpoint | `service/fsm.go`、`storage/wal.go` | 60 分钟 | PUT 如何从请求变成可恢复的数据？ |
| MemTable 实现 | 跳表、active/dirty、背压 | `storage/zstorage/memtable.go` | 60 分钟 | 为什么写入快，刷盘时如何避免阻塞全部写？ |
| SSTable 格式 | 块索引、Bloom、墓碑、重载 | `storage/zstorage/SSTable.go` | 90 分钟 | 重启后如何找到已刷盘数据？读路径如何减少扫描？ |
| Compaction | K 路归并、去重、流式写出 | `storage/zstorage/merge_iterator.go`、`storage/zstorage/iterator.go` | 45 分钟 | 多个 SSTable 如何合并并保留最新版本？ |
| 背压组件 | 信用池、阻塞唤醒、预算关闭 | `pkg/credit/credit.go`、`pkg/credit/credit_test.go` | 30 分钟 | 高频摄入时如何给内存上限？ |
| 边缘过滤钩子 | 校验、脱敏、丢弃策略 | `service/ingesthook/filter.go`、`service/router.go` | 45 分钟 | 如何在落盘前处理畸形帧和敏感字段？ |
| Scan 协议与谓词 | 协议编码、谓词下推、结果上限 | `pkg/proto/scan.go`、`pkg/predicate/predicate.go`、`service/fsm.go` | 45 分钟 | 如何只返回命中的边缘数据切片？ |
| 自研 TCP 框架 | TLV 协议、路由、worker pool | `network/banNet/DataPack.go`、`network/banNet/msgHandle.go`、`network/banNet/server.go` | 60 分钟 | 请求如何被拆包、分发、处理和回包？ |
| Raft 写复制 | 选举、心跳、日志、快照 | `Raft/raft.go`、`Raft/rpc.go`、`Raft/raft_wal.go` | 120 分钟 | raft 模式下写请求如何提交到状态机？ |
| 性能排障复盘 | 指标、基线、瓶颈归因、修复验证 | `docs/iteration-2026-07-19-standalone-write-durability.md` | 45 分钟 | 面试时如何讲一个完整的性能优化故事？ |
| 高频摄入 benchmark | 开环压测、内存增长、背压验证 | `docs-step/M1-ingest-benchmark-result.md`、`docs-step/M2-backpressure-result.md` | 45 分钟 | 怎么证明系统能扛高频写，以及边界在哪里？ |

## 5. 自学提醒

若某文件或原理看不懂，请继续追问 AI；本 skill 负责给学习路径与题目，不提供逐行讲解。建议你每次只拿一个文件或一个调用链来问，例如“从 `KVServer.Write` 开始逐行讲 standalone 写路径”，这样学习效率最高。

## 6. 项目技术定位

- 倾向：后端 / 基础设施，带一点分布式系统与边缘计算场景。
- 依据：项目主体用 Go 实现网络入口、KV 服务层、WAL、LSM 存储、Raft 写复制、benchmark 和指标，核心能力集中在数据写入、持久化、恢复、查询与高频摄入控制。

## 7. 核心原理解析

### 7.1 持久化写入

- 问题：如果服务在写入返回成功后崩溃，用户已经收到确认的数据不能丢。
- 机制：standalone 模式先把写命令追加到 WAL 并 fsync，再写入 MemTable；重启时重放 WAL 恢复未刷盘数据。
- 在本项目中的落点：`service/fsm.go` 的 `writeStandalone` 和 `applyStandalone`，`storage/wal.go` 的 `Append`、`Replay`。

### 7.2 group commit

- 问题：每条写都 fsync 会把吞吐限制在单机磁盘同步次数上。
- 机制：多个并发写进入同一个 WAL flushLoop，由唯一写者批量写入后只 fsync 一次，再唤醒整批请求。
- 在本项目中的落点：`storage/wal.go` 的 `flushLoop`、`drainInto`、`commit`；文档中 PUT 从 225 qps 提升到 6k/22k qps 的对比。

### 7.3 active/dirty 双表刷盘

- 问题：MemTable 达到阈值后需要刷盘，但不能长时间阻塞所有新写入。
- 机制：把 active 表切换为 dirty 快照，创建新的 active 接收后续写，刷盘在锁外完成；刷盘失败时保留 dirty 并重试。
- 在本项目中的落点：`storage/zstorage/memtable.go` 的 `Flush`、`StartFlush`、`FlushWorker`。

### 7.4 删除语义与墓碑

- 问题：如果删除只在内存表里物理删除，旧 SSTable 中同名 key 仍可能被读出来，造成“删除后复活”。
- 机制：删除写入一个 value 为 nil 的墓碑，读路径命中墓碑就返回不存在，刷盘和归并也保留这个删除语义。
- 在本项目中的落点：`storage/zstorage/memtable.go` 的 `Delete`，`storage/zstorage/SSTable.go` 的 `tombstoneValLen`。

### 7.5 SSTable 读优化

- 问题：每次 GET 全量扫描所有 SSTable 文件会随文件数量增长而退化。
- 机制：先用 MinKey/MaxKey 做范围过滤，再用分区 Bloom 快速否决，再用块索引二分定位目标块，只扫描小块。
- 在本项目中的落点：`storage/zstorage/SSTable.go` 的 `ReadFromSSTable`、`getBloom`、`getBlockIndex`、`searchBlock`。

### 7.6 边缘查询与谓词下推

- 问题：弱网边缘设备不适合把整段原始流都传回中心侧再筛选。
- 机制：服务端先按 key 范围扫描，再对 JSON value 执行单字段谓词，只把命中条目编码返回，并限制单次结果数量。
- 在本项目中的落点：`pkg/predicate/predicate.go`、`pkg/proto/scan.go`、`service/fsm.go` 的 `Scan`。

## 8. 关键设计决策

| 决策点 | 备选 | 取舍 | 风险 | 验证 |
|---|---|---|---|---|
| standalone 写先 WAL 再 MemTable | 只写内存、先内存后 WAL、先 WAL 后内存 | 选择先 WAL 后内存，保证返回成功后的崩溃恢复 | WAL append 成功但 MemTable 尚未写入时与 checkpoint 有竞态 | `service/fsm.go` 用 `cpMu` 包住两步；`storage/wal_test.go` 与恢复测试验证 |
| WAL group commit | 每条写 fsync、定时刷盘、批量 fsync | 选择并发请求共享一次 fsync，兼顾吞吐和确认语义 | 批次过大导致等待时间增加，flushLoop 关闭时可能卡请求 | `walMaxBatch` 限制批次；`Close` 排空已投递请求；benchmark 对比 |
| WAL checkpoint 重写 | 不清理 WAL、就地 truncate、临时文件原子替换 | 选择临时文件 fsync + rename + fsync 父目录 | rename 持久化或旧 tmp 残留处理不当会影响恢复 | `TestWALRewriteAtomicReplace` 覆盖旧 tmp、墓碑、空值、继续追加 |
| MemTable 刷盘模型 | 单表锁内刷盘、active/dirty 双表、完全异步队列 | 选择 active/dirty，写入与刷盘尽量解耦 | dirty 刷盘失败若被覆盖会丢数据 | 失败保留 dirty 并重试，刷盘成功后释放信用 |
| 高频摄入背压 | 不背压、按条数背压、按字节背压 | 选择字节级信用池，更贴近内存风险 | 小值高频下内存增长可能来自 SSTable 元数据而不是未刷盘数据 | M2 文档明确验证背压封顶能力和未覆盖问题 |
| Bloom 设计 | 全局 Bloom、每文件 Bloom、按命名空间分区 Bloom | 选择分区 Bloom，利用 key 前缀隔离不同数据仓库/设备域 | 分区过多会带来额外元数据常驻内存 | `partitioned_bloom_test.go` 与 M2 对元数据增长的边界说明 |
| SCAN 覆盖范围 | 只扫 MemTable、扫全部 SSTable、建二级索引 | 当前只扫热数据，先验证边缘切片查询路径 | 已刷盘历史数据查不到，不能过度承诺 | `storage/engine.go` 注释和 README 路线图明确标注 |
| Raft 模式耐久性 | standalone WAL、Raft WAL、双写两套 WAL | raft 模式写经 Raft 日志，standalone 模式才使用存储层 WAL | 两种模式的恢复路径不同，面试中容易讲混 | `service/fsm.go` 的 `NewKVServer` 分支清晰区分 |

## 9. 量化与验证（含待测，建议）

- 已有性能数据：README 和 `docs/iteration-2026-07-19-standalone-write-durability.md` 记录了 GET 16,806 qps、PUT 每写 fsync 225 qps、WAL group commit 后 50 并发 6,087 qps、200 并发 22,231 qps。面试中可以引用，但要说明环境是本机 macOS/APFS、256B value、gRPC 入口。
- 已有高频摄入数据：`docs-step/M1-ingest-benchmark-result.md` 记录了进程内引擎峰值约 2.0M w/s，但也证明未加背压时内存不稳定；`docs-step/M2-backpressure-result.md` 证明字节预算可把未刷盘 inflight 精确限制在 16MiB。
- 建议补测一：在 Linux 环境重跑 `go test ./...`、gRPC benchmark 和 ingest benchmark，记录 CPU、heap、P50/P95/P99、WAL 大小、SSTable 文件数。
- 建议补测二：为 SCAN 增加 SSTable 历史数据覆盖后，分别测热数据扫描、冷数据扫描、带谓词和不带谓词的返回条数、延迟、网络字节数。
- 建议补测三：为 Raft 多节点写复制补充 3 节点场景下 leader 切换、少数派故障、日志追赶、快照恢复的集成测试，指标包括提交延迟、恢复耗时和数据一致性。
- 建议补测四：针对 Bloom 和块索引做 A/B：关闭 Bloom、开启普通 Bloom、开启分区 Bloom，比较负查询延迟、磁盘读取次数和常驻内存。
- 待测项：生产级稳定性、长时间 compaction 收敛、跨平台 fsync 表现、真实传感器 payload 下的 tail latency 都还需要更长压测验证。
