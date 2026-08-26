# RFC: AI-native kind 分组分区（kind-partition）

- 状态：**设计就绪、实现待证据触发**（证据门未过 = 本 RFC 只归档不施工，见 §5）
- 日期：2026-08-26
- 关联：docs/bench/storage-verdict.md（M5-B 存储裁决）、任务 #52（句柄生命周期修复）
- 范围：存储层键空间组织。不写代码、不改协议字节、不引外部库。

---

## 1. 现状

### 1.1 键空间事实清单（kind → 前缀，代码实测，全部在本节列出的"唯一权威"处定义）

| kind（任务书语义） | 键前缀 | 唯一权威定义位置（代码） | 说明 |
|---|---|---|---|
| quote | `quote:`（`quote:{YYYY-MM-DD}:{code}`） | `service/ingesthook/schema/quote.go:10-13`（`quotePrefix`），分派见 `schema/registry.go` 最长前缀匹配 | 行情快照，TypeID=1；`docs/Kair-协议规范.md §7` 记载 key 布局 |
| proposal | `proposal:` | `internal/identity/identity.go:78`（`ProposalKeyPrefix`） | 协议层角色强制的唯一判据（`IsProposalKey`，identity.go:82-84）；aiplane.ProposalKey 转发此常量，不重复字面量（proposal.go:143-148） |
| experiment（记录） | `strategy:index:`（`{fp}` 尾段） | `internal/jobctl/registry_import.go:11-42`（`RegistryIndexKey`/`RegistryIndexPrefix`） | M3 registry 导入的 QuantBrew 实验/verdict 记录，走 PUT_VERSIONED |
| factor-similarity | `factor:similarity:` | `internal/aiplane/similarity.go:164`（`FindSuspectDuplicate` 的 `ListPrefix` 调用） | kind 枚举 `KindFactorSimilarity = "factor_similarity"`（aiplane/identity.go:47） |
| job（含 job-events） | `job:spec:` / `job:status:` / `job:events:` | `internal/jobctl/keys.go:19-23`（`SpecKey`/`StatusKey`/`EventsKey`） | 声明式 Job 控制面三段布局（M3 里程碑，总览 §5 表）；`job:events:{name}:v{seq}` 走同一套版本语义 |
| 证据边（evidence） | `evidence:factor:` / `evidence:experiment:` / `evidence:strategy:` / `evidence:paper:` | `internal/aiplane/evidence.go:19-41` | 证据图谱键布局，"对象间引用走逻辑键、BTree 前缀扫描实现" |
| strategy / paper / review 对象 | `strategy:obj:` / `paper:obj:` / `review:obj:` | `internal/aiplane/evidence.go:43-45` | 引擎裁决落成的正式对象 |
| 其它 kind 枚举 | proposal / factor_similarity / evidence_edge / strategy_object / paper_account / review | `internal/aiplane/identity.go:43-52`（`ObjectKind`） | `WriteAsAgent` 只放行 `KindProposal`（identity.go:80-89） |

**dataset：无账本键前缀**（全仓 `rg 'dataset:'` 零命中）。`DatasetContractInfo` 来自 `contracts/*.schema.json` 文件读取（`internal/aiplane/context.go:33-40`、`loadContracts`），不是时态内核里的键空间对象。→ 见 §7 张力 T1。

**唯一权威原则**：kind → 前缀映射的唯一权威 = `internal/identity` 协议层常量 + 各对象包自身的前缀常量（aiplane `ObjectKind` 枚举、jobctl keys.go、evidence.go 前缀族、similarity.go、schema 注册表）。本 RFC 的分组映射**从这些常量派生**，不另立第二份映射；组边界判定规则见 §6.3。

### 1.2 存储形态：全键空间单 LSM

- 一个 `storage.Engine` = active/dirty 两张内存表 + 全部 SSTable 混排（`storage/engine.go`，metas 按落盘先后升序）。
- 100w 档实测：**2963 张 SSTable**，SCAN/LIST_WRITES 因读路径句柄耗尽未能测——正是任务 #52 的现场（storage-verdict.md §数字回顾）。
- **既有表级剪枝（已实现）**：
  - `ScanRange`：逐文件按 `[MinKey,MaxKey]` 排除与扫描区间无交集的文件（engine.go:166-175）——`MaxKeyKnown && start > MaxKey` 或 `MinKey > end` 则不开迭代器。注释自述：游标推进场景决定性，实测 113 个文件时若不剪枝投递"几乎爬不动"。
  - 点读 `GET`：同型剪枝（engine.go:426-434）——**MinKey 取自文件头部恒可信；MaxKey 仅在取自块索引时可信，`MaxKeyKnown=false` 时跳过上界判断（"宁可多扫一个文件，也不能因为猜错上界而漏掉命中的 key"）**。
  - `ReclaimUpTo` 同理只在 `MaxKeyKnown` 时整文件回收（engine.go:521-554）。

### 1.3 AI 数据平面的查询模式 = kind-scoped

- **Context 查询**（`internal/aiplane/context.go:166` `BuildContext`）：账本读取全部走 `AsOfReader`/`PrefixLister`——`ListExperiments` 按 `strategy:index:` 前缀扫描（experiment.go:71-76）、`listStrategyStates` 按 `strategy:obj:` 前缀扫描（context.go:145-146）、proposal 按 fingerprint 定点 GET（proposal.go）。即 Context 只碰 experiment/proposal/strategy 三组。
- **审计**（LIST_WRITES）：按 source 聚合（`BySource`，identity.go:45-47），写源集中在 job 组（jobctl EngineSource="jobctl"）。
- **Quote 查询**：`quote:{date}:{code}` 前缀范围扫描。

结论：单次查询的命中集几乎全部落在**一个** kind 前缀内，但现状每次查询打开的"墙"= 全库表数。

---

## 2. 剪枝失效分析（为什么墙 = 全库表数）

表级 `[MinKey,MaxKey]` 剪枝是"扫描区间 × 文件区间"两级判交：只要两个区间**有交集**就为该文件开迭代器（engine.go:170-175）。kind-scoped 扫描区间是 `[prefix, prefix+0xFF)`——很窄；而文件区间是文件内全部键的 `[min,max]`，**跨全键空间**。两个失效源：

1. **MaxKeyKnown 覆盖率不足**：老格式/尾部残缺文件的 MaxKey 不可信 → 上界判断被跳过（engine.go:426-428），任何 kind 扫描都要打开这类文件；文件越多，打开的越多。
2. **表内键序跨 kind 交错**：组间键在字典序上交错排列（`evidence:…` < `factor:…` < `job:…` < `paper:…` < `proposal:…` < `quote:…` < `review:…` < `strategy:…`），单张表内 `[min,max]` 大概率横跨多个 kind → 与某 kind 扫描区间"有交集"却无一条命中 → 打开文件数 ≈ 全库表数（最坏）。

**确定性红线（迁移前后一致）**：`ScanRange` 无论打开多少文件，命中键值由多路归并按"新写覆盖旧写、最新墓碑跳过"产生（engine.go:141-152），文件打开数不影响结果字节——剪枝只是性能优化，不是正确性来源。

---

## 3. 第一档（低风险，先行）：剪枝有效性增强

不分组，直接降低 kind-scoped 查询的文件打开数：

- **补齐 MaxKeyKnown 覆盖率**：老格式文件读 footer/块索引补全 MaxKey，使上界判断对全部文件生效（新写入本身即带可信 MaxKey）。
- **剪枝判据升级为"前缀 × 文件"**：表级元数据增加每文件的前缀分布（有序前缀集合或前缀布隆，复用既有 `storage/bloom.go`/`partitioned_bloom.go` 模块），使 kind-scoped 扫描只打开"前缀命中"的文件——把"区间-文件"判交升级为"前缀-文件"判交。
- **不动**：压实策略、表内键序、协议字节、SSTable 文件格式红线。

验收指标：MaxKeyKnown 覆盖率 → 100%；kind-scoped 扫描打开文件数 → f(本组数据量) 而非 f(全库表数)。无证据门，但实施应排在第二档之前——它本身可能已消除大部分扇出（见 §7 张力 T3）。

---

## 4. 第二档（证据触发）：物理 kind 分组

### 4.1 设计

- 按 §1.1 的 kind 前缀分组，**每组独立 SSTable 集 + 独立压实 + 独立块索引**；组内仍是既有 LSM——不引外部库、不 TSDB 化、不动协议字节/向量红线。
- 组内写路径、版本语义、as-of 判定、指纹定义（总览 §4）全部不变。
- 分组粒度以任务书 kind 清单为锚（quote / proposal / experiment / job-events / factor-similarity），代码中未列入清单的 kind（`strategy:obj:`、`paper:obj:`、`review:obj:`、`evidence:*`）的分组归属见 §7 张力 T2。
- 分组的唯一映射来源 = §1.1 权威常量派生表，随新 kind 注册同步更新，不允许手写第二份。

### 4.2 查询路由

- **kind 已知的查询**（Context/Quote/审计等，§1.3）→ 路由到单组扫描：墙从"全库表数"变成"本组那面墙"。
- **无 kind 提示的全局查询**：语义不变，扫全组，结果与迁移前逐字节一致（确定性红线，§2）。
- **LIST_WRITES / REPLAY_FINGERPRINT 的组感知形态**：可选 `scope` 参数；缺省 = 全组，行为不变（向后兼容，同 M5 分页"可选字段只追加在既有请求/响应上"的先例）。

### 4.3 触发证据门（数字没到，本 RFC 只归档不施工）

与 storage-verdict.md 裁决一致，三条件任一：

1. **#52 句柄修复后**，100w 档 kind-scoped 查询补测（填补 01.md 的 SCAN/LIST_WRITES 空缺格）数字超阈值——以剪枝失效实测指标为准：kind-scoped 查询打开文件数 / 全库表数占比、MaxKeyKnown 覆盖率、p50/p99，**不是泛泛的延迟指标**；
2. 生产服务器 Context 查询 p99 超标的实测证据；
3. 数据量跨入 500w+（storage-verdict.md 裁决 1 的重审条件）。

**前置依赖**：#52 未修复前不实施任何分区改动——句柄泄漏 bug 不解决，打开文件数指标本身测不准（100w 档扫描直接失败）。

---

## 5. 迁移路径（两段式）

1. **版本标记 + 懒迁移**：新写入直接落组（新路径）；旧数据**读到即迁**（命中旧 SSTable 的键在读路径上按组搬移）。先例：M2 操作元数据信封——`envelopeMarkerBit`（`1<<63`）标记新格式，`DecodeVersionRecord` 是唯一读入口，按 marker 位分发新旧两格式，旧格式记录无 source 时按默认语义读取，无需重写即兼容（`internal/temporal/temporal.go:201-274`）。同一模式：旧数据不预扫描、不批量重写，读到才迁。
2. **显式离线 re-group 工具**：迁移完成后清理旧表；工具以 **REPLAY_FINGERPRINT 校验迁移前后逐字节一致**（Kair v2 opcode 0x0D，响应含 keyCount/mismatchCount/fingerprint/mismatchKeys，见 `proto/temporal_frame.go:150-156`；M2 起带 Bounded 字段的 V2 编码）。
3. **旧键组边界判定规则**：单前缀**最左匹配**、无歧义——以"冒号结尾的完整前缀 + 字典序区间"为组边界（与 `schema/registry.go` 最长前缀分派同构，但边界判定只依赖 §1.1 权威前缀表，该表前缀互不为对方前缀，天然无歧义）。

---

## 6. 风险与不做

- **明确不做**：分布式/远程存储、时序语义（双时态 M4+ 仍是 roadmap，不因此 RFC 提前）、TSDB 化、任何外部库、协议字节变更、SSTable 文件格式红线（MaxKeyKnown 补全是第一档的例外，属文件内元数据，非协议字节）。
- **压实放大**：组内独立压实后，组间压实策略不复存在，单组数据倾斜时压实写放大需按组观测——与 storage-verdict.md 裁决 1 的"压实写放大导致写入退化曲线"同一条重审信号。
- **组间大小不均**：quote 组远大于其他组 → **预留设计：组内再按时间窗分桶**（同样待证据触发，不在本 RFC 定死）。
- **迁移期双份元数据状态**：新旧路径并行期间以确定性双跑验收兜底（§7），不允许出现"同一键两处可见"的窗口——懒迁移以组为单位原子搬移，搬移前读旧表、搬移后读新表，读路径单入口（同 `DecodeVersionRecord` 先例）。

---

## 7. 验证计划（实现当日）

1. **确定性**：迁移前后 `REPLAY_FINGERPRINT` 逐字节一致（含 mismatchKeys 为空、keyCount 相等）。
2. **E2E 五件套全绿**：PUT_VERSIONED → GET_AS_OF（含 as-of 定点语义）→ LIST_VERSIONS → REPLAY_FINGERPRINT（无界对账 + 确定性）→ LIST_WRITES（审计 + 按来源计数），见 `kairosflux_test.go:26-28`（`TestEngineEmbeddedFullFlow`）；另 `TestEngineProposalAndContext` 覆盖 Context 访问口。
3. **性能对比表**：kind-scoped 查询迁移前后 p50/p99 + 打开文件数 + MaxKeyKnown 覆盖率，按 §4.3 证据门指标口径。

---

## 8. 开放问题（只提出，不裁决）

- **T1**：任务书 kind 清单含 dataset，但代码事实是 dataset 无账本键前缀（contracts/*.schema.json 文件）。分组映射是否覆盖 dataset？若覆盖，锚点是什么？
- **T2**：代码前缀数（12+）多于任务书 kind 清单（6 组）——`strategy:obj:`/`paper:obj:`/`review:obj:`/`evidence:*` 的分组归属待定；且"experiment 组"在代码里实际是 `strategy:index:` 前缀（jobctl 持有），与任务书命名不一致。
- **T3**：第一档的"前缀 × 文件"判交若实施到位，可能已消除大部分 kind 扇出——第二档的增量价值需要以第一档后的实测为基线重新论证，防止为分组而分组。
- **T4**：仓内无名为"M3 对象注册表"的实体（M3 里程碑 = 声明式 Job 控制面，总览 §5 表，未开工；schema 注册表按 KeyPrefix/TypeID 索引，aiplane ObjectKind 枚举在 identity.go:43-52）——本 RFC 的"唯一权威"按 §1.1 落地为 identity + 各对象包常量 + schema 注册表三处，若后续需要收敛成单张注册表，是独立工作。
- **T5**：#52 是任务编号（storage-verdict.md），仓内 GitHub 无对应 issue——证据门第一项的跟踪载体待定。
