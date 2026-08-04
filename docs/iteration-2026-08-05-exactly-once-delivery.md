# 迭代复盘：下游投递从 at-least-once 到 effectively-once

日期：2026-08-05　范围：`service/delivery`

承接分布式投递骨架（见 [`distributed-delivery-cluster-skeleton.md`](distributed-delivery-cluster-skeleton.md)），把「投递正确性」这条硬问题端到端做深：搭压测台、注入故障、量化重复/丢失，再修复验证。

## 目标

骨架期投递语义是 at-least-once：崩溃/重启后可能重投。目标是把它收敛为 **effectively-once**（下游零重复、零丢失），并用可复现的数字证明，而不是口头承诺。

## 方法：正确性压测台

`service/delivery/exactly_once_test.go` 手动驱动投递循环（复刻 deliverer 的 `Fetch → Send → Commit`），在崩溃点注入故障，然后解析 sink 的 JSONL 输出，统计：

- `delivered`：总行数（含重复）
- `unique`：去重后 key 数
- `dups = delivered - unique`：重复条数
- `losses`：灌入但 sink 缺失的 key 数

对照两个 sink：`FileSink`（纯追加）与 `IdempotentFileSink`（按 key 高水位去重）。500 条记录、批大小 50。

## 发现：崩溃下 plain sink 重复

在「第 3 批 Send 成功后、offset Commit 前」注入一次崩溃（重启后游标未推进、重投该批）：

| sink | ingested | delivered | unique | **dups** | losses |
| --- | --- | --- | --- | --- | --- |
| `FileSink`（改前） | 500 | 550 | 500 | **50** | 0 |

重复条数恰好等于一个批的大小（50）——被重投的那一批整批重复。丢失为 0，因为 offset 走 `kv.Write` 强一致持久，游标本身不丢。

## 归因

投递是「`Send` 成功 → `Commit` 游标」两步，二者**非原子**。任何落在中间的崩溃都会在重启后重投上一批：`Send` 已把数据写进下游，`Commit` 却没记下进度，重启只能从上一个已提交游标重来。这是 at-least-once 的固有重复窗口，靠 offset 本身无法消除——offset 只能保证不丢，不能保证不重。

## 修复：IdempotentFileSink——文件即高水位线

`service/delivery/idempotent_sink.go`。核心：投递按 key 升序进行，文件即按升序追加，**文件最后一条记录的 key 就是高水位线（HWM）**。

- `Send` 先滤掉 `key <= HWM` 的记录（它们已在文件里），把剩余追加并 **单次 fsync**，再把 HWM 推进到本批最大 key。
- 重开时从文件末尾恢复 HWM。

关键在于**记录写入与 HWM 前进共享同一次 fsync**：HWM 就是文件里的数据本身，没有独立的 HWM 存储，也就没有「数据已落、HWM 未落」的原子性缺口。崩溃只有两种结局——fsync 前（整批不持久，干净重投）或 fsync 后（整批 + HWM 同时持久，重投被 HWM 跳过）。配合强一致 offset 的零丢失，二者合起来达成 effectively-once。

## 验证：同样故障下 0 重复 0 丢失

| 场景 | sink | delivered | dups | losses |
| --- | --- | --- | --- | --- |
| 崩溃（Send 后 Commit 前） | `FileSink` | 550 | **50** | 0 |
| 崩溃（Send 后 Commit 前） | `IdempotentFileSink` | 500 | **0** | 0 |
| offset 完全丢失、从头全量重投 | `IdempotentFileSink` | 500 | **0** | 0 |

第三行是更强的一击：即便 offset 存储彻底失效（`Load` 返回 nil，从头重投全部 500 条），幂等 sink 也把重投整轮吸收为 0 重复——正确性不依赖 offset，只依赖 sink 自身的 HWM。

复现：

```powershell
go test ./service/delivery/ -run ExactlyOnce -v
```

## 语义边界（诚实标注）

- **前提**：投递有序（按 key 升序）、按 key 幂等、且 **key→value 稳定**（append-only 事件模型；同 key 覆盖写不适用此去重）。这与路线图 #1「append-only event 模型」一致。
- **原子性**以单次 `fsync` 为界：把 fsync 视作整批的持久化边界。跨多块的 fsync 在极端介质故障下并非绝对原子，这是所有 WAL 类系统共享的假设，非本层特有。
- **推广**：对 `FileSink` 用「文件自身 max key 作 HWM」实现幂等；对真实下游（ClickHouse ReplacingMergeTree、Doris/DB 主键 upsert）则用其**按 key upsert** 天然幂等——`IdempotentFileSink` 是这一模式在文件 sink 上的具体化。
- **raft 模式**：投递仅在 Leader 进行（见骨架文档），`FileSink` 是本地 sink，故输出落在当时 Leader 磁盘；真实共享 sink 下才是全局一次性。

## 接线

投递启用时默认走幂等 sink（`config.DeliveryExactlyOnce=true`），无需额外操作即得 effectively-once；置 false 可回退 at-least-once 做对照。

## 后续

- 幂等 sink 的 HWM 恢复当前 O(file) 扫描，可优化为从文件尾反向读一行。
- 接真实 ClickHouse/Doris sink 后，用其主键 upsert 复现同一套正确性压测台。
