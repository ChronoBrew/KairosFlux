# 迭代复盘：compaction 重启塌缩——从"以为是文件数无界"到"实测写放大 + 重启全量重写"

日期：2026-08-05　范围：`storage/zstorage`

目标是把存储核心的 compaction「做到极致」。方法照搬本仓一贯的 **测量优先**：先搭压测台拿真实曲线，再让数据决定修什么——而不是照着"文件数无界"的直觉去修一个可能不存在的问题。

## 方法：compaction 压测台

`storage/zstorage/compaction_bench_test.go` 确定性地驱动真实的 flush + compaction 级联（小 memtable、`MaxCompactionSize=4`），量化：per-level 文件分布、写放大（新增 `compaction_stats.go` 的刷盘/合并字节计数）、单轮 compaction stall、常驻 heap，并模拟一次重启。

```powershell
go test ./storage/zstorage/ -run CompactionBench -v
```

## 发现：直觉错了，真实代价在别处

300 次 flush × 200 条 = 6 万条、6.4 MiB 灌入，稳态结果：

| 指标 | 实测 | 解读 |
| --- | --- | --- |
| 文件总数 | **6**（L1=3 L2=2 L4=1） | 文件数**本就有界**（~对数级），不是问题 |
| 常驻 HeapAlloc | **0.4 MiB** | 元数据内存在此规模也不是问题 |
| **写放大** | **5.07x**（compaction/flush=3.66x） | 真实代价：merge-all-at-once 把数据反复重写 |
| 单轮 compaction stall | 236 ms | 一次合并整层，阻塞写 |

诚实结论：**"compaction 有界"按字面理解其实已经成立**——文件数和内存都有界。真正的成本是**写放大**和一个更尖锐的缺口——**重启塌缩**：

| 重启（LoadSSTableMetaList）| 改前 | 
| --- | --- |
| per-level 分布 | **L0=6（全部塌缩到 L0）** |
| 重启后首轮 compaction | **6.9 MiB / 193 ms（一次性重写全部数据）** |

BanDB 把崩溃恢复当作核心卖点，而**每次重启都要付一次全量数据重写**——因为 level 根本没被持久化。

## 归因

`storage/zstorage/SSTable.go` 的 `LoadSSTableMetaList` 对每个文件硬编码 `Level: 0`——level 只活在内存里，重启即丢。于是重启后所有文件塌缩到 L0，下一次 `compactCh` 触发 `CompactSSTable(0)`，把 L0 的全部文件一次合并（`len(files) >= MaxCompactionSize`），把整个数据集重写一遍。

（写放大 5x 的根因是 `CompactSSTable` 每次把一整层合并进下一层、逐级重写；这是更大的结构性成本，见"边界与后续"。）

## 修复：把 level 编进文件名，重启从文件名恢复

外科手术式、只动元数据、不改数据格式：

- flush 落盘文件名 `sstable_L0_<ts>.sst`（`WriteToSSTable`）。
- 合并输出文件名 `sstable_merged_L<targetLevel>_<ts>.sst`（`MergeSSTable`）。
- `LoadSSTableMetaList` 用 `parseLevelFromName` 从文件名解析 level；老格式文件名不含 `_L<n>_` 段，默认 L0，**向后兼容**。

level 编进文件名（而非仅内存 / 或改 footer 格式）是刻意选择：改文件名零格式风险、老文件自动降级到 L0、无需读文件即可恢复 level。

## 验证：重启不再塌缩、不再全量重写

| 重启 | 改前 | 改后 |
| --- | --- | --- |
| per-level 分布 | L0=6（塌缩） | **L1=3 L2=2 L4=1（保留）** |
| 重启后首轮 compaction | 6.9 MiB / 193 ms | **0 MiB / 1 µs** |

- `TestCompaction_LevelPersistedAcrossRestart`：回归守卫，断言重启后 per-level 分布与重启前一致，而非塌缩。
- `TestCompactionBench` 内含正确性守卫：compaction 后抽样 key 必须可读且值正确（无丢失、无错值）。
- `go test ./storage/...` 全绿。

## 边界与后续（诚实标注）

- **本次只修了重启塌缩**，这是一个正确性/恢复代价缺口，收益明确（每次重启省一次全量重写）。
- **写放大 5.07x 仍在**，根因是 `CompactSSTable` 逐层「合并整层」的 size-tiered 式重写。要压下它需要 overlap-aware / 真正 leveled compaction（只合并 key 范围重叠的文件、分层放大比），那是结构性改动、correctness-critical，列为独立 stretch，不塞进本次外科手术。
- **文件数 / 常驻 heap 在测量规模下本就有界**，所以没有去"修"一个不存在的问题——这正是测量优先的价值。
