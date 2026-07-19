# 迭代报告：standalone 写路径性能与持久化排障

> 日期: 2026-07-19 · 范围: `storage` `storage/zstorage` `service` · 分支起点: `fix/grpc-standalone-write`
> 关联 PR: #150（group commit）· #153（SSTable 重载修复）· #157（WAL 自清洁）· #159（原子重写）· #158（README）

本次迭代从一个反常现象出发——「PUT 只有 225 qps」——一路层层归因、验证、修复，中途**挖出一个先前就存在的、会导致 standalone 重启后已刷盘数据全丢的致命 bug**，并在给自己的新特性收尾时又**发现并堵上了自己引入的崩溃丢数据窗口**。全程实测驱动、原子提交、CI 门禁、自动合并闭环。

这篇不是功能清单，是一条**排障链**的复盘——重点在「不被表象骗、每一步都实测验证、发现问题就承认并往下挖」的工程判断。

---

## 一、起点：225 qps 的反常

用 gRPC 入口对 standalone 模式压测，PUT 只有 **225 qps、P50 203ms**（50 并发）。用户记忆里这个项目"以前有 8000"。两个数字对不上，先不猜，先量。

对照测 GET（纯内存读路径）：**16,806 qps、P50 2.45ms**。写比读慢 **75×**。

再测本机单次 `fsync`：**3.93ms/次 ≈ 254 次/秒**。

结论一步到位：**225 qps ≈ 本机 fsync 上限 254/s**，写吞吐 100% 卡在「每写一条就 `fsync` 一次」。GET 不落盘所以快 75 倍，差距全部来自 fsync。

顺手验证"以前的 8000"：切到 `main` 分支实测，standalone PUT 直接 **panic（nil raft），成功率 0%**，benchmark 显示的 84 万"qps"全是 error 响应，根本没写进去。所以"8000"是更早的**纯 memtable、无 fsync** 路径的记忆——不是同一回事。**表象被证伪，别拿它当基线。**

---

## 二、第一修：WAL group commit（#150）

瓶颈既然是 fsync，标准解法是 **group commit**：不是每条写各自 fsync，而是让一个后台 flusher 把当前并发排队的写攒成一批，整批只 `fsync` 一次，再唤醒全部等待者。

- 所有 `Append` 投递到 `reqCh`，唯一的 `flushLoop` 排空当前队列凑批 → 写完只 `fsync` 一次。
- `flushLoop` 成为文件唯一写者，**顺带消除了原先无锁并发 `Write`+`Sync` 的隐患**。
- 持久化契约不变：`Append` 返回即代表该记录已随批落盘。

实测（256B value）：

| 并发 | 改前 | 改后 | 倍数 |
|---|---|---|---|
| 50 | 225 qps / P50 203ms | **6,087 qps** / P50 8ms | 27× |
| 200 | ~225 qps | **22,231 qps** / P50 8ms | ~99× |

并发越高，一次 fsync 摊销的写越多——这正是 group commit 的价值：**写吞吐随并发线性摊销，而旧路径无论多少并发都卡在 254/s**。

---

## 三、意外：写恢复测试暴露"重启后已刷盘数据全丢"（#153）

给自清洁特性写"崩溃恢复"测试时，用小 memtable 阈值逼数据刷到 SSTable，然后重启读回——**50 个 key 丢了 45 个**，只剩还在 memtable/WAL 里的尾部数据。

先排除是不是自己新写的 checkpoint 引入的：写了个**不含 checkpoint** 的纯 SSTable 恢复隔离测试——**照样丢 45/50**。**这是先前就存在的 bug，不是我引入的。**（原有的 `TestStandalone_WriteAndRecover` 特意用超大 memtable 避开刷盘，正好一直掩盖着它。）

继续定位。运行时读 `k0001` 正常，重启后读不到。打印重载后每个 SSTable 文件的 `MinKey`/`MaxKey`：**MaxKey 全是空串 `""`**。

根因锁定：`LoadSSTableMetaList` 靠 `EnsureMeta` 懒加载 MaxKey，而 `EnsureMeta` 按 `[klen][key][vlen][val]` 顺序**扫到文件尾**——但 SSTable 数据段之后还接了块索引/布隆/footer，扫过边界后错位解析，最终 MaxKey 退化成空串。于是 `getFromSSTables` 的 `[MinKey,MaxKey]` 范围过滤对任意真实 key 都判 `key > ""` 为真而**整段跳过该文件** → 已刷盘数据全部读不到。

修复：`LoadSSTableMetaList` 直接用**块索引末项的 `LastKey`** 作 MaxKey（那就是文件的最大 key），置 `MaxKeyLoaded=true`，绕开会越界的 `EnsureMeta`。老格式无 footer 时保留懒加载兜底。

验证：新增 `TestReloadRecoversFlushedKeys`，小阈值逼多轮刷盘 → 停机 → 同目录重载 → **50/50 恢复**。

> 这是本次最有价值的发现：它让 standalone 的 LSM 持久化**第一次真正能用**——在此之前，任何刷盘到 SSTable 的数据一重启就消失。

---

## 四、收尾特性：WAL 自清洁 checkpoint（#157）

WAL 此前只增不减：数据已落 SSTable 后其 WAL 记录仍永久保留，反复覆盖/删除让 WAL 无限膨胀（压测后实测 11MB+）。

新增 checkpoint：每累计 `2×MaxMemTableSize` 次写，把 WAL 整体重写为**当前未刷盘热数据（active+dirty，含墓碑）**的快照，回收已落 SSTable 的历史记录，令 WAL 稳态大小有界。

正确性关键——写路径 `wal.Append` + `storage.Put` **两步非原子**，二者之间存在"已落 WAL 未进 memtable"的窗口。故引入 `cpMu`：

- 每次写持 **RLock** 罩住两步（写之间仍并发，不伤 group commit）；
- checkpoint 持 **Lock** 独占，待所有写静默后 active+dirty 快照才必然一致。

快照保留墓碑、区分 `nil`/空切片，避免被删 key 从 SSTable 复活。

---

## 五、自我审查：堵上自己引入的崩溃丢数据窗口（#159）

自清洁上线后复审 `doRewrite`：它做的是 `Truncate(0)` → 重写 → `Sync`，**三步非原子**。若在 `Truncate(0)` 之后、重写 `fsync` 之前崩溃，WAL 被清空/残缺——而 checkpoint 快照的 active+dirty **此刻尚无 SSTable 副本**（正因如此才在快照里），会静默丢失已 ack 的写。

**这直接违背了我自己刚写进 commit / README 的持久化契约**：为了让 WAL 有界，反而在崩溃路径重新引入了丢数据。

修复用标准原子替换：records 写满并 `fsync` 到 `wal.log.tmp` → `os.Rename` 覆盖（POSIX 原子）→ `fsync` 父目录让 rename 持久。恢复时只会看到完整的旧 WAL 或完整的新 WAL，**绝不残缺**。

验证：`TestWALRewriteAtomicReplace`——陈旧 `.tmp` 残留不干扰恢复、重写后恰只剩新记录（含墓碑/空值边界）、可继续 Append、重开仍完整。

---

## 六、复盘：这条链的教训

1. **数字对不上时先量，别猜。** 225 vs 8000 的真相是：一个是真持久化写、一个是 panic 空转，根本不可比。
2. **实测归因优于经验直觉。** 225 ≈ 254(fsync 上限) 这个吻合，一步锁定瓶颈。
3. **自己的新特性会掩盖旧 bug，也会引入新 bug。** SSTable 重载 bug 被"超大 memtable 测试"掩盖多时；自清洁的原子性缺陷是自己引入的。都靠"写一个会真的重启/崩溃的测试"才暴露。
4. **收益要和契约对齐。** 自清洁让 WAL 有界（收益），但若牺牲崩溃安全（违约）就是负优化——收尾必须回到契约上验收。

最终整条链满足契约：**`Append` 返回即已 fsync 落盘；崩溃不丢已 ack 写；已刷盘数据重启可恢复；WAL 大小有界。**

---

## 附：性能层次对比（实测，256B value，本机 macOS/APFS）

| 路径 | 并发 | QPS | P50 | 瓶颈 |
|---|---|---|---|---|
| GET（gRPC→service→memtable，纯内存） | 50 | 16,806 | 2.45ms | CPU/网络 |
| PUT 改前（每写一次 fsync） | 50 | 225 | 203ms | fsync（254/s 封顶） |
| PUT group commit | 50 | 6,087 | 8ms | fsync 已摊销 |
| PUT group commit | 200 | 22,231 | 8ms | 随并发线性摊销 |

> 复现：`go run ./server_grpc` 起服务，另开终端 `go run ./grpc_benchmark -mode put -c 200 -d 8s`（或 `-mode get`）。
