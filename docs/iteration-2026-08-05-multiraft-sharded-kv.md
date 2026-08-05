# 迭代复盘：Multi-Raft 分片 KV——把打通的 Raft 接进真 KV（收官集成）

日期：2026-08-05　范围：`service/shardkv`

## 背景

Multi-Raft 五步前置全部打通后，最后一步是把它接进 KV：**每分片一个 Raft 组、写按 key 路由到分片组 leader、每分片一个 FSM 应用到该分片 store**。这是分布式网关+服务治理这条线的收官集成。

## 做了什么（`service/shardkv`）

- **Shard**：一个 Raft 组 + 该分片的 `KVStore` + 一个排空 `ApplyCh` 的 apply 循环。
- **Node**：托管所有分片组，共享一个 RPC 端点（`RaftGroupServer` 按 `GroupID=shardID` 分发）。
- **Put/Delete**：`ShardOf(key)` 选分片组，`ProposeToGroup` leader-aware 提交（换届自动重定向）；各节点 apply 循环把已提交命令复制应用到本地分片 store。
- **LocalGet**：读本地分片副本。

## 三个诚实决策（评审前置）

1. **拓扑是副本，不是数据分区**：v1 所有节点托管所有分片 → 「多组副本」。与 network 层 #191 的一致性哈希**分区**转发（key→属主节点、无副本）是两套不同一致性模型；ShardKV 自成一层、**不劫持 `kv.Write`**。真正按分片分区放置（shard→节点子集）留后续。
2. **内存 FSM，刻意推迟存储隔离**：每分片 FSM 用内存 store，避开「每分片存储隔离 + 全局配置」难点。FSM 边界是 `KVStore` 接口，真 LSM 引擎后续在此插入。故 v1 是「Multi-Raft 组 + 内存每分片 FSM」，不是「接了 LSM 的分片存储」。
3. **本地读最终一致**：apply 异步，本地副本读可能落后最新提交；线性一致的 leader 读/read-index 留后续。

## 从第一行就防住的死锁（Trap 1）

Raft 的 `ApplyCh <- entry` 是**持锁的阻塞发送**（缓冲 100）。若 FSM 不排空，累计 100 条后发送会卡在锁内、拖死整个组。此前集群测试只提交一条、从不排空 ApplyCh，故没暴露。ShardKV 每分片**构造即起 apply 循环**排空 ApplyCh、`Stop` 时退出——从设计第一步就建进去，而非事后调试。

## 验证

`TestShardKV_MultiRaftShardedReplicated`：3 节点、6 分片；经 node 0 写入跨 ≥2 分片的 key。

- 各 key 经**其分片组的 leader** 提交、复制到全部节点、读回一致。
- 实测 6 个分片的 leader 分布在**全部 3 个节点**（3 个不同 leader）——证明是**相互独立选主/提交的 Raft 组**，而非一个组。
- 连跑 **15 次 0 失败**；`-race` 干净；全仓 17 包全绿；零依赖不变。

这次**没有再挖出新 bug**——因为前面几轮已经把 Raft 地基（off-by-one、心跳复制、心跳抖动、选举阻塞、生命周期）逐个测出来修好了。集成能一次跑通，正是那几轮"先测→挖 bug→修→验证"的复利。

## 后续（真 Multi-Raft 分片集群的剩余集成）

- 把真 LSM 引擎插到 `KVStore` 边界（需存储路径按分片参数化，解掉全局配置耦合）。
- 真正按分片分区放置（shard→节点子集 + 放置控制面），把 network 层 #191 的分区路由与 ShardKV 的复制统一到一个入口。
- 线性一致读（leader read-index）。
- 传输连接复用（现 `SendAppendEntries` 每次 `rpc.Dial`；多分片下每秒数百次拨号，是已知待优化点）。
