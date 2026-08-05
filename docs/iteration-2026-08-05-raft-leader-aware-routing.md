# 迭代复盘：leader-aware 路由——收官一步，又逼出两个 leader 稳定性 bug

日期：2026-08-05　范围：`Raft/`

## 背景

Multi-Raft 前置齐了（体检 #196、`Stop()` #199、groupID 传输 #202、多节点复制 #204）。最后一步：**leader-aware 路由**——写请求到任意节点，定位分片组的**当前 leader**，非 leader 则重定向。

## 做了什么（leader-aware 路由）

- **leader 追踪**：Raft 从 AppendEntries 的 `LeaderID` 学到当前 leader，记 `currentLeader`；`LeaderHint()` 返回 leader 地址供重定向。
- **客户端提交 RPC**：`RaftGroupServer.Propose`——本节点是该组 leader 则 `AppendEntry` 并等待提交，否则回 `NotLeader + LeaderHint`。
- **重定向客户端**：`ProposeToGroup(addrs, groupID, cmd, perCall, total)`——依次尝试各节点、按 `LeaderHint` 立刻重定向到 leader，按截止时间反复重试以容忍选主/换届。

## 又逼出的真 bug（一测就现）

做集群测试时，leader-aware 重定向本身可靠（0/15 失败），但暴露了两个 **leader 稳定性** bug：

1. **心跳从不重置选举计时器**：`electionLoop` 在 `<-r.heartbeatCh` 上等待重置，但**全代码从没有人向 `heartbeatCh` 发送**。于是 follower 无视心跳、每 150–300ms 照常发起选举 → **leader 持续抖动**，`currentLeader` 永远指向过期/自己。修复：AppendEntries 处理器收到合法 leader 心跳时非阻塞 `heartbeatCh <- true`，并把 Candidate 退回 Follower。

2. **收票阻塞选举循环**：`startElection` 同步等票最多 500ms，期间 `electionLoop` 无法处理现任 leader 的心跳（本会让候选人退回 Follower）→ 无谓改选。修复：抽 `awaitVotes` 异步收票，`startElection` 立即返回；只在「仍是本轮任期的 Candidate」时当选/退选。

## 顺带修掉的 flaky（非 Raft bug）

- **groupID 竞争**（`-race`）：`startElection` 把同一 `RequestVoteArgs` 指针分给多 goroutine 并发写 `GroupID` → 改在 args 构造点设。
- **临时端口 TIME_WAIT**：集群测试固定端口跨运行冲突 → 改用 `:0` 临时端口预绑。
- **清理顺序**：`t.TempDir` 删除与 Raft WAL 写竞争致 `directory not empty` → 所有节点 WAL 放同一 base 临时目录（最后清理），单独收尾先关监听器停入站 RPC、再 `Stop`、再 drain。

## 验证

- `TestRaftCluster_LeaderAwarePropose`：只把请求发给一个 **follower**，经 `LeaderHint` 重定向命中 leader、提交并复制到全部节点。
- `TestRaftCluster_ElectReplicateCommit`：经 `ProposeToGroup` 提交并全节点收敛。
- 连跑 **40 次 0 失败**；`go test ./Raft/` 在 `-count=1`、`-count=2`、`-race` 下全绿；全仓 16 包全绿。

## 意义

Multi-Raft 的**五步前置全部打通**：体检修 off-by-one → `Stop()` 生命周期 → groupID 传输 → 多节点复制 → **leader-aware 路由**。每一步都是「先测、一测就挖出真 bug、修完再验证」。至此 BanDB 有了一个**被验证过能选主、稳定、复制、按 leader 路由**的 Raft 组，可作为分片写入路由到分片组 leader 的底座——把它接进分片 KV 路径（每分片 FSM）是自然的后续集成。
