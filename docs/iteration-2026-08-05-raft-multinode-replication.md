# 迭代复盘：3 节点集群一测，才发现 Raft 多节点复制从未跑通

日期：2026-08-05　范围：`Raft/`

## 背景

Multi-Raft 前置都通了（体检 #196、`Stop()` #199、groupID 传输 #202）。做 leader-aware 路由前，按纪律先证明**多节点 Raft 集群能不能跑通**——这从没被端到端测过。

## 发现：选主成功，但复制为 0

`TestRaftCluster_ElectReplicateCommit`：进程内起 3 节点、选出唯一 Leader、在 Leader 上 propose 一条命令、等它复制到全部节点。结果：

```
node 0 (leader) log len=1
node 1 log len=0
node 2 log len=0
command did not replicate to all nodes
```

选主 OK（说明 RPC 通、投票通），但 Leader 的日志**一条都没复制**到 follower。

## 归因：空心跳 + 虚假推进 matchIndex

两处叠加成致命 bug：

1. **心跳发空包**：心跳走的 `SendHeartBeat` 永远发 `Entries: []LogEntry{}`——从不携带待复制条目。
2. **成功却按 last index 推进**：无论发的是空包还是真条目，回复成功后都执行 `matchIndex[peer] = getLastLogIndex()`、`nextIndex[peer] = getLastLogIndex()+1`——**把 follower 误标为"已追平 Leader 最后一条"**。

于是：`AppendEntry` 触发一次 `replicateLog`（会发真条目），但 50ms 的空心跳抢先返回成功，把 `nextIndex[peer]` 推到 last+1；`replicateLog` 再算 `entries = log[relIndex(nextIndex):]` 就成了**空**——真条目永远发不出去，而 Leader 还以为 follower 都有了（甚至会据此推进 commitIndex）。这条无快照多节点复制路径此前只有单节点/RPC 单测覆盖，从未真正跑通。

## 修复

- **心跳与复制统一**：心跳循环改为调 `replicateLog()`（携带 `nextIndex` 起的待复制条目），删除只发空包的 `SendHeartBeat`。
- **matchIndex 只按实际发送推进**：回复成功时 `matched = args.PrevLogIndex + len(args.Entries)`，`matchIndex/nextIndex` 按 `matched` 推进（带 max 防回退），**绝不按 Leader 的 last index**。失败时 `nextIndex--` 且下限为 0。

## 顺带修复：groupID 传输的数据竞争

`-race` 暴露 `startElection` 把**同一个 `RequestVoteArgs` 指针**分给多个 peer goroutine，而 `Send*` 里 `args.GroupID = r.groupID` 让它们并发写同一字段（虽同值仍是 race）。改为在 args **构造点**设 `GroupID`（单 goroutine 写），`Send*` 不再改 args；外部手动构造 args 的调用方自行设 `GroupID`。

## 验证

- `TestRaftCluster_ElectReplicateCommit`：选主→propose→复制→**全 3 节点日志收敛**，连跑 5 次稳定。
- `go test ./Raft/` 在 `-count=1`、`-count=2`、`-race` 下全绿；全仓 16 包全绿。

## 意义

这是 BanDB 的 **Raft 首次真正多节点跑通**（选举 + 日志复制 + 提交 + 收敛）。与 README "Raft 是项目型实现、需更多 3 节点故障注入" 的诚实标注呼应——一注入就挖出真 bug。leader-aware 路由现在可以建在一个**被验证过能复制**的集群上。
