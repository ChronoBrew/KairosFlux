# 迭代复盘：为上 Multi-Raft 做体检，反而挖出 Raft follower 复制的系统性 off-by-one

日期：2026-08-05　范围：`Raft/`

## 背景

准备给分片集群加 Multi-Raft（每分片一个 Raft 组）副本前，先给现有 Raft 做体检——因为它的单测长期在本机 **全红 panic**。原则：地基没验证过，不在上面盖楼。

## 发现一：测试全红是"非隔离持久化"，不是 Raft 逻辑

`NewRaft` 默认落盘目录 `raft_data`，且**测试从不清理**。跨运行累积 `term`/`log`，`readPersist` 把陈旧状态读回内存——于是断言 "Expected term 0, got 12"、"got 4 log entries"。这是测试卫生问题，不是核心 bug。

**修复**：所有污染测试改用 `t.TempDir()` 逐测隔离；RPC 测试的固定端口监听器加 `t.Cleanup` 关闭（避免 `-count=2` 端口占用）。

## 发现二：隔离后暴露真 bug——无快照场景相对下标 off-by-one

隔离后 15/16 通过，只剩 `TestAppendEntriesRPC` **panic: index out of range [-1]**。这正是"污染背后藏着真 bug"。

**归因**：日志用 0-based 绝对 index（首条 index 0，`commitIndex` 初始 -1）。但 `LastIncludedIndex` 初始为 0，而代码以 `LastIncludedIndex > 0` 判定"有快照"。相对数组下标却统一用：

```
relativeIndex = absIndex - LastIncludedIndex - 1
```

无快照时（`LastIncludedIndex == 0`）把绝对 index 0 算成 **-1**。后果遍布无快照路径：

- `AppendEntries` 追加：`log[-1]` panic（follower 收第一条日志即崩）。
- `getTermAt(0)`：返回错误 term 0（影响选举投票比较、`PrevLogTerm` 一致性检查）。
- `replicateLog`：`log[relativeStart:]` = `log[-1:]` panic（leader 向 follower 发第一条即崩）。
- `applyCommittedLogs`：index-0 条目被 `>=0` 守卫跳过、**永不 apply**。

这些无快照路径此前只被**单节点/快照**用例覆盖，**多节点无快照复制从未真正跑通**——与 README "Raft 是项目型实现、需更多 3 节点故障注入" 的诚实标注吻合。

**修复**：抽出统一映射：

```go
func (r *Raft) relIndex(absIndex int) int {
    base := -1
    if r.LastIncludedIndex > 0 {
        base = int(r.LastIncludedIndex)
    }
    return absIndex - base - 1
}
```

无快照基准 -1（首条 index 0 → 下标 0）；有快照时 `= LastIncludedIndex`，与旧公式**字节等价**，故快照用例不受影响。统一替换 6 处相对下标计算，并在切片/索引处补 `>= 0` 守卫。

## 验证

- `go test ./Raft/ -count=1`：**16/16 全绿**（此前全红），连跑 3 次稳定。
- 全仓库 16 个包 `go test ./...` 全绿。
- 快照相关用例（`LastIncludedIndex > 0`）行为不变。

## 遗留（Multi-Raft 的前置条件，未在本次做）

- **无 `Stop()`/干净生命周期**：每个 `NewRaft` 起的 electionLoop goroutine 永不退出。`-count=2` 下泄漏的选举 goroutine 跨用例互扰致 `TestRequestVoteRPC` flaky。这既是测试问题，也是 **Multi-Raft 硬前置**（要能启停每个组）。
- **传输无 groupID 多路复用**、**leader-aware 路由/重定向**：Multi-Raft 的真正工作量在这两块。

**结论**：体检的价值兑现了——Raft 地基原来有系统性 bug，现已修复并首次让单测全绿。Multi-Raft 应在补齐"干净生命周期 + groupID 传输 + leader 路由"后再上，而不是盖在刚修好的地基上急着加层。
