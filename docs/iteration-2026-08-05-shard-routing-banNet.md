# 迭代复盘：分片路由打通——从"控制面骨架"到"BanNet 跨节点转发的分片集群"

日期：2026-08-05　范围：`service/cluster`、`service/router.go`、`network/banNet`

## 背景

分布式分片集群此前是**零接线的控制面骨架**：`service/cluster` 有一致性哈希环、注册表、放置控制面，且各自有单测，但**请求路径根本不经过它**——每个请求直接落本地 `kv.Write`/`kv.Get`。目标是把它**打通成能跑的分片集群**：多节点部署下每个 key 路由到属主节点、数据自然分片；节点间走项目自研的 **BanNet TCP**（不是 gRPC——BanNet 是项目招牌，README 明确"核心 KV 路径不依赖 gRPC"）。

范围按"先路由，再谈副本"：本次只做路由 + 转发，Multi-Raft 副本作为独立后续。

## 做了什么

1. **可复用 BanNet 客户端**（`network/banNet/client.go`）：命令行客户端在 `cmd client/`（package main）不可被库导入，故在 banNet 包内提供库版 TCP 客户端（Put/Get/Delete，二进制 TLV）。
2. **转发连接池**（`service/cluster/peerpool.go`）：按 peer 懒建、缓存 BanNet 连接；单连接请求-响应式不能并发交错，故每 peer 一把锁串行化，出错丢弃重连。
3. **路由接进请求路径**（`service/router.go`）：`OwnerOf(key)`，属本节点则本地（走抽出的 `KVStore` 接口，便于测试注入隔离 store），否则经 BanNet 转发到 owner 并回传响应。默认关闭（`config.ShardRoutingEnabled`），单机行为不变。

## 顺带发现并修复的真 bug

写多节点集成测试时，`Server.Stop()` **必死锁**：`ConnManager.ClearConn` 持写锁遍历连接调 `conn.Stop()`，而 `Stop` 回调 `Remove` 再次 `Lock` 同一把非重入 `sync.Mutex`。修复为锁内快照清表、锁外 Stop（独立 PR）。这是任何优雅停机都会踩的既有缺陷，被集成测试逼了出来。

## 验证：真实 TCP 上的多节点集成测试

`TestShardRouting_MultiNode` 在一个进程内起 **3 个真实 BanNet 节点**（各监听独立端口、各自隔离的内存 store），断言：

- 经入口节点写入一个属主为**其它节点**的 key → 只有**属主的 store** 有该 key，入口节点没有（**数据确实分片、确实转发**）。
- 从入口、以及从第三个（既非入口也非属主）节点读 → 都经 BanNet 转发到属主命中。
- 删除经转发 → 属主 store 不再有该 key。

隔离的内存 store 是关键：用"哪个节点存了这个 key"证明转发真的发生了（全局存储配置下无法区分本地/转发）。

```powershell
go test ./service/ -run TestShardRouting_MultiNode -v
```

## 边界（诚实标注）

- **无副本 / 无高可用**：属主节点挂了，其分片不可用（转发拨号失败）。这正是"先路由，再谈副本"的分界——Multi-Raft 每分片副本是独立后续。
- **无动态成员 / 无再均衡**：节点集是静态配置；`Rebalance` 仍是桩。
- **SCAN 不分片**：范围查询跨分片需 scatter-gather，当前只扫本地，属后续。
- **无健康探测/故障转移**："先路由"阶段所有节点视为存活；转发到死节点直接报错，不 failover。
- **正确性依赖一致性哈希的确定性**：各节点用相同 peers+vnodes 构环，故对同一 key 一致地算出同一 owner，owner 恒视自己为 owner → 本地处理，无转发环。
