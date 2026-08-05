# 迭代复盘：节点间 RPC 连接池——先量化瓶颈，再优化，接进 ShardKV

日期：2026-08-05　范围：`Raft`（透传到 `service/shardkv`）

## 背景

Multi-Raft / ShardKV 的节点间 RPC（`SendRequestVote`/`SendAppendEntries`/`SendInstallSnapshot`/`Propose`）此前每次调用都 `rpc.Dial + Call + Close`——每次一次 TCP 三次握手 + 连接建立 + 拆除。多分片下每秒数百次拨号（此前标注的 Trap 2）。

按本仓「测量优先」纪律：**先量化它是不是真瓶颈**（别优化不存在的问题），证实了再做。

## 量化：dial-per-call vs 连接池

`BenchmarkRPC_*`（本机 echo RPC 服务）：

| 场景 | ns/op | allocs/op | 相对 |
| --- | --- | --- | --- |
| DialPerCall（串行） | 133,015 | 428 | 基线 |
| Pooled（串行） | 25,365 | 16 | **5.2× 快、27× 少分配** |
| DialPerCall（并发） | 65,459 | 428 | 基线 |
| **Pooled（并发）** | **5,293** | 16 | **12.4× 快** |

并发下差距**拉大到 12 倍**——正是多分片 Raft 的真实场景：dial-per-call 卡在 TCP 握手上串行化，而连接池在一条连接上多路复用。瓶颈证实，优化划算。

## 优化：每对端复用一个 `*rpc.Client`

`Raft/rpcpool.go`：`rpcPool` 每个对端地址缓存并复用一个 `*rpc.Client`。

- **可行性**：net/rpc 的 Client 本身并发安全（内部按 seq 号在一条连接上多路复用并发调用），故一个缓存 client 就能被所有调用方 / 所有 Raft 组并发复用。
- **健壮性**：`net.DialTimeout` 带超时拨号（比原 `rpc.Dial` 无超时更好）；`ErrShutdown` 时丢弃重拨重试一次（纯传输失败，安全）；`callTimeout` 用 `Go`+select 支持 Propose 的调用超时（超时不丢连接，慢调用不影响其它多路复用调用）。
- **共享池**：包级 `defaultRPCPool`——同节点多个组拨向相同对端，共享一池即每对端只维持一条被多路复用的连接。

## 接进 ShardKV

Raft 的 `Send*` 与 `callPropose` 改用 `defaultRPCPool`。ShardKV **无需改动即受益**：其 `Put → ProposeToGroup → callPropose` 走池；各分片组的选举/复制 `Send*` 也走池。

## 验证

Raft 集群 + ShardKV 测试全绿、`-race` 干净、集群稳定 10/10；全仓 18 包全绿、零依赖不变。

## 后续

- 池的空闲连接回收 / 上限（当前只增不淘，对固定拓扑无碍；动态成员场景可加）。
- InstallSnapshot 大包可考虑独立连接，避免与心跳争用多路复用队头。
