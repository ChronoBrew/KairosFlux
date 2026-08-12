# BanDB Flux —— 高性能数仓前置存储引擎

单二进制、零第三方依赖（除 gRPC/Protobuf），开箱即用；面向数据仓库的写入前置场景。

## 定位

BanDB 坐在数据仓库（ClickHouse、Doris 等）的写入入口之前，把上游突发、高频、易失的写入流，整形成下游能平稳消费的数据。上游只管高速写，BanDB 负责吸收、清洗、缓冲，再以可控节奏投递下游。

它不是数仓本体，也不替代 Kafka——是数仓前置的**摄入缓冲 + 投递**层。

## 相比 Kafka + Spark 清洗链路

传统数仓清洗是 `Source → Kafka（原始数据全量落盘） → Spark 拉回反序列化清洗 → 数仓`，跨两套重型分布式系统、多次网络跳转与序列化往返。BanDB 把这段收敛进一个零依赖引擎：

| 维度 | Kafka + Spark | BanDB |
| --- | --- | --- |
| 清洗时机 | 原始数据先全量落 Kafka，下游再拉出来洗 | 写入进系统的一刻就地清洗，脏数据不进缓冲 |
| 数据搬运 | 写 Kafka + 拉取 + 反序列化才开始处理 | 摄入与清洗一体完成，少一套系统、少一跳、少一次序列化往返 |
| 延迟 | Spark 微批秒级批延迟 | 写入即清洗，投递紧循环，无攒批延迟 |
| 运维 | Kafka 集群 + Spark 集群 + 调度，JVM 重内存 | 单二进制、零依赖、内存有界 |

适用区间是**中小规模 / 边缘 / 低延迟**。大规模复杂有状态流处理（跨流 join、窗口聚合）与多消费者回放仍是 Kafka+Spark 的主场，BanDB 不做替代。

## 功能

- **高并发写入吸收**：突发高频写入平稳落地，内存占用有界、不会被写入打爆。
- **落盘前数据清洗**：写入进系统的一刻即校验、脱敏、丢弃畸形帧，脏数据不进缓冲。
- **崩溃恢复与断点重续**：进程崩溃重启自动恢复数据；投递从上次已提交位点续传，已投数据不重投。
- **高并发限流**：过载时自适应限流、主动拒绝多余请求，保护系统不被压垮。
- **可靠投递下游**：按位点批量投递、失败自动重试；下游故障时熔断隔离、恢复后自动探测放行，至少一次送达。
- **横向分片扩展**：数据量增大时按分片扩展到多节点，多副本容错；读请求自动在副本间择优、分摊负载。

## 架构

```mermaid
flowchart TD
    Up([上游写入端])

    subgraph L1[入口层]
      direction LR
      BanNet[BanNet · TCP TLV]
      GRPC[gRPC]
      KV["KVServer 服务层<br/>PreHandle 落盘前预处理"]
    end

    subgraph L2[存储层]
      WAL[存储层 WAL]
      Raft[Raft 日志复制]
      Engine[LSM 存储引擎]
      MemTable[MemTable 跳表 active/dirty]
      SSTable[分层 SSTable]
    end

    subgraph L3[投递层]
      Deliverer["投递循环<br/>熔断 / 健康 / 重试"]
      Sink[下游 sink]
    end

    subgraph L4[分布式层]
      Router[一致性哈希路由 + 准入限流]
      Shards["Multi-Raft 分片副本组<br/>P2C 读 + 共享连接池"]
    end

    Up --> BanNet
    Up --> GRPC
    BanNet --> KV
    GRPC --> KV
    KV -->|standalone| WAL
    KV -->|raft| Raft
    WAL --> Engine
    Raft --> Engine
    Engine --> MemTable
    MemTable -->|Flush| SSTable
    Engine -->|按位点批量拉取| Deliverer
    Deliverer --> Sink
    KV -.横向切分.-> Router
    Router --> Shards

    classDef entry fill:#e3f2fd,stroke:#1565c0,color:#0d47a1;
    classDef store fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;
    classDef deliver fill:#fff3e0,stroke:#ef6c00,color:#e65100;
    classDef dist fill:#f3e5f5,stroke:#6a1b9a,color:#4a148c;
    class BanNet,GRPC,KV entry;
    class WAL,Raft,Engine,MemTable,SSTable store;
    class Deliverer,Sink deliver;
    class Router,Shards dist;
```

- **入口**：支持二进制 TCP 与 gRPC 两种接入，统一到同一服务层，落盘前跑清洗钩子。
- **存储**：写入先落盘保证不丢，内存分层管理热数据、历史数据顺序归档，重启自动恢复。
- **投递**：从缓冲按位点取数，经熔断 / 健康 / 重试治理后投递下游数仓。
- **分布式**：请求按分片路由并限流，多副本容错，读请求在副本间择优、分摊负载。

## 性能

环境：本机 macOS、单机，16B key / 256B value，50 并发，`bash benchmark/bench.sh` 口径。
读取用 20 万 key 的工作集——远超单张内存表容量，故读必然下穿到 SSTable，衡量的是真实
磁盘读路径而非内存命中。

| 操作 | QPS | P50 | P99 |
| --- | --- | --- | --- |
| GET（读，工作集 20 万 key） | 130,000 – 187,000 | 195µs – 270µs | ~2ms |
| GET（读，工作集常驻内存表） | 116,513 | 248µs | 3.86ms |
| PUT（写，返回即已 fsync 持久化） | 6,415 | 7.66ms | 14.87ms |

- **读路径不受文件大小与条目数影响**：布隆过滤器否决 → 块索引二分 → 单次读取目标块，
  每次点查恒定一次 read 系统调用；命中块缓存时不触达磁盘。
- **写吞吐由 fsync 决定**：group commit 把一批并发写摊销为一次 fsync，故写吞吐随在途
  并发上升；本机 `F_FULLFSYNC` 单次约 3.94ms，即为写延迟的物理下界。
- **分布式通信高效**：节点间通信复用连接，高并发下相比每次新建连接快 5–12×。
- **读自动均衡**：并发读在副本间自动分摊（3 节点、每分片 2 副本，两副本实测 157/163）。

读取一栏给出区间而非单值，原因有三，均影响复现：

- **同一份代码的运行间波动约 ±16%**：长时间连续压测后本机热节流，实测同一二进制可从
  187,000 漂到 130,000。区间上界为冷机值，下界为持续压测后的值。也因此，评估小幅改动
  必须交替 A/B 测量——顺序测 before/after 会被漂移完全淹没。
- **压测客户端与服务端同机共享 8 核**：该数字含客户端自身开销，是「同机端到端」口径，
  不代表服务端处理上限。客户端独立部署时读吞吐应更高。
- **`MaxConn` 默认 100**：100 并发以上的压测需同步调高该配置，否则超出的连接会被拒绝。

## 使用

环境 Go 1.26+。

```bash
# BanNet 服务（默认读 config/config.json，监听 127.0.0.1:8080）
cd Server && go run .

# 另开终端启动客户端
cd client && go run .
```

或 gRPC 服务（standalone，监听 :9090）：

```bash
go run ./server_grpc -addr localhost:9090
```

交互：

```text
> put order:1001 {"amount":128,"ts":1754380800}
OK
> get order:1001
"{...}"
> quit
```

BanNet 协议（定长帧头二进制）：

```text
[dataLen: uint32] [msgID: uint32] [payload]
```

| msgID | 操作 | 负载 |
| --- | --- | --- |
| `1` | PUT | `keyLen:uint32 + valueLen:uint32 + key + value` |
| `2` | GET | `keyLen:uint32 + key` |
| `3` | DELETE | `keyLen:uint32 + key` |

响应首字节为状态标志（`0x00` 成功 / `0x01` 失败）；GET 成功时其后接 `valueLen:uint32 + value`。
