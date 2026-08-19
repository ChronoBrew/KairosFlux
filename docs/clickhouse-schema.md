# ClickHouse 建表与调优（ClickHouseSink 参考配置）

`service/delivery.ClickHouseSink`（见 `service/delivery/clickhouse_sink.go`）本身
不关心表结构——它只是把每条 `Record` 编码成一行 JSON（原始字段 + 注入的 `_key`
字段）POST 到 `INSERT INTO <database>.<table> FORMAT JSONEachRow`。去重/幂等靠
下面这份 DDL 里的 `ReplacingMergeTree`，配合上层 offset 机制的 at-least-once 语义，
达成「最终去重」。

## 建表 DDL：行情快照（`quote_snapshot`）

```sql
CREATE TABLE IF NOT EXISTS default.quote_snapshot
(
    code        String,                     -- 标的代码，如 "600000"
    date        Date,                       -- 交易日
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    -- volume 单位契约：手（A 股惯例，1 手 = 100 股）。QuantScout 按此单位写入。
    -- 量纲写死在这里、写死在 schema 校验注释里（service/ingesthook/schema/quote.go）、
    -- 也写死在下游任何消费这张表的地方——量纲不显式约定，将来极易出现整数倍（如
    -- 误按"股"消费手数据）的账务/回测错账，且不会报错，只会悄悄算错。
    volume      Float64,
    prev_close  Nullable(Float64),          -- 可选：无昨收（首个交易日/复牌）时为 NULL
    _key        String,                     -- 原始 BanDB key（quote:<date>:<code>），仅供审计，不是去重键
    _ingested_at DateTime DEFAULT now()     -- 落库时间，供运维排查投递延迟
)
ENGINE = ReplacingMergeTree
ORDER BY (code, date)                        -- 去重键：同一 (code, date) 重复插入，
                                              -- 在后台 merge 时折叠为一行（保留最后写入的）。
                                              -- 查询侧如需强一致去重（merge 尚未发生前），
                                              -- 用 `SELECT ... FINAL` 或 argMax(col, _ingested_at)。
PARTITION BY toYYYYMM(date)                  -- 按月分区：quote:<日期>:<代码> 的 key 布局
                                              -- 令同一天的写入在 BanDB 侧连续，这里按月分区
                                              -- 是常见的 ClickHouse 时间序列约定，二者互不冲突。
SETTINGS index_granularity = 8192;
```

字段与 `service/ingesthook/schema/quote.go` 的 `QuoteSnapshot` 校验规则一一对应：
必填字段齐全、价格 `>0`、`volume>=0`、OHLC 一致、涨跌幅在 ±21% 以内（阈值取
21% 而非更常见的 20%，是为了容纳创业板/科创板 ±20% 涨跌停在四舍五入到分后
可能出现的 +20.0x% 边界值，见 `docs/iteration-2026-08-19-quantscout-realdata-fixes.md`
的 D1 记录）——校验已经在 BanDB 落盘前做过一遍，这张表的约束更多是审计/回归用，
不代表 ClickHouse 侧还要重新校验。

## 服务器内存调优（4GB 内存服务器）

ClickHouse 默认配置假设的是数十 GB 内存的机器，直接拿默认值在 4GB 服务器上会
频繁 OOM 或被内核 OOM killer 杀掉。以下是 4GB 机器的调优起点，不是唯一解——
按实际负载（并发查询数、单查询数据量）再调。

`/etc/clickhouse-server/config.d/memory.xml`：

```xml
<clickhouse>
    <!-- 服务端总内存硬顶：默认按物理内存的 90% 估算，在 4GB 机器上这个默认值
         太激进（几乎不给 OS/其它进程留余量）。显式设为 2GB，给 OS 页缓存、
         BanDB 自身（如果同机部署）、以及内核其它开销留出另外一半。 -->
    <max_server_memory_usage>2000000000</max_server_memory_usage>

    <!-- 标记缓存（mark cache）：默认 5GB，在 4GB 机器上比总内存还大，必须调低。
         128MB 对本表的数据量（日频行情快照，单表读多写少）足够；随表增多/
         数据量增长可按需上调，但要与 max_server_memory_usage 联动考虑。 -->
    <mark_cache_size>134217728</mark_cache_size>

    <!-- 未压缩块缓存：默认关闭（0），4GB 机器上保持关闭，优先把内存留给
         mark cache 与查询本身，除非后续发现同一批数据被高频重复扫描。 -->
    <uncompressed_cache_size>0</uncompressed_cache_size>

    <!-- 后台合并/变更线程池：默认按 CPU 核数估算，在核数多但内存小的机器上
         (常见于云厂商的"计算优化型"小规格) 每个后台线程都占内存，调低并发度
         换内存余量。 -->
    <background_pool_size>4</background_pool_size>
    <background_merges_mutations_concurrency_ratio>1</background_merges_mutations_concurrency_ratio>
</clickhouse>
```

`/etc/clickhouse-server/users.d/memory.xml`（单查询内存上限，防一条重查询把
`max_server_memory_usage` 顶穿）：

```xml
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage>500000000</max_memory_usage> <!-- 单查询 500MB 上限 -->
        </default>
    </profiles>
</clickhouse>
```

## ClickHouseSink 侧配置（`config.json`）

```json
{
  "DeliverySinkType": "clickhouse",
  "ClickHouseAddr": "http://127.0.0.1:8123",
  "ClickHouseDatabase": "default",
  "ClickHouseTable": "quote_snapshot",
  "ClickHouseUsername": "",
  "ClickHousePassword": "",
  "ClickHouseTimeoutMs": 5000,
  "ClickHouseMaxRetries": 3,
  "ClickHouseRetryBackoffMs": 200,
  "DeliveryBreakerFailThreshold": 3,
  "DeliveryBreakerOpenTimeoutMs": 10000
}
```

启用后，投递路由是「ClickHouse 主 + FileSink 兜底」：`governance.NewPriorityRouter`
总是先尝试 ClickHouse，连续失败 `DeliveryBreakerFailThreshold` 次后熔断，之后的
批次直接降级落 `DeliveryFilePath`；`DeliveryBreakerOpenTimeoutMs` 后熔断器转
half-open，下一批次会重新尝试 ClickHouse——成功即视为恢复，后续批次自动切回主，
不需要重启进程或人工介入。见 `service/delivery_bootstrap.go` 的
`newClickHouseRoutedSink`，故障切换的实测见
`service/delivery/clickhouse_router_test.go`。
