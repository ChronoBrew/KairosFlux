package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/raft"
	"github.com/NeverENG/BanDB/service/delivery"
	"github.com/NeverENG/BanDB/service/delivery/governance"
	"github.com/NeverENG/BanDB/service/delivery/offset"
)

// deliveryTarget 是 newDeliverySink 的返回类型：Name+Send，与 delivery 包内部
// 未导出的 sender 接口结构相同（Go 按方法集结构匹配，无需引用该未导出类型）。
// 用它而不是 delivery.Sink，是因为 governance.Router 故意不实现 Health()——
// 它的健康状态由内部持有的每 sink Breaker 与各 sink 自身 Health() 共同决定，
// Router 本身不该被当作另一层 Router 的"一个 sink"来做健康探测。
type deliveryTarget interface {
	Name() string
	Send(ctx context.Context, batch []delivery.Record) error
}

// StartDeliveryFromConfig 按配置启动下游投递循环，默认关闭（DeliveryEnabled=false 时直接返回，
// 不影响既有写/读路径）。启用时：按 DeliverySinkType 选择投递目标（见 newDeliverySink），
// 游标经 KVServer 强一致 offset 续传，后台 go deliverer.Run。ctx 取消时投递循环退出。
func StartDeliveryFromConfig(ctx context.Context, kv *KVServer) {
	if !config.G.DeliveryEnabled {
		return
	}
	sink, err := newDeliverySink()
	if err != nil {
		slog.Error("delivery: construct sink failed, delivery disabled", "sink_type", config.G.DeliverySinkType, "error", err)
		return
	}
	var store offset.OffsetStore = offset.NewKVOffsetStore(NewOffsetCommitter(kv))
	if config.G.RetentionEnabled {
		store = &reclaimingOffsetStore{inner: store, kv: kv}
		slog.Info("delivery: retention enabled, delivered sstables will be reclaimed")
	}
	src := delivery.NewKVSource(kv, nil)
	interval := time.Duration(config.G.DeliveryIntervalMs) * time.Millisecond
	d := delivery.NewDelivererWithOffset(src, sink, sink.Name(), store, config.G.DeliveryBatchSize, interval)
	// raft 模式：把投递限定在 Leader。Follower 上 offset Commit 走 kv.Write 会「not
	// leader」失败，导致游标不推进、同一批被反复写入本地 sink；且多节点同投会 N 倍重复。
	// 限定 Leader 后为一次性投递；主换届时新 Leader 从强一致 offset 续投（standalone 恒放行）。
	if r := kv.Raft(); r != nil {
		d.SetGate(func() bool {
			state, _ := r.State()
			return state == raft.Leader
		})
	}
	go d.Run(ctx)
	slog.Info("delivery: started from config", "sink_type", config.G.DeliverySinkType, "batch", config.G.DeliveryBatchSize, "interval", interval, "exactly_once", config.G.DeliveryExactlyOnce)
}

// newDeliverySink 按 config.G.DeliverySinkType 选择投递目标：
//   - "clickhouse"：ClickHouse 主 + FileSink 兜底，经 governance.Router 健康感知路由
//     （ClickHouse 不健康/连续失败自动降级落文件，恢复后自动切回主，见 newClickHouseRoutedSink）。
//   - 其余（含默认空值 "file"）：单一 FileSink（DeliveryExactlyOnce 为真时用幂等 sink，
//     按 key HWM 去重，崩溃/重投下 effectively-once；否则普通 FileSink，at-least-once）。
func newDeliverySink() (deliveryTarget, error) {
	if config.G.DeliverySinkType == "clickhouse" {
		return newClickHouseRoutedSink()
	}
	return newFileSink()
}

func newFileSink() (delivery.Sink, error) {
	if config.G.DeliveryExactlyOnce {
		return delivery.NewIdempotentFileSink("file", config.G.DeliveryFilePath)
	}
	return delivery.NewFileSink("file", config.G.DeliveryFilePath)
}

// newClickHouseRoutedSink 构造「ClickHouse 主 + FileSink 兜底」的健康感知路由。
// FileSink 仍走 DeliveryExactlyOnce 的既有选择——ClickHouse 不健康期间落盘的数据
// 与「file 单独作为主投递目标」时行为一致，恢复主后这段兜底期数据仍在本地可核对。
func newClickHouseRoutedSink() (deliveryTarget, error) {
	fileSink, err := newFileSink()
	if err != nil {
		return nil, fmt.Errorf("delivery: 兜底 FileSink 构造失败: %w", err)
	}
	chSink := delivery.NewClickHouseSink(
		"clickhouse",
		config.G.ClickHouseAddr,
		config.G.ClickHouseDatabase,
		config.G.ClickHouseTable,
		config.G.ClickHouseUsername,
		config.G.ClickHousePassword,
		time.Duration(config.G.ClickHouseTimeoutMs)*time.Millisecond,
		config.G.ClickHouseMaxRetries,
		time.Duration(config.G.ClickHouseRetryBackoffMs)*time.Millisecond,
	)
	router := governance.NewPriorityRouter(
		[]delivery.Sink{chSink, fileSink},
		config.G.DeliveryBreakerFailThreshold,
		time.Duration(config.G.DeliveryBreakerOpenTimeoutMs)*time.Millisecond,
	)
	return router, nil
}

// reclaimingOffsetStore 在游标提交成功后回收已整体投递完的 SSTable。
//
// 挂在提交之后而非投递之后：游标落地才代表「这批不会再被重投」，此时回收才不会删掉仍需
// 重投的数据。提交失败则不回收——宁可多留，不可早删。
//
// 做成装饰器是为了不改投递主体：Deliverer 只认 OffsetStore 接口，回收对它是透明的。
type reclaimingOffsetStore struct {
	inner offset.OffsetStore
	kv    *KVServer
}

func (s *reclaimingOffsetStore) Load(sink string) ([]byte, error) { return s.inner.Load(sink) }

func (s *reclaimingOffsetStore) Commit(sink string, cursor []byte) error {
	if err := s.inner.Commit(sink, cursor); err != nil {
		return err
	}
	if n := s.kv.ReclaimDelivered(cursor); n > 0 {
		slog.Info("retention: reclaimed sstables below delivered cursor", "sink", sink, "files", n)
	}
	return nil
}
