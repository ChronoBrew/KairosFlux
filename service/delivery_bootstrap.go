package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/raft"
	"github.com/NeverENG/BanDB/service/delivery"
	"github.com/NeverENG/BanDB/service/delivery/offset"
)

// StartDeliveryFromConfig 按配置启动下游投递循环，默认关闭（DeliveryEnabled=false 时直接返回，
// 不影响既有写/读路径）。启用时：以 FileSink 为落地目标，游标经 KVServer 强一致 offset 续传，
// 后台 go deliverer.Run。骨架期只接单个 FileSink，多 sink 治理路由（governance.Router）
// 的接线留待后续。ctx 取消时投递循环退出。
func StartDeliveryFromConfig(ctx context.Context, kv *KVServer) {
	if !config.G.DeliveryEnabled {
		return
	}
	sink, err := newDeliverySink()
	if err != nil {
		slog.Error("delivery: open file sink failed, delivery disabled", "path", config.G.DeliveryFilePath, "error", err)
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
	slog.Info("delivery: started from config", "path", config.G.DeliveryFilePath, "batch", config.G.DeliveryBatchSize, "interval", interval, "exactly_once", config.G.DeliveryExactlyOnce)
}

// newDeliverySink 按配置选择落地 sink：DeliveryExactlyOnce 为真时用幂等 sink
// （按 key HWM 去重，崩溃/重投下 effectively-once），否则用普通 FileSink（at-least-once）。
func newDeliverySink() (delivery.Sink, error) {
	if config.G.DeliveryExactlyOnce {
		return delivery.NewIdempotentFileSink("file", config.G.DeliveryFilePath)
	}
	return delivery.NewFileSink("file", config.G.DeliveryFilePath)
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
