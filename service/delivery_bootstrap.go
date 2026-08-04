package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/NeverENG/BanDB/Raft"
	"github.com/NeverENG/BanDB/config"
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
	store := offset.NewKVOffsetStore(NewOffsetCommitter(kv))
	src := delivery.NewKVSource(kv, nil)
	interval := time.Duration(config.G.DeliveryIntervalMs) * time.Millisecond
	d := delivery.NewDelivererWithOffset(src, sink, sink.Name(), store, config.G.DeliveryBatchSize, interval)
	// raft 模式：把投递限定在 Leader。Follower 上 offset Commit 走 kv.Write 会「not
	// leader」失败，导致游标不推进、同一批被反复写入本地 sink；且多节点同投会 N 倍重复。
	// 限定 Leader 后为一次性投递；主换届时新 Leader 从强一致 offset 续投（standalone 恒放行）。
	if r := kv.GetRaft(); r != nil {
		d.SetGate(func() bool {
			state, _ := r.GetState()
			return state == Raft.Leader
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
