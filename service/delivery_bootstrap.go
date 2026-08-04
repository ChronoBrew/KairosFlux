package service

import (
	"context"
	"log/slog"
	"time"

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
	sink, err := delivery.NewFileSink("file", config.G.DeliveryFilePath)
	if err != nil {
		slog.Error("delivery: open file sink failed, delivery disabled", "path", config.G.DeliveryFilePath, "error", err)
		return
	}
	store := offset.NewKVOffsetStore(NewOffsetCommitter(kv))
	src := delivery.NewKVSource(kv, nil)
	interval := time.Duration(config.G.DeliveryIntervalMs) * time.Millisecond
	d := delivery.NewDelivererWithOffset(src, sink, sink.Name(), store, config.G.DeliveryBatchSize, interval)
	go d.Run(ctx)
	slog.Info("delivery: started from config", "path", config.G.DeliveryFilePath, "batch", config.G.DeliveryBatchSize, "interval", interval)
}
