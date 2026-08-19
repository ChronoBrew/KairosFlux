//go:build experimental

// 隔离说明见同包 breaker.go 顶部注释。
package governance

import (
	"context"
	"log/slog"
	"time"

	"github.com/NeverENG/BanDB/service/delivery"
)

// StartHealthProbe 周期性读取每个 sink 的 Health() 并以 slog 打健康快照（骨架，观测用）。
// 阻塞运行直到 ctx 取消；通常由主体在独立 goroutine 中启动。interval<=0 取 5s。
func StartHealthProbe(ctx context.Context, sinks []delivery.Sink, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, s := range sinks {
				h := s.Health()
				slog.Info("governance: health snapshot", "sink", s.Name(), "healthy", h.Healthy, "reason", h.Reason)
			}
		}
	}
}
