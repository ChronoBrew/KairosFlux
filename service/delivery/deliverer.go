package delivery

import (
	"context"
	"log/slog"
	"time"
)

// Deliverer 是投递循环：周期性从 source 取一批，交给 sink 投递，成功后推进游标。
//
// B1 骨架阶段游标只在内存推进（进程重启从头再投，at-least-once）；B2 会把游标
// 换成经 kv.Write 落地的强一致 offset，实现崩溃续传与不重投已提交批。
type Deliverer struct {
	source    Source
	sink      Sink
	batchSize int
	interval  time.Duration

	cursor []byte // 下一批的起始位置；nil 表示从最小 key 开始
}

// NewDeliverer 构造投递循环。batchSize<=0 取 1，interval<=0 取 1s。
func NewDeliverer(source Source, sink Sink, batchSize int, interval time.Duration) *Deliverer {
	if batchSize <= 0 {
		batchSize = 1
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &Deliverer{source: source, sink: sink, batchSize: batchSize, interval: interval}
}

// Run 启动投递循环，直到 ctx 取消。
func (d *Deliverer) Run(ctx context.Context) {
	slog.Info("delivery: deliverer started", "sink", d.sink.Name(), "batch", d.batchSize, "interval", d.interval)
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("delivery: deliverer stopped", "sink", d.sink.Name())
			return
		case <-ticker.C:
			if err := d.deliverOnce(ctx); err != nil {
				slog.Error("delivery: batch failed", "sink", d.sink.Name(), "error", err)
			}
		}
	}
}

// deliverOnce 取一批并投递。空批直接返回；投递成功后推进游标。
func (d *Deliverer) deliverOnce(ctx context.Context) error {
	batch, next, err := d.source.Fetch(d.cursor, d.batchSize)
	if err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	if err := d.sink.Send(ctx, batch); err != nil {
		return err // 未 ack：游标不推进，下轮重投（at-least-once）
	}
	d.cursor = next
	slog.Debug("delivery: batch delivered", "sink", d.sink.Name(), "n", len(batch))
	return nil
}
