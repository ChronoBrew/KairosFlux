package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/service/delivery/offset"
)

// sender 是 deliverer 的投递目标抽象：只需 Name/Send 两个方法。delivery.Sink 与
// governance.Router 都满足它——如此 deliverer 无需 import governance（governance 反过来
// import delivery），既避免 import 环，又让主体可把 governance.Router 直接当投递目标传入。
type sender interface {
	Name() string
	Send(ctx context.Context, batch []Record) error
}

// Deliverer 是投递循环：周期性从 source 取一批，交给 sink（sender）投递，成功后推进游标。
//
// B2 起游标经 offset.OffsetStore 落地：Run 启动时 Load 已提交游标，每批 Send 成功后
// 先 Commit 再推进内存 cursor——Commit 失败则不推进，下轮重投（at-least-once）。
// raft 模式下 offset 经 Raft 日志强一致复制，故崩溃/重启可从已提交位置续投、不重投
// 已提交批（exactly-once 仍需 sink 幂等兜底，见 offset 包 godoc）。
type Deliverer struct {
	source      Source
	sink        sender
	offsetStore offset.OffsetStore
	sinkName    string // offset key 依据；显式传入以免受 sink.Name() 变化影响
	batchSize   int
	interval    time.Duration

	// gate 是每轮投递前的准入判据：返回 false 时本轮跳过（不 Fetch/Send/Commit）。
	// raft 模式用它把投递限定在 Leader——否则 Follower 上 Commit 走 kv.Write 会
	// 「not leader」失败致游标永不推进、把同一批反复写入本地 sink。nil 视为恒放行
	// （standalone 无需限制）。
	gate func() bool

	cursor []byte // 下一批的起始位置；nil 表示从最小 key 开始
}

// memOffsetStore 是纯内存的 OffsetStore，用于 NewDeliverer 的无持久化默认值：
// 游标只在进程内存活，重启从头再投（保持 B1 语义，不破坏既有测试）。
type memOffsetStore struct {
	cursors map[string][]byte
}

func newMemOffsetStore() *memOffsetStore { return &memOffsetStore{cursors: map[string][]byte{}} }

func (m *memOffsetStore) Load(sink string) ([]byte, error) { return m.cursors[sink], nil }

func (m *memOffsetStore) Commit(sink string, cursor []byte) error {
	m.cursors[sink] = cursor
	return nil
}

// NewDeliverer 构造投递循环。batchSize<=0 取 1，interval<=0 取 1s。
// 游标只在进程内存推进（内存 OffsetStore，无持久化）；崩溃续传见 NewDelivererWithOffset。
func NewDeliverer(source Source, sink Sink, batchSize int, interval time.Duration) *Deliverer {
	return NewDelivererWithOffset(source, sink, sink.Name(), newMemOffsetStore(), batchSize, interval)
}

// NewDelivererWithOffset 构造带持久化 offset 的投递循环：游标经 store 落地，实现崩溃续传。
// sink 参数为 sender（Name/Send），故 delivery.Sink 与 governance.Router 均可传入——
// 主体接线治理路由时把 *governance.Router 按结构化满足直接传进来（无需 import 环）。
// sinkName 作为 offset key（不用 sink.Name()，以免换成治理路由后 key 漂移）。
// batchSize<=0 取 1，interval<=0 取 1s。
func NewDelivererWithOffset(source Source, sink sender, sinkName string, store offset.OffsetStore, batchSize int, interval time.Duration) *Deliverer {
	if batchSize <= 0 {
		batchSize = 1
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &Deliverer{
		source:      source,
		sink:        sink,
		offsetStore: store,
		sinkName:    sinkName,
		batchSize:   batchSize,
		interval:    interval,
	}
}

// SetGate 设置每轮投递的准入判据（见 gate 字段），返回自身以便链式调用。
func (d *Deliverer) SetGate(fn func() bool) *Deliverer {
	d.gate = fn
	return d
}

// Run 启动投递循环，直到 ctx 取消。启动时从 offsetStore 载入已提交游标以续投。
func (d *Deliverer) Run(ctx context.Context) {
	if cursor, err := d.offsetStore.Load(d.sinkName); err != nil {
		slog.Error("delivery: load offset failed, start from head", "sink", d.sinkName, "error", err)
	} else {
		d.cursor = cursor
	}
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

// deliverOnce 取一批并投递。空批直接返回；Send 成功后先 Commit offset 再推进内存游标，
// Commit 失败则不推进，下轮重投（at-least-once）。
func (d *Deliverer) deliverOnce(ctx context.Context) error {
	if d.gate != nil && !d.gate() {
		return nil // 未获准入（如 raft 非 Leader）：本轮不投递，游标不动
	}
	batch, next, err := d.source.Fetch(d.cursor, d.batchSize)
	if err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	if err := d.sink.Send(ctx, batch); err != nil {
		metrics.DeliveryFailed.Add(1)
		return err // 未 ack：游标不推进，下轮重投（at-least-once）
	}
	if err := d.offsetStore.Commit(d.sinkName, next); err != nil {
		return err // offset 未落地：不推进内存游标，下轮重投同一批
	}
	d.cursor = next
	metrics.Delivered.Add(int64(len(batch)))
	metrics.OffsetCommits.Add(1)
	slog.Debug("delivery: batch delivered", "sink", d.sink.Name(), "n", len(batch))
	return nil
}
