package governance

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/NeverENG/BanDB/service/delivery"
)

// ErrNoHealthySink 表示一次投递轮转中没有任何 sink 既健康又被熔断器放行。
var ErrNoHealthySink = errors.New("governance: no healthy sink available")

// Router 是健康感知路由（骨架但可用）：持有一组 delivery.Sink 与每 sink 一个 Breaker，
// round-robin 选一个「Health().Healthy && breaker.Allow()」的 sink 投递；成功 OnSuccess，
// 失败 OnFailure 并尝试下一个；一轮全不可用返回 ErrNoHealthySink。
//
// Router 满足 deliverer 的 sender 小接口（Name/Send），故可作为 Deliverer 的投递目标。
type Router struct {
	sinks    []delivery.Sink
	breakers []*Breaker
	next     atomic.Uint64 // round-robin 游标
}

// NewRouter 为每个 sink 配一个独立 Breaker（共享 failThreshold/openTimeout 参数）。
func NewRouter(sinks []delivery.Sink, failThreshold int, openTimeout time.Duration) *Router {
	breakers := make([]*Breaker, len(sinks))
	for i := range sinks {
		breakers[i] = NewBreaker(failThreshold, openTimeout)
	}
	return &Router{sinks: sinks, breakers: breakers}
}

// Name 返回路由器名字，用于日志与（当作 sender 时的）标签。
func (r *Router) Name() string { return "router" }

// Send 投递一批：从 round-robin 起点扫一圈，选首个健康且被熔断器放行的 sink 投递。
// 成功即返回并记 OnSuccess；失败记 OnFailure 后继续尝试下一个健康 sink；
// 一圈内无任何可用 sink（或全部尝试失败）返回错误。
func (r *Router) Send(ctx context.Context, batch []delivery.Record) error {
	n := len(r.sinks)
	if n == 0 {
		return ErrNoHealthySink
	}
	start := int(r.next.Add(1)-1) % n
	var lastErr error
	attempted := false
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		sink := r.sinks[idx]
		br := r.breakers[idx]
		if !sink.Health().Healthy || !br.Allow() {
			continue // 不健康或被熔断，跳过
		}
		attempted = true
		if err := sink.Send(ctx, batch); err != nil {
			br.OnFailure()
			lastErr = err
			continue // 尝试下一个健康 sink
		}
		br.OnSuccess()
		return nil
	}
	if !attempted {
		return ErrNoHealthySink
	}
	return lastErr // 所有被尝试的 sink 都失败
}
