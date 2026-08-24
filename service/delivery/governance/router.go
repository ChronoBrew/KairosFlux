package governance

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/ChronoBrew/KairosFlux/service/delivery"
)

// ErrNoHealthySink 表示一次投递轮转中没有任何 sink 既健康又被熔断器放行。
var ErrNoHealthySink = errors.New("governance: no healthy sink available")

// Router 是健康感知路由：持有一组 delivery.Sink 与每 sink 一个 Breaker，选一个
// 「Health().Healthy && breaker.Allow()」的 sink 投递；成功 OnSuccess，失败
// OnFailure 并尝试下一个；一轮全不可用返回 ErrNoHealthySink。
//
// 两种起点策略（见 NewRouter/NewPriorityRouter）：
//   - round-robin（NewRouter）：多个对等 sink 间分摊负载，起点随每次 Send 轮转。
//   - priority（NewPriorityRouter）：sinks[0] 是主选择，每次 Send 都从头尝试——
//     只有主 sink 不健康/被熔断/发送失败时才降级到 sinks[1..]，主恢复后自动切回。
//     这是「主 + 兜底」场景（如 ClickHouse 主、FileSink 兜底）需要的语义，与
//     round-robin 的负载分摊语义不同：兜底不该分走本该走主链路的流量。
//
// Router 满足 deliverer 的 sender 小接口（Name/Send），故可作为 Deliverer 的投递目标。
type Router struct {
	sinks    []delivery.Sink
	breakers []*Breaker
	next     atomic.Uint64 // round-robin 游标；priority 模式下不使用
	priority bool
}

// NewRouter 为每个 sink 配一个独立 Breaker（共享 failThreshold/openTimeout 参数），
// Send 按 round-robin 起点扫描——多个对等 sink 间分摊负载。
func NewRouter(sinks []delivery.Sink, failThreshold int, openTimeout time.Duration) *Router {
	breakers := make([]*Breaker, len(sinks))
	for i := range sinks {
		breakers[i] = NewBreaker(failThreshold, openTimeout)
	}
	return &Router{sinks: sinks, breakers: breakers}
}

// NewPriorityRouter 与 NewRouter 参数相同，但 Send 总是从 sinks[0] 开始按顺序尝试
// （不轮转起点）：sinks[0] 是主选择，只有它不可用时才降级到后面的兜底 sink。
func NewPriorityRouter(sinks []delivery.Sink, failThreshold int, openTimeout time.Duration) *Router {
	r := NewRouter(sinks, failThreshold, openTimeout)
	r.priority = true
	return r
}

// Name 返回路由器名字，用于日志与（当作 sender 时的）标签。
func (r *Router) Name() string { return "router" }

// Send 投递一批：round-robin 模式从游标起点扫一圈；priority 模式总从 sinks[0] 起。
// 选首个健康且被熔断器放行的 sink 投递，成功即返回并记 OnSuccess；失败记 OnFailure
// 后继续尝试下一个健康 sink；一圈内无任何可用 sink（或全部尝试失败）返回错误。
func (r *Router) Send(ctx context.Context, batch []delivery.Record) error {
	n := len(r.sinks)
	if n == 0 {
		return ErrNoHealthySink
	}
	start := 0
	if !r.priority {
		start = int(r.next.Add(1)-1) % n
	}
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
