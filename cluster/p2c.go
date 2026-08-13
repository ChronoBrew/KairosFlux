package cluster

import (
	"math/rand"
	"sync"
	"time"
)

// P2CBalancer 是「Power of Two Choices + 峰值 EWMA」延迟感知负载均衡器（Finagle/Envoy 同款）：
// 每次随机取两个后端，选其中「负载×延迟」代价更低者。相比纯随机/轮询，它用极小的开销
// （只比两个）就把请求持续导向更快、更空闲的后端，显著压低平均与尾延迟；相比"全局选最优"
// 又避免了羊群效应（大家一窝蜂涌向同一个瞬时最优）。
//
// 代价函数：score = ewmaLatency × (inflight + 1)——既看历史延迟（EWMA 平滑），又看当前在途
// （+1 使空闲后端也有区分度）。冷启动 ewma=0 → score=0 → 优先被选中以探测（类慢启动）。
//
// 二者都是无状态请求/副本 LB 原语（如把读请求在一组副本间择优），非有状态数据放置。
//
// 并发安全。
type P2CBalancer struct {
	mu       sync.Mutex
	backends []string
	ewma     map[string]float64 // 各后端延迟的 EWMA（纳秒）
	inflight map[string]int
	decay    float64 // EWMA 平滑系数 (0,1]，越大越跟新样本
	rng      *rand.Rand
}

// NewP2CBalancer 创建均衡器。decay<=0 或 >1 取默认 0.2。
func NewP2CBalancer(backends []string, decay float64) *P2CBalancer {
	if decay <= 0 || decay > 1 {
		decay = 0.2
	}
	b := &P2CBalancer{
		backends: append([]string(nil), backends...),
		ewma:     make(map[string]float64, len(backends)),
		inflight: make(map[string]int, len(backends)),
		decay:    decay,
		rng:      rand.New(rand.NewSource(rand.Int63())),
	}
	return b
}

// score 返回后端的选择代价（越低越优）：ewma 延迟 × (在途 + 1)。调用方须持锁。
func (b *P2CBalancer) score(node string) float64 {
	return b.ewma[node] * float64(b.inflight[node]+1)
}

// Pick 选择一个后端并计一次在途；返回的 done 须在请求完成时以实际耗时调用（回填 EWMA、
// 释放在途）。空后端返回 ("", no-op)。
func (b *P2CBalancer) Pick() (backend string, done func(latency time.Duration)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(b.backends)
	if n == 0 {
		return "", func(time.Duration) {}
	}

	var chosen string
	if n == 1 {
		chosen = b.backends[0]
	} else {
		i := b.rng.Intn(n)
		j := b.rng.Intn(n - 1)
		if j >= i { // 保证取到两个不同下标
			j++
		}
		a, c := b.backends[i], b.backends[j]
		if b.score(a) <= b.score(c) {
			chosen = a
		} else {
			chosen = c
		}
	}

	b.inflight[chosen]++
	return chosen, b.completer(chosen)
}

// completer 返回记录一次完成的回调：更新该后端的 EWMA 延迟并释放在途。
func (b *P2CBalancer) completer(node string) func(time.Duration) {
	return func(latency time.Duration) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.inflight[node] > 0 {
			b.inflight[node]--
		}
		sample := float64(latency)
		if cur, ok := b.ewma[node]; ok {
			b.ewma[node] = cur*(1-b.decay) + sample*b.decay
		} else {
			b.ewma[node] = sample // 首个样本直接作为基准
		}
	}
}

// Inflight 返回各后端当前在途数快照（供观测/测试）。
func (b *P2CBalancer) Inflight() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]int, len(b.inflight))
	for k, v := range b.inflight {
		out[k] = v
	}
	return out
}
