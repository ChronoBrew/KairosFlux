package cluster

import (
	"testing"
	"time"
)

// TestP2C_RoutesAwayFromSlowBackend：一个慢后端 + 三个快后端，P2C 应把绝大多数请求导向快
// 后端，使平均延迟远低于纯随机（随机会均摊 1/4 到慢后端）。
func TestP2C_RoutesAwayFromSlowBackend(t *testing.T) {
	backends := []string{"slow", "fast1", "fast2", "fast3"}
	lat := map[string]time.Duration{
		"slow":  50 * time.Millisecond,
		"fast1": 5 * time.Millisecond,
		"fast2": 5 * time.Millisecond,
		"fast3": 5 * time.Millisecond,
	}

	b := NewP2CBalancer(backends, 0.3)
	const M = 5000
	counts := map[string]int{}
	var p2cTotal time.Duration
	for i := 0; i < M; i++ {
		node, done := b.Pick()
		counts[node]++
		done(lat[node]) // 回填该后端的观测延迟
		p2cTotal += lat[node]
	}

	// 纯随机基线：均摊到 4 个后端，期望平均 = 各延迟均值。
	var mean time.Duration
	for _, d := range lat {
		mean += d
	}
	mean /= time.Duration(len(lat))
	randTotal := time.Duration(M) * mean

	p2cAvg := p2cTotal / M
	randAvg := randTotal / M
	t.Logf("P2C: slow=%d fast=%d/%d/%d 平均延迟=%v；随机平均延迟=%v",
		counts["slow"], counts["fast1"], counts["fast2"], counts["fast3"], p2cAvg, randAvg)

	// 慢后端应远少于随机的 M/4。
	if counts["slow"] >= M/10 {
		t.Fatalf("P2C should route far fewer to slow backend: got %d (random baseline ~%d)", counts["slow"], M/4)
	}
	// P2C 平均延迟应显著低于随机。
	if p2cAvg >= randAvg/2 {
		t.Fatalf("P2C avg latency %v should be well below random %v", p2cAvg, randAvg)
	}
}

// TestP2C_SingleAndEmpty：边界——单后端总选它；空后端返回空串与 no-op done。
func TestP2C_SingleAndEmpty(t *testing.T) {
	one := NewP2CBalancer([]string{"only"}, 0)
	n, done := one.Pick()
	if n != "only" {
		t.Fatalf("single backend should be picked, got %q", n)
	}
	done(time.Millisecond)

	empty := NewP2CBalancer(nil, 0)
	n, done = empty.Pick()
	if n != "" {
		t.Fatalf("empty balancer should return empty, got %q", n)
	}
	done(time.Millisecond) // 不应 panic
}
