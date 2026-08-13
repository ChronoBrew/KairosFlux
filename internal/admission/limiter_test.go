package admission

import (
	"testing"
	"time"
)

// TestLimiter_GrowsUnderLowLatency：延迟稳定在基准附近时，并发上限应向上探测扩张。
func TestLimiter_GrowsUnderLowLatency(t *testing.T) {
	l := New(Config{InitialLimit: 10, MinLimit: 1, MaxLimit: 1000})
	for i := 0; i < 300; i++ {
		l.observe(1 * time.Millisecond)
	}
	if got := l.Limit(); got <= 10 {
		t.Fatalf("expected limit to grow above 10 under low latency, got %d", got)
	}
}

// TestLimiter_ShrinksUnderHighLatency：延迟远高于基准（排队重）时，上限应显著收缩。
func TestLimiter_ShrinksUnderHighLatency(t *testing.T) {
	l := New(Config{InitialLimit: 200, MinLimit: 1, MaxLimit: 1000})
	// 先用低延迟确立 minRTT 基准，并让上限稳定。
	for i := 0; i < 50; i++ {
		l.observe(1 * time.Millisecond)
	}
	high := l.Limit()
	// 延迟飙到基准的 ~100 倍（排队）→ gradient 触底 0.5，持续收缩。
	for i := 0; i < 100; i++ {
		l.observe(100 * time.Millisecond)
	}
	low := l.Limit()
	if low >= high {
		t.Fatalf("expected limit to shrink under high latency: before=%d after=%d", high, low)
	}
}

// TestLimiter_ShedsWhenFull：在途达上限时应拒绝（shed）；释放后恢复准入。
func TestLimiter_ShedsWhenFull(t *testing.T) {
	l := New(Config{InitialLimit: 3, MinLimit: 1, MaxLimit: 3})
	var starts []time.Time
	for i := 0; i < 3; i++ {
		st, ok := l.Acquire()
		if !ok {
			t.Fatalf("acquire %d should succeed (under limit)", i)
		}
		starts = append(starts, st)
	}
	if _, ok := l.Acquire(); ok {
		t.Fatal("acquire beyond limit should shed (ok=false)")
	}
	// 释放一个后应可再次准入。
	l.Release(starts[0])
	if _, ok := l.Acquire(); !ok {
		t.Fatal("acquire after release should succeed")
	}

	_, shed := l.Stats()
	if shed == 0 {
		t.Fatal("expected shed count > 0")
	}
}
