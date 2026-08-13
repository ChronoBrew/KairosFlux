package credit

import (
	"testing"
	"time"
)

func TestAcquireReleaseUsed(t *testing.T) {
	p := New(100)
	p.Acquire(40)
	p.Acquire(30)
	if got := p.Used(); got != 70 {
		t.Fatalf("used = %d, want 70", got)
	}
	p.Release(40)
	if got := p.Used(); got != 30 {
		t.Fatalf("used after release = %d, want 30", got)
	}
}

func TestTryAcquireRejectsWhenFull(t *testing.T) {
	p := New(100)
	if !p.TryAcquire(90) {
		t.Fatal("first TryAcquire(90) should succeed")
	}
	if p.TryAcquire(20) {
		t.Fatal("TryAcquire(20) should fail: 90+20 > 100")
	}
	if !p.TryAcquire(10) {
		t.Fatal("TryAcquire(10) should succeed: 90+10 == 100")
	}
}

func TestAcquireBlocksUntilRelease(t *testing.T) {
	p := New(100)
	p.Acquire(100) // 占满

	done := make(chan struct{})
	go func() {
		p.Acquire(50) // 应阻塞，直到下面 Release
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Acquire(50) returned while pool full — should have blocked")
	case <-time.After(50 * time.Millisecond):
		// 仍阻塞，符合预期
	}

	p.Release(60) // used: 100 -> 40，腾出空间
	select {
	case <-done:
		// 解除阻塞，符合预期
	case <-time.After(time.Second):
		t.Fatal("Acquire(50) did not unblock after Release")
	}
	if got := p.Used(); got != 90 {
		t.Fatalf("used = %d, want 90 (40 + 50)", got)
	}
}

func TestOversizeAllowedWhenEmpty(t *testing.T) {
	p := New(100)
	// 单条超预算：池为空时放行，避免永久阻塞。
	p.Acquire(150)
	if got := p.Used(); got != 150 {
		t.Fatalf("used = %d, want 150", got)
	}
}

func TestZeroBudgetNeverBlocks(t *testing.T) {
	p := New(0) // 关闭背压
	p.Acquire(1 << 30)
	if !p.TryAcquire(1 << 30) {
		t.Fatal("TryAcquire should always succeed when budget disabled")
	}
}

func TestReleaseClampsAtZero(t *testing.T) {
	p := New(100)
	p.Acquire(10)
	p.Release(50) // 多还也不应为负
	if got := p.Used(); got != 0 {
		t.Fatalf("used = %d, want 0", got)
	}
}
