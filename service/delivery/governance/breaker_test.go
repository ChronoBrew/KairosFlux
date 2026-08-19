//go:build experimental

// 隔离说明见同包 breaker.go 顶部注释。
package governance

import (
	"testing"
	"time"
)

// fakeClock 提供可控时钟，用于驱动 open→half-open 的超时迁移。
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(failThreshold int, openTimeout time.Duration) (*Breaker, *fakeClock) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := NewBreaker(failThreshold, openTimeout)
	b.now = clk.now
	return b, clk
}

// TestClosedToOpen 验证 closed 态累计失败达阈值后转 open 并拒绝放行。
func TestClosedToOpen(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
	b.OnFailure()
	b.OnFailure()
	if !b.Allow() {
		t.Fatal("below threshold should still allow")
	}
	b.OnFailure() // 第 3 次，达阈值
	if b.Allow() {
		t.Fatal("open breaker should reject")
	}
}

// TestOpenToHalfOpenAfterTimeout 验证 open 到期后转 half-open 放一个探测，且只放一个。
func TestOpenToHalfOpenAfterTimeout(t *testing.T) {
	b, clk := newTestBreaker(1, time.Second)
	b.OnFailure() // 达阈值 1，open
	if b.Allow() {
		t.Fatal("just-opened breaker should reject before timeout")
	}
	clk.advance(2 * time.Second)
	if !b.Allow() {
		t.Fatal("after timeout breaker should allow one probe (half-open)")
	}
	if b.Allow() {
		t.Fatal("half-open must allow only a single probe in flight")
	}
}

// TestHalfOpenToClosedOnSuccess 验证 half-open 探测成功后转 closed。
func TestHalfOpenToClosedOnSuccess(t *testing.T) {
	b, clk := newTestBreaker(1, time.Second)
	b.OnFailure()
	clk.advance(2 * time.Second)
	if !b.Allow() { // 探测放行
		t.Fatal("expected half-open probe to be allowed")
	}
	b.OnSuccess() // 探测成功 → closed
	if !b.Allow() {
		t.Fatal("after success breaker should be closed and allow")
	}
	if !b.Allow() {
		t.Fatal("closed breaker should allow repeatedly")
	}
}

// TestHalfOpenToOpenOnFailure 验证 half-open 探测失败后重新 open。
func TestHalfOpenToOpenOnFailure(t *testing.T) {
	b, clk := newTestBreaker(1, time.Second)
	b.OnFailure()
	clk.advance(2 * time.Second)
	if !b.Allow() { // 探测放行
		t.Fatal("expected half-open probe to be allowed")
	}
	b.OnFailure() // 探测失败 → 重新 open
	if b.Allow() {
		t.Fatal("half-open failure should re-open and reject")
	}
	clk.advance(2 * time.Second) // 再次到期应能放探测，证明计时已重置
	if !b.Allow() {
		t.Fatal("after re-open timeout should allow one probe again")
	}
}
