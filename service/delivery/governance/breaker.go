// Package governance 是投递层的「治理」子包，借鉴 dubbo-go 的服务治理模型但落在数据面：
// 把多个下游 sink 当作一组要被治理的后端——熔断（breaker）、健康感知路由（router）、
// 健康探测（health）、退避重试（retry）。全程零依赖，只用标准库。
//
// 依赖方向：governance import 父包 delivery（用其 Sink/Record），无 import 环。
package governance

import (
	"sync"
	"time"
)

// state 是熔断器的三态。
type state int

const (
	// stateClosed：正常放行，累计失败达阈值则转 open。
	stateClosed state = iota
	// stateOpen：熔断，直接拒绝；经 openTimeout 后转 half-open 放一个探测。
	stateOpen
	// stateHalfOpen：半开，仅放行一个探测请求；成功→closed，失败→重新 open。
	stateHalfOpen
)

// Breaker 是真实的三态熔断器（closed/open/half-open），并发安全。
// half-open 态只允许一个探测在途：Allow 放行探测后置 probeInFlight，直到 OnSuccess/
// OnFailure 结算前，后续 Allow 一律拒绝，避免半开态被打爆。
//
// 时钟经 now 字段注入以便测试（默认 time.Now）。
type Breaker struct {
	mu            sync.Mutex
	failThreshold int           // closed 态累计失败达此值则 open
	openTimeout   time.Duration // open 态持续多久后转 half-open
	now           func() time.Time

	st            state
	failures      int       // closed 态的连续失败计数
	openedAt      time.Time // 进入 open 态的时刻，用于计算 openTimeout
	probeInFlight bool      // half-open 态是否已有探测在途
}

// NewBreaker 构造熔断器。failThreshold<=0 取 1，openTimeout<=0 取 1s。
func NewBreaker(failThreshold int, openTimeout time.Duration) *Breaker {
	if failThreshold <= 0 {
		failThreshold = 1
	}
	if openTimeout <= 0 {
		openTimeout = time.Second
	}
	return &Breaker{
		failThreshold: failThreshold,
		openTimeout:   openTimeout,
		now:           time.Now,
		st:            stateClosed,
	}
}

// Allow 判断当前是否放行一次调用。
//   - closed：放行。
//   - open：未到 openTimeout 拒绝；到期转 half-open 并放行本次作为探测。
//   - half-open：仅当无探测在途时放行一个探测，否则拒绝。
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.st {
	case stateClosed:
		return true
	case stateOpen:
		if b.now().Sub(b.openedAt) < b.openTimeout {
			return false
		}
		// 到期：转 half-open，放行本次作为探测。
		b.st = stateHalfOpen
		b.probeInFlight = true
		return true
	case stateHalfOpen:
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	default:
		return false
	}
}

// OnSuccess 记录一次成功：half-open/closed 均归位到 closed 并清零失败计数。
func (b *Breaker) OnSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.st = stateClosed
	b.failures = 0
	b.probeInFlight = false
}

// OnFailure 记录一次失败：
//   - half-open：探测失败，重新 open（重置计时）。
//   - closed：累计失败达阈值则 open。
func (b *Breaker) OnFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.st {
	case stateHalfOpen:
		b.trip()
	case stateClosed:
		b.failures++
		if b.failures >= b.failThreshold {
			b.trip()
		}
	}
}

// trip 进入 open 态并记录时刻（调用方须持锁）。
func (b *Breaker) trip() {
	b.st = stateOpen
	b.openedAt = b.now()
	b.probeInFlight = false
}
