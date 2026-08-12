// Package admission 提供网关入口的「自适应并发限流 / 准入控制」。
//
// 与存储层字节背压（pkg/credit）的区别：credit 在 MemTable 写路径按未 flush 字节阻塞，管的是
// 内存边界；本包在网络入口按「并发在途请求数」准入，用请求延迟反馈**自适应**探测系统容量
// 上限（类 TCP 拥塞控制 / Netflix gradient limiter），过载时**快速 shed（拒绝）** 而非无限
// 排队阻塞。两者不同层、互补：准入在过载压垮存储前就把超额请求挡在门外。
//
// 算法（gradient）：以观测到的最优 RTT（minRTT）为无排队基准，采样 RTT 越大说明排队越重。
//
//	gradient  = clamp(minRTT / sampleRTT, 0.5, 1)   // ≤1，越小说明排队越重
//	newLimit  = limit*gradient + queueHeadroom       // 排队重则收缩；轻则借 headroom 扩张
//	limit     = 平滑(limit, newLimit)                 // EWMA 平滑，clamp 到 [min,max]
//
// 准入：在途 >= floor(limit) 即 shed。minRTT 周期性重探，跟随基准漂移。
package admission

import (
	"math"
	"sync"
	"time"
)

// Limiter 是自适应并发限流器，并发安全。
type Limiter struct {
	mu       sync.Mutex
	limit    float64
	inFlight int

	minLimit  float64
	maxLimit  float64
	smoothing float64 // EWMA 平滑系数 (0,1]

	minRTT       time.Duration
	sampleCount  int
	probeSamples int // 每这么多样本重探一次 minRTT，跟随基准漂移

	// 观测计数（原子性由 mu 保护）
	admitted int64
	shed     int64
}

// Config 配置限流器。零值字段取合理默认。
type Config struct {
	InitialLimit int
	MinLimit     int
	MaxLimit     int
	Smoothing    float64 // 默认 0.2
	ProbeSamples int     // 默认 1000
}

// New 创建限流器。
func New(c Config) *Limiter {
	if c.InitialLimit <= 0 {
		c.InitialLimit = 10
	}
	if c.MinLimit <= 0 {
		c.MinLimit = 1
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = 1000
	}
	if c.Smoothing <= 0 || c.Smoothing > 1 {
		c.Smoothing = 0.2
	}
	if c.ProbeSamples <= 0 {
		c.ProbeSamples = 1000
	}
	return &Limiter{
		limit:        float64(c.InitialLimit),
		minLimit:     float64(c.MinLimit),
		maxLimit:     float64(c.MaxLimit),
		smoothing:    c.Smoothing,
		probeSamples: c.ProbeSamples,
	}
}

// Acquire 尝试准入一个请求。ok=false 表示应 shed（拒绝）。准入成功须在完成时调用返回的
// done(rtt 起点)。
func (l *Limiter) Acquire() (start time.Time, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if float64(l.inFlight) >= math.Floor(l.limit) {
		l.shed++
		return time.Time{}, false
	}
	l.inFlight++
	l.admitted++
	return time.Now(), true
}

// Release 记录一次已准入请求的完成，据其 RTT 自适应更新并发上限。
func (l *Limiter) Release(start time.Time) {
	rtt := time.Since(start)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inFlight--
	l.updateLocked(rtt)
}

// observe 用一次 RTT 样本更新 limit（不改 inFlight）；供测试直接喂合成样本。
func (l *Limiter) observe(rtt time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.updateLocked(rtt)
}

// updateLocked 用一次 RTT 样本更新 limit（调用方须持锁）。
func (l *Limiter) updateLocked(rtt time.Duration) {
	if rtt <= 0 {
		rtt = time.Nanosecond
	}
	// 周期性重探 minRTT，使基准能随负载/硬件漂移上下走。
	l.sampleCount++
	if l.minRTT == 0 || rtt < l.minRTT || l.sampleCount%l.probeSamples == 0 {
		l.minRTT = rtt
	}

	gradient := float64(l.minRTT) / float64(rtt)
	if gradient > 1 {
		gradient = 1
	}
	if gradient < 0.5 {
		gradient = 0.5 // 限制单步收缩，避免抖动
	}

	headroom := math.Sqrt(l.limit) // 允许探测扩张的余量
	newLimit := l.limit*gradient + headroom

	// EWMA 平滑
	l.limit = l.limit*(1-l.smoothing) + newLimit*l.smoothing
	if l.limit < l.minLimit {
		l.limit = l.minLimit
	}
	if l.limit > l.maxLimit {
		l.limit = l.maxLimit
	}
}

// Limit 返回当前并发上限（取整）。
func (l *Limiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(math.Floor(l.limit))
}

// InFlight 返回当前在途请求数。
func (l *Limiter) InFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inFlight
}

// Stats 返回累计准入/拒绝计数。
func (l *Limiter) Stats() (admitted, shed int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.admitted, l.shed
}
