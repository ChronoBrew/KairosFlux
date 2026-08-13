// Package credit 提供字节级信用池（令牌桶式背压）：写入方 Acquire 占用字节信用，
// 持久化方 Release 归还信用；信用不足时 Acquire 阻塞，从而把未持久化数据的内存占用
// 限制在预算之内。它本身与具体存储无关，可被任何"写入快、落盘慢"的路径复用。
package credit

import "sync"

// Pool 是一个按字节计量的阻塞式信用池。零值不可用，请用 New 构造。
type Pool struct {
	mu     sync.Mutex
	cond   *sync.Cond
	budget int64 // <=0 表示不限制（背压关闭）
	used   int64
}

// New 构造一个预算为 budget 字节的信用池；budget <= 0 表示不限制。
func New(budget int64) *Pool {
	p := &Pool{budget: budget}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// fits 报告在已用 used 之上再占 n 是否允许（调用者须持锁）。
// 规则：预算关闭、或池空（保证至少一条能写入，避免超大单条永久阻塞）、或不超预算 → 允许。
func (p *Pool) fits(n int64) bool {
	return p.budget <= 0 || p.used == 0 || p.used+n <= p.budget
}

// TryAcquire 不阻塞地尝试占用 n 字节信用，成功返回 true。
func (p *Pool) TryAcquire(n int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fits(n) {
		p.used += n
		return true
	}
	return false
}

// Acquire 占用 n 字节信用；信用不足时阻塞，直到他方 Release 释放出空间。
func (p *Pool) Acquire(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.fits(n) {
		p.cond.Wait()
	}
	p.used += n
}

// Release 归还 n 字节信用并唤醒所有等待者。
func (p *Pool) Release(n int64) {
	p.mu.Lock()
	p.used -= n
	if p.used < 0 {
		p.used = 0
	}
	p.mu.Unlock()
	p.cond.Broadcast()
}

// Used 返回当前已占用字节数。
func (p *Pool) Used() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.used
}
