package lifecycle

import "sync/atomic"

// Lifecycle 是单个连接持有的显式状态机 + 关闭协调原语，取代此前
// connection.go 里裸的 ctx/cancel/stopOnce 组合（见 docs/rfc/bannet-重构.md
// 迁移映射表："connection.go 的 ctx/cancel/stopOnce/recoverConnGoroutine
// → lifecycle，需重写为显式状态机"）。
//
// 关键设计：Closing 与 Closed 是两个不同的、有实际语义差异的状态，分别对应
// 两个不同的广播信号：
//
//   - Draining()：Closing 时关闭。表示"已经决定要关闭了，不该再开始新的
//     工作（比如读新的一帧、分派新的请求）"，但连接尚未被物理关闭——写路径
//     （SendMsg/SendBuffMsg 排队、Writer 把队列里的内容发出去）仍然正常
//     工作。这是本次重构修复 bug①的关键：如果 Closing 和物理关闭共用同一个
//     信号，"优雅关闭"就会立刻掐断写路径，在途请求处理完之后产出的响应会
//     因为连接"已经关闭"而发不出去——这正是 bug① 的病因。
//   - Done()：Closed 时关闭。表示"资源已经/即将被物理释放"（socket 关闭、
//     从连接表移除），此后不应该再有任何新的写操作。
//
// 两者的收敛关系：Draining 关闭不代表 Done 也关闭；反过来 Done 关闭时
// Draining 一定已经关闭（Close 内部保证）。调用方（transport 层）负责在
// 观察到 Draining 后，排空所有已经产生的写操作，再调用 Close。
type Lifecycle struct {
	state atomic.Int32

	drainCh chan struct{}
	drainOn atomic.Bool // 保证 drainCh 只 close 一次

	closeCh chan struct{}
	closeOn atomic.Bool // 保证 closeCh 只 close 一次

	started atomic.Bool // MarkActive 是否被调用过，供 Stop 判断是否需要等 Writer
}

// New 构造一个处于 Idle 状态的 Lifecycle。
func New() *Lifecycle {
	l := &Lifecycle{
		drainCh: make(chan struct{}),
		closeCh: make(chan struct{}),
	}
	l.state.Store(int32(Idle))
	return l
}

// State 返回当前状态，可在任意 goroutine 安全调用。
func (l *Lifecycle) State() State {
	return State(l.state.Load())
}

// Started 返回 MarkActive 是否被调用过——供调用方判断"这个连接是否真的
// 启动过收发 goroutine"，用于决定物理关闭时要不要等待 Writer 排空
// （从未启动过的连接没有 Writer 可等）。
func (l *Lifecycle) Started() bool {
	return l.started.Load()
}

// MarkActive 把状态从 Idle 推进到 Active（连接的收发 goroutine 已启动）。
// 若当前已经不是 Idle（比如极端时序下 Transition 抢先把它推进到了
// Closing），本调用是安全的空操作——不会把状态"拉回" Active。
func (l *Lifecycle) MarkActive() {
	l.started.Store(true)
	l.state.CompareAndSwap(int32(Idle), int32(Active))
}

// Transition 把四种（及以上）终止诱因统一收敛到 Closing——见
// docs/rfc/bannet-重构.md C.1："Closing 是收敛点：无论触发原因是什么，
// 都进入同一个状态"。event 只用于调用方自行记录日志，不影响收敛路径。
//
// 幂等：只有第一次成功的调用（从 Idle 或 Active 转到 Closing）会真正关闭
// drainCh；后续并发/重复调用是安全的空操作。返回值表示本次调用是否是
// "第一次"——调用方可用它判断日志里该记的 event 是否会真的生效（比如
// panic 恢复与显式 Stop 同时发生时，只有先到的那个的 event 会被记为
// "生效的收敛原因"）。
func (l *Lifecycle) Transition(event Event) bool {
	won := l.state.CompareAndSwap(int32(Idle), int32(Closing)) ||
		l.state.CompareAndSwap(int32(Active), int32(Closing))
	if won && l.drainOn.CompareAndSwap(false, true) {
		close(l.drainCh)
	}
	return won
}

// Draining 在状态进入 Closing 时关闭——语义见本文件顶部注释：只表示
// "决定关闭"，写路径仍然打开。
func (l *Lifecycle) Draining() <-chan struct{} {
	return l.drainCh
}

// Close 把状态推进到 Closed，广播"物理关闭"信号。返回值表示本次调用是否
// 是第一次真正执行了这个转换——调用方应据此判断是否需要执行只做一次的物理
// 清理动作（关 socket、从注册表移除、触发回调），常见用法：
//
//	if !lc.Close() { return } // 已经有人做过了，本次是空操作
//	// ...只会执行一次的物理清理...
//
// Close 会隐式先完成 Draining（若尚未进入 Closing，直接跳到 Closed 之前
// 也会关闭 drainCh）——Closed 蕴含 Draining，反之不成立。
func (l *Lifecycle) Close() bool {
	// 无论当前是 Idle/Active/Closing，都允许直接推进到 Closed——不要求
	// 调用方一定先手动过一遍 Transition，比如从未启动过的连接直接 Stop()
	// 的场景。
	if l.drainOn.CompareAndSwap(false, true) {
		close(l.drainCh)
	}
	if !l.closeOn.CompareAndSwap(false, true) {
		return false
	}
	l.state.Store(int32(Closed))
	close(l.closeCh)
	return true
}

// Done 在状态进入 Closed 时关闭：此后不应该再有任何新的写操作，连接的
// 底层资源已经或即将被释放。
func (l *Lifecycle) Done() <-chan struct{} {
	return l.closeCh
}
