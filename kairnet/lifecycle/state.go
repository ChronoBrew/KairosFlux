// Package lifecycle 是连接生命周期层：一个显式的状态机（Idle→Active→
// Closing→Closed），取代此前散落在 connection.go 里的裸
// context.Context+sync.Once 组合——见 docs/rfc/bannet-重构.md B.2/C.1：
// 重构前没有任何地方能回答"这个连接现在处于什么状态"，状态只存在于一堆
// 副作用的组合里。本包不认字节、不认业务，只认"事件 → 状态转换"这一件事。
package lifecycle

// State 是连接生命周期的四个显式状态，对应 docs/rfc/bannet-重构.md C.1
// 的状态图：Idle -> Active -> Closing -> Closed。
type State int32

const (
	// Idle 已构造，尚未启动收发 goroutine。
	Idle State = iota
	// Active 正常收发中。
	Active
	// Closing 已决定关闭，清理进行中——幂等，可能被多个触发源并发进入，
	// 但只会实际执行一次收敛；处于这个状态时连接的写路径仍然打开
	// （见 Lifecycle.Draining 与 Lifecycle.Done 的注释），这是区分
	// Closing 与 Closed 的关键：不能把两者混为一谈，否则会重新引入
	// "响应因连接已关闭而发不出去"这个竞态。
	Closing
	// Closed 清理完成，资源已释放。
	Closed
)

func (s State) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Active:
		return "Active"
	case Closing:
		return "Closing"
	case Closed:
		return "Closed"
	default:
		return "Unknown"
	}
}
