package lifecycle

// Event 标记触发 Closing 的原因，只用于日志/可观测性——不影响状态机的收敛
// 路径：无论 event 是什么，效果都是"决定关闭"，四种（及以上）原因都归约成
// 同一次 Transition 调用，见 docs/rfc/bannet-重构.md C.1："Closing 是收敛点：
// 无论触发原因是什么，都进入同一个状态，用同一套清理逻辑处理"。
//
// 本次重构任务要求的四种终止诱因（EOF/超时/显式 Stop/panic 被 recover）
// 各有对应的 Event 常量；另外两个（EventReadError/EventWriteError）覆盖
// RFC 状态图里"读错误/写错误"这个更宽的分类（如帧解析失败、超限帧、写
// 失败），不是任务要求的四种之一，但同样收敛到 Closing——多出来的分类
// 不违反"四种诱因都收敛到 Closing"这个要求，只是让日志更精确。
type Event int

const (
	// EventEOF 对端在帧边界正常断开（io.EOF）。
	EventEOF Event = iota
	// EventReadTimeout 读超时（net.Error 且 Timeout()==true）。
	EventReadTimeout
	// EventReadError 其它读/解码错误（帧中途断开、解析失败、超限帧等）。
	EventReadError
	// EventWriteError 写失败（对端不读、连接已断等）。
	EventWriteError
	// EventExplicitStop 显式调用 Stop()（无论是外部直接调用，还是
	// Server.Stop() 优雅关闭广播的一部分）。
	EventExplicitStop
	// EventPanicRecovered 连接的收发 goroutine 中一个 panic 被 recover。
	EventPanicRecovered
)

func (e Event) String() string {
	switch e {
	case EventEOF:
		return "EOF"
	case EventReadTimeout:
		return "ReadTimeout"
	case EventReadError:
		return "ReadError"
	case EventWriteError:
		return "WriteError"
	case EventExplicitStop:
		return "ExplicitStop"
	case EventPanicRecovered:
		return "PanicRecovered"
	default:
		return "Unknown"
	}
}
