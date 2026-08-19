package bannet

// 同包内测试：直接调用未导出的 recoverConnGoroutine，验证第四种终止诱因
// （panic 被 recover）真的会把状态机推进到 Closing。
//
// 为什么不构造一个真的会在 StartReader/StartWriter 里 panic 的连接来做
// 端到端验证：业务 Handler 的 panic 由 dispatch.MsgHandle.DoMsgHandle 自己
// 的 recover 兜住，根本不会传到这一层（见 panic_recovery_test.go 的
// TestHandlerPanicRecovered，测的是另一个机制）；而人为制造一个会在
// StartReader/StartWriter 内部真正 panic 的场景（比如给 Connection.Conn
// 塞一个 nil *net.TCPConn），会导致 Stop() 的物理清理阶段（c.Conn.Close()）
// 也对着 nil 指针再 panic 一次——这第二次 panic 不在任何 recover 的保护
// 范围内，会直接打断测试进程，而不是给出一个干净的测试失败。直接调用
// recoverConnGoroutine 这个 StartReader/StartWriter/Start 三者共享的真实
// 函数，是能验证到"panic 被 recover 时会调用 lc.Transition(EventPanicRecovered)"
// 这条具体接线、又不引入上述副作用的最小验证方式。
import (
	"testing"

	"github.com/NeverENG/BanDB/bannet/lifecycle"
)

func TestRecoverConnGoroutineTransitionsLifecycleToClosing(t *testing.T) {
	lc := lifecycle.New()
	lc.MarkActive()

	func() {
		defer recoverConnGoroutine(1, "test", lc)
		panic("simulated panic inside a connection goroutine")
	}()

	if got := lc.State(); got != lifecycle.Closing {
		t.Fatalf("State() = %v, want %v（panic 被 recover 之后应该收敛到 Closing）", got, lifecycle.Closing)
	}
}

// TestRecoverConnGoroutineNilLifecycleIsSafe 覆盖 CallConnStartFunc/
// CallConnStopFunc 传 lc=nil 的场景（见 server.go）：即便没有 lifecycle
// 可用，recover 本身仍然必须生效，不能因为 lc 是 nil 就 panic。
func TestRecoverConnGoroutineNilLifecycleIsSafe(t *testing.T) {
	func() {
		defer recoverConnGoroutine(1, "test", nil)
		panic("simulated panic with no lifecycle attached")
	}()
	// 能跑到这里就说明 recover 生效、且 lc==nil 没有导致二次 panic。
}
