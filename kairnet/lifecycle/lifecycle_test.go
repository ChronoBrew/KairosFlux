package lifecycle

import (
	"sync"
	"testing"
)

func TestNewIsIdle(t *testing.T) {
	l := New()
	if got := l.State(); got != Idle {
		t.Fatalf("初始状态 = %v, want %v", got, Idle)
	}
	if l.Started() {
		t.Fatal("未 MarkActive 前 Started() 应为 false")
	}
	select {
	case <-l.Draining():
		t.Fatal("Idle 状态下 Draining() 不应已关闭")
	default:
	}
	select {
	case <-l.Done():
		t.Fatal("Idle 状态下 Done() 不应已关闭")
	default:
	}
}

func TestMarkActiveTransitionsFromIdle(t *testing.T) {
	l := New()
	l.MarkActive()
	if got := l.State(); got != Active {
		t.Fatalf("State() = %v, want %v", got, Active)
	}
	if !l.Started() {
		t.Fatal("MarkActive 之后 Started() 应为 true")
	}
}

func TestMarkActiveNoopAfterClosing(t *testing.T) {
	l := New()
	l.Transition(EventExplicitStop) // Idle -> Closing
	l.MarkActive()                  // 不应把状态拉回 Active
	if got := l.State(); got != Closing {
		t.Fatalf("State() = %v, want %v（MarkActive 不应覆盖已经进入的 Closing）", got, Closing)
	}
}

func TestTransitionFromIdleReachesClosingAndDrains(t *testing.T) {
	l := New()
	won := l.Transition(EventEOF)
	if !won {
		t.Fatal("从 Idle 出发的 Transition 应该是第一次生效的调用")
	}
	if got := l.State(); got != Closing {
		t.Fatalf("State() = %v, want %v", got, Closing)
	}
	select {
	case <-l.Draining():
	default:
		t.Fatal("Transition 成功后 Draining() 应已关闭")
	}
	select {
	case <-l.Done():
		t.Fatal("仅 Transition（未 Close）时 Done() 不应关闭——Closing 与 Closed 是两个不同的信号")
	default:
	}
}

func TestTransitionFromActiveReachesClosing(t *testing.T) {
	l := New()
	l.MarkActive()
	if got := l.State(); got != Active {
		t.Fatalf("前置条件失败: State() = %v, want %v", got, Active)
	}
	if !l.Transition(EventReadTimeout) {
		t.Fatal("从 Active 出发的 Transition 应该是第一次生效的调用")
	}
	if got := l.State(); got != Closing {
		t.Fatalf("State() = %v, want %v", got, Closing)
	}
}

// TestFourTerminationCausesAllConvergeToClosing 覆盖任务要求的四种终止诱因
// （EOF/超时/显式 Stop/panic 被 recover），逐个断言都收敛到同一个 Closing
// 状态——不是各自一套状态，见 docs/rfc/bannet-重构.md C.1。
func TestFourTerminationCausesAllConvergeToClosing(t *testing.T) {
	causes := []Event{EventEOF, EventReadTimeout, EventExplicitStop, EventPanicRecovered}
	for _, event := range causes {
		t.Run(event.String(), func(t *testing.T) {
			l := New()
			l.MarkActive()
			l.Transition(event)
			if got := l.State(); got != Closing {
				t.Fatalf("event=%v 之后 State() = %v, want %v", event, got, Closing)
			}
		})
	}
}

func TestTransitionIsIdempotentOnlyFirstWins(t *testing.T) {
	l := New()
	l.MarkActive()

	first := l.Transition(EventEOF)
	second := l.Transition(EventExplicitStop)

	if !first {
		t.Fatal("第一次 Transition 应该生效")
	}
	if second {
		t.Fatal("第二次 Transition（状态已经是 Closing）不应再次生效")
	}
	if got := l.State(); got != Closing {
		t.Fatalf("State() = %v, want %v", got, Closing)
	}
}

// TestConcurrentTransitionOnlyOneWins 用并发触发验证 Transition 在竞态下
// 仍然只有一次真正生效（Closing 只会被广播一次）——这是"可能被多个触发源
// 并发进入，但只执行一次实际清理"这条不变量的直接验证，跑 -race 时尤其
// 有意义。
func TestConcurrentTransitionOnlyOneWins(t *testing.T) {
	l := New()
	l.MarkActive()

	const n = 50
	var wg sync.WaitGroup
	wins := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wins[i] = l.Transition(EventReadError)
		}(i)
	}
	wg.Wait()

	winCount := 0
	for _, w := range wins {
		if w {
			winCount++
		}
	}
	if winCount != 1 {
		t.Fatalf("并发 Transition 中生效次数 = %d, want 1", winCount)
	}
	if got := l.State(); got != Closing {
		t.Fatalf("State() = %v, want %v", got, Closing)
	}
}

func TestCloseTransitionsToClosedAndClosesBothSignals(t *testing.T) {
	l := New()
	l.MarkActive()
	l.Transition(EventEOF) // Active -> Closing

	won := l.Close()
	if !won {
		t.Fatal("第一次 Close 应该生效")
	}
	if got := l.State(); got != Closed {
		t.Fatalf("State() = %v, want %v", got, Closed)
	}
	select {
	case <-l.Done():
	default:
		t.Fatal("Close 之后 Done() 应已关闭")
	}
	select {
	case <-l.Draining():
	default:
		t.Fatal("Close 之后 Draining() 也应已关闭（Closed 蕴含 Draining）")
	}
}

// TestCloseWithoutPriorTransition 覆盖"从未 Transition 就直接 Close"的场景
// （比如连接从未真正进入过某个显式关闭流程，直接调用 Stop）：Close 应该
// 隐式补上 Draining 的广播，不要求调用方一定先手动调用 Transition。
func TestCloseWithoutPriorTransition(t *testing.T) {
	l := New()
	if !l.Close() {
		t.Fatal("从 Idle 直接 Close 应该生效")
	}
	if got := l.State(); got != Closed {
		t.Fatalf("State() = %v, want %v", got, Closed)
	}
	select {
	case <-l.Draining():
	default:
		t.Fatal("直接 Close 也应该隐式关闭 Draining")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	l := New()
	l.MarkActive()
	l.Transition(EventExplicitStop)

	first := l.Close()
	second := l.Close()
	if !first {
		t.Fatal("第一次 Close 应该生效")
	}
	if second {
		t.Fatal("第二次 Close 不应再次生效")
	}
}

// TestConcurrentCloseOnlyOneWins 与 TestConcurrentTransitionOnlyOneWins 对称，
// 验证 Close 在并发调用下同样只有一次生效——这是 Connection.Stop() 依赖的
// 不变量：多个触发源（比如 EOF 与 Server.Stop() 几乎同时发生）并发调用
// Stop 时，物理清理动作只应该真正执行一次。
func TestConcurrentCloseOnlyOneWins(t *testing.T) {
	l := New()
	l.MarkActive()

	const n = 50
	var wg sync.WaitGroup
	wins := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wins[i] = l.Close()
		}(i)
	}
	wg.Wait()

	winCount := 0
	for _, w := range wins {
		if w {
			winCount++
		}
	}
	if winCount != 1 {
		t.Fatalf("并发 Close 中生效次数 = %d, want 1", winCount)
	}
}
