package jobctl

// recovery_test.go 覆盖 M3 已知局限（"Reconcile 写 event 然后写 status 两次
// 独立调用;中途崩溃→status 陈旧→重启后重执行+重复 event"）的修复：写序改为
// ①status→running ②执行 ③event 落盘 ④status→终态，并在进程启动、tick 循环
// 之前跑启动恢复扫描（Reconciler.Recover）把 status 重建为事件账本的派生
// 视图（"状态=事件的函数"，M4 裁决）。
//
// 三个崩溃点注入各一条（panic 模拟进程死亡；崩溃现场 = 真实 Reconcile 写到
// 一半的持久状态，恢复扫描看到的正是这些状态）：
//
//   - 崩溃点①：①status→running 写后崩溃 → 恢复后按幂等键 (slot, fp) 重执行
//     恰好一次、补 event，事件账本恰好一条。
//   - 崩溃点②：③event 落盘后、④终态 status 写前崩溃 → 以 event 重建终态
//     status，不重执行、账本不重复——修复的核心断言（旧行为会重执行 + 重复
//     event）。
//   - 崩溃点③：M3 原文窗口的遗留现场（旧代码写 event 后、写 status 前崩溃，
//     status 停留在陈旧终态）→ 以最新 event 为准重建 status，不重执行、
//     不产生重复 event。
//
// 另有一条扩展用例：失败重试执行的事件落盘后崩溃 → 从最新匹配 event 重建
// failed（退避锚点跟着账本走，不烧重试预算、账本不重复）。

import (
	"context"
	"sync"
	"testing"
	"time"
)

// panicExecutor 是崩溃点①的注入执行体：Run 一被调用就 panic（等价于进程在
// 执行点死亡）。崩溃前的持久状态由 Reconcile 的写入顺序决定——①status→running
// 已落盘、事件账本还没有任何记录，这正是崩溃点①的现场。
type panicExecutor struct{}

func (panicExecutor) Run(ctx context.Context, spec JobSpec) ExecResult {
	panic("jobctl 测试注入:模拟进程崩溃于执行点")
}

// panicOnPut 是崩溃点②的注入 Store：armFailAfter(n) 武装后，从武装时刻起
// 第 n 次 PutVersioned 调用时 panic（该次写入不落盘）——模拟"进程在某次
// 写入完成之前崩溃"。未武装时纯透传。GetLatest/ListVersions 直通底层。
type panicOnPut struct {
	Store
	mu   sync.Mutex
	call int
	fail int // 0 = 未武装；否则 = 触发崩溃的绝对调用序号
}

// armFailAfter 武装崩溃注入：从当前时刻起第 n 次 PutVersioned 时崩溃。
func (s *panicOnPut) armFailAfter(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = s.call + n
}

func (s *panicOnPut) PutVersioned(logicalKey string, payload []byte, source string) (uint64, error) {
	s.mu.Lock()
	s.call++
	shouldPanic := s.fail != 0 && s.call == s.fail
	s.mu.Unlock()
	if shouldPanic {
		panic("jobctl 测试注入:模拟进程崩溃于写入中途")
	}
	return s.Store.PutVersioned(logicalKey, payload, source)
}

// mustCrash 断言注入的崩溃确实发生了，在 defer 中调用（panic 不跨测试函数）。
func mustCrash(t *testing.T, what string) {
	t.Helper()
	if x := recover(); x == nil {
		t.Fatalf("%s: 预期注入崩溃，但没有发生", what)
	}
}

// currentStatus 读回某个 job 当前（最新）的 status。
func currentStatus(t *testing.T, store Store, name string) JobStatus {
	t.Helper()
	raw, found, err := store.GetLatest(StatusKey(name))
	if err != nil {
		t.Fatalf("读 %s 状态出错: %v", name, err)
	}
	if !found {
		t.Fatalf("%s 的状态不存在", name)
	}
	st, ok := decodeJobStatus(raw)
	if !ok {
		t.Fatalf("%s 的状态无法解析: %s", name, raw)
	}
	return st
}

// currentEvents 读回某个 job 事件账本的全部事件（版本序）。
func currentEvents(t *testing.T, store Store, name string) []Event {
	t.Helper()
	versions, err := store.ListVersions(EventsKey(name))
	if err != nil {
		t.Fatalf("读 %s 事件账本出错: %v", name, err)
	}
	events, err := decodeEvents(versions)
	if err != nil {
		t.Fatalf("解析 %s 事件账本出错: %v", name, err)
	}
	return events
}

// TestRecover_CrashPointOne_AfterRunningStatusWrite_RerunsExactlyOnce 是崩溃
// 点①：①status→running 写完后、执行中崩溃。崩溃现场 = running(1) + 空账本；
// 恢复扫描修复不了"没有账本事实"的悬空执行（不写 status、更不产生 event），
// 由随后的 Reconcile 按幂等键 (slot, fingerprint) 重执行一次并补 event——
// 事件账本恰好一条、状态收敛终态。
func TestRecover_CrashPointOne_AfterRunningStatusWrite_RerunsExactlyOnce(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(fixedTestTime())
	spec := testSpec("crash_point_1")

	// 崩溃段：真实 Reconcile 驱动到崩溃点——panicExecutor 保证 ②执行 处
	// 死亡，①running 已落盘。
	crashR := &Reconciler{Store: store, Executor: panicExecutor{}, Clock: clock, AlertSink: &nullAlertSink{}}
	func() {
		defer mustCrash(t, "崩溃点①(status→running 写后)")
		crashR.Reconcile(spec)
	}()

	// 崩溃现场：running 已落盘、事件账本为空。
	st := currentStatus(t, store, spec.Name)
	if st.Phase != PhaseRunning || st.Attempt != 1 || st.Slot != spec.Slot(clock.Now()) || st.SpecFingerprint != spec.Fingerprint() {
		t.Fatalf("崩溃现场应为 running/attempt=1，实际 %+v", st)
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 0 {
		t.Fatalf("崩溃现场事件账本应为空，实际 %d 条", got)
	}

	// 重启段：同一 store 上的新 Reconciler（进程重启，内存状态清空）。
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}

	if err := r.Recover(spec.Name); err != nil {
		t.Fatalf("恢复扫描出错: %v", err)
	}
	// 恢复过程只写 status、不产生新 event 副本（修复而非重跑）。
	if got := store.versionCount(EventsKey(spec.Name)); got != 0 {
		t.Fatalf("恢复扫描不应产生 event，实际 %d 条", got)
	}
	if got := store.versionCount(StatusKey(spec.Name)); got != 1 {
		t.Fatalf("恢复扫描不应改写悬空 running，status 版本应仍为 1，实际 %d", got)
	}

	// 悬空执行由 Reconcile 按幂等键重执行一次并补 event。
	status, err := r.Reconcile(spec)
	if err != nil {
		t.Fatalf("重执行 Reconcile 出错: %v", err)
	}
	if status.Phase != PhaseSucceeded || status.Attempt != 1 {
		t.Fatalf("重执行后应收敛 succeeded/attempt=1，实际 %+v", status)
	}
	if exec.callCount() != 1 {
		t.Fatalf("重执行应恰好 1 次，实际 %d 次", exec.callCount())
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 1 {
		t.Fatalf("事件账本应恰好 1 条（重执行补上缺失的终态），实际 %d 条", got)
	}
	if got := store.versionCount(StatusKey(spec.Name)); got != 2 {
		t.Fatalf("job:status 应为 2 条版本（running + 终态），实际 %d 条", got)
	}
}

// TestRecover_CrashPointTwo_AfterEventWrite_RebuildsStatusFromEvent 是崩溃点
// ②：③event 已落盘、④终态 status 写之前崩溃（第 3 次 PutVersioned 注入
// panic）。崩溃现场 = running(1) + 一条 succeeded 事件；恢复扫描以事件重建
// 终态 status——不重执行、账本不重复（旧行为在此崩溃点会重执行 + 重复
// event，正是 M3 原文的缺陷）。
func TestRecover_CrashPointTwo_AfterEventWrite_RebuildsStatusFromEvent(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(fixedTestTime())
	spec := testSpec("crash_point_2")

	// 崩溃段：真实 Reconcile 驱动到崩溃点——前两次写入（running、event）
	// 落盘，第 3 次写入（终态 status）前死亡。
	wrapped := &panicOnPut{Store: store}
	wrapped.armFailAfter(3) // 第 3 次写入（终态 status）前崩溃
	crashR := &Reconciler{Store: wrapped, Executor: newCountingExecutor(ExecResult{ExitCode: 0}), Clock: clock, AlertSink: &nullAlertSink{}}
	func() {
		defer mustCrash(t, "崩溃点②(event 落盘后)")
		crashR.Reconcile(spec)
	}()

	// 崩溃现场：status=running + 一条 succeeded 事件。
	st := currentStatus(t, store, spec.Name)
	if st.Phase != PhaseRunning || st.Attempt != 1 {
		t.Fatalf("崩溃现场应为 running/attempt=1，实际 %+v", st)
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 1 {
		t.Fatalf("崩溃现场事件账本应有 1 条，实际 %d 条", got)
	}

	// 重启段。
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}

	if err := r.Recover(spec.Name); err != nil {
		t.Fatalf("恢复扫描出错: %v", err)
	}
	// 状态修复：以 event 重建终态——不重执行、不补 event（账本不重复）。
	if exec.callCount() != 0 {
		t.Fatalf("已落账的执行不应重执行，实际 %d 次", exec.callCount())
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 1 {
		t.Fatalf("恢复后事件账本应仍为 1 条（不产生重复 event），实际 %d 条", got)
	}
	if got := store.versionCount(StatusKey(spec.Name)); got != 2 {
		t.Fatalf("job:status 应为 2 条版本（running + 重建的终态），实际 %d 条", got)
	}

	// 重建出的 status 必须与账本事件逐字段一致（同一派生函数 jobStatusFromEvent）。
	status := currentStatus(t, store, spec.Name)
	evs := currentEvents(t, store, spec.Name)
	ev := evs[0]
	if status.Phase != PhaseSucceeded || status.Attempt != 1 ||
		status.Slot != ev.Slot || status.SpecFingerprint != ev.SpecFingerprint ||
		status.LastAttemptTime != ev.WriteNanos || status.LastVerdict != ev.Outcome ||
		status.LastExitCode != ev.ExitCode || status.Message != "ok" {
		t.Fatalf("重建 status 与事件不一致: status=%+v event=%+v", status, ev)
	}

	// 终态后再 Reconcile：no-op，账本与状态都不再增长。
	if _, err := r.Reconcile(spec); err != nil {
		t.Fatalf("终态后 Reconcile 出错: %v", err)
	}
	if exec.callCount() != 0 || store.versionCount(EventsKey(spec.Name)) != 1 || store.versionCount(StatusKey(spec.Name)) != 2 {
		t.Fatalf("终态后不应产生任何写入或执行: exec=%d events=%d status=%d",
			exec.callCount(), store.versionCount(EventsKey(spec.Name)), store.versionCount(StatusKey(spec.Name)))
	}
}

// TestRecover_CrashPointThree_LegacyStaleTerminalStatus_RebuiltFromEvent 是
// 崩溃点③：M3 原文崩溃窗口的遗留现场——旧代码"写 event 然后写 status"两步
// 独立调用，崩溃在第二步之前，status 停留在上一个 slot 的陈旧终态、新 slot
// 的 event 已经落盘。旧行为重启后把已执行过的 slot 重执行一遍 + 重复 event；
// 恢复扫描必须以最新 event 为准重建 status：不重执行、账本不重复。
func TestRecover_CrashPointThree_LegacyStaleTerminalStatus_RebuiltFromEvent(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(fixedTestTime())
	spec := testSpec("crash_point_3")
	slotA := spec.Slot(clock.Now())
	slotB := spec.Slot(clock.Now().Add(24 * time.Hour))
	fp := spec.Fingerprint()

	// 旧代码留下的现场：陈旧终态 status（slot A）+ 新 slot 的 event（slot B，
	// 事件已写、对应 status 写前崩溃）。
	stale := JobStatus{
		Phase: PhaseSucceeded, Slot: slotA, SpecFingerprint: fp, Attempt: 1,
		LastAttemptTime: clock.Now().UnixNano(), LastExitCode: 0, LastVerdict: EventSucceeded, Message: "ok",
	}
	if _, err := store.PutVersioned(StatusKey(spec.Name), encodeJobStatus(stale), EngineSource); err != nil {
		t.Fatalf("写入遗留 status 失败: %v", err)
	}
	evA := Event{Slot: slotA, Attempt: 1, SpecFingerprint: fp, Outcome: EventSucceeded, ExitCode: 0, WriteNanos: clock.Now().UnixNano()}
	evB := Event{Slot: slotB, Attempt: 1, SpecFingerprint: fp, Outcome: EventSucceeded, ExitCode: 0, WriteNanos: clock.Now().Add(24 * time.Hour).UnixNano()}
	if _, err := store.PutVersioned(EventsKey(spec.Name), encodeEvent(evA), EngineSource); err != nil {
		t.Fatalf("写入遗留事件失败: %v", err)
	}
	if _, err := store.PutVersioned(EventsKey(spec.Name), encodeEvent(evB), EngineSource); err != nil {
		t.Fatalf("写入遗留事件失败: %v", err)
	}

	// 崩溃发生在 slot B 的 event 写完后，重启时的墙钟已在 slot B 内。
	clock.Advance(24 * time.Hour)

	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}

	if err := r.Recover(spec.Name); err != nil {
		t.Fatalf("恢复扫描出错: %v", err)
	}
	// 以最新 event 为准重建 status：不重执行、账本不重复（旧行为两者都违反）。
	if exec.callCount() != 0 {
		t.Fatalf("已落账的执行不应重执行，实际 %d 次", exec.callCount())
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 2 {
		t.Fatalf("事件账本应保持 2 条（不产生重复 event），实际 %d 条", got)
	}
	status := currentStatus(t, store, spec.Name)
	if status.Phase != PhaseSucceeded || status.Slot != slotB || status.Attempt != 1 {
		t.Fatalf("status 应以最新 event（slot B）为准重建，实际 %+v", status)
	}
	if got := store.versionCount(StatusKey(spec.Name)); got != 2 {
		t.Fatalf("job:status 应为 2 条版本（遗留 + 重建），实际 %d 条", got)
	}

	// 重建后再 Reconcile：已是终态，no-op。
	if _, err := r.Reconcile(spec); err != nil {
		t.Fatalf("重建后 Reconcile 出错: %v", err)
	}
	if exec.callCount() != 0 {
		t.Fatalf("重建终态后不应再执行，实际 %d 次", exec.callCount())
	}
}

// TestRecover_CrashAfterFailedEvent_RebuildsFailedWithLedgerBackoffAnchor 是
// 扩展崩溃点：失败重试执行（attempt=2）的事件落盘后、failed status 写前
// 崩溃。崩溃现场 = running(2) + [failed(1), failed(2)]。恢复扫描从最新匹配
// event 重建 failed(2)，退避锚点（LastAttemptTime）跟着账本走——不烧重试
// 预算、不重复账本；退避到期后从 attempt=3 继续。
func TestRecover_CrashAfterFailedEvent_RebuildsFailedWithLedgerBackoffAnchor(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(fixedTestTime())
	spec := testSpec("crash_failed_rebuild")

	// 第一轮（不注入）：attempt=1 失败 → failed(1)。
	wrapped := &panicOnPut{Store: store}
	exec := newCountingExecutor(ExecResult{ExitCode: 1, Stderr: "第一次失败"})
	r := &Reconciler{Store: wrapped, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}
	status, err := r.Reconcile(spec)
	if err != nil {
		t.Fatalf("第 1 次 Reconcile 出错: %v", err)
	}
	if status.Phase != PhaseFailed || status.Attempt != 1 {
		t.Fatalf("第 1 次后应为 failed/attempt=1，实际 %+v", status)
	}

	// 第二轮（崩溃注入）：退避到期后重试 attempt=2，running(2)[写1]✓、
	// event(failed,2)[写2]✓、failed(2) status[写3]→崩溃。
	clock.Advance(61 * time.Second)
	wrapped.armFailAfter(3)
	func() {
		defer mustCrash(t, "崩溃点(failed event 落盘后)")
		r.Reconcile(spec)
	}()

	// 崩溃现场：status=running(2) + 两条失败事件。
	st := currentStatus(t, store, spec.Name)
	if st.Phase != PhaseRunning || st.Attempt != 2 {
		t.Fatalf("崩溃现场应为 running/attempt=2，实际 %+v", st)
	}
	evs := currentEvents(t, store, spec.Name)
	if len(evs) != 2 || evs[0].Outcome != EventFailed || evs[1].Outcome != EventFailed || evs[1].Attempt != 2 {
		t.Fatalf("崩溃现场账本应为两条失败事件（attempt=1,2），实际 %+v", evs)
	}

	// 重启段。
	newExec := newCountingExecutor(ExecResult{ExitCode: 1, Stderr: "第二次失败"})
	r2 := &Reconciler{Store: store, Executor: newExec, Clock: clock, AlertSink: &nullAlertSink{}}
	if err := r2.Recover(spec.Name); err != nil {
		t.Fatalf("恢复扫描出错: %v", err)
	}
	// 从最新匹配 event 重建 failed(2)：不重执行，退避锚点 = 账本事件时间。
	if newExec.callCount() != 0 {
		t.Fatalf("重建不应重执行，实际 %d 次", newExec.callCount())
	}
	status = currentStatus(t, store, spec.Name)
	if status.Phase != PhaseFailed || status.Attempt != 2 {
		t.Fatalf("应重建为 failed/attempt=2，实际 %+v", status)
	}
	if status.LastAttemptTime != evs[1].WriteNanos {
		t.Fatalf("退避锚点应跟账本（attempt=2 事件时间），实际 %d vs %d", status.LastAttemptTime, evs[1].WriteNanos)
	}

	// 退避未到期：Reconcile no-op，不执行、不写（status 版本停在 4：
	// running(1)+failed(1)+running(2)+重建 failed(2)）。
	if _, err := r2.Reconcile(spec); err != nil {
		t.Fatalf("退避未到期 Reconcile 出错: %v", err)
	}
	if newExec.callCount() != 0 || store.versionCount(StatusKey(spec.Name)) != 4 {
		t.Fatalf("退避未到期不应执行或写状态: exec=%d status=%d", newExec.callCount(), store.versionCount(StatusKey(spec.Name)))
	}

	// 退避到期：重试 attempt=3，账本不重复（恰好 3 条）。
	clock.Advance(61 * time.Second)
	status, err = r2.Reconcile(spec)
	if err != nil {
		t.Fatalf("退避到期 Reconcile 出错: %v", err)
	}
	if status.Attempt != 3 {
		t.Fatalf("重试应接 attempt=3，实际 %d", status.Attempt)
	}
	if newExec.callCount() != 1 {
		t.Fatalf("重试应执行恰好 1 次，实际 %d 次", newExec.callCount())
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 3 {
		t.Fatalf("事件账本应恰好 3 条（不重复），实际 %d 条", got)
	}
}
