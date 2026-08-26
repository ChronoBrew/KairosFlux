package jobctl

import (
	"testing"
	"time"
)

func testSpec(name string, deps ...string) JobSpec {
	return JobSpec{
		Name:                    name,
		Command:                 []string{"/bin/true"},
		ScheduleIntervalSeconds: 86400,
		MaxRetries:              2,
		RetryBackoffSeconds:     60,
		DependsOn:               deps,
	}
}

// TestReconcile_TenThousandRerunsAreIdempotent 是任务书的核心验收标准："同一
// Job 重跑一万次，结果与账本一致"。固定 Clock（同一个 slot）、固定 spec、
// 一直成功的 Executor，反复调用 Reconcile 一万次：只应该真正执行一次、
// 只应该往 job:events 写一条版本、job:status 写两条版本（①running + ④终态
// ——写序修复后的固定两段，见 reconciler.go 的 Reconcile 文档）——用幂等键
// （slot + spec 指纹）证明其余 9999 次调用都是纯粹的读、不产生任何新副作用。
func TestReconcile_TenThousandRerunsAreIdempotent(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC))
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}
	spec := testSpec("idempotent_job")

	const reruns = 10000
	start := time.Now()
	var lastStatus JobStatus
	for i := 0; i < reruns; i++ {
		status, err := r.Reconcile(spec)
		if err != nil {
			t.Fatalf("第 %d 次 Reconcile 出错: %v", i, err)
		}
		lastStatus = status
	}
	elapsed := time.Since(start)
	t.Logf("一万次重跑真实耗时: %s", elapsed)

	if got := exec.callCount(); got != 1 {
		t.Fatalf("Executor 应只被真正调用 1 次，实际 %d 次", got)
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 1 {
		t.Fatalf("job:events 应只有 1 条版本，实际 %d 条", got)
	}
	if got := store.versionCount(StatusKey(spec.Name)); got != 2 {
		t.Fatalf("job:status 应有 2 条版本（①running + ④终态），实际 %d 条", got)
	}
	if lastStatus.Phase != PhaseSucceeded {
		t.Fatalf("最终 phase 应为 succeeded，实际 %s", lastStatus.Phase)
	}
	if lastStatus.Attempt != 1 {
		t.Fatalf("attempt 应为 1，实际 %d", lastStatus.Attempt)
	}
}

// TestReconcile_RetriesThenAlertsAfterMaxRetriesExhausted 验证"失败自动
// 重试、重试仍失败告警"这条验收标准：MaxRetries=2 意味着最多尝试 3 次
// （首次 + 2 次重试），第 3 次仍失败后转 Alerting 终态，AlertSink 被调用
// 恰好一次，job:events 恰好 3 条（每次尝试一条）。
func TestReconcile_RetriesThenAlertsAfterMaxRetriesExhausted(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC))
	exec := &countingExecutor{results: []ExecResult{
		{ExitCode: 1, Stderr: "第一次失败"},
		{ExitCode: 1, Stderr: "第二次失败"},
		{ExitCode: 1, Stderr: "第三次失败"},
	}}
	sink := &nullAlertSink{}
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: sink}
	spec := testSpec("flaky_job")

	// 第 1 次尝试：立即失败，进入 failed，退避 60s。
	status, err := r.Reconcile(spec)
	if err != nil {
		t.Fatalf("第 1 次 Reconcile 出错: %v", err)
	}
	if status.Phase != PhaseFailed || status.Attempt != 1 {
		t.Fatalf("第 1 次后应为 failed/attempt=1，实际 %+v", status)
	}

	// 退避未到期：不应该重试。
	status, err = r.Reconcile(spec)
	if err != nil {
		t.Fatalf("退避未到期时 Reconcile 出错: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("退避未到期不应重试，Executor 调用次数应仍为 1，实际 %d", exec.callCount())
	}
	if status.Attempt != 1 {
		t.Fatalf("退避未到期状态不应变化，attempt 仍应为 1，实际 %d", status.Attempt)
	}

	// 退避到期，第 2 次尝试：仍失败。
	clock.Advance(61 * time.Second)
	status, err = r.Reconcile(spec)
	if err != nil {
		t.Fatalf("第 2 次 Reconcile 出错: %v", err)
	}
	if status.Phase != PhaseFailed || status.Attempt != 2 {
		t.Fatalf("第 2 次后应为 failed/attempt=2，实际 %+v", status)
	}

	// 退避到期，第 3 次尝试：仍失败，MaxRetries(2)+1=3 次已耗尽 -> alerting。
	clock.Advance(61 * time.Second)
	status, err = r.Reconcile(spec)
	if err != nil {
		t.Fatalf("第 3 次 Reconcile 出错: %v", err)
	}
	if status.Phase != PhaseAlerting || status.Attempt != 3 {
		t.Fatalf("第 3 次后应为 alerting/attempt=3，实际 %+v", status)
	}
	if exec.callCount() != 3 {
		t.Fatalf("Executor 应恰好被调用 3 次，实际 %d 次", exec.callCount())
	}
	if len(sink.alerts) != 1 {
		t.Fatalf("AlertSink 应恰好被调用 1 次，实际 %d 次", len(sink.alerts))
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 3 {
		t.Fatalf("job:events 应恰好 3 条（每次尝试一条），实际 %d 条", got)
	}

	// 已是终态：再重跑一万次也不应该再触发任何新的执行/事件/告警。
	for i := 0; i < 10000; i++ {
		if _, err := r.Reconcile(spec); err != nil {
			t.Fatalf("终态后第 %d 次 Reconcile 出错: %v", i, err)
		}
	}
	if exec.callCount() != 3 {
		t.Fatalf("alerting 终态后不应再执行，Executor 调用次数应仍为 3，实际 %d", exec.callCount())
	}
	if len(sink.alerts) != 1 {
		t.Fatalf("alerting 终态后不应再告警，AlertSink 调用次数应仍为 1，实际 %d", len(sink.alerts))
	}
}

// TestReconcile_WaitsOnUnsatisfiedDependency 验证依赖解析：依赖未满足时
// 不执行，phase=pending；依赖满足后才真正执行。
func TestReconcile_WaitsOnUnsatisfiedDependency(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC))
	childExec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: childExec, Clock: clock, AlertSink: &nullAlertSink{}}
	child := testSpec("child_job", "parent_job")

	status, err := r.Reconcile(child)
	if err != nil {
		t.Fatalf("Reconcile 出错: %v", err)
	}
	if status.Phase != PhasePending {
		t.Fatalf("依赖未满足时应为 pending，实际 %s", status.Phase)
	}
	if childExec.callCount() != 0 {
		t.Fatalf("依赖未满足时不应执行，实际调用了 %d 次", childExec.callCount())
	}

	// 重复 reconcile：pending 状态内容不变，不应产生新的 status 版本
	// （幂等：等待中的中间态不随轮询次数膨胀账本）。
	for i := 0; i < 100; i++ {
		if _, err := r.Reconcile(child); err != nil {
			t.Fatalf("第 %d 次 Reconcile 出错: %v", i, err)
		}
	}
	if got := store.versionCount(StatusKey(child.Name)); got != 1 {
		t.Fatalf("pending 期间 job:status 应只有 1 条版本，实际 %d 条", got)
	}

	// parent 成功后，child 应该能真正执行。
	parentExec := newCountingExecutor(ExecResult{ExitCode: 0})
	parentR := &Reconciler{Store: store, Executor: parentExec, Clock: clock, AlertSink: &nullAlertSink{}}
	parent := testSpec("parent_job")
	if _, err := parentR.Reconcile(parent); err != nil {
		t.Fatalf("parent Reconcile 出错: %v", err)
	}

	status, err = r.Reconcile(child)
	if err != nil {
		t.Fatalf("依赖满足后 Reconcile 出错: %v", err)
	}
	if status.Phase != PhaseSucceeded {
		t.Fatalf("依赖满足后应执行成功，实际 %s", status.Phase)
	}
	if childExec.callCount() != 1 {
		t.Fatalf("依赖满足后应执行恰好 1 次，实际 %d 次", childExec.callCount())
	}
}

// TestReconcile_NewSlotTriggersFreshExecution 验证调度节律翻篇后（进入下一
// 个 slot）会重新执行，即便上一个 slot 已经是终态 succeeded。
func TestReconcile_NewSlotTriggersFreshExecution(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC))
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}
	spec := testSpec("daily_job")

	if _, err := r.Reconcile(spec); err != nil {
		t.Fatalf("第一个 slot Reconcile 出错: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("第一个 slot 应执行 1 次，实际 %d 次", exec.callCount())
	}

	clock.Advance(24 * time.Hour) // 翻到下一个 daily slot

	status, err := r.Reconcile(spec)
	if err != nil {
		t.Fatalf("第二个 slot Reconcile 出错: %v", err)
	}
	if status.Attempt != 1 {
		t.Fatalf("新 slot 的 attempt 应重新从 1 起，实际 %d", status.Attempt)
	}
	if exec.callCount() != 2 {
		t.Fatalf("第二个 slot 应再执行 1 次（累计 2 次），实际 %d 次", exec.callCount())
	}
	if got := store.versionCount(EventsKey(spec.Name)); got != 2 {
		t.Fatalf("两个 slot 各执行一次，job:events 应有 2 条，实际 %d 条", got)
	}
}

// TestReconcile_SpecChangeMidSlotTriggersFreshExecution 验证"spec 变了要
// 重新触发首次执行，即便 slot 相同"——指纹是幂等键的一部分，不只是 slot。
func TestReconcile_SpecChangeMidSlotTriggersFreshExecution(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC))
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}
	spec := testSpec("mutable_job")

	if _, err := r.Reconcile(spec); err != nil {
		t.Fatalf("初次 Reconcile 出错: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("初次应执行 1 次，实际 %d 次", exec.callCount())
	}

	spec.Command = []string{"/bin/true", "--changed"} // 改变 spec -> 指纹变化

	status, err := r.Reconcile(spec)
	if err != nil {
		t.Fatalf("spec 变化后 Reconcile 出错: %v", err)
	}
	if status.Attempt != 1 {
		t.Fatalf("spec 变化后应重新从 attempt=1 起，实际 %d", status.Attempt)
	}
	if exec.callCount() != 2 {
		t.Fatalf("spec 变化应触发重新执行（累计 2 次），实际 %d 次", exec.callCount())
	}
}

func TestReconcile_RejectsInvalidSpec(t *testing.T) {
	store := newFakeStore()
	r := NewReconciler(store)
	_, err := r.Reconcile(JobSpec{Name: "bad"}) // 缺 Command 与 ScheduleIntervalSeconds
	if err == nil {
		t.Fatal("非法 spec 应返回错误")
	}
}
