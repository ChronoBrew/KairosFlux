package jobctl

import "testing"

func TestApply_WritesValidatedSpecAndRejectsInvalid(t *testing.T) {
	store := newFakeStore()
	spec := testSpec("loop_job")

	seq, err := Apply(store, spec)
	if err != nil {
		t.Fatalf("apply 出错: %v", err)
	}
	if seq != 1 {
		t.Fatalf("首次 apply 的 seq 应为 1，实际 %d", seq)
	}

	if _, err := Apply(store, JobSpec{Name: "bad"}); err == nil {
		t.Fatal("非法 spec 的 apply 应被拒绝")
	}
}

func TestLoop_TickReadsSpecAndReconcilesInSortedOrder(t *testing.T) {
	store := newFakeStore()
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	clock := newFakeClock(fixedTestTime())
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}

	specB := testSpec("b_job")
	specA := testSpec("a_job")
	if _, err := Apply(store, specB); err != nil {
		t.Fatalf("apply b_job 出错: %v", err)
	}
	if _, err := Apply(store, specA); err != nil {
		t.Fatalf("apply a_job 出错: %v", err)
	}

	loop := NewLoop(store, r, []string{"b_job", "a_job"}, 0)
	if loop.JobNames[0] != "a_job" || loop.JobNames[1] != "b_job" {
		t.Fatalf("JobNames 应按字典序排好: %v", loop.JobNames)
	}

	errs := loop.Tick()
	if len(errs) != 0 {
		t.Fatalf("Tick 不应报错: %v", errs)
	}
	if exec.callCount() != 2 {
		t.Fatalf("两个 job 都应被执行，实际调用次数 %d", exec.callCount())
	}
}

func TestLoop_TickReportsErrorForMissingSpec(t *testing.T) {
	store := newFakeStore()
	r := NewReconciler(store)
	loop := NewLoop(store, r, []string{"never_applied"}, 0)

	errs := loop.Tick()
	if len(errs) != 1 {
		t.Fatalf("应有 1 条错误（spec 未 apply），实际 %d 条: %v", len(errs), errs)
	}
	if errs[0].JobName != "never_applied" {
		t.Fatalf("错误应指向 never_applied，实际 %+v", errs[0])
	}
}
