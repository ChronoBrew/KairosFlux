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

// TestLoop_RepeatedTicksAfterApplyRoundTripAreIdempotent 覆盖生产路径真正
// 走的那条链路：Apply -> CanonicalJSON -> Store -> GetLatest -> ParseJobSpec
// -> Fingerprint，而不是像其它测试那样把 Go 值直接传给 Reconcile。如果这条
// JSON 编解码往返在任何字段上有损（尤其 DependsOn/Env 这两个 nil vs 非空
// 切片/map 最容易在序列化后变得不对称的字段），拿到的指纹就会和上次写入
// job:status 时的指纹不一致，sameTrack 每次都判 false，job 会被每个 tick
// 重新执行一次——对 paper_daily 这种真实调用交易脚本的 job 是严重后果。
// 两个 job 分别覆盖 DependsOn 为 nil 与非空两种情况。
func TestLoop_RepeatedTicksAfterApplyRoundTripAreIdempotent(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(fixedTestTime())
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}

	noDeps := testSpec("no_deps_job")            // DependsOn == nil
	withDeps := testSpec("with_deps_job", "dep") // DependsOn 非空

	if _, err := Apply(store, noDeps); err != nil {
		t.Fatalf("apply no_deps_job 出错: %v", err)
	}
	if _, err := Apply(store, withDeps); err != nil {
		t.Fatalf("apply with_deps_job 出错: %v", err)
	}
	// with_deps_job 依赖 "dep"，本测试不打算让它真正执行成功（依赖永远不
	// 满足）——重点不是它能不能跑起来，是"反复 Tick 会不会因为 spec 往返
	// 失真而被判定成指纹变了、从而重复执行/重复写 pending"。

	loop := NewLoop(store, r, []string{"no_deps_job", "with_deps_job"}, 0)

	for i := 0; i < 100; i++ {
		if errs := loop.Tick(); len(errs) != 0 {
			t.Fatalf("第 %d 次 Tick 出错: %v", i, errs)
		}
	}

	if got := exec.callCount(); got != 1 {
		t.Fatalf("no_deps_job 应只被真正执行 1 次（spec 往返后指纹应保持稳定），实际 %d 次", got)
	}
	if got := store.versionCount(StatusKey(noDeps.Name)); got != 2 {
		t.Fatalf("no_deps_job 的 job:status 应有 2 条版本（①running + ④终态），实际 %d 条", got)
	}
	if got := store.versionCount(StatusKey(withDeps.Name)); got != 1 {
		t.Fatalf("with_deps_job（依赖始终不满足）的 job:status 应只有 1 条 pending 版本，不应随 Tick 次数膨胀，实际 %d 条", got)
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
