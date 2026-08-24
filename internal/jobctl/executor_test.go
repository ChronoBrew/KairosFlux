package jobctl

import (
	"context"
	"strings"
	"testing"
)

func TestCmdExecutor_SucceedsAndCapturesStdout(t *testing.T) {
	spec := JobSpec{
		Name:                    "echo_job",
		Command:                 []string{"/bin/sh", "-c", "echo hello"},
		ScheduleIntervalSeconds: 1,
	}
	result := CmdExecutor{}.Run(context.Background(), spec)
	if !result.Succeeded() {
		t.Fatalf("应成功: %+v", result)
	}
	if !strings.Contains(result.Stdout, "hello") {
		t.Fatalf("stdout 应包含 hello，实际 %q", result.Stdout)
	}
}

func TestCmdExecutor_NonZeroExitIsNotErrButRecordedExitCode(t *testing.T) {
	spec := JobSpec{
		Name:                    "fail_job",
		Command:                 []string{"/bin/sh", "-c", "exit 7"},
		ScheduleIntervalSeconds: 1,
	}
	result := CmdExecutor{}.Run(context.Background(), spec)
	if result.Err != nil {
		t.Fatalf("非零退出码不应算 Err（进程确实启动并跑完了）: %v", result.Err)
	}
	if result.ExitCode != 7 {
		t.Fatalf("退出码应为 7，实际 %d", result.ExitCode)
	}
	if result.Succeeded() {
		t.Fatal("非零退出码不应算成功")
	}
}

func TestCmdExecutor_MissingBinaryIsErr(t *testing.T) {
	spec := JobSpec{
		Name:                    "missing_job",
		Command:                 []string{"/definitely/not/a/real/binary/kairosflux"},
		ScheduleIntervalSeconds: 1,
	}
	result := CmdExecutor{}.Run(context.Background(), spec)
	if result.Err == nil {
		t.Fatal("找不到可执行文件应算 Err")
	}
	if result.Succeeded() {
		t.Fatal("找不到可执行文件不应算成功")
	}
}

func TestCmdExecutor_EnvOverridesAreVisibleToChild(t *testing.T) {
	spec := JobSpec{
		Name:                    "env_job",
		Command:                 []string{"/bin/sh", "-c", "echo $JOBCTL_TEST_VAR"},
		Env:                     map[string]string{"JOBCTL_TEST_VAR": "kairosflux"},
		ScheduleIntervalSeconds: 1,
	}
	result := CmdExecutor{}.Run(context.Background(), spec)
	if !result.Succeeded() {
		t.Fatalf("应成功: %+v", result)
	}
	if !strings.Contains(result.Stdout, "kairosflux") {
		t.Fatalf("子进程应看到注入的环境变量，实际 stdout=%q", result.Stdout)
	}
}

func TestMergeEnv_DeterministicOrderForOverrides(t *testing.T) {
	overrides := map[string]string{"Z_VAR": "1", "A_VAR": "2", "M_VAR": "3"}
	a := mergeEnv(overrides)
	b := mergeEnv(overrides)
	if len(a) != len(b) {
		t.Fatalf("两次 mergeEnv 长度应一致: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("两次 mergeEnv 应逐项相同（覆盖项按 key 排序），第 %d 项不同: %q vs %q", i, a[i], b[i])
		}
	}
	// 覆盖项部分（追加在 base 之后）必须按 key 字典序：A_VAR, M_VAR, Z_VAR。
	tail := a[len(a)-3:]
	want := []string{"A_VAR=2", "M_VAR=3", "Z_VAR=1"}
	for i, w := range want {
		if tail[i] != w {
			t.Fatalf("覆盖项顺序不符，第 %d 项期望 %q 实际 %q", i, w, tail[i])
		}
	}
}
