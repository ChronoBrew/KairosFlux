package jobctl

import (
	"bytes"
	"context"
	"os/exec"
	"sort"
)

// ExecResult 是一次 job 执行体运行的结果。
type ExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	// Err 是"没能启动进程"这类失败（找不到可执行文件、工作目录不存在等），
	// 与"进程启动了但退出码非零"（体现在 ExitCode）是两种不同的失败——
	// 调用方（reconciler）按需要区分对待。
	Err error
}

// Succeeded 报告本次执行是否算成功：没有启动失败且退出码为 0。
func (r ExecResult) Succeeded() bool {
	return r.Err == nil && r.ExitCode == 0
}

// Executor 是 job 执行体的抽象，把"运行一个 JobSpec"与"reconcile 决策
// 逻辑"解耦——一万次重跑幂等性测试需要注入一个只计数、不真的 fork 子进程
// 的假执行体（否则测试要么跑一万次真实子进程、要么根本没测到"决策层是否
// 幂等"这件事，见测试文件里的 countingExecutor）。生产路径用 CmdExecutor。
type Executor interface {
	Run(ctx context.Context, spec JobSpec) ExecResult
}

// CmdExecutor 是生产实现：用 os/exec 跑 spec.Command，工作目录/环境变量
// 都来自显式配置（硬性边界：Go 代码里不硬编码对方仓路径，路径只从
// JobSpec.Dir/Command 这类配置数据来）。
type CmdExecutor struct{}

func (CmdExecutor) Run(ctx context.Context, spec JobSpec) ExecResult {
	if len(spec.Command) == 0 {
		return ExecResult{Err: errEmptyCommand}
	}
	cmd := exec.CommandContext(ctx, spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Dir
	if len(spec.Env) > 0 {
		cmd.Env = mergeEnv(spec.Env)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
			err = nil // 进程确实启动并跑完了，非零退出码走 ExitCode，不算 Err
		}
	}
	return ExecResult{ExitCode: exitCode, Stdout: stdout.String(), Stderr: stderr.String(), Err: err}
}

// mergeEnv 把 spec.Env 的覆盖项追加到继承的环境变量之后，按 key 字典序
// 追加——禁止依赖 map 迭代序（本仓库明令禁止），os.Environ() 本身的顺序
// 是操作系统给的、不受本函数控制，但"额外覆盖项"这部分的顺序完全由本函数
// 决定，必须确定。
func mergeEnv(overrides map[string]string) []string {
	base := osEnviron()
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(base)+len(keys))
	out = append(out, base...)
	for _, k := range keys {
		out = append(out, k+"="+overrides[k])
	}
	return out
}
