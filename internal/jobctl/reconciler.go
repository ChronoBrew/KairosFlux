package jobctl

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"
)

// AlertSink 是"重试仍失败后的告警"出口。告警先落 job:events（Reconcile
// 总是会把告警对应的 Event 写进账本，这一步不经过 AlertSink，AlertSink
// 只负责"日志"这一条通道——任务书："告警先落 job:events + 日志（飞书通道
// 等凭据到位另接）"，接飞书只需要新写一个 AlertSink 实现，不改 Reconciler。
type AlertSink interface {
	Alert(jobName string, ev Event)
}

// LogAlertSink 是默认实现：写标准库 log。
type LogAlertSink struct{}

func (LogAlertSink) Alert(jobName string, ev Event) {
	log.Printf("jobctl ALERT: job=%s slot=%d attempt=%d exit_code=%d message=%s",
		jobName, ev.Slot, ev.Attempt, ev.ExitCode, ev.Message)
}

// Reconciler 是单进程 reconcile loop 的决策核心：给定一个 JobSpec 与当前
// Store 里的观测状态，决定"这次调用该不该执行、要不要重试、要不要告警"，
// 并把结果写回 Store。不依赖 map 迭代序（依赖解析走 spec.DependsOn 声明的
// 切片顺序）；除 Clock 之外没有任何隐藏的全局状态，可重复调用、可并发跑
// 多个不同 name 的 Reconciler.Reconcile（同一个 name 不假设并发安全——
// 单进程 reconcile loop 按 job 名排序串行处理，见 Loop.Tick）。
type Reconciler struct {
	Store     Store
	Executor  Executor
	Clock     Clock
	AlertSink AlertSink
}

// NewReconciler 构造一个使用生产默认值（CmdExecutor/SystemClock/LogAlertSink）
// 的 Reconciler，只需要传入 Store。
func NewReconciler(store Store) *Reconciler {
	return &Reconciler{Store: store, Executor: CmdExecutor{}, Clock: SystemClock{}, AlertSink: LogAlertSink{}}
}

// Reconcile 对单个 job 跑一次 reconcile 决策。返回该 job 当前（可能刚更新
// 过的）JobStatus。同一个 (slot, spec 指纹) 组合内重复调用是幂等的——已终态
// （succeeded/alerting）直接返回、不产生新的 status 版本也不产生新事件；
// 退避未到期的 failed 状态同样直接返回。这是一万次重跑验收标准的实现基础：
// 用固定 Clock 反复调用本方法，Store 里的版本数不会随调用次数增长。
func (r *Reconciler) Reconcile(spec JobSpec) (JobStatus, error) {
	if err := spec.Validate(); err != nil {
		return JobStatus{}, err
	}

	now := r.Clock.Now()
	slot := Slot(now, spec.ScheduleIntervalSeconds)
	fp := spec.Fingerprint()
	nowNanos := now.UnixNano()

	prev, prevFound, err := r.readStatus(spec.Name)
	if err != nil {
		return JobStatus{}, err
	}

	sameTrack := prevFound && prev.Slot == slot && prev.SpecFingerprint == fp

	if sameTrack && prev.Phase.Terminal() {
		return prev, nil // 已是本 slot 的终态：no-op
	}

	if sameTrack && prev.Phase == PhaseFailed {
		nextEligible := prev.LastAttemptTime + spec.RetryBackoffSeconds*int64(time.Second)
		if nowNanos < nextEligible {
			return prev, nil // 退避未到期：no-op
		}
	}

	// 依赖检查的时机：这个 (slot, fp) 组合第一次被处理（sameTrack==false），
	// 或者上一次调用已经确认"还没执行、在等依赖"（sameTrack==true &&
	// Phase==PhasePending）——两种情况都还没真正跑过 Executor，必须（重新）
	// 确认依赖是否满足。sameTrack==true && Phase==PhaseFailed 不重新检查：
	// 走到 Failed 意味着第一次尝试时依赖已经满足过，重试不应该因为依赖
	// 状态后续发生了什么变化而被卡住——依赖满足与否只在"第一次尝试前"把关。
	needsDepsCheck := !sameTrack || prev.Phase == PhasePending
	if needsDepsCheck {
		ok, unmet, err := r.depsSatisfied(spec, slot)
		if err != nil {
			return JobStatus{}, err
		}
		if !ok {
			pending := JobStatus{
				Phase:           PhasePending,
				Slot:            slot,
				SpecFingerprint: fp,
				LastVerdict:     EventWaitingOnDeps,
				Message:         fmt.Sprintf("等待依赖: %v", unmet),
			}
			if err := r.writeStatusIfChanged(spec.Name, prev, prevFound, pending); err != nil {
				return JobStatus{}, err
			}
			return pending, nil
		}
	}

	// 到这里：依赖已满足（或本来就不需要检查）。要么是第一次真正执行
	// （attempt=1），要么是退避已到期的重试（attempt=prev.Attempt+1）。
	attempt := 1
	if sameTrack {
		attempt = prev.Attempt + 1
	}

	result := r.Executor.Run(context.Background(), spec)

	var status JobStatus
	var ev Event
	switch {
	case result.Succeeded():
		status = JobStatus{
			Phase: PhaseSucceeded, Slot: slot, SpecFingerprint: fp,
			Attempt: attempt, LastAttemptTime: nowNanos, LastExitCode: 0,
			LastVerdict: EventSucceeded, Message: "ok",
		}
		ev = Event{Slot: slot, Attempt: attempt, SpecFingerprint: fp, Outcome: EventSucceeded, ExitCode: 0, WriteNanos: nowNanos}

	case attempt >= spec.MaxRetries+1:
		// attempt 计入了这一次执行——攒够 MaxRetries 次重试后（首次 + N 次
		// 重试 = MaxRetries+1 次尝试）转终态告警。"> spec.MaxRetries+1" 的
		// 情况理论上不会出现（见下一分支不会再重试），大于号只是纵深防御：
		// 状态被外部直接改写这种输入异常也要有确定的处理路径，不能 panic。
		msg := execFailureMessage(result)
		status = JobStatus{
			Phase: PhaseAlerting, Slot: slot, SpecFingerprint: fp,
			Attempt: attempt, LastAttemptTime: nowNanos, LastExitCode: result.ExitCode,
			LastVerdict: EventAlert, Message: msg,
		}
		ev = Event{Slot: slot, Attempt: attempt, SpecFingerprint: fp, Outcome: EventAlert, ExitCode: result.ExitCode, Message: msg, WriteNanos: nowNanos}

	default:
		msg := execFailureMessage(result)
		status = JobStatus{
			Phase: PhaseFailed, Slot: slot, SpecFingerprint: fp,
			Attempt: attempt, LastAttemptTime: nowNanos, LastExitCode: result.ExitCode,
			LastVerdict: EventFailed, Message: msg,
		}
		ev = Event{Slot: slot, Attempt: attempt, SpecFingerprint: fp, Outcome: EventFailed, ExitCode: result.ExitCode, Message: msg, WriteNanos: nowNanos}
	}

	if _, err := r.Store.PutVersioned(EventsKey(spec.Name), encodeEvent(ev)); err != nil {
		return JobStatus{}, fmt.Errorf("写事件账本失败: %w", err)
	}
	if status.Phase == PhaseAlerting && r.AlertSink != nil {
		r.AlertSink.Alert(spec.Name, ev)
	}
	if _, err := r.Store.PutVersioned(StatusKey(spec.Name), encodeJobStatus(status)); err != nil {
		return JobStatus{}, fmt.Errorf("写状态失败: %w", err)
	}
	return status, nil
}

func execFailureMessage(r ExecResult) string {
	if r.Err != nil {
		return fmt.Sprintf("启动失败: %v", r.Err)
	}
	return fmt.Sprintf("退出码=%d stderr=%s", r.ExitCode, truncate(r.Stderr, 500))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(截断)"
}

func (r *Reconciler) readStatus(name string) (JobStatus, bool, error) {
	raw, found, err := r.Store.GetLatest(StatusKey(name))
	if err != nil {
		return JobStatus{}, false, fmt.Errorf("读状态失败: %w", err)
	}
	if !found {
		return JobStatus{}, false, nil
	}
	status, ok := decodeJobStatus(raw)
	if !ok {
		return JobStatus{}, false, nil // 解不出的历史脏数据当作"没有"处理，不panic
	}
	return status, true, nil
}

// writeStatusIfChanged 只在新状态与已知旧状态不同时才写一个新版本——
// JobStatus 全字段可比较（无 slice/map），用 == 判等。这是"同一个 job
// 重跑一万次，账本不随调用次数线性增长"的关键：等待依赖这种"决策没变"
// 的中间态不应该每 tick 都产生一条新版本。
func (r *Reconciler) writeStatusIfChanged(name string, prev JobStatus, prevFound bool, next JobStatus) error {
	if prevFound && prev == next {
		return nil
	}
	_, err := r.Store.PutVersioned(StatusKey(name), encodeJobStatus(next))
	if err != nil {
		return fmt.Errorf("写状态失败: %w", err)
	}
	return nil
}

// depsSatisfied 按 spec.DependsOn 声明的顺序（不是 map，天然确定）检查每个
// 依赖在同一个 slot 内是否已成功。
func (r *Reconciler) depsSatisfied(spec JobSpec, slot int64) (bool, []string, error) {
	var unmet []string
	for _, dep := range spec.DependsOn {
		raw, found, err := r.Store.GetLatest(StatusKey(dep))
		if err != nil {
			return false, nil, fmt.Errorf("读依赖 %s 状态失败: %w", dep, err)
		}
		if !found {
			unmet = append(unmet, dep)
			continue
		}
		ds, ok := decodeJobStatus(raw)
		if !ok || ds.Slot != slot || ds.Phase != PhaseSucceeded {
			unmet = append(unmet, dep)
		}
	}
	sort.Strings(unmet) // 展示用；输入顺序已确定，这里再排序是为了 Message 的可比较性/可测试性
	return len(unmet) == 0, unmet, nil
}
