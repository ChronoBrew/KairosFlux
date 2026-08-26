package jobctl

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/ChronoBrew/KairosFlux/proto"
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
//
// 真正执行一次的写序（M3 已知局限"写 event 然后写 status 两次独立调用"
// 的修复，M4 裁决"状态=事件的函数"）：①status→running 落盘（崩溃恢复的
// 锚点）→ ②执行 → ③event 落盘 → ④status→终态（由刚落盘的 event 派生，
// 见 jobStatusFromEvent）。每次执行因此写两条 status 版本（running + 终态）、
// 一条 event 版本，都是固定次数、不随重跑次数增长。
func (r *Reconciler) Reconcile(spec JobSpec) (JobStatus, error) {
	if err := spec.Validate(); err != nil {
		return JobStatus{}, err
	}

	now := r.Clock.Now()
	slot := spec.Slot(now) // 声明了 schedule 锚点走本地墙钟时槽，否则纪元锚定（M3 遗留修正）
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
	// （attempt=1），要么是退避已到期的重试（attempt=prev.Attempt+1）；
	// 唯一例外是 prev 本身就是 running——上次执行悬空、终态未落，这里补完
	// 同一次尝试（attempt=prev.Attempt），不消耗新的重试预算（重执行是安全
	// 语义：幂等键 (slot, fingerprint) 保证业务重复无害，见 Recover）。
	attempt := 1
	if sameTrack {
		if prev.Phase == PhaseRunning {
			attempt = prev.Attempt
		} else {
			attempt = prev.Attempt + 1
		}
	}

	// 写序第①步：status→running 先落盘。崩溃后重启时，启动恢复扫描
	// （Recover）以它为锚判断"上次执行是否悬空"——没有对应 event 就按幂等
	// 键重执行并补 event。写入发生在真正执行之前：终态 status 是事件账本的
	// 派生视图，执行侧事实必须先于它要派生的视图落盘。running 没有对应事件
	// （事件在执行后才写），verdict 留空是准确语义。
	running := JobStatus{
		Phase:           PhaseRunning,
		Slot:            slot,
		SpecFingerprint: fp,
		Attempt:         attempt,
		LastAttemptTime: nowNanos,
		LastExitCode:    0,
		LastVerdict:     "",
		Message:         "执行中",
	}
	if err := r.writeStatusIfChanged(spec.Name, prev, prevFound, running); err != nil {
		return JobStatus{}, err
	}

	result := r.Executor.Run(context.Background(), spec)

	// ②执行 ③event 落盘 ④status→终态。终态 status 不单独构造，而是由刚落
	// 盘的 event 派生（jobStatusFromEvent）——写与恢复两条路径共用同一个
	// 派生函数，"状态=事件的函数"不会在写/修之间产生分歧。
	var ev Event
	switch {
	case result.Succeeded():
		ev = Event{Slot: slot, Attempt: attempt, SpecFingerprint: fp, Outcome: EventSucceeded, ExitCode: 0, WriteNanos: nowNanos}

	case attempt >= spec.MaxRetries+1:
		// attempt 计入了这一次执行——攒够 MaxRetries 次重试后（首次 + N 次
		// 重试 = MaxRetries+1 次尝试）转终态告警。"> spec.MaxRetries+1" 的
		// 情况理论上不会出现（见下一分支不会再重试），大于号只是纵深防御：
		// 状态被外部直接改写这种输入异常也要有确定的处理路径，不能 panic。
		msg := execFailureMessage(result)
		ev = Event{Slot: slot, Attempt: attempt, SpecFingerprint: fp, Outcome: EventAlert, ExitCode: result.ExitCode, Message: msg, WriteNanos: nowNanos}

	default:
		msg := execFailureMessage(result)
		ev = Event{Slot: slot, Attempt: attempt, SpecFingerprint: fp, Outcome: EventFailed, ExitCode: result.ExitCode, Message: msg, WriteNanos: nowNanos}
	}

	if _, err := r.Store.PutVersioned(EventsKey(spec.Name), encodeEvent(ev), EngineSource); err != nil {
		return JobStatus{}, fmt.Errorf("写事件账本失败: %w", err)
	}
	if ev.Outcome == EventAlert && r.AlertSink != nil {
		r.AlertSink.Alert(spec.Name, ev)
	}
	status := jobStatusFromEvent(ev)
	if _, err := r.Store.PutVersioned(StatusKey(spec.Name), encodeJobStatus(status), EngineSource); err != nil {
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
	_, err := r.Store.PutVersioned(StatusKey(name), encodeJobStatus(next), EngineSource)
	if err != nil {
		return fmt.Errorf("写状态失败: %w", err)
	}
	return nil
}

// Recover 是启动恢复扫描：进程启动后、tick 循环之前，对每个已知 job 各调用
// 一次（见 Loop.Recover）。原理是 M4 裁决的"状态=事件的函数"：status 是
// 事件账本的派生视图，这里把 status 重建为账本推出的事实。恢复过程只写
// status、不产生新 event 副本——"修复而非重跑"（悬空执行的重跑由随后的
// Reconcile 完成，见下）。Recover 幂等：重复调用不会产生额外写入。
//
// 四种情况：
//
//   - status=running 且账本里没有同 (slot, spec_fingerprint) 的 event：
//     上次执行悬空（①running 已写、③event 未落），status 原样保留，由随后
//     的 Reconcile 按幂等键 (slot, fingerprint) 重执行一次并补 event——
//     重执行是安全语义：幂等键保证业务重复无害。
//   - status=running 且有同 (slot, fp) 的 event（崩溃发生在 event 落盘后、
//     终态 status 写前）：按该 event 重建 status，不重执行、不补 event。
//     status=failed 但账本已有更新的匹配 event（崩溃发生在 failed 的 status
//     写前）同样按最新匹配 event 重建——退避锚点跟着账本走。
//   - status 是终态但账本最新 event 与之不符：以 event 为准重建 status。
//     这是 M3 原文崩溃窗口（"写 event 然后写 status 两次独立调用,中途崩溃
//     →status 陈旧"）的遗留现场——旧代码崩溃留下的陈旧终态在此修复，重启
//     后不再重执行、不再产生重复 event。
//   - status 不存在但账本有 event（旧代码首次执行崩溃的遗留）：以最新
//     event 重建，同样避免重启后把已执行过的 slot 再执行一遍。
//
// PhasePending 不修：依赖未满足的等待态，尚无对应事件，属于正常状态。
func (r *Reconciler) Recover(name string) error {
	status, found, err := r.readStatus(name)
	if err != nil {
		return err
	}
	versions, err := r.Store.ListVersions(EventsKey(name))
	if err != nil {
		return fmt.Errorf("读事件账本失败: %w", err)
	}
	events, err := decodeEvents(versions)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil // 没有账本事实可派生：无事可修
	}
	var rebuilt JobStatus
	switch {
	case !found:
		rebuilt = jobStatusFromEvent(events[len(events)-1])
	case status.Phase == PhaseRunning || status.Phase == PhaseFailed:
		ev, ok := latestMatchingEvent(events, status.Slot, status.SpecFingerprint)
		if !ok {
			// running 无匹配 = 悬空执行，留给 Reconcile 重执行；
			// failed 无匹配 = 异常输入，保守不修（Reconcile 的重试路径会收敛）。
			return nil
		}
		rebuilt = jobStatusFromEvent(ev)
	case status.Phase.Terminal():
		rebuilt = jobStatusFromEvent(events[len(events)-1])
	default: // PhasePending：等待态，不修
		return nil
	}
	return r.writeStatusIfChanged(name, status, found, rebuilt)
}

// decodeEvents 把 ListVersions 返回的版本负载按版本序解成 Event 列表。
// 任一条解不出就整体失败：账本里出现无法解析的 payload 是数据损坏，恢复
// 扫描不该在坏数据上继续推导状态。
func decodeEvents(versions []proto.VersionEntryView) ([]Event, error) {
	events := make([]Event, 0, len(versions))
	for _, v := range versions {
		ev, ok := decodeEvent(v.Payload)
		if !ok {
			return nil, fmt.Errorf("事件账本版本 seq=%d 无法解析", v.Seq)
		}
		events = append(events, ev)
	}
	return events, nil
}

// latestMatchingEvent 从事件账本（版本序升序）里找与 (slot, fp) 匹配的最新
// 一条事件——同 (slot, fp) 的重试会产生多条事件，决定 status 的是最后一条。
func latestMatchingEvent(events []Event, slot int64, fp string) (Event, bool) {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Slot == slot && events[i].SpecFingerprint == fp {
			return events[i], true
		}
	}
	return Event{}, false
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
