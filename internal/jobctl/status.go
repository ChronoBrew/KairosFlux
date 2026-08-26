package jobctl

import "encoding/json"

// JobPhase 是 job:status:{name} 里的观测状态枚举（契约原文："phase/last_run/
// retry/verdict"）。枚举类型表达角色/状态，不用字符串匹配承载语义——
// reconciler.go 的分支只对这几个常量做 == 比较。
type JobPhase string

const (
	// PhasePending：本 slot 内还没有任何一次执行尝试（可能在等依赖，也可能
	// 还没轮到 reconcile 处理它），或者 spec 变了、上一个 slot 的终态已经
	// 不适用于当前 slot。
	PhasePending JobPhase = "pending"
	// PhaseRunning：本 slot 内一次执行正在进行中——写序第①步 status→running
	// 已落盘、终态未落（M3 已知局限"写 event 然后写 status 两次独立调用"的
	// 修复：终态 status 不再直接写，而是先落 running 标记、执行、落事件、再
	// 落终态）。不是终态；它是崩溃恢复的关键锚点：进程崩溃重启后，启动恢复
	// 扫描（Reconciler.Recover）以它为依据判断"上次执行是否悬空"。
	PhaseRunning JobPhase = "running"
	// PhaseSucceeded：本 slot 内已成功执行过一次，终态，不会再重试。
	PhaseSucceeded JobPhase = "succeeded"
	// PhaseFailed：本 slot 内最近一次尝试失败，重试预算未耗尽，等待退避
	// 到期后重试。
	PhaseFailed JobPhase = "failed"
	// PhaseAlerting：本 slot 内重试预算已耗尽仍失败，终态，需要人工介入——
	// 告警已经落 job:events + 日志（见 reconciler.go alertLogger）。
	PhaseAlerting JobPhase = "alerting"
)

// Terminal 报告该 phase 是否是"本 slot 不会再变化"的终态。
func (p JobPhase) Terminal() bool {
	return p == PhaseSucceeded || p == PhaseAlerting
}

// JobStatus 是 job:status:{name} 的观测状态（契约原文字段："phase/last_run/
// retry/verdict"——Phase 对应 phase，Slot 对应 last_run（"最近跑的是哪个
// 调度时间片"），Attempt 对应 retry（已尝试次数，含首次），LastVerdict
// 对应 verdict）。M4 裁决后它是事件账本的派生视图：每次执行先写 running
// 标记（①）、终态由刚落盘的事件派生（④，见 jobStatusFromEvent），启动
// 恢复扫描（Reconciler.Recover）会把崩溃遗留的不一致重建回账本事实。
type JobStatus struct {
	Phase           JobPhase     `json:"phase"`
	Slot            int64        `json:"slot"`
	SpecFingerprint string       `json:"spec_fingerprint"`
	Attempt         int          `json:"attempt"`           // 本 slot 内已尝试次数（首次执行=1）
	LastAttemptTime int64        `json:"last_attempt_time"` // unix nanos，供退避判定与展示
	LastExitCode    int          `json:"last_exit_code"`
	LastVerdict     EventOutcome `json:"last_verdict"` // 与本次状态变化对应的那条 Event.Outcome
	Message         string       `json:"message"`
}

func encodeJobStatus(s JobStatus) []byte {
	// JobStatus 全部是 JSON 原生可编码类型，编码不会失败；与 temporal 包
	// 的既有先例一致（Encode 系列函数不返回 error）。
	out, _ := json.Marshal(s)
	return out
}

func decodeJobStatus(raw []byte) (JobStatus, bool) {
	var s JobStatus
	if err := json.Unmarshal(raw, &s); err != nil {
		return JobStatus{}, false
	}
	return s, true
}

// EventOutcome 是 job:events:{name} 一条事件记录的结果枚举。
type EventOutcome string

const (
	EventSucceeded      EventOutcome = "succeeded"
	EventFailed         EventOutcome = "failed"
	EventRetryScheduled EventOutcome = "retry_scheduled"
	EventAlert          EventOutcome = "alert"
	EventWaitingOnDeps  EventOutcome = "waiting_on_deps"
)

// Event 是 job:events:{name}:v{seq} 一条版本记录的负载（每次 reconcile 真正
// 执行/决策变化产生一条；纯粹的"本 slot 已是终态，本次调用什么都没做"不
// 产生新事件——否则一万次重跑会产生一万条事件，与验收标准"结果与账本一致"
// 的"账本"含义相悖：账本记录的是发生过的事，不是被轮询过的次数）。
type Event struct {
	Slot            int64        `json:"slot"`
	Attempt         int          `json:"attempt"`
	SpecFingerprint string       `json:"spec_fingerprint"`
	Outcome         EventOutcome `json:"outcome"`
	ExitCode        int          `json:"exit_code"`
	Message         string       `json:"message"`
	WriteNanos      int64        `json:"write_nanos"`
}

func encodeEvent(e Event) []byte {
	out, _ := json.Marshal(e)
	return out
}

func decodeEvent(raw []byte) (Event, bool) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return Event{}, false
	}
	return e, true
}

// jobStatusFromEvent 是"状态=事件的函数"的唯一定义：从一条事件派生与它一致
// 的 JobStatus。Reconcile 写终态 status（写序第④步）与启动恢复扫描
// （Reconciler.Recover）重建 status 都走这一个函数——写与修两条路径共用
// 同一个派生函数，不可能产生分歧。
//
// succeeded/alert 派生为终态；failed 派生为 PhaseFailed（重试预算未耗尽，
// 退避锚点 LastAttemptTime 取事件写入时刻，跟着账本走）；其余 outcome 在
// Reconcile 写出的账本里不会出现，按 failed 兜底——可重试、绝不误报成功。
func jobStatusFromEvent(ev Event) JobStatus {
	st := JobStatus{
		Phase:           PhaseFailed,
		Slot:            ev.Slot,
		SpecFingerprint: ev.SpecFingerprint,
		Attempt:         ev.Attempt,
		LastAttemptTime: ev.WriteNanos,
		LastExitCode:    ev.ExitCode,
		LastVerdict:     ev.Outcome,
		Message:         ev.Message,
	}
	switch ev.Outcome {
	case EventSucceeded:
		st.Phase = PhaseSucceeded
		if st.Message == "" {
			// Reconcile 写 succeeded 事件时不带 Message；status 展示层沿用
			// 既有行为给 "ok"（零回归）。
			st.Message = "ok"
		}
	case EventAlert:
		st.Phase = PhaseAlerting
	}
	return st
}
