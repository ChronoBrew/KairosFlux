package jobctl

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// TickError 是某个 job 在一次 Tick 里 reconcile 失败的记录（job 名 + 原因），
// 一个 job 出错不影响其它 job 被处理——每个 job 的 spec/status/events 都是
// 独立的逻辑键，互不阻塞。
type TickError struct {
	JobName string
	Err     error
}

func (e TickError) Error() string { return fmt.Sprintf("job=%s: %v", e.JobName, e.Err) }

// Loop 是单进程 reconcile loop（任务书第 2 项）："调度（定时/依赖触发）、
// 依赖解析、重试退避、幂等、告警"——依赖解析/重试退避/幂等/告警都在
// Reconciler 里实现；Loop 自身只负责"每隔多久、按什么顺序把注册的 job
// 挨个喂给 Reconciler"这一层调度节律，且这一层是本仓库里唯一允许直接用
// time.Sleep/time.Ticker（真实墙钟）的地方——它是"调度触发"本身，不是
// 会被写进指纹/幂等键的确定性状态。
//
// JobNames 是本进程管理的 job 名字清单（首批三个：scout_daily/paper_daily/
// evaluate_and_notify），来自显式配置，不是从存储层 SCAN 出来的——M3 范围
// 内不需要"动态发现全部 job"这个能力，SCAN 也不在本任务允许使用的三个
// opcode（PUT_VERSIONED/GET/GET_AS_OF）之内，等真的需要动态发现时再评估
// 要不要引入。
type Loop struct {
	Store      Store
	Reconciler *Reconciler
	JobNames   []string
	TickPeriod time.Duration
}

// NewLoop 构造一个 Loop；jobNames 会被复制并排序一份，保证 Tick 内部处理
// 顺序确定（不依赖调用方传入的原始顺序，也不依赖任何 map 迭代序）。
func NewLoop(store Store, reconciler *Reconciler, jobNames []string, tickPeriod time.Duration) *Loop {
	names := append([]string(nil), jobNames...)
	sort.Strings(names)
	return &Loop{Store: store, Reconciler: reconciler, JobNames: names, TickPeriod: tickPeriod}
}

// Tick 对 JobNames 里的每个 job 读 job:spec:{name}、解析、调用一次
// Reconciler.Reconcile。按字典序处理（Loop.JobNames 已排序），任何一个
// job 读 spec 失败或 reconcile 失败都记进返回的 []TickError、不中断后续
// job 的处理。
func (l *Loop) Tick() []TickError {
	var errs []TickError
	for _, name := range l.JobNames {
		raw, found, err := l.Store.GetLatest(SpecKey(name))
		if err != nil {
			errs = append(errs, TickError{JobName: name, Err: fmt.Errorf("读 spec 失败: %w", err)})
			continue
		}
		if !found {
			errs = append(errs, TickError{JobName: name, Err: fmt.Errorf("job:spec:%s 不存在（尚未 apply）", name)})
			continue
		}
		spec, err := ParseJobSpec(raw)
		if err != nil {
			errs = append(errs, TickError{JobName: name, Err: err})
			continue
		}
		if _, err := l.Reconciler.Reconcile(spec); err != nil {
			errs = append(errs, TickError{JobName: name, Err: err})
		}
	}
	return errs
}

// Run 按 TickPeriod 周期性调用 Tick，直到 ctx 被取消。onTick（可为 nil）
// 在每次 Tick 后被调用一次，供调用方（cmd 入口）把 []TickError 打日志——
// Loop 自身不做任何 I/O 之外的副作用假设，日志/告警渠道全部由调用方注入。
func (l *Loop) Run(ctx context.Context, onTick func([]TickError)) {
	ticker := time.NewTicker(l.TickPeriod)
	defer ticker.Stop()
	for {
		errs := l.Tick()
		if onTick != nil {
			onTick(errs)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Apply 把一个已校验过的 JobSpec 写进 job:spec:{name}（"期望状态，apply
// 写入"）。走 PUT_VERSIONED——每次 apply 都是一条新版本，spec 的变更历史
// 本身可审计可重放。
func Apply(store Store, spec JobSpec) (uint64, error) {
	if err := spec.Validate(); err != nil {
		return 0, err
	}
	seq, err := store.PutVersioned(SpecKey(spec.Name), spec.CanonicalJSON())
	if err != nil {
		return 0, fmt.Errorf("apply job spec 失败: %w", err)
	}
	return seq, nil
}
