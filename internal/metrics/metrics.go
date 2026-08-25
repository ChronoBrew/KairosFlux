// Package metrics 提供零依赖的进程内可观测性：一组原子计数器 + 仪表回调，
// 以「周期性 slog 快照」作为暴露出口——headless 边缘设备直接 tail 日志即可观测，
// 无需开端口、无需 Prometheus/Grafana 等外部基础设施。
//
// 设计上把「埋点」与「暴露」解耦：业务代码只管在此处累加计数，未来若要再接
// HTTP /metrics 或 Prometheus 出口，只需新增读取出口，无需改动任何埋点。
package metrics

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// 计数器：单调递增的累计量。各业务路径直接 .Add(1)。
var (
	FramesDroppedMalformed    atomic.Int64 // 被钩子按「畸形帧」丢弃
	FramesDroppedOversized    atomic.Int64 // 被钩子按「value 超限」丢弃
	FramesDroppedNonMonotonic atomic.Int64 // 被钩子按「时间戳回退/重放」丢弃
	FramesDroppedSchema       atomic.Int64 // 被 schema 校验器按「类型规则不满足」丢弃（见 service/ingesthook/schema）
	FramesDroppedUnauthorized atomic.Int64 // 被 PUT_VERSIONED 协议层角色校验拒绝（agent 身份写非 Proposal kind，见 service/router_v2.go handlePutVersioned）
	SchemaChecksSkipped       atomic.Int64 // schema 校验器因缺少可选比对字段而跳过某一项检查的次数（如无昨收时跳过涨跌幅校验）
	Writes                    atomic.Int64 // 成功写入（PUT）次数
	Reads                     atomic.Int64 // 读取（GET）次数
	Scans                     atomic.Int64 // 边缘范围查询（SCAN）次数
	Deletes                   atomic.Int64 // 成功删除（DEL）次数
	WriteErrors               atomic.Int64 // 写入/删除失败次数
	BackpressureStalls        atomic.Int64 // 写入被字节信用背压阻塞的次数
	Delivered                 atomic.Int64 // 成功投递到下游 sink 的记录数
	DeliveryFailed            atomic.Int64 // 投递批失败（未 ack）次数
	OffsetCommits             atomic.Int64 // 投递游标 offset 提交次数
	CircuitOpen               atomic.Int64 // 下游 sink 熔断器跳到 open 的次数
	AdmissionShed             atomic.Int64 // 网关自适应准入拒绝（shed）的请求数
	PanicsRecovered           atomic.Int64 // kairnet 连接/请求处理路径捕获并恢复的 panic 次数（应恒为 0，非 0 说明有 bug）
)

// 仪表：当前瞬时值，由持有者注册回调，快照时实时读取。
var (
	memTableInflightFn atomic.Value // 存 func() int64
	memTableBudget     atomic.Int64
)

// SetMemTableGauges 注册 MemTable 未 flush 字节数的实时读取回调与字节预算。
// 通常在 MemTable 构造时调用一次。
func SetMemTableGauges(inflight func() int64, budget int64) {
	if inflight != nil {
		memTableInflightFn.Store(inflight)
	}
	memTableBudget.Store(budget)
}

func memTableInflight() int64 {
	if f, ok := memTableInflightFn.Load().(func() int64); ok {
		return f()
	}
	return 0
}

// Snapshot 是某一时刻全部指标的取值。
type Snapshot struct {
	DroppedMalformed      int64
	DroppedOversized      int64
	DroppedNonMonotonic   int64
	DroppedSchema         int64
	DroppedUnauthorized   int64
	SchemaChecksSkipped   int64
	Writes                int64
	Reads                 int64
	Scans                 int64
	Deletes               int64
	WriteErrors           int64
	BackpressureStalls    int64
	Delivered             int64
	DeliveryFailed        int64
	OffsetCommits         int64
	CircuitOpen           int64
	AdmissionShed         int64
	PanicsRecovered       int64
	MemTableInflightBytes int64
	MemTableBudgetBytes   int64
}

// Take 读取当前各指标，组成一份快照。
func Take() Snapshot {
	return Snapshot{
		DroppedMalformed:      FramesDroppedMalformed.Load(),
		DroppedOversized:      FramesDroppedOversized.Load(),
		DroppedNonMonotonic:   FramesDroppedNonMonotonic.Load(),
		DroppedSchema:         FramesDroppedSchema.Load(),
		DroppedUnauthorized:   FramesDroppedUnauthorized.Load(),
		SchemaChecksSkipped:   SchemaChecksSkipped.Load(),
		Writes:                Writes.Load(),
		Reads:                 Reads.Load(),
		Scans:                 Scans.Load(),
		Deletes:               Deletes.Load(),
		WriteErrors:           WriteErrors.Load(),
		BackpressureStalls:    BackpressureStalls.Load(),
		Delivered:             Delivered.Load(),
		DeliveryFailed:        DeliveryFailed.Load(),
		OffsetCommits:         OffsetCommits.Load(),
		CircuitOpen:           CircuitOpen.Load(),
		AdmissionShed:         AdmissionShed.Load(),
		PanicsRecovered:       PanicsRecovered.Load(),
		MemTableInflightBytes: memTableInflight(),
		MemTableBudgetBytes:   memTableBudget.Load(),
	}
}

// LogSnapshot 用默认 slog logger 打印一行指标快照。
func LogSnapshot() {
	s := Take()
	slog.Info("metrics",
		"dropped_malformed", s.DroppedMalformed,
		"dropped_oversized", s.DroppedOversized,
		"dropped_non_monotonic", s.DroppedNonMonotonic,
		"dropped_schema", s.DroppedSchema,
		"dropped_unauthorized", s.DroppedUnauthorized,
		"schema_checks_skipped", s.SchemaChecksSkipped,
		"writes", s.Writes,
		"reads", s.Reads,
		"scans", s.Scans,
		"deletes", s.Deletes,
		"write_errors", s.WriteErrors,
		"backpressure_stalls", s.BackpressureStalls,
		"delivered", s.Delivered,
		"delivery_failed", s.DeliveryFailed,
		"offset_commits", s.OffsetCommits,
		"circuit_open", s.CircuitOpen,
		"admission_shed", s.AdmissionShed,
		"panics_recovered", s.PanicsRecovered,
		"memtable_inflight_bytes", s.MemTableInflightBytes,
		"memtable_budget_bytes", s.MemTableBudgetBytes,
	)
}

// StartLogger 启动后台 goroutine，每隔 interval 打印一次指标快照，
// 直到 ctx 取消。interval<=0 时不启动。
func StartLogger(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				LogSnapshot()
			}
		}
	}()
}
