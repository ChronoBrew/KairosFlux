package governance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/service/delivery"
)

// stubSink 是可控 sink：healthy 决定 Health()，err 决定 Send() 结果，calls 记投递次数。
type stubSink struct {
	name    string
	healthy bool
	err     error
	calls   int
}

func (s *stubSink) Name() string { return s.name }

func (s *stubSink) Send(_ context.Context, _ []delivery.Record) error {
	s.calls++
	return s.err
}

func (s *stubSink) Health() delivery.SinkHealth {
	return delivery.SinkHealth{Healthy: s.healthy}
}

var batch = []delivery.Record{{Key: []byte("k"), Value: []byte("v")}}

// TestRouterSkipsUnhealthyAndFailsOver 验证 Router 跳过不健康 sink 并在失败时切到下一个健康 sink。
func TestRouterSkipsUnhealthyAndFailsOver(t *testing.T) {
	bad := &stubSink{name: "bad", healthy: false}
	failing := &stubSink{name: "failing", healthy: true, err: errors.New("boom")}
	good := &stubSink{name: "good", healthy: true}
	r := NewRouter([]delivery.Sink{bad, failing, good}, 3, time.Second)

	if err := r.Send(context.Background(), batch); err != nil {
		t.Fatalf("expected failover to good sink, got %v", err)
	}
	if bad.calls != 0 {
		t.Fatalf("unhealthy sink must not be sent to, got %d calls", bad.calls)
	}
	if good.calls != 1 {
		t.Fatalf("healthy sink should receive the batch, got %d calls", good.calls)
	}
}

// TestRouterUsableAsDelivererSender 验证 Router 可作为 Deliverer 的投递目标（sender）传入——
// 这是 B3 的接线目标，只要能编译通过即证明 Router 结构化满足 deliverer 的 sender 接口。
func TestRouterUsableAsDelivererSender(t *testing.T) {
	r := NewRouter([]delivery.Sink{&stubSink{name: "s", healthy: true}}, 3, time.Second)
	d := delivery.NewDelivererWithOffset(nil, r, "router", nil, 1, time.Second)
	if d == nil {
		t.Fatal("expected deliverer constructed with Router as sender")
	}
}

// TestRouterAllUnavailableReturnsError 验证无任何健康 sink 时返回 ErrNoHealthySink。
func TestRouterAllUnavailableReturnsError(t *testing.T) {
	s1 := &stubSink{name: "s1", healthy: false}
	s2 := &stubSink{name: "s2", healthy: false}
	r := NewRouter([]delivery.Sink{s1, s2}, 3, time.Second)
	if err := r.Send(context.Background(), batch); !errors.Is(err, ErrNoHealthySink) {
		t.Fatalf("expected ErrNoHealthySink, got %v", err)
	}
}

// TestPriorityRouter_FailsOverToBackupAndReturnsToPrimary 验证「主 + 兜底」语义：
// 主 sink 健康时永远优先命中（不像 round-robin 会分流到兜底）；主故障时自动降级到
// 兜底；主恢复健康后（模拟运维介入/自愈，不依赖熔断器的 half-open 探测节奏）
// 立即切回主——这正是 ClickHouse 主 + FileSink 兜底场景需要的行为。
func TestPriorityRouter_FailsOverToBackupAndReturnsToPrimary(t *testing.T) {
	primary := &stubSink{name: "primary", healthy: true}
	backup := &stubSink{name: "backup", healthy: true}
	r := NewPriorityRouter([]delivery.Sink{primary, backup}, 1, time.Millisecond)

	// 主健康：即便兜底也健康，round-robin 会分流，priority 必须恒选主。
	for i := 0; i < 3; i++ {
		if err := r.Send(context.Background(), batch); err != nil {
			t.Fatalf("主健康时 Send 不应报错: %v", err)
		}
	}
	if primary.calls != 3 || backup.calls != 0 {
		t.Fatalf("主健康时应恒选主，得到 primary=%d backup=%d", primary.calls, backup.calls)
	}

	// 主变为不健康：应自动降级到兜底。
	primary.healthy = false
	if err := r.Send(context.Background(), batch); err != nil {
		t.Fatalf("降级到兜底不应报错: %v", err)
	}
	if backup.calls != 1 {
		t.Fatalf("主不健康时应降级到兜底，得到 backup.calls=%d", backup.calls)
	}

	// 主恢复健康：下一次 Send 应立即切回主，不再继续走兜底。
	primary.healthy = true
	if err := r.Send(context.Background(), batch); err != nil {
		t.Fatalf("主恢复后 Send 不应报错: %v", err)
	}
	if primary.calls != 4 {
		t.Fatalf("主恢复后应立即切回主，得到 primary.calls=%d", primary.calls)
	}
	if backup.calls != 1 {
		t.Fatalf("切回主后不应再调用兜底，得到 backup.calls=%d", backup.calls)
	}
}

// TestPriorityRouter_PrimaryFailureRoutesToBackup 验证主 Send 报错（而非 Health
// 不健康）时同一轮内也会降级到兜底——不要求先观测到 Health()=false 才切换。
func TestPriorityRouter_PrimaryFailureRoutesToBackup(t *testing.T) {
	primary := &stubSink{name: "primary", healthy: true, err: errors.New("mock 5xx")}
	backup := &stubSink{name: "backup", healthy: true}
	r := NewPriorityRouter([]delivery.Sink{primary, backup}, 3, time.Minute)

	if err := r.Send(context.Background(), batch); err != nil {
		t.Fatalf("应在同一轮内降级到兜底成功，得到: %v", err)
	}
	if primary.calls != 1 || backup.calls != 1 {
		t.Fatalf("应先尝试主再降级到兜底，得到 primary=%d backup=%d", primary.calls, backup.calls)
	}
}

// TestRouterBreakerTripsAfterFailures 验证连续失败触发熔断后，该 sink 被 Allow() 拦下不再投递。
func TestRouterBreakerTripsAfterFailures(t *testing.T) {
	only := &stubSink{name: "only", healthy: true, err: errors.New("boom")}
	r := NewRouter([]delivery.Sink{only}, 2, time.Minute) // 阈值 2

	// 两次投递失败使熔断器 open（阈值 2）。
	_ = r.Send(context.Background(), batch)
	_ = r.Send(context.Background(), batch)
	if only.calls != 2 {
		t.Fatalf("expected 2 attempts before trip, got %d", only.calls)
	}
	// 第三次：熔断 open，Allow() 拒绝，sink 不再被投递，返回 ErrNoHealthySink。
	if err := r.Send(context.Background(), batch); !errors.Is(err, ErrNoHealthySink) {
		t.Fatalf("expected ErrNoHealthySink after breaker open, got %v", err)
	}
	if only.calls != 2 {
		t.Fatalf("breaker-open sink must not be sent to, got %d calls", only.calls)
	}
}
