package service

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// 内存护栏测试：注入假 RSS 采样器（rssSamplerFunc 字段直接赋值），tick()
// 同步驱动状态机——不依赖真实进程内存、不开 goroutine，全确定性。
func newTestGuardrail(t *testing.T, limitMb int64) (*MemoryGuardrail, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	g := NewMemoryGuardrail(limitMb, logger)
	if g == nil {
		t.Fatalf("NewMemoryGuardrail(%d) 返回 nil", limitMb)
	}
	return g, &buf
}

func TestGuardrailDisabledWhenMaxRSSMbNonPositive(t *testing.T) {
	if g := NewMemoryGuardrail(0, nil); g != nil {
		t.Fatalf("NewMemoryGuardrail(0) 应为 nil（未启用），得到 %+v", g)
	}
	if g := NewMemoryGuardrail(-1, nil); g != nil {
		t.Fatalf("NewMemoryGuardrail(-1) 应为 nil（未启用），得到 %+v", g)
	}
}

func TestGuardrailBlocksOnlyAboveLimit(t *testing.T) {
	g, _ := newTestGuardrail(t, 100) // 100MiB
	// 上限之下：不拒收。
	g.sample = func() uint64 { return 90 << 20 }
	g.tick()
	if g.Blocked() {
		t.Fatal("RSS 90MiB < 100MiB 上限，不应 blocked")
	}
	// 临界值本身：不拒收（严格大于才超限）。
	g.sample = func() uint64 { return 100 << 20 }
	g.tick()
	if g.Blocked() {
		t.Fatal("RSS 恰好等于上限不应 blocked（严格大于才超限）")
	}
	// 超限：拒收。
	g.sample = func() uint64 { return 101 << 20 }
	g.tick()
	if !g.Blocked() {
		t.Fatal("RSS 101MiB > 100MiB 上限，应 blocked")
	}
}

func TestGuardrailHysteresisRecoversAtOrBelow90Percent(t *testing.T) {
	g, _ := newTestGuardrail(t, 100)
	g.sample = func() uint64 { return 200 << 20 }
	g.tick()
	if !g.Blocked() {
		t.Fatal("前置条件：超限应 blocked")
	}
	// 滞回区（上限的 90%~100% 之间）：保持 blocked，不提前放行。
	g.sample = func() uint64 { return 95 << 20 }
	g.tick()
	if !g.Blocked() {
		t.Fatal("RSS 95MiB 仍在滞回区（>90MiB），应保持 blocked")
	}
	// 回落至 <=90% 上限：解除。
	g.sample = func() uint64 { return 90 << 20 }
	g.tick()
	if g.Blocked() {
		t.Fatal("RSS 90MiB == 90% 上限，应解除 blocked")
	}
	// 解除后再次超限：重新进入。
	g.sample = func() uint64 { return 150 << 20 }
	g.tick()
	if !g.Blocked() {
		t.Fatal("解除后再次超限应重新 blocked")
	}
}

func TestGuardrailLogsOnlyOnTransitions(t *testing.T) {
	g, buf := newTestGuardrail(t, 100)
	sample := func(mb uint64) { g.sample = func() uint64 { return mb << 20 } }

	// 稳态（限内、限上、滞回区）不产生日志。
	sample(80)
	g.tick()
	sample(95)
	g.tick()
	if logs := buf.String(); logs != "" {
		t.Fatalf("稳态不应产生日志，得到: %s", logs)
	}

	// 进入 blocked：恰一条 entry 日志，携带 rss/limit/reason。
	sample(120)
	g.tick()
	logs := buf.String()
	if !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "memory_limit_reached") {
		t.Fatalf("进入 blocked 应有一条 ERROR 告警（含 reason），得到: %s", logs)
	}
	if strings.Count(logs, "level=ERROR") != 1 {
		t.Fatalf("进入 blocked 应恰一条 ERROR 日志，得到: %s", logs)
	}

	// 持续超限：不重复刷日志。
	sample(180)
	g.tick()
	if strings.Count(buf.String(), "level=ERROR") != 1 {
		t.Fatalf("持续超限不应重复告警，得到: %s", buf.String())
	}

	// 解除：恰一条 warn。
	sample(85)
	g.tick()
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("解除 blocked 应有一条 WARN 日志，得到: %s", buf.String())
	}
	if strings.Count(buf.String(), "level=WARN") != 1 {
		t.Fatalf("解除应恰一条 WARN 日志，得到: %s", buf.String())
	}
}

func TestGuardrailReadProcessRSSBytesSanity(t *testing.T) {
	if rss := readProcessRSSBytes(); rss == 0 {
		t.Fatal("readProcessRSSBytes 应返回非零 RSS")
	}
}
