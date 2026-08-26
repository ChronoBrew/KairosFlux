// Package service 的内存护栏（M5 方案 §A.2，见 docs/bench/memory-guardrail.md）：
// 周期性采样进程 RSS，超限后把"写路径"切到拒收状态并告警，防止进程在
// swap 死亡/OOM 被 kill 上越走越远——边缘设备（2 核 3.6G 目标机）没有托管
// 环境的内存回收，OOM 的唯一出口是换页到死或被内核杀死。
//
// 与 admission.Limiter（并发准入，按 in-flight 请求数）刻意区分：护栏管的是
// 进程级 RSS 预算，不是并发度——两者独立开关、独立配置。
//
// 判定语义：
//   - 超限进入 blocked（transition 时打一条结构化告警日志，含 rss/limit）；
//   - 回落至 <=90% 上限才解除（滞回，防止在临界值上反复抖动刷日志）；
//   - 状态只在切换时记日志，稳态不刷。
//
// 拒收行为在调用方（v1 Router.Handle / v2 RouterV2.applyWrite、
// handlePutVersioned）：v1 复用既有 overloaded 状态，v2 回
// codec.ErrCodeMemoryLimit("memory_limit_reached")——错误码裁决见协调者
// 确认（0x1003，0x1xxx 帧/传输段）。
package service

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"
)

// guardrailSampleInterval 是 RSS 采样周期：内存超限通常以秒级爬升，2s 足够
// 在 OOM 之前数秒切到拒收；再密的采样只换来更多 /proc 读取。
const guardrailSampleInterval = 2 * time.Second

// guardrailRecoverRatio 是滞回解除阈值：RSS 回落至上限的 90% 及以下才解除
// blocked，防止在临界值附近抖动造成"拒收-放行-拒收"抖动与刷屏日志。
const guardrailRecoverRatio = 0.9

// rssSamplerFunc 返回进程当前 RSS（字节）。生产用 readProcessRSSBytes；
// 测试注入假采样器，避免测试依赖真实进程内存。
type rssSamplerFunc func() uint64

// MemoryGuardrail 是内存护栏组件：采样 + 状态机 + transition-only 告警。
// 构造于服务装配时（maxRSSMb>0 才非 nil），Start 后随 ctx 生命周期运行，
// Blocked 是写路径的同步查询入口。
type MemoryGuardrail struct {
	maxRSSBytes uint64
	interval    time.Duration
	logger      *slog.Logger
	sample      rssSamplerFunc

	mu      sync.Mutex
	blocked bool
}

// NewMemoryGuardrail 构造护栏；maxRSSMb<=0 返回 nil（未启用，调用方按
// nil 跳过全部检查——配置默认 0=不限，显式开启才生效）。logger 为 nil 时
// 用 slog.Default()。
func NewMemoryGuardrail(maxRSSMb int64, logger *slog.Logger) *MemoryGuardrail {
	if maxRSSMb <= 0 {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MemoryGuardrail{
		maxRSSBytes: uint64(maxRSSMb) << 20, // MiB -> bytes
		interval:    guardrailSampleInterval,
		logger:      logger,
		sample:      readProcessRSSBytes,
	}
}

// Blocked 报告当前是否处于"拒收"状态（线程安全，写路径每请求调用一次，
// 只读一个 bool，无锁竞争热点之外的任何开销）。
func (g *MemoryGuardrail) Blocked() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.blocked
}

// Start 启动采样循环，随 ctx 取消退出（服务关停即停，不泄漏 goroutine）。
func (g *MemoryGuardrail) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(g.interval)
		defer ticker.Stop()
		g.tick()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				g.tick()
			}
		}
	}()
}

// tick 采样一次并按滞回规则更新状态；仅在状态翻转时记日志（含当时的
// rss/limit，日志可作 OOM 复盘依据）。测试直接调用 tick 走确定性路径。
func (g *MemoryGuardrail) tick() {
	rss := g.sample()
	limit := g.maxRSSBytes

	g.mu.Lock()
	wasBlocked := g.blocked
	nowBlocked := rss > limit || (wasBlocked && rss > uint64(float64(limit)*guardrailRecoverRatio))
	g.blocked = nowBlocked
	g.mu.Unlock()

	switch {
	case nowBlocked && !wasBlocked:
		g.logger.Error("memory guardrail: 进程 RSS 超限，新写入被拒",
			"rss_mb", rss>>20, "limit_mb", limit>>20, "reason", "memory_limit_reached")
	case !nowBlocked && wasBlocked:
		g.logger.Warn("memory guardrail: 进程 RSS 回落至限内，恢复接受写入",
			"rss_mb", rss>>20, "limit_mb", limit>>20)
	}
}

// readProcessRSSBytes 读取进程当前 RSS（字节）。Linux 优先解析
// /proc/self/status 的 VmRSS（真实驻留集，不含 page cache 中未使用的部分）；
// 读不到时退回 runtime.ReadMemStats().Sys 的近似值（Go 堆+栈+系统保留，
// 平台无关，是"进程约占内存"的偏保守估计——护栏宁严勿宽）。
func readProcessRSSBytes() uint64 {
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						return kb << 10 // kB -> bytes
					}
				}
			}
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys
}
