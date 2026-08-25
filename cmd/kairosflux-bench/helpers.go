package main

// helpers.go：压测工具集共享的确定性数据生成与报告头。种子固定 42——
// 同参数两次运行产出的键/负载序列逐字节相同（阶段 B 确定性纪律）。

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// seededRand 返回固定种子 42 的随机源。
func seededRand() *rand.Rand { return rand.New(rand.NewSource(42)) }

// genKey 生成确定性键：quote:2026-08-17:<5 位 code>，code 空间 10 万
// （100w 档 = 每键约 10 个版本，贴近"多版本账本"的真实形状）。
func genKey(i int) string {
	return fmt.Sprintf("quote:2026-08-17:%05d", i%100000)
}

// genPayload 从种子随机源生成一条合法行情 JSON（quote 契约字段）。OHLC 必须
// 自洽：high ≥ max(open,close)、low ≤ min(open,close)——服务端 filter 的
// ValidateVersioned 会在网络路径校验这一点，四个值各自独立随机生成的负载
// 约 91% 违反约束而被整批拒绝（阶段 B 实测踩坑，修复后本工具所有写路径的
// 负载都通过契约校验；embedded 进程内路径不经 filter，但账本数据同样保持
// 合法，保证 100w 档账本形状与生产一致）。
func genPayload(r *rand.Rand) []byte {
	// 乘性构造保证全部价格严格为正（filter 另有一条 non-positive price 校验）：
	// open/close ∈ (1,101]，low = min·(0.5,1]，high = max·[1,2)。
	open := 1 + r.Float64()*100
	closePx := 1 + r.Float64()*100
	lo := open
	if closePx < lo {
		lo = closePx
	}
	hi := open
	if closePx > hi {
		hi = closePx
	}
	low := lo * (0.5 + 0.5*r.Float64())
	high := hi * (1 + r.Float64())
	return []byte(fmt.Sprintf(
		`{"code":"%05d","date":"2026-08-17","open":%.2f,"high":%.2f,"low":%.2f,"close":%.2f,"volume":%d}`,
		r.Intn(100000), open, high, low, closePx, r.Intn(1e6)))
}

// totalRAMGiB 返回机器总内存（GiB），darwin 用 sysctl hw.memsize，linux 用
// /proc/meminfo，取不到返回 0（调用方按"未知"处理，不编造数字）。
func totalRAMGiB() float64 {
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			if n, perr := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); perr == nil {
				return float64(n) / 1024 / 1024 / 1024
			}
		}
	case "linux":
		if b, err := os.ReadFile("/proc/meminfo"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "MemTotal:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						if kb, perr := strconv.ParseInt(fields[1], 10, 64); perr == nil {
							return float64(kb) / 1024 / 1024
						}
					}
					break
				}
			}
		}
	}
	return 0
}

// machineSpecs 返回执行机器的规格摘要（GOOS/GOARCH/核数/总内存）。
func machineSpecs() string {
	if giB := totalRAMGiB(); giB > 0 {
		return fmt.Sprintf("%s/%s，%d 核，总内存约 %.1f GiB", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), giB)
	}
	return fmt.Sprintf("%s/%s，%d 核", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
}

// benchReportHeader 写 markdown 报告头部（标题 + 说明 + 生成时间 + 机器特征）。
func benchReportHeader(f *os.File, title, note string, started time.Time) {
	fmt.Fprintf(f, "# KairosFlux 发布批次阶段 B 基准报告\n\n## %s\n\n", title)
	if note != "" {
		fmt.Fprintf(f, "> %s\n\n", note)
	}
	fmt.Fprintf(f, "- 生成时间：%s\n", started.Format("2006-01-02 15:04:05 MST"))
	fmt.Fprintf(f, "- 机器：%s\n", machineSpecs())
	fmt.Fprintln(f)
}

// emitMeasurementCorrections 写"测量修正记录"节：本批次发布校准教训（harness 测量假象根因），
// 防止未来重跑误读数字。各报告生成器在收尾前调用一次。
func emitMeasurementCorrections(f *os.File) {
	fmt.Fprintf(f, "\n---\n\n## 测量修正记录（校准教训，先于结论阅读）\n\n")
	fmt.Fprintf(f, "- **现象**：修复前 smoke 报告显示 server v2 写路径 QPS 5600–15508、p50 66µs，远超本机 fsync 地板（约 250–530 次/s；v2 每次写 2 次 WAL append），且与 WAL 时间线（v2 期间 WAL 以 ~530 写/s 增长）矛盾，曾一度疑似 ack 早于 fsync。\n")
	fmt.Fprintf(f, "- **根因（已实证）**：bench harness 的 genPayload 独立随机生成 OHLC 四价，约 91%% 违反 quote 契约（OHLC 一致性 / 非正价格），被服务端网络路径 schema filter 以 opcode=0x81 reason=\"quote: OHLC inconsistent\" 拒绝；被拒请求按瞬时往返计入了 QPS/延迟统计而错误数未上报——**是 harness 测量假象，不是引擎缺陷**。\n")
	fmt.Fprintf(f, "- **实证链**：① WAL 时间线 v2 期间增长速率 = fsync-bound 诚实速率（~178KB/s ≈ ack 率 486 w/s × 2 append/写 × ~183B）；② kill -9 复验恢复数 ≥ 已 ack 数（无丢失，见 03-adversarial 维度 1）；③ instrumented harness 抓出 first error reason=\"OHLC inconsistent\"；④ 修复后矩阵物理自洽（v1≈2×v2、全行 0 错误）。\n")
	fmt.Fprintf(f, "- **修复**：genPayload 改乘性构造（open/close ∈ (1,101]，low = min·(0.5,1]，high = max·[1,2)，volume ≥ 0），quote 契约全约束满足；putVersioned 错误信息带 reason；写路径行新增错误列（行尾）。\n")
	fmt.Fprintf(f, "- **发布口径**：修复前的 server 路径数字全部作废，以本报告为准。ack-after-fsync 契约（wal.Append 返回 = 已 fsync）在代码审查与 kill 复验中均未破坏。\n")
}
