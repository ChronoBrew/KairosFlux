package main

// soak100.go：100+ 生产日长期混压（阶段 B，2026-08-25 加码）。每日并发跑
// 三类负载——采集突发写（PUT_VERSIONED）+ agent 上下文查询（BuildContext，
// 读 contracts/ + riskredlines/redlines.json）+ 有界审计扫描（LIST_WRITES），
// 全部落在同一个不断增长的账本上（第 d 天数据量 ≈ d×每日写入，第 100 天
// 账本 ≈ 20 万条版本，贴近"运行了 100 个生产日"的累积形状）。
//
// 断言：第 1 天 vs 第 100 天同类负载 p50 漂移 ≤ ±20%、p99 漂移 ≤ ±30%。
// 劣化即把 M5（storage 优化）从 optional 升为 mandatory，劣化曲线进报告。
// 每类负载每天 2000/500/50 次的样本量足以支撑 p50/p99 分位估计。

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux"
)

func cmdSoak100(args []string) error {
	fs := flag.NewFlagSet("soak100", flag.ExitOnError)
	outPath := fs.String("out", "docs/bench/04-soak100.md", "报告输出路径")
	dataDir := fs.String("data-dir", "/tmp/kf-bench-soak", "数据目录（自动重建）")
	days := fs.Int("days", 100, "模拟生产天数（>=100 才是加码口径）")
	writesPerDay := fs.Int("writes-per-day", 2000, "每日采集突发写条数")
	ctxPerDay := fs.Int("contexts-per-day", 500, "每日 agent 上下文查询次数")
	scansPerDay := fs.Int("scans-per-day", 50, "每日审计扫描次数")
	fs.Parse(args)

	started := time.Now()
	f, err := os.Create(*outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	benchReportHeader(f, fmt.Sprintf("100+ 生产日长期混压（%d 天，账本增长到 %d 万条版本）", *days, *days*(*writesPerDay)/10000),
		fmt.Sprintf("每日并发：%d 突发写（8 worker）+ %d BuildContext 查询 + %d 有界审计扫描。断言：第 1 天 vs 第 %d 天 p50 漂移 ≤±20%%，p99 漂移 ≤±30%%。",
			*writesPerDay, *ctxPerDay, *scansPerDay, *days), started)

	os.RemoveAll(*dataDir)
	e, err := kairosflux.NewEmbedded(kairosflux.Options{DataDir: *dataDir})
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer e.Close()

	contractsDir := filepath.Join(repoRoot(), "contracts")
	redlinesPath := filepath.Join(repoRoot(), "riskredlines", "redlines.json")
	if _, err := os.Stat(contractsDir); err != nil {
		return fmt.Errorf("contracts 目录不存在: %w", err)
	}
	if _, err := os.Stat(redlinesPath); err != nil {
		return fmt.Errorf("redlines 文件不存在: %w", err)
	}

	daySec := int64(86400e9)
	now := time.Now().UnixNano()
	// day d 的写入时间戳：落在"该生产日"内（now-(days-d)*1天 .. now-(days-d-1)*1天），
	// 确定性：同一 d 两次运行的时间戳序列完全相同。
	dayStart := func(d int) int64 { return now - int64(*days-d)*daySec }
	// asOf 查询时刻 = 当日结束（能看到当日全部写入）。
	asOf := func(d int) int64 { return dayStart(d) + daySec - 1e9 }

	// 每类负载每天聚合：raw 采样（100 天 × 2000+500+50 次 ≈ 25.5 万次，内存无虞）。
	type dayAgg struct {
		d      int
		writeS *Stats
		ctxS   *Stats
		scanS  *Stats
	}
	aggs := make([]*dayAgg, *days)

	for d := 0; d < *days; d++ {
		agg := &dayAgg{d: d, writeS: NewStats(), ctxS: NewStats(), scanS: NewStats()}
		aggs[d] = agg
		agg.writeS.Start()
		agg.ctxS.Start()
		agg.scanS.Start()

		base := dayStart(d)
		r := seededRand()
		payloads := make([][]byte, 16)
		for i := range payloads {
			payloads[i] = genPayload(r)
		}

		var wg sync.WaitGroup
		// 三类负载并发：采集突发写（8 worker）+ 上下文查询 + 审计扫描。
		wg.Add(1)
		go func() { // 采集突发写
			defer wg.Done()
			var next atomic.Int64
			var wg2 sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg2.Add(1)
				go func() {
					defer wg2.Done()
					for {
						i := int(next.Add(1))
						if i > *writesPerDay {
							return
						}
						key := genKey(d**writesPerDay + i)
						writeTS := base + int64(i%86400)*1e9 // 确定性分布在当日 24h
						start := time.Now()
						_, err := e.PutVersioned(key, payloads[i%len(payloads)], writeTS, "bench-soak", 2)
						agg.writeS.Record(time.Since(start), err)
					}
				}()
			}
			wg2.Wait()
		}()
		wg.Add(1)
		go func() { // agent 上下文查询（BuildContext：契约 + 红线 + 实验/策略态）
			defer wg.Done()
			for i := 0; i < *ctxPerDay; i++ {
				start := time.Now()
				_, err := e.BuildContext(kairosflux.ContextRequest{AsOfNanos: asOf(d)}, contractsDir, redlinesPath)
				agg.ctxS.Record(time.Since(start), err)
			}
		}()
		wg.Add(1)
		go func() { // 有界审计扫描（当日窗口，避开 LIST_WRITES 全扫巨帧问题）
			defer wg.Done()
			for i := 0; i < *scansPerDay; i++ {
				start := time.Now()
				_, err := e.ListWrites("quote:", base, base+daySec, "")
				agg.scanS.Record(time.Since(start), err)
			}
		}()
		wg.Wait()
		agg.writeS.Stop()
		agg.ctxS.Stop()
		agg.scanS.Stop()

		if errs := agg.writeS.TotalErrs() + agg.ctxS.TotalErrs() + agg.scanS.TotalErrs(); errs > 0 {
			return fmt.Errorf("第 %d 天出现 %d 次操作错误（write=%d ctx=%d scan=%d）",
				d+1, errs, agg.writeS.TotalErrs(), agg.ctxS.TotalErrs(), agg.scanS.TotalErrs())
		}
		if (d+1)%10 == 0 || d == *days-1 {
			fmt.Printf("  [soak] 第 %d/%d 天完成（账本 %d 条版本）\n", d+1, *days, (d+1)**writesPerDay)
		}
	}

	// —— 报告 ——
	fmt.Fprintf(f, "## 每日并发负载延迟（p50 / p99，三类并发同时进行）\n\n")
	fmt.Fprintf(f, "| 天 | 账本版本数 | 写 p50 | 写 p99 | 上下文 p50 | 上下文 p99 | 审计扫描 p50 | 审计扫描 p99 |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, agg := range aggs {
		fmt.Fprintf(f, "| %d | %d | %s | %s | %s | %s | %s | %s |\n",
			agg.d+1, (agg.d+1)**writesPerDay,
			latencyStr(agg.writeS.P50()), latencyStr(agg.writeS.P99()),
			latencyStr(agg.ctxS.P50()), latencyStr(agg.ctxS.P99()),
			latencyStr(agg.scanS.P50()), latencyStr(agg.scanS.P99()))
	}

	// —— 第 1 天 vs 第 100 天断言 ——
	first, last := aggs[0], aggs[*days-1]
	fmt.Fprintf(f, "\n## 断言：第 1 天 vs 第 %d 天不劣化\n\n", *days)
	fmt.Fprintf(f, "| 负载 | 指标 | 第 1 天 | 第 %d 天 | 漂移 | 容忍带 | 结果 |\n|---|---|---|---|---|---|---|\n", *days)
	type pair struct {
		name                   string
		p50a, p99a, p50b, p99b time.Duration
	}
	drift := func(a, b time.Duration) float64 {
		if a == 0 {
			return 0
		}
		return float64(b-a) / float64(a) * 100
	}
	pairs := []pair{
		{"采集突发写", first.writeS.P50(), first.writeS.P99(), last.writeS.P50(), last.writeS.P99()},
		{"agent 上下文查询", first.ctxS.P50(), first.ctxS.P99(), last.ctxS.P50(), last.ctxS.P99()},
		{"审计扫描（当日窗口）", first.scanS.P50(), first.scanS.P99(), last.scanS.P50(), last.scanS.P99()},
	}
	degraded := []string{}
	for _, p := range pairs {
		d50, d99 := drift(p.p50a, p.p50b), drift(p.p99a, p.p99b)
		ok50 := d50 <= 20.0
		ok99 := d99 <= 30.0
		verdict := "✓"
		if !ok50 || !ok99 {
			verdict = "✗"
			degraded = append(degraded, p.name)
		}
		fmt.Fprintf(f, "| %s | p50 | %s | %s | %+.1f%% | ±20%% | %s |\n", p.name, latencyStr(p.p50a), latencyStr(p.p50b), d50, verdict)
		fmt.Fprintf(f, "| %s | p99 | %s | %s | %+.1f%% | ±30%% | %s |\n", p.name, latencyStr(p.p99a), latencyStr(p.p99b), d99, verdict)
	}

	// —— 劣化曲线（每 10 天取样）——
	fmt.Fprintf(f, "\n## 劣化曲线（每 10 天取样，账本增长 vs 延迟）\n\n")
	fmt.Fprintf(f, "| 天 | 账本版本数 | 写 p50 | 上下文 p50 | 扫描 p50 |\n|---|---|---|---|---|\n")
	for d := 0; d < *days; d += 10 {
		agg := aggs[d]
		fmt.Fprintf(f, "| %d | %d | %s | %s | %s |\n",
			d+1, (d+1)**writesPerDay, latencyStr(agg.writeS.P50()), latencyStr(agg.ctxS.P50()), latencyStr(agg.scanS.P50()))
	}

	fmt.Fprintf(f, "\n## 结论\n\n")
	if len(degraded) == 0 {
		fmt.Fprintf(f, "第 1 天 vs 第 %d 天全部负载 p50/p99 落在容忍带内：账本增长（%d → %d 条版本）无劣化。\n",
			*days, *writesPerDay, *days**writesPerDay)
		fmt.Fprintf(f, "M5（storage 优化）保持 optional；若后续观测到劣化曲线拐点，按本报告的曲线口径重跑本命令即可复测。\n")
	} else {
		fmt.Fprintf(f, "以下负载劣化超出容忍带：%v。\n", degraded)
		fmt.Fprintf(f, "M5（storage 优化）从 optional 升为 mandatory；劣化曲线见上表，修复后重跑本命令验证。\n")
	}
	emitMeasurementCorrections(f)
	fmt.Fprintf(f, "\n报告生成耗时 %s。\n", time.Since(started).Round(time.Second))
	return nil
}
