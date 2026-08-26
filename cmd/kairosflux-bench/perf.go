package main

// perf.go：性能矩阵（阶段 B 性能面）。数据量 10w/100w/1000w 三档，每档：
// 载入（固定种子、并发载入器）→ 写路径测量（embedded PUT_VERSIONED / server
// v2 PUT_VERSIONED × ack every/window/none / server v1 PUT）→ 读路径测量
// （as-of / 前缀扫描 / LIST_WRITES）。QPS/p50/p99 全部由本工具实测生成。
//
// 已知口径（报告里会写）：
//   - 载入与测量写都会继续增加同一账本（版本化写不覆盖），键空间固定
//     100k 个逻辑键（quote:2026-08-17:%05d 模 100000），与数据量无关。
//   - LIST_WRITES 服务端响应受 config.G.MaxPackageSize（默认 16MiB）帧长
//     上限约束：全前缀无界审计扫描在 100w+ 档会被服务端以 result_too_large
//     拒绝（本工具实测并如实记录）；因此服务端 LIST_WRITES 在各档用
//     "最近 1% 时间窗"的有界窗口测量，embedded 侧则无界全扫可测。
//   - 各测量行固定采样数（-samples），QPS = 采样数/该行总耗时。

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux"
	"github.com/ChronoBrew/KairosFlux/client"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/predicate"
)

func cmdPerf(args []string) error {
	fs := flag.NewFlagSet("perf", flag.ExitOnError)
	outPath := fs.String("out", "docs/bench/01-perf-matrix.md", "报告输出路径")
	dataDir := fs.String("data-dir", "/tmp/kf-bench-perf", "数据目录（自动重建）")
	vol := fs.Int("vol", 100_000, "数据量档位：10w/100w/1000w 用 100000/1000000/10000000")
	loadWorkers := fs.Int("load-workers", 16, "载入并发数")
	measureWorkers := fs.Int("measure-workers", 8, "测量并发数")
	samples := fs.Int("samples", 2000, "每行测量采样数")
	skipLoad := fs.Bool("skip-load", false, "跳过载入/写路径,直接对既有 data-dir 做读路径测量(另补 1 条锚点版本化写)")
	fs.Parse(args)

	started := time.Now()
	f, err := os.Create(*outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	benchReportHeader(f, "性能矩阵：v1 PUT / v2 PUT_VERSIONED × ack 三档 × embedded/server × 10w/100w/1000w",
		fmt.Sprintf("数据量档位=%d；载入并发=%d；测量并发=%d；每行采样=%d。", *vol, *loadWorkers, *measureWorkers, *samples), started)
	fmt.Fprintf(f, "> 数据生成固定种子 42：同参数两次运行的键/负载序列逐字节相同；实测 QPS/分位数随机器负载波动，重跑以报告值为准。\n\n")

	// —— 载入 ——
	dir := *dataDir
	if !*skipLoad {
		os.RemoveAll(dir)
	}
	e, err := kairosflux.NewEmbedded(kairosflux.Options{DataDir: dir})
	if err != nil {
		return fmt.Errorf("载入引擎: %w", err)
	}
	if !*skipLoad {
		loadStart := time.Now()
		loadQPS := loadVolume(e, *vol, *loadWorkers)
		loadDur := time.Since(loadStart)
		fp, err := e.ReplayFingerprint("quote:", 0)
		if err != nil {
			return fmt.Errorf("载入后指纹: %w", err)
		}
		fmt.Fprintf(f, "## 载入阶段\n\n- 档位：%d 条版本化写入（%d 并发，固定种子 42）\n- 耗时：%s（载入吞吐 %.0f w/s）\n- 载入后 REPLAY_FINGERPRINT：逻辑键=%d 指纹=%s 对账不一致=%d\n\n",
			*vol, *loadWorkers, loadDur.Round(time.Millisecond), loadQPS, fp.KeyCount, fp.Fingerprint, len(fp.Mismatches))

		// —— 确定性验证（小档位双跑，避免耗时翻倍）——
		if *vol <= 100_000 {
			if err := determinismCheck(f); err != nil {
				return err
			}
		}

		// —— 写路径测量 ——
		fmt.Fprintf(f, "## 写路径（数据量=%d 时实测）\n\n", *vol)
		fmt.Fprintf(f, "| 路径 | QPS | p50 | p95 | p99 | 均值 | max | 错误 |\n|---|---|---|---|---|---|---|---|\n")

		// embedded PUT_VERSIONED
		stats := measureEmbeddedWrite(e, *measureWorkers, *samples)
		writeRow(f, "embedded PUT_VERSIONED（进程内直调）", stats)
	} else {
		fmt.Fprintf(f, "> -skip-load 模式：跳过载入/写路径，对既有 data-dir 直接读路径测量（另补 1 条锚点版本化写）。\n\n")
	}

	// server 各路径：Serve 网络壳
	port := freePort()
	srv, err := kairosflux.Serve(kairosflux.Options{DataDir: dir, Port: port})
	if err != nil {
		return fmt.Errorf("Serve: %w", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	defer srv.Close()

	if *skipLoad {
		// 锚点版本化写（1 条）：有界 LIST_WRITES 窗口须锚定最后一次版本化写。
		if _, err := measureV2Write(addr, negotiate.AckEvery, 1, 1); err != nil {
			return fmt.Errorf("锚点写: %w", err)
		}
	} else {
		// v2 PUT_VERSIONED × ack 三档（每行各开 measureWorkers 条连接）
		for _, ack := range []negotiate.AckTier{negotiate.AckEvery, negotiate.AckWindow, negotiate.AckNone} {
			stats, err := measureV2Write(addr, ack, *measureWorkers, *samples)
			if err != nil {
				return fmt.Errorf("v2 ack=%d 测量: %w", ack, err)
			}
			writeRow(f, fmt.Sprintf("server v2 PUT_VERSIONED ack=%v", ackName(ack)), stats)
		}
		// v1 PUT（client.New v1 客户端）
		stats, err := measureV1Put(addr, *measureWorkers, *samples)
		if err != nil {
			return fmt.Errorf("v1 PUT 测量: %w", err)
		}
		writeRow(f, "server v1 PUT（v1 协议客户端）", stats)
	}
	// v1 PUT 不产生时态账本条目：有界 LIST_WRITES 窗口须锚定最后一次版本化写。
	lastV2End := time.Now()

	// —— 读路径测量 ——
	fmt.Fprintf(f, "\n## 读路径（数据量=%d 时实测）\n\n", *vol)
	fmt.Fprintf(f, "| 路径 | 采样 | p50 | p95 | p99 | 均值 | max |\n|---|---|---|---|---|---|---|\n")

	// embedded as-of
	if stats, rerr := measureEmbeddedAsOf(e, *measureWorkers, *samples); rerr != nil {
		fmt.Fprintf(f, "| embedded GET_AS_OF（进程内） | 失败: %v |\n", rerr)
	} else {
		readRow(f, "embedded GET_AS_OF（进程内）", stats)
	}

	// server v2 as-of
	if stats, rerr := measureV2AsOf(addr, *measureWorkers, *samples); rerr != nil {
		fmt.Fprintf(f, "| server v2 GET_AS_OF | 失败: %v |\n", rerr)
	} else {
		readRow(f, "server v2 GET_AS_OF", stats)
	}

	// server v1 SCAN 全前缀（100k 个当前值）
	if stats, rerr := measureV1Scan(addr, *measureWorkers, *samples); rerr != nil {
		fmt.Fprintf(f, "| server v1 SCAN quote: 全前缀 | 失败: %v |\n", rerr)
	} else {
		readRow(f, "server v1 SCAN quote: 全前缀", stats)
	}

	// embedded LIST_WRITES 全前缀无界
	if stats, rerr := measureEmbeddedListWrites(e, *measureWorkers, 20); rerr != nil {
		fmt.Fprintf(f, "| embedded LIST_WRITES 全前缀无界（进程内） | 失败: %v |\n", rerr)
	} else {
		readRow(f, "embedded LIST_WRITES 全前缀无界（进程内）", stats)
	}

	// server LIST_WRITES：有界时间窗（最近 1%，跨度=载入写入跨度 1%）；无界
	// 全扫在 100w+ 会被 16MiB 帧长上限拒绝——单独实测并如实记录。
	tFrom := lastV2End.UnixNano() - int64(*vol)*10_000 // 锚定最后一次版本化写；窗口跨度=载入跨度 1%
	stats, boundedErr := measureV2ListWrites(addr, *measureWorkers, 20, tFrom)
	if boundedErr != nil {
		fmt.Fprintf(f, "| server v2 LIST_WRITES（最近1%%窗口） | 失败: %v |\n", boundedErr)
	} else {
		readRow(f, "server v2 LIST_WRITES（最近1%窗口）", stats)
	}
	if unbounded, uerr := measureV2ListWrites(addr, 1, 1, 0); uerr != nil {
		fmt.Fprintf(f, "| server v2 LIST_WRITES 全前缀无界 | 被拒绝: %v |\n", uerr)
	} else {
		readRow(f, "server v2 LIST_WRITES 全前缀无界", unbounded)
	}

	fmt.Fprintf(f, "\n---\n\n## 结论（数据量=%d 档）\n\n", *vol)
	fmt.Fprintf(f, "写路径瓶颈：standalone 模式每条写 = 1 次 WAL fsync（group commit 因 reqCh 无缓冲实际批次=1），\n吞吐被磁盘 fsync 速率钉住（见 02-resource-footprint 与 03-adversarial 的同一现象）。\n\n")
	emitMeasurementCorrections(f)
	fmt.Fprintf(f, "\n报告生成耗时 %s。\n", time.Since(started).Round(time.Second))
	return nil
}

func ackName(a negotiate.AckTier) string {
	switch a {
	case negotiate.AckEvery:
		return "every"
	case negotiate.AckWindow:
		return "window"
	default:
		return "none"
	}
}

// loadVolume 用 loadWorkers 个 goroutine 载入 vol 条确定性命名的版本化写入，
// 返回实测吞吐（w/s）。
func loadVolume(e *kairosflux.Engine, vol, loadWorkers int) float64 {
	r := seededRand()
	payloads := make([][]byte, 0, 64)
	for i := 0; i < 64; i++ {
		payloads = append(payloads, genPayload(r))
	}
	base := time.Now().UnixNano() - int64(vol) // 全部载入写入落在"过去"，as-of 读可直接用 now
	var done atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < loadWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < vol; i += loadWorkers {
				key := genKey(i)
				if _, err := e.PutVersioned(key, payloads[i%len(payloads)], base+int64(i), "bench-load", 2); err != nil {
					panic(err)
				}
				done.Add(1)
			}
		}(w)
	}
	wg.Wait()
	el := time.Since(start)
	return float64(done.Load()) / el.Seconds()
}

// determinismCheck 双跑小档位（10k，串行保证 seq 分配确定），验证同参数两
// 次载入的账本指纹逐字节一致。
func determinismCheck(f *os.File) error {
	const n = 10_000
	base := time.Now().UnixNano() - int64(n)
	fp := func(dir string) (string, error) {
		e, err := kairosflux.NewEmbedded(kairosflux.Options{DataDir: dir})
		if err != nil {
			return "", err
		}
		defer e.Close()
		r := seededRand()
		for i := 0; i < n; i++ {
			if _, err := e.PutVersioned(genKey(i), genPayload(r), base+int64(i), "bench-det", 2); err != nil {
				return "", err
			}
		}
		res, err := e.ReplayFingerprint("quote:", 0)
		if err != nil {
			return "", err
		}
		return res.Fingerprint, nil
	}
	os.RemoveAll("/tmp/kf-bench-det1")
	os.RemoveAll("/tmp/kf-bench-det2")
	f1, err := fp("/tmp/kf-bench-det1")
	if err != nil {
		return err
	}
	f2, err := fp("/tmp/kf-bench-det2")
	if err != nil {
		return err
	}
	equal := f1 == f2
	fmt.Fprintf(f, "\n## 确定性验证（固定种子 42 双跑 %d 条）\n\n- 第一次指纹：%s\n- 第二次指纹：%s\n- 结果：%s\n\n",
		n, f1, f2, map[bool]string{true: "一致（同参数两次载入逐字节同账本）", false: "不一致（数据确定性破坏）"}[equal])
	if !equal {
		return fmt.Errorf("确定性验证失败：两次载入指纹不一致 %s != %s", f1, f2)
	}
	return nil
}

// —— 测量行 ——

type row struct {
	label string
	stats *Stats
}

// measure 通用并发采样器：workers 个 goroutine 各持自己的连接（或直调），
// 每个做 n/workers 次 op，耗时入 stats。op 收到 worker 下标 w——并发体需要
// 确定性随机源时用 42+w 派生各自流（rand.Rand 非并发安全，共享会竞态崩溃）。
func runSamples(workers, total int, op func(w int) error) *Stats {
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				i := int(next.Add(1))
				if i > total {
					return
				}
				start := time.Now()
				err := op(w)
				s.Record(time.Since(start), err)
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	return s
}

// runSamplesErr 同 runSamples，另返回首个采样错误（供读路径如实上报失败）。
func runSamplesErr(workers, total int, op func(w int) error) (*Stats, error) {
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				i := int(next.Add(1))
				if i > total {
					return
				}
				start := time.Now()
				err := op(w)
				s.Record(time.Since(start), err)
				if err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	return s, firstErr
}

// keySeq 记录测量阶段写到的键下标（从载入量之后继续，保证不重写同 seq
// 归属——版本化写本来也不覆盖，只是让键分布更真实）。
var measureSeq atomic.Int64

func nextMeasureKey() string {
	return genKey(int(measureSeq.Add(1)) + 1)
}

func measureEmbeddedWrite(e *kairosflux.Engine, workers, samples int) *Stats {
	base := time.Now().UnixNano()
	randoms := make([]*rand.Rand, workers)
	for w := range randoms {
		randoms[w] = rand.New(rand.NewSource(42 + int64(w)))
	}
	return runSamples(workers, samples, func(w int) error {
		_, err := e.PutVersioned(nextMeasureKey(), genPayload(randoms[w]), base, "bench-write", 2)
		return err
	})
}

func measureV2Write(addr string, ack negotiate.AckTier, workers, samples int) (*Stats, error) {
	conns := make([]*benchV2Conn, workers)
	for i := range conns {
		c, err := dialV2(addr, ack)
		if err != nil {
			return nil, err
		}
		conns[i] = c
		defer c.Close()
	}
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	var firstErr error
	var errOnce sync.Once
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(42 + int64(w)))
			c := conns[w]
			for {
				i := int(next.Add(1))
				if i > samples {
					return
				}
				start := time.Now()
				_, err := c.putVersioned([]byte(nextMeasureKey()), genPayload(r), "bench-write")
				if err != nil {
					errOnce.Do(func() { firstErr = err })
				}
				s.Record(time.Since(start), err)
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	if errs := s.TotalErrs(); errs > 0 {
		fmt.Printf("  [warn] v2 ack=%v: 错误 %d/%d (%.1f%%) first=%v\n",
			ack, errs, s.TotalOps(), 100*float64(errs)/float64(s.TotalOps()), firstErr)
	}
	// 注：PUT_VERSIONED 的逐帧应答与 ack 档位无关（handlePutVersioned 恒
	// sendOK/sendErr），ack 档位只影响连接级计数/窗口，不影响本行延迟口径。
	return s, nil
}

func measureV1Put(addr string, workers, samples int) (*Stats, error) {
	clients := make([]*client.Client, workers)
	for i := range clients {
		c, err := client.New(client.Options{Addrs: []string{addr}})
		if err != nil {
			return nil, err
		}
		clients[i] = c
		defer c.Close()
	}
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	var firstErr error
	var errOnce sync.Once
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(42 + int64(w)))
			c := clients[w]
			for {
				i := int(next.Add(1))
				if i > samples {
					return
				}
				start := time.Now()
				err := c.Put(context.Background(), []byte(genKey(i)), genPayload(r))
				if err != nil {
					errOnce.Do(func() { firstErr = err })
				}
				s.Record(time.Since(start), err)
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	if errs := s.TotalErrs(); errs > 0 {
		fmt.Printf("  [warn] v1 PUT: 错误 %d/%d (%.1f%%) first=%v\n",
			errs, s.TotalOps(), 100*float64(errs)/float64(s.TotalOps()), firstErr)
	}
	return s, nil
}

func measureEmbeddedAsOf(e *kairosflux.Engine, workers, samples int) (*Stats, error) {
	now := time.Now().UnixNano()
	return runSamplesErr(workers, samples, func(w int) error {
		_, found, err := e.GetAsOf(genKey(int(time.Now().UnixNano()%100000)), now)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("as-of 未命中（不应发生）")
		}
		return nil
	})
}

func measureV2AsOf(addr string, workers, samples int) (*Stats, error) {
	conns := make([]*benchV2Conn, workers)
	for i := range conns {
		c, err := dialV2(addr, negotiate.AckEvery)
		if err != nil {
			return nil, err
		}
		conns[i] = c
		defer c.Close()
	}
	now := time.Now().UnixNano()
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := conns[w]
			for {
				i := int(next.Add(1))
				if i > samples {
					return
				}
				start := time.Now()
				_, found, err := c.getAsOf([]byte(genKey(int(time.Now().UnixNano()%100000))), now)
				if err == nil && !found {
					err = fmt.Errorf("as-of 未命中")
				}
				s.Record(time.Since(start), err)
				if err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	return s, firstErr
}

func measureV1Scan(addr string, workers, samples int) (*Stats, error) {
	clients := make([]*client.Client, workers)
	for i := range clients {
		c, err := client.New(client.Options{Addrs: []string{addr}})
		if err != nil {
			return nil, err
		}
		clients[i] = c
		defer c.Close()
	}
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := clients[w]
			for {
				i := int(next.Add(1))
				if i > samples {
					return
				}
				start := time.Now()
				_, err := c.Scan(context.Background(), []byte("quote:2026-08-17:"), []byte("quote:2026-08-17:\xff\xff"), predicate.Predicate{})
				s.Record(time.Since(start), err)
				if err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	return s, firstErr
}

func measureEmbeddedListWrites(e *kairosflux.Engine, workers, samples int) (*Stats, error) {
	return runSamplesErr(workers, samples, func(w int) error {
		res, err := e.ListWrites("quote:", 0, 0, "")
		if err != nil {
			return err
		}
		if len(res.Entries) == 0 {
			return fmt.Errorf("LIST_WRITES 空结果")
		}
		return nil
	})
}

// measureV2ListWrites：tFrom>0 时用 [tFrom, +∞) 时间窗（有界），tFrom<=0
// 无界全扫。
func measureV2ListWrites(addr string, workers, samples int, tFrom int64) (*Stats, error) {
	conns := make([]*benchV2Conn, workers)
	for i := range conns {
		c, err := dialV2(addr, negotiate.AckEvery)
		if err != nil {
			return nil, err
		}
		conns[i] = c
		defer c.Close()
	}
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := conns[w]
			for {
				i := int(next.Add(1))
				if i > samples {
					return
				}
				start := time.Now()
				n, err := c.listWrites([]byte("quote:"), tFrom, 0)
				if err == nil && n == 0 {
					err = fmt.Errorf("LIST_WRITES 空结果")
				}
				s.Record(time.Since(start), err)
				if err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	return s, firstErr
}

// —— 表格输出 ——

func writeRow(f *os.File, label string, s *Stats) {
	errs := s.TotalErrs()
	errc := "0"
	if errs > 0 {
		errc = fmt.Sprintf("%.1f%% (%d)", 100*float64(errs)/float64(s.TotalOps()), errs)
	}
	fmt.Fprintf(f, "| %s | %.0f | %s | %s | %s | %s | %s | %s |\n",
		label, s.QPS(), latencyStr(s.P50()), latencyStr(s.P95()), latencyStr(s.P99()), latencyStr(s.AvgLatency()), latencyStr(s.MaxLatency()), errc)
}

func readRow(f *os.File, label string, s *Stats) {
	fmt.Fprintf(f, "| %s | %d | %s | %s | %s | %s | %s |\n",
		label, s.TotalOps(), latencyStr(s.P50()), latencyStr(s.P95()), latencyStr(s.P99()), latencyStr(s.AvgLatency()), latencyStr(s.MaxLatency()))
}

// freePort 找空闲端口（与 kairosflux_test 同一模式）。
func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
