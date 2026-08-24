// 命令 ingest 是 M1 的 A1 层压测：进程内直接驱动存储引擎，证明存储引擎
// 在内存封顶下扛得住高频顺序写。分两相：
//   - 饱和相（闭环打满）：找写吞吐天花板 Rmax + 内存峰值。
//   - 开环相（定速率）：在若干 IMU 速率档下证「定速率不丢帧」+ 尾延迟 + 内存封顶。
//
// 注意边界：存储引擎层无 WAL（已被移除），未 flush 的 MemTable 数据易失，
// 故本程序「不」测崩溃恢复——崩溃 0 丢失是 Raft 全链路(A2)的属性。
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/storage"
)

func main() {
	rates := flag.String("rates", "1000,10000,50000,200000", "开环采集速率档位 (Hz)，逗号分隔")
	dur := flag.Duration("d", 10*time.Second, "每相/每档的持续时间")
	satDur := flag.Duration("sat", 10*time.Second, "饱和相持续时间；0 跳过")
	valueSize := flag.Int("vs", 64, "IMU 样本 value 字节数")
	qDepth := flag.Int("qdepth", 1024, "有界队列深度；满即记为丢帧")
	memTableSize := flag.Int("memtable", 4096, "MaxMemTableSize（active 表条目阈值，调小以强制频繁 flush 验内存封顶）")
	budget := flag.Int64("budget", 64<<20, "MemTableMaxInflightBytes 字节预算（令牌桶背压）；0 关闭背压")
	flag.Parse()

	config.G.MemTableMaxInflightBytes = *budget // 由命令行覆盖，便于 before/after 对比

	rateList, err := parseRates(*rates)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -rates: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("========================================")
	fmt.Println("  KairosFlux Ingest Benchmark (A1 · engine)")
	fmt.Println("========================================")
	fmt.Printf("  Value size:   %d bytes\n", *valueSize)
	fmt.Printf("  Queue depth:  %d\n", *qDepth)
	fmt.Printf("  MemTable cap: %d entries\n", *memTableSize)
	fmt.Printf("  Inflight budget: %d bytes (0=背压关闭)\n", *budget)
	fmt.Printf("  Duration:     sat=%s  open-loop=%s/rate\n", *satDur, *dur)
	fmt.Printf("  Open rates:   %v Hz\n", rateList)
	fmt.Println("========================================")
	fmt.Println()

	// 1) 饱和相：闭环打满，找吞吐天花板。-sat 0 可跳过（避免与开环相互相污染内存测量）。
	var sat Result
	hasSat := *satDur > 0
	if hasSat {
		sat = runSaturation(*satDur, *valueSize, *memTableSize)
		fmt.Printf("[Saturation] throughput=%.0f writes/s  mean=%s  inflight_peak=%s  heap_peak=%s\n\n",
			sat.Throughput, lat(sat.MeanLat), mib(sat.InflightPeak), mib(sat.HeapPeak))
	}

	// 2) 开环相：各速率档证 0 丢帧 + 尾延迟 + 内存封顶。
	results := make([]Result, 0, len(rateList))
	for _, r := range rateList {
		res := runOpenLoop(r, *dur, *valueSize, *qDepth, *memTableSize)
		results = append(results, res)
		printResult(res)
	}

	printTable(hasSat, sat, results)
}

// Result 单相/单档的压测结果
type Result struct {
	Label                      string // "saturated" 或 "<rate>Hz"
	RateHz                     int    // 开环目标速率；饱和相为 0
	Duration                   time.Duration
	Produced                   int64         // 应投递样本数（饱和相 = 写入数）
	Dropped                    int64         // 队列满导致的丢帧数
	Written                    int64         // 实际写入引擎的样本数
	Throughput                 float64       // 实际写入吞吐 (writes/sec)
	MeanLat                    time.Duration // 饱和相用：1/throughput
	P50, P99, P999, P9999, Max time.Duration
	HeapPeak                   uint64 // HeapAlloc 峰值 (bytes)
	SysPeak                    uint64 // Sys 峰值 (bytes)
	InflightPeak               uint64 // 未 flush 字节信用峰值 (bytes)
}

// setupEngine 指向临时目录并以小 memtable 创建引擎，返回引擎与清理函数。
func setupEngine(memTableSize int) (*storage.Engine, func()) {
	tmp, err := os.MkdirTemp("", "bandb-ingest-*")
	if err != nil {
		panic(err)
	}
	config.G.SSTablePath = tmp
	config.G.WALPath = filepath.Join(tmp, "wal.log")
	config.G.MaxMemTableSize = memTableSize

	memTable := storage.NewEngine(storage.DefaultOptions())
	cleanup := func() {
		_ = memTable.Close()
		os.RemoveAll(tmp)
	}
	return memTable, cleanup
}

// memSampler 每 100ms 采样一次堆内存与未 flush 字节信用，返回停止函数（调用后回填峰值）。
func memSampler(mt *storage.Engine, heapPeak, sysPeak, inflightPeak *uint64) func() {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ms runtime.MemStats
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > *heapPeak {
					*heapPeak = ms.HeapAlloc
				}
				if ms.Sys > *sysPeak {
					*sysPeak = ms.Sys
				}
				if inflight := uint64(mt.InflightBytes()); inflight > *inflightPeak {
					*inflightPeak = inflight
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		wg.Wait()
	}
}

// runSaturation 闭环打满：不做 per-op 计时（避免计时器开销污染亚微秒写），
// 只数总量得到吞吐天花板，平均延迟由 1/throughput 推导。
func runSaturation(dur time.Duration, valueSize, memTableSize int) Result {
	memTable, cleanup := setupEngine(memTableSize)
	defer cleanup()

	runtime.GC() // 清掉上一轮残留，使本轮 heap 峰值只反映本轮活对象
	var heapPeak, sysPeak, inflightPeak uint64
	stopMem := memSampler(memTable, &heapPeak, &sysPeak, &inflightPeak)

	var written, seq int64
	start := time.Now()
	deadline := start.Add(dur)
	for time.Now().Before(deadline) {
		// 每 1024 次才查一次时钟，降低 time.Now 开销对吞吐的干扰。
		for i := 0; i < 1024; i++ {
			key := []byte(fmt.Sprintf("imu:dev0:%020d", seq))
			seq++
			value := make([]byte, valueSize) // 每条独立 value，保证内存计量真实
			_ = memTable.Put(key, value)
			written++
		}
	}
	elapsed := time.Since(start)
	stopMem()

	tput := float64(written) / elapsed.Seconds()
	var mean time.Duration
	if tput > 0 {
		mean = time.Duration(float64(time.Second) / tput)
	}
	return Result{
		Label:        "saturated",
		Duration:     elapsed,
		Produced:     written,
		Written:      written,
		Throughput:   tput,
		MeanLat:      mean,
		HeapPeak:     heapPeak,
		SysPeak:      sysPeak,
		InflightPeak: inflightPeak,
	}
}

// runOpenLoop 开环定速率：生产者按固定速率非阻塞投递，队列满即丢帧；
// 消费者单写入者，per-op 计时（此处速率有界，计时开销可忽略），得到尾延迟。
func runOpenLoop(rateHz int, dur time.Duration, valueSize, qDepth, memTableSize int) Result {
	memTable, cleanup := setupEngine(memTableSize)
	defer cleanup()

	runtime.GC() // 清掉上一轮残留，使本轮 heap 峰值只反映本轮活对象
	var heapPeak, sysPeak, inflightPeak uint64
	stopMem := memSampler(memTable, &heapPeak, &sysPeak, &inflightPeak)

	type sample struct{ key, value []byte }
	q := make(chan sample, qDepth)

	var produced, dropped int64
	go func() {
		interval := time.Duration(int64(time.Second) / int64(rateHz))
		next := time.Now()
		deadline := time.Now().Add(dur)
		var seq int64
		for time.Now().Before(deadline) {
			key := []byte(fmt.Sprintf("imu:dev0:%020d", seq))
			seq++
			produced++
			value := make([]byte, valueSize) // 每条独立 value
			select {
			case q <- sample{key: key, value: value}:
			default:
				dropped++ // 队列满 = 引擎跟不上 = 丢帧
			}
			next = next.Add(interval)
			if d := time.Until(next); d > 0 {
				time.Sleep(d)
			}
		}
		close(q)
	}()

	latencies := make([]time.Duration, 0, rateHz*int(dur/time.Second)+1024)
	var written int64
	start := time.Now()
	for s := range q {
		t0 := time.Now()
		_ = memTable.Put(s.key, s.value)
		latencies = append(latencies, time.Since(t0))
		written++
	}
	elapsed := time.Since(start)
	stopMem()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return Result{
		Label:        fmt.Sprintf("%dHz", rateHz),
		RateHz:       rateHz,
		Duration:     elapsed,
		Produced:     produced,
		Dropped:      dropped,
		Written:      written,
		Throughput:   float64(written) / elapsed.Seconds(),
		P50:          pct(latencies, 0.50),
		P99:          pct(latencies, 0.99),
		P999:         pct(latencies, 0.999),
		P9999:        pct(latencies, 0.9999),
		Max:          pct(latencies, 1.0),
		HeapPeak:     heapPeak,
		SysPeak:      sysPeak,
		InflightPeak: inflightPeak,
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printResult(r Result) {
	fmt.Printf("--- %s ---\n", r.Label)
	fmt.Printf("  produced=%d  written=%d  dropped=%d\n", r.Produced, r.Written, r.Dropped)
	fmt.Printf("  throughput=%.0f writes/s  p99.9=%s  p99.99=%s  max=%s\n",
		r.Throughput, lat(r.P999), lat(r.P9999), lat(r.Max))
	fmt.Printf("  inflight_peak=%s  heap_peak=%s  sys_peak=%s\n", mib(r.InflightPeak), mib(r.HeapPeak), mib(r.SysPeak))
	fmt.Println()
}

func printTable(hasSat bool, sat Result, rs []Result) {
	fmt.Println("========================================")
	fmt.Println("  Summary")
	fmt.Println("========================================")
	if hasSat {
		fmt.Printf("  ceiling (saturated): %.0f writes/s  heap_peak=%s\n", sat.Throughput, mib(sat.HeapPeak))
	}
	fmt.Println("  --- open-loop (fixed rate) ---")
	fmt.Printf("  %-10s %-12s %-9s %-9s %-13s %-10s\n",
		"rate", "throughput", "dropped", "max", "inflight_peak", "heap_peak")
	for _, r := range rs {
		fmt.Printf("  %-10s %-12.0f %-9d %-9s %-13s %-10s\n",
			r.Label, r.Throughput, r.Dropped, lat(r.Max), mib(r.InflightPeak), mib(r.HeapPeak))
	}
	fmt.Println("========================================")
	fmt.Println("  Durability: N/A at engine layer (no storage WAL — see A2 / Raft path)")
	fmt.Println("========================================")
}

func parseRates(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		if v <= 0 {
			return nil, fmt.Errorf("rate must be > 0, got %d", v)
		}
		out = append(out, v)
	}
	return out, nil
}

func lat(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fus", float64(d.Nanoseconds())/1000)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func mib(b uint64) string {
	return fmt.Sprintf("%.1fMiB", float64(b)/(1024*1024))
}
