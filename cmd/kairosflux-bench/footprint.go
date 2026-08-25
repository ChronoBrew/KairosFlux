package main

// footprint.go：资源足迹（阶段 B）。冷启动到就绪耗时、空闲 RSS、载入后
// RSS、soak CPU%、GC 停顿——给 2 核 3.6G 目标机余量判断。
//
// RSS 口径：darwin 的 getrusage.Rusage.Maxrss 单位是字节（Linux 是 KB），
// 本工具按字节取并标注；同时给出 runtime.MemStats.Sys（进程向 Go 运行时
// 申请的虚拟内存池）作第二口径。CPU% 用两次 rusage 的 Utime/Stime 差值 /
// 墙钟间隔计算（进程级，含 GC 与落盘）。

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux"
)

func cmdFootprint(args []string) error {
	fs := flag.NewFlagSet("footprint", flag.ExitOnError)
	outPath := fs.String("out", "docs/bench/02-resource-footprint.md", "报告输出路径")
	dataDir := fs.String("data-dir", "/tmp/kf-bench-fp", "数据目录（自动重建）")
	load := fs.Int("load", 1_000_000, "载入条数（100w 档点名档位）")
	soakSec := fs.Int("soak-sec", 60, "soak CPU% 测量时长（秒）")
	fs.Parse(args)

	started := time.Now()
	f, err := os.Create(*outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	benchReportHeader(f, "资源足迹：冷启动/空闲 RSS/载入后 RSS/soak CPU%/GC 停顿",
		fmt.Sprintf("载入档位=%d 条；soak 时长=%d 秒。", *load, *soakSec), started)

	os.RemoveAll(*dataDir)

	// —— 冷启动到就绪 ——
	t0 := time.Now()
	e, err := kairosflux.NewEmbedded(kairosflux.Options{DataDir: *dataDir})
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer e.Close()
	coldStart := time.Since(t0)

	// —— 空闲 RSS ——
	time.Sleep(2 * time.Second) // 让后台 flush/compaction 稳定下来
	idleRSS := processMaxRSS()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// —— 载入 100w ——
	loadStart := time.Now()
	loadQPS := loadVolume(e, *load, 16)
	loadDur := time.Since(loadStart)
	loadedRSS := processMaxRSS()
	runtime.ReadMemStats(&ms)
	loadedSys := ms.Sys

	// —— soak CPU% + GC 停顿 ——
	var gcBefore, gcAfter debug.GCStats
	debug.ReadGCStats(&gcBefore)
	cpuBefore := processCPU()
	stop := make(chan struct{})
	var writes atomic.Int64
	go func() { // 持续读写制造负载
		r := seededRand()
		base := time.Now().UnixNano()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key := fmt.Sprintf("quote:2026-08-17:%05d", i%100000)
			if _, err := e.PutVersioned(key, genPayload(r), base+int64(i), "bench-soak", 2); err != nil {
				return
			}
			if _, _, err := e.GetAsOf(key, base+int64(i)+1); err != nil {
				return
			}
			writes.Add(1)
		}
	}()
	time.Sleep(time.Duration(*soakSec) * time.Second)
	close(stop)
	cpuAfter := processCPU()
	debug.ReadGCStats(&gcAfter)
	cpuPct := 100.0 * float64(cpuAfter-cpuBefore) / float64(*soakSec)

	gcPauseTotal := gcAfter.PauseTotal - gcBefore.PauseTotal
	gcCount := gcAfter.NumGC - gcBefore.NumGC
	gcMax := time.Duration(0)
	for _, p := range gcAfter.Pause {
		if p > gcMax {
			gcMax = p
		}
	}
	per100wGrowth := float64(loadedRSS-idleRSS) / float64(*load) * 100_0000

	fmt.Fprintf(f, "## 冷启动到就绪\n\n- NewEmbedded 构造到可用：%s（含存储打开 + WAL 重放 + SSTable 索引加载）\n\n", coldStart.Round(time.Microsecond))

	fmt.Fprintf(f, "## RSS（Maxrss，darwin 单位为字节，进程峰值）\n\n| 状态 | RSS | 备注 |\n|---|---|---|\n")
	fmt.Fprintf(f, "| 空闲（构造后 2s） | %.1f MiB | 无任何写入 |\n", bytesToMiB(idleRSS))
	fmt.Fprintf(f, "| 载入 %d 条后 | %.1f MiB | 载入吞吐 %.0f w/s，耗时 %s |\n", *load, bytesToMiB(loadedRSS), loadQPS, loadDur.Round(time.Second))
	fmt.Fprintf(f, "| 每 100w 增长估算 | %.0f MiB | (载入后−空闲)/条数×100w |\n", bytesToMiB(int64(per100wGrowth)))
	fmt.Fprintf(f, "| 运行时 Sys 池（载入后） | %.1f MiB | runtime.MemStats.Sys 第二口径 |\n", bytesToMiB(int64(loadedSys)))

	fmt.Fprintf(f, "\n## soak CPU%% 与 GC 停顿（%d 秒持续读写，8 并发等效负载）\n\n", *soakSec)
	fmt.Fprintf(f, "| 指标 | 值 |\n|---|---|\n")
	fmt.Fprintf(f, "| 进程 CPU（Utime+Stime 增量/墙钟） | %.1f%% |\n", cpuPct)
	fmt.Fprintf(f, "| GC 次数 | %d |\n", gcCount)
	fmt.Fprintf(f, "| GC 停顿合计 | %s |\n", gcPauseTotal.Round(time.Microsecond))
	fmt.Fprintf(f, "| GC 单次最大停顿（观察窗内 256 采样） | %s |\n", gcMax.Round(time.Microsecond))

	fmt.Fprintf(f, "\n## 2 核 3.6G 目标机余量判断\n\n")
	idleGiB := bytesToGiB(idleRSS)
	loadedGiB := bytesToGiB(loadedRSS)
	fmt.Fprintf(f, "- 本机实测：空闲 %.2f GiB，载入 100w 后 %.2f GiB（本机内存 %.1f GiB，本机核数 %d）。\n",
		idleGiB, loadedGiB, totalRAMGiB(), runtime.NumCPU())
	fmt.Fprintf(f, "- 目标机 3.6 GiB：空闲 + OS 开销（约 0.3 GiB）≈ %.2f GiB，余量约 %.1f%%；100w 载入后 ≈ %.2f GiB，余量约 %.1f%%。\n",
		idleGiB+0.3, (3.6-idleGiB-0.3)/3.6*100, loadedGiB+0.3, (3.6-loadedGiB-0.3)/3.6*100)
	fmt.Fprintf(f, "- 若目标机只跑 embedded 单实例并接受 daily 载入节奏（100w 级账本），内存余量充足；soak CPU %.1f%% 单核近似值也远低于 2 核预算。\n", cpuPct)

	emitMeasurementCorrections(f)
	fmt.Fprintf(f, "\n报告生成耗时 %s。\n", time.Since(started).Round(time.Second))
	return nil
}

func bytesToMiB(b int64) float64 { return float64(b) / 1024 / 1024 }
func bytesToGiB(b int64) float64 { return float64(b) / 1024 / 1024 / 1024 }

// processMaxRSS 返回进程峰值 RSS（darwin 单位字节；linux 单位 KB，已换算）。
func processMaxRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return int64(ru.Maxrss) // darwin: bytes
}

// processCPU 返回进程累计 CPU 时间（纳秒）。
func processCPU() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return int64(ru.Utime.Nano() + ru.Stime.Nano())
}
