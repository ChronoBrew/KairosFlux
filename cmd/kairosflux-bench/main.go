// 命令 kairosflux-bench 是压测工具集：
//
//   - 无子命令：既有 Kair-vs-gRPC 线协议基准（scripts/bench.sh 调用的入口，
//     行为不变）。
//   - perf        发布批次阶段 B 性能矩阵：v1 PUT / v2 PUT_VERSIONED × ack
//     every/window/none × embedded/server × 10w/100w/1000w；QPS/p50/p99 +
//     as-of/前缀扫描/LIST_WRITES 延迟；固定种子确定性验证。报告：
//     docs/bench/01-perf-matrix.md
//   - footprint   资源足迹：冷启动到就绪、空闲/载入后 RSS、soak CPU%、GC
//     停顿。报告：docs/bench/02-resource-footprint.md
//   - adversarial 找茬面：kill -9 一致性、并发客户端(10/50)、大 payload、
//     畸形帧回放、磁盘写满模拟、时钟回拨。报告：docs/bench/03-adversarial.md
//   - soak100     100+ 生产日长期混压：每日采集突发写 + agent 上下文查询 +
//     审计扫描并发，第 1 天 vs 第 100 天 p50/p99 不劣化断言。报告：
//     docs/bench/04-soak100.md
//
// 数据生成一律固定种子 42；报告由工具直接写 markdown（数字不经过人工抄写）。
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "perf", "footprint", "adversarial", "soak100":
			var err error
			switch os.Args[1] {
			case "perf":
				err = cmdPerf(os.Args[2:])
			case "footprint":
				err = cmdFootprint(os.Args[2:])
			case "adversarial":
				err = cmdAdversarial(os.Args[2:])
			case "soak100":
				err = cmdSoak100(os.Args[2:])
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, "kairosflux-bench", os.Args[1], "失败:", err)
				os.Exit(1)
			}
			return
		}
	}

	// 无子命令：既有 flags 模式（Kair-vs-gRPC 线协议基准，scripts/bench.sh
	// 调用入口，保持行为不变）。
	addr := flag.String("addr", "localhost:8080", "server address")
	workers := flag.Int("c", 10, "concurrent connections")
	duration := flag.Duration("d", 10*time.Second, "benchmark duration")
	keySize := flag.Int("ks", 16, "key size in bytes")
	valueSize := flag.Int("vs", 256, "value size in bytes")
	keyCount := flag.Int("n", 10000, "number of unique keys")
	readRatio := flag.Float64("r", 0.5, "read ratio in mixed mode (0-1)")
	mode := flag.String("mode", "mixed", "benchmark mode: put, get, delete, mixed")
	warmup := flag.Duration("w", 2*time.Second, "warmup duration")
	flag.Parse()

	cfg := Config{
		Addr:      *addr,
		Workers:   *workers,
		Duration:  *duration,
		KeySize:   *keySize,
		ValueSize: *valueSize,
		KeyCount:  *keyCount,
		ReadRatio: *readRatio,
		Mode:      *mode,
		Warmup:    *warmup,
	}

	fmt.Println("========================================")
	fmt.Println("  KairosFlux Benchmark")
	fmt.Println("========================================")
	fmt.Printf("  Server:    %s\n", cfg.Addr)
	fmt.Printf("  Mode:      %s\n", cfg.Mode)
	fmt.Printf("  Workers:   %d\n", cfg.Workers)
	fmt.Printf("  Duration:  %s (+ %s warmup)\n", cfg.Duration, cfg.Warmup)
	fmt.Printf("  Key size:  %d bytes\n", cfg.KeySize)
	fmt.Printf("  Val size:  %d bytes\n", cfg.ValueSize)
	fmt.Printf("  Key count: %d\n", cfg.KeyCount)
	if cfg.Mode == "mixed" {
		fmt.Printf("  Read ratio: %.0f%%\n", cfg.ReadRatio*100)
	}
	fmt.Println("========================================")
	fmt.Println()

	b := NewBenchmark(cfg)
	if err := b.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}
}
