package main

import (
	"fmt"
	"strings"
	"time"
)

func PrintReport(cfg Config, s *Stats) {
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("  Benchmark Results")
	fmt.Println("========================================")
	fmt.Printf("  Total ops:    %d\n", s.TotalOps())
	fmt.Printf("  Errors:       %d\n", s.TotalErrs())
	fmt.Printf("  Duration:     %s\n", s.Duration().Round(time.Millisecond))
	fmt.Printf("  Success rate: %.2f%%\n", s.SuccessRate())
	fmt.Println("----------------------------------------")
	fmt.Printf("  QPS:          %.0f req/s\n", s.QPS())
	fmt.Println("----------------------------------------")
	fmt.Printf("  Avg latency:  %s\n", latencyStr(s.AvgLatency()))
	fmt.Printf("  P50 latency:  %s\n", latencyStr(s.P50()))
	fmt.Printf("  P95 latency:  %s\n", latencyStr(s.P95()))
	fmt.Printf("  P99 latency:  %s\n", latencyStr(s.P99()))
	fmt.Printf("  Max latency:  %s\n", latencyStr(s.MaxLatency()))
	fmt.Println("========================================")

	// bandwidth estimate
	avgBytesPerOp := float64(cfg.KeySize + cfg.ValueSize)
	bandwidth := s.QPS() * avgBytesPerOp
	fmt.Printf("  ~Bandwidth:   %s/s\n", byteStr(bandwidth))
	fmt.Println("========================================")
}

func latencyStr(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%d ns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.2f us", float64(d.Nanoseconds())/1000)
	case d < time.Second:
		return fmt.Sprintf("%.2f ms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.3f s", d.Seconds())
	}
}

func byteStr(bytes float64) string {
	switch {
	case bytes < 1024:
		return fmt.Sprintf("%.0f B", bytes)
	case bytes < 1024*1024:
		return fmt.Sprintf("%.2f KB", bytes/1024)
	case bytes < 1024*1024*1024:
		return fmt.Sprintf("%.2f MB", bytes/(1024*1024))
	default:
		return fmt.Sprintf("%.2f GB", bytes/(1024*1024*1024))
	}
}

func ProgressBar(current, total int64, start time.Time) string {
	if total == 0 {
		return ""
	}
	pct := float64(current) / float64(total) * 100
	barLen := 30
	filled := int(float64(barLen) * float64(current) / float64(total))
	bar := strings.Repeat("=", filled) + strings.Repeat("-", barLen-filled)

	elapsed := time.Since(start)
	qps := float64(current) / elapsed.Seconds()

	return fmt.Sprintf("\r[%s] %.0f%% | %d/%d ops | %.0f qps | %s elapsed",
		bar, pct, current, total, qps, elapsed.Round(time.Second))
}

func modeDescription(cfg Config) string {
	switch cfg.Mode {
	case "put":
		return fmt.Sprintf("PUT only (%d B keys, %d B values, %d workers)",
			cfg.KeySize, cfg.ValueSize, cfg.Workers)
	case "get":
		return fmt.Sprintf("GET only (%d keys, %d workers)",
			cfg.KeyCount, cfg.Workers)
	case "delete":
		return fmt.Sprintf("DELETE only (%d keys, %d workers)",
			cfg.KeyCount, cfg.Workers)
	case "mixed":
		return fmt.Sprintf("Mixed PUT/GET (%.0f%% reads, %d workers)",
			cfg.ReadRatio*100, cfg.Workers)
	default:
		return cfg.Mode
	}
}
