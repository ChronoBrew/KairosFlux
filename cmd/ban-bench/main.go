package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
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
	fmt.Println("  BanDB Benchmark")
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
