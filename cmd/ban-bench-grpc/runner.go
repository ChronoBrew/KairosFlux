package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/test_grpc"
)

type Config struct {
	Addr      string
	Workers   int
	Duration  time.Duration
	KeySize   int
	ValueSize int
	KeyCount  int
	ReadRatio float64
	Mode      string
	Warmup    time.Duration
}

type Benchmark struct {
	cfg   Config
	stats *Stats
}

func NewBenchmark(cfg Config) *Benchmark {
	return &Benchmark{
		cfg:   cfg,
		stats: NewStats(),
	}
}

func (b *Benchmark) Run() error {
	if b.cfg.Mode == "get" || b.cfg.Mode == "mixed" || b.cfg.Mode == "delete" {
		fmt.Println("[Phase 1] Pre-populating data...")
		if err := b.prePopulate(); err != nil {
			return fmt.Errorf("pre-populate failed: %w", err)
		}
	}

	if b.cfg.Warmup > 0 {
		fmt.Printf("[Phase 2] Warming up (%s)...\n", b.cfg.Warmup)
		b.runPhase(b.cfg.Warmup, nil)
	}

	fmt.Printf("[Phase 3] Running benchmark (%s)...\n", b.cfg.Duration)
	b.stats.Start()
	b.runPhase(b.cfg.Duration, b.stats)
	b.stats.Stop()

	PrintReport(b.cfg, b.stats)
	return nil
}

func (b *Benchmark) prePopulate() error {
	stats := NewStats()
	stats.Start()

	var wg sync.WaitGroup
	keyCh := make(chan int, b.cfg.Workers*2)

	go func() {
		for i := 0; i < b.cfg.KeyCount; i++ {
			keyCh <- i
		}
		close(keyCh)
	}()

	for w := 0; w < b.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := test_grpc.NewClient(b.cfg.Addr)
			if err != nil {
				return
			}
			defer c.Close()

			value := make([]byte, b.cfg.ValueSize)
			for i := range keyCh {
				key := makeKey(i, b.cfg.KeySize)
				start := time.Now()
				err := c.Put(key, value)
				stats.Record(time.Since(start), err)
			}
		}()
	}

	wg.Wait()
	stats.Stop()

	if stats.TotalErrs() > 0 {
		return fmt.Errorf("%d errors during pre-population", stats.TotalErrs())
	}

	fmt.Printf("  Pre-populated %d keys in %s (%.0f qps)\n",
		stats.TotalOps(), stats.Duration().Round(time.Millisecond), stats.QPS())
	return nil
}

func (b *Benchmark) runPhase(dur time.Duration, stats *Stats) {
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	if stats != nil {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					ops := stats.TotalOps()
					elapsed := time.Since(stats.startTime)
					qps := float64(ops) / elapsed.Seconds()
					fmt.Printf("\r  Running... %d ops | %.0f qps | %s elapsed",
						ops, qps, elapsed.Round(time.Second))
				case <-stopCh:
					return
				}
			}
		}()
	}

	for w := 0; w < b.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.workerLoop(dur, stopCh, stats)
		}()
	}

	time.Sleep(dur)
	close(stopCh)
	wg.Wait()

	if stats != nil {
		fmt.Print("\r")
	}
}

func (b *Benchmark) workerLoop(dur time.Duration, stopCh chan struct{}, stats *Stats) {
	c, err := test_grpc.NewClient(b.cfg.Addr)
	if err != nil {
		return
	}
	defer c.Close()

	value := make([]byte, b.cfg.ValueSize)
	deadline := time.Now().Add(dur)

	for time.Now().Before(deadline) {
		select {
		case <-stopCh:
			return
		default:
		}

		keyIdx, _ := rand.Int(rand.Reader, big.NewInt(int64(b.cfg.KeyCount)))
		key := makeKey(int(keyIdx.Int64()), b.cfg.KeySize)

		op := b.chooseOp()

		var opErr error
		start := time.Now()

		switch op {
		case "put":
			opErr = c.Put(key, value)
		case "get":
			_, opErr = c.Get(key)
		case "delete":
			opErr = c.Delete(key)
		}

		lat := time.Since(start)
		if stats != nil {
			stats.Record(lat, opErr)
		}
	}
}

func (b *Benchmark) chooseOp() string {
	switch b.cfg.Mode {
	case "put":
		return "put"
	case "get":
		return "get"
	case "delete":
		return "delete"
	case "mixed":
		r, _ := rand.Int(rand.Reader, big.NewInt(100))
		if r.Int64() < int64(b.cfg.ReadRatio*100) {
			return "get"
		}
		return "put"
	default:
		return "put"
	}
}

func makeKey(idx int, keySize int) []byte {
	s := fmt.Sprintf("%0*x", keySize, idx)
	return []byte(s)
}
