package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkWALAppendConcurrent 测量 group commit 在各并发度下的聚合吞吐与单次
// Append 延迟，用于把写路径的成本归因到 WAL 本身而非上层（memtable 锁 / 刷盘 /
// checkpoint / 网络）。报告 append/s 与平均延迟。
//
// 用法: go test -run XXX -bench WALAppendConcurrent ./storage/
func BenchmarkWALAppendConcurrent(b *testing.B) {
	for _, conc := range []int{1, 8, 50, 200} {
		b.Run(fmt.Sprintf("conc=%d", conc), func(b *testing.B) {
			w, err := NewWAL(filepath.Join(b.TempDir(), "wal.log"))
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			key := []byte("0123456789abcdef") // 16B，与压测口径一致
			value := make([]byte, 256)        // 256B
			const dur = 2 * time.Second

			var ops atomic.Int64
			var wg sync.WaitGroup
			deadline := time.Now().Add(dur)
			start := time.Now()

			for i := 0; i < conc; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for time.Now().Before(deadline) {
						if err := w.Append(WALOpPut, key, value); err != nil {
							b.Error(err)
							return
						}
						ops.Add(1)
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			n := ops.Load()
			b.ReportMetric(float64(n)/elapsed.Seconds(), "append/s")
			b.ReportMetric(float64(elapsed.Microseconds())/float64(n)*float64(conc), "us/append")
		})
	}
}
