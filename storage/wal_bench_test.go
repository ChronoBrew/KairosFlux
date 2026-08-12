package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkWALSharded 在固定总并发下比较「单个 WAL」与「N 个各自独立 group commit 的
// WAL（按 key 散列路由）」的聚合吞吐，用于判断分片 WAL 是否值得投入。
//
// 必要性：设备级 fsync 探针测的是「每次 fsync 只带一条记录」，而本项目的 WAL 有 group
// commit——分片实际上是用更小的批次换取并行的 fsync，二者方向相反，净效果无法从探针推得。
// 本基准直接用现有 WAL 类型搭出分片形态，无需先改生产代码即可给出结论。
//
// 用法: go test -run XXX -bench WALSharded ./storage/
func BenchmarkWALSharded(b *testing.B) {
	const conc = 50 // 与压测口径一致的总并发
	for _, shards := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			dir := b.TempDir()
			wals := make([]*WAL, shards)
			for i := range wals {
				w, err := NewWAL(filepath.Join(dir, fmt.Sprintf("wal%d.log", i)))
				if err != nil {
					b.Fatal(err)
				}
				wals[i] = w
				defer w.Close()
			}

			value := make([]byte, 256)
			const dur = 3 * time.Second

			var ops atomic.Int64
			var wg sync.WaitGroup
			deadline := time.Now().Add(dur)
			start := time.Now()

			for i := 0; i < conc; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					// 每个 writer 用互不相同的 key 前缀，散列后分布到各分片。
					seq := 0
					for time.Now().Before(deadline) {
						key := []byte(fmt.Sprintf("k%06d%09d", id, seq))
						seq++
						// 与生产路由一致：按 key 散列选分片，保证同 key 恒定同分片
						// （从而单 key 的写入顺序在其分片内天然保序）。
						h := uint64(14695981039346656037)
						for _, c := range key {
							h ^= uint64(c)
							h *= 1099511628211
						}
						if err := wals[h%uint64(shards)].Append(WALOpPut, key, value); err != nil {
							b.Error(err)
							return
						}
						ops.Add(1)
					}
				}(i)
			}
			wg.Wait()
			elapsed := time.Since(start)

			n := ops.Load()
			b.ReportMetric(float64(n)/elapsed.Seconds(), "append/s")
			b.ReportMetric(float64(elapsed.Microseconds())/float64(n)*conc, "us/append")
		})
	}
}

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
