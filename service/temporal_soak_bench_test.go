package service

// 时态内核 M2 任务书明确要求的 soak 实测："时间二级索引先不做：先用版本键
// 前缀扫描 + benchmark 实测（含单日百万级写入 soak 测试），扫描慢于阈值再
// 立项索引，报告里给真实数字。"
//
// 用 go test -bench（不是普通 Test）：与 service/router_bench_test.go 同一
// 惯例，Benchmark 函数天然不会被 `go test ./...`/CI 默认跑到（只有显式传
// -bench 才会执行），不会拖慢日常测试循环，同时仍然是可重复执行、可被
// -benchtime 控制精确迭代次数的标准 Go 基准设施——不需要额外发明一套
// soak/环境变量开关机制。
//
// 数据形状："单日" 的真实含义是"很多标的、每个标的写入次数不多"（trading-day
// shape），不是"一个 key 写一百万次"——ReplayFingerprint/ListWrites 的扫描
// 成本模型是 O(逻辑键数)（对每个候选逻辑键各发起一次 ScanRaw，见
// TemporalStore.ReplayFingerprint/ListWrites 的文档），键数才是决定"扫描慢
// 不慢"的主变量，本基准的种子数据形状必须反映这一点，否则测出来的数字对
// "是否要上时间二级索引"这个决策没有意义（对照见 advisor 复核意见：
// "该测的是 per-key scan cost，不只是聚合吞吐"）。
//
// 运行（建议显式 -benchtime=1x，只跑一次真实迭代，避免 Go 基准框架为了统计
// 稳定性反复重跑百万级写入）：
//
//	go test ./service/ -run '^$' -bench '^BenchmarkTemporalSoak' -benchtime=1x -v

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// soakKeyCount/soakVersionsPerKey：10 万个逻辑键 × 每键 10 个版本 = 100 万次
// 版本化写入，对应"单日百万级写入"。10 万个键的量级已经明显超过任何真实
// A 股全市场标的数（约 5000+），刻意留出安全边际。
const (
	soakKeyCount       = 100_000
	soakVersionsPerKey = 10
	soakWriterFanout   = 256 // 并发写协程数：让 WAL group commit（storage/wal.go walMaxBatch=256）有机会把并发写合并进同一次 fsync
)

// BenchmarkTemporalSoak_OneMillionWritesThenScan 是任务书要求的单日百万级
// 写入 soak 测试主体：并发写入 100,000 键 × 10 版本 = 1,000,000 条版本化
// 记录，随后各跑一次 REPLAY_FINGERPRINT（无界，与 :current 对账）与
// LIST_WRITES（无过滤，全量审计）的全量扫描，b.ReportMetric 把三段耗时与
// 派生的 per-key 扫描延迟都写进 -bench 输出，供报告直接摘录真实数字。
func BenchmarkTemporalSoak_OneMillionWritesThenScan(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runSoakIteration(b)
	}
}

func runSoakIteration(b *testing.B) {
	b.Helper()
	kv := setupBenchKV(b) // 复用 router_bench_test.go 的大 MaxMemTableSize standalone 装配，避免中途触发 flush 混入计时
	ts := NewTemporalStore(kv)

	keys := make([]string, soakKeyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("quote:2026-08-19:soak:%06d", i)
	}
	payload := benchQuotePayload("600000") // 复用既有的真实行情快照负载形状，不用占位空字符串

	writeStart := time.Now()
	var wg sync.WaitGroup
	var nextKeyIdx atomic.Int64
	var firstErr atomic.Pointer[error] // testing.B.Fatalf 只能从跑 benchmark 的那个 goroutine 调用（go vet 会拦，见其文档），并发写协程只记录首个错误，主 goroutine 汇合后再 Fatalf
	totalWrites := soakKeyCount * soakVersionsPerKey
	for w := 0; w < soakWriterFanout; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := nextKeyIdx.Add(1) - 1
				if idx >= int64(soakKeyCount) {
					return
				}
				logical := keys[idx]
				for v := 0; v < soakVersionsPerKey; v++ {
					if _, err := ts.PutVersioned(logical, payload, time.Now().UnixNano(), "soak-writer", 1); err != nil {
						firstErr.CompareAndSwap(nil, &err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	writeElapsed := time.Since(writeStart)
	if p := firstErr.Load(); p != nil {
		b.Fatalf("PutVersioned 失败: %v", *p)
	}

	replayStart := time.Now()
	replay, err := ts.ReplayFingerprint("quote:2026-08-19:soak:", 0)
	replayElapsed := time.Since(replayStart)
	if err != nil {
		b.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if int(replay.KeyCount) != soakKeyCount || len(replay.Mismatches) != 0 {
		b.Fatalf("soak 数据集重放应零不一致且键数=%d: got keyCount=%d mismatches=%d",
			soakKeyCount, replay.KeyCount, len(replay.Mismatches))
	}

	listStart := time.Now()
	writes, err := ts.ListWrites("quote:2026-08-19:soak:", 0, 0, "")
	listElapsed := time.Since(listStart)
	if err != nil {
		b.Fatalf("ListWrites 失败: %v", err)
	}
	if len(writes.Entries) != totalWrites {
		b.Fatalf("soak 数据集 LIST_WRITES 应看到全部 %d 条: got %d", totalWrites, len(writes.Entries))
	}

	b.ReportMetric(float64(totalWrites)/writeElapsed.Seconds(), "writes/sec")
	b.ReportMetric(float64(replayElapsed.Microseconds())/float64(soakKeyCount), "replay_us/key")
	b.ReportMetric(float64(replayElapsed.Milliseconds()), "replay_ms_total")
	b.ReportMetric(float64(listElapsed.Microseconds())/float64(soakKeyCount), "listwrites_us/key")
	b.ReportMetric(float64(listElapsed.Milliseconds()), "listwrites_ms_total")

	b.Logf("soak: keys=%d versionsPerKey=%d totalWrites=%d writeElapsed=%s (%.0f writes/sec) replayElapsed=%s (%.2f us/key) listWritesElapsed=%s (%.2f us/key)",
		soakKeyCount, soakVersionsPerKey, totalWrites, writeElapsed, float64(totalWrites)/writeElapsed.Seconds(),
		replayElapsed, float64(replayElapsed.Microseconds())/float64(soakKeyCount),
		listElapsed, float64(listElapsed.Microseconds())/float64(soakKeyCount))
}
