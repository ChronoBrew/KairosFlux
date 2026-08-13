package storage

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
)

// TestCompactionBench 是 compaction 压测台：确定性地驱动真实的 flush + compaction 级联，
// 量化写放大、per-level 文件分布、常驻堆，并模拟一次重启以暴露「level 塌缩」代价。
// 它同时作为正确性守卫：compaction 后所有 key 必须可读且值正确。
//
// 运行：go test ./storage/zstorage/ -run CompactionBench -v
func TestCompactionBench(t *testing.T) {
	// 小 memtable + 小 compaction 阈值，逼出频繁 flush 与多级 compaction。
	dir := t.TempDir()
	oldPath, oldComp := config.G.SSTablePath, config.G.MaxCompactionSize
	config.G.SSTablePath = dir
	config.G.MaxCompactionSize = 4
	defer func() { config.G.SSTablePath, config.G.MaxCompactionSize = oldPath, oldComp }()

	ResetCompactionStats()

	// 不经 NewEngine（避免启动异步 FlushWorker/ListenCompactCh 造成非确定性），
	// 只用 sst，手动驱动 flush 与 compaction。
	mt := newBareMemTable(NewSSTable())

	const (
		flushes   = 300
		chunkSize = 200
		valueSize = 100
	)
	val := make([]byte, valueSize)
	for i := range val {
		val[i] = 'x'
	}

	var ingestedBytes int64
	global := 0
	var maxMergeStall time.Duration

	for f := 0; f < flushes; f++ {
		entries := make([]LogEntry, chunkSize)
		for i := 0; i < chunkSize; i++ {
			k := []byte(fmt.Sprintf("key%08d", global))
			global++
			entries[i] = LogEntry{Key: k, Value: val}
			ingestedBytes += int64(len(k) + valueSize)
		}
		if err := mt.FlushToSSTable(entries); err != nil {
			t.Fatalf("flush %d failed: %v", f, err)
		}
		// 复刻 ListenCompactCh：每次 flush 后跑一轮 compaction 级联，并计一次 stall。
		t0 := time.Now()
		mt.CompactSSTable(0)
		if d := time.Since(t0); d > maxMergeStall {
			maxMergeStall = d
		}
	}

	// 采样 per-level 分布与常驻堆。
	dist, total := levelDistribution(mt.sst)
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	stats := ReadCompactionStats()

	t.Logf("=== 稳态（%d flush × %d entries = %d 条，%.1f MiB 灌入）===",
		flushes, chunkSize, flushes*chunkSize, float64(ingestedBytes)/(1<<20))
	t.Logf("文件总数=%d  per-level=%s", total, dist)
	t.Logf("写放大: flush=%.1fMiB compaction=%.1fMiB  写放大=%.2fx  compaction/flush=%.2fx",
		float64(stats.FlushBytes)/(1<<20), float64(stats.CompactionBytes)/(1<<20),
		float64(stats.FlushBytes+stats.CompactionBytes)/float64(ingestedBytes),
		float64(stats.CompactionBytes)/float64(stats.FlushBytes))
	t.Logf("最大单轮 compaction stall=%v  常驻 HeapAlloc=%.1fMiB", maxMergeStall, float64(ms.HeapAlloc)/(1<<20))

	// 正确性守卫：抽样验证 compaction 后数据无丢失、值正确。
	for _, idx := range []int{0, 1, 137, chunkSize, flushes * chunkSize / 2, flushes*chunkSize - 1} {
		k := []byte(fmt.Sprintf("key%08d", idx))
		v, found := mt.getFromSSTables(k)
		if !found {
			t.Fatalf("correctness: key %s lost after compaction", k)
		}
		if len(v) != valueSize {
			t.Fatalf("correctness: key %s value len=%d, want %d", k, len(v), valueSize)
		}
	}

	// === 模拟重启：LoadSSTableMetaList 把所有文件 Level 归 0（level 未持久化）===
	sst2 := NewSSTable()
	sst2.LoadSSTableMetaList()
	time.Sleep(150 * time.Millisecond) // 等异步预热 goroutine 落定，避免与后续 compaction 竞争
	dist2, total2 := levelDistribution(sst2)
	t.Logf("=== 重启后（LoadSSTableMetaList）===")
	collapsed := "level 已保留（未塌缩）"
	if dist2 != dist {
		collapsed = "level 塌缩"
	}
	t.Logf("文件总数=%d  per-level=%s  ← %s", total2, dist2, collapsed)

	mt2 := newBareMemTable(sst2)
	before := ReadCompactionStats()
	t0 := time.Now()
	mt2.CompactSSTable(0)
	restartMerge := time.Since(t0)
	after := ReadCompactionStats()
	t.Logf("重启后首轮 compaction: 耗时=%v  写出=%.1fMiB（一次性重写全部数据）",
		restartMerge, float64(after.CompactionBytes-before.CompactionBytes)/(1<<20))
}

// levelDistribution 统计各 level 文件数，返回形如 "L0=3 L1=2 L2=1" 与总数。
func levelDistribution(sst *SSTable) (string, int) {
	s := ""
	total := 0
	for level := 0; level < 13; level++ {
		n := len(sst.LevelFiles(level))
		if n > 0 {
			s += fmt.Sprintf("L%d=%d ", level, n)
			total += n
		}
	}
	if s == "" {
		s = "(空)"
	}
	return s, total
}
