package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
)

// TestRecency_RandomizedOverwritesSurviveRestart 是 newest-wins 的随机化守卫：
// 对一小组 key 做多轮覆盖写，穿插 flush 与 compaction，用参考 map 记录每个 key 的最新值；
// 然后模拟重启，逐一校验读到的都是最新值。这能捕捉 metas 重建顺序错误导致的陈旧值倒挂。
func TestRecency_RandomizedOverwritesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	oldPath, oldComp := config.G.SSTablePath, config.G.MaxCompactionSize
	config.G.SSTablePath = dir
	config.G.MaxCompactionSize = 3
	defer func() { config.G.SSTablePath, config.G.MaxCompactionSize = oldPath, oldComp }()

	mt := newBareMemTable(NewSSTable())

	const keyspace = 40
	ref := make(map[string]string) // 参考真值：key → 最新 value

	// 确定性伪随机（避免依赖 math/rand，保持可复现）：线性同余。
	rng := uint64(12345)
	next := func(n int) int {
		rng = rng*6364136223846793005 + 1442695040888963407
		return int((rng >> 33) % uint64(n))
	}

	round := 0
	for flush := 0; flush < 120; flush++ {
		// 每个 flush 覆盖写若干 key。
		batch := 1 + next(5)
		entries := make([]LogEntry, 0, batch)
		for i := 0; i < batch; i++ {
			k := fmt.Sprintf("k%03d", next(keyspace))
			v := fmt.Sprintf("v%d", round)
			round++
			entries = append(entries, LogEntry{Key: []byte(k), Value: []byte(v)})
			ref[k] = v // 同一 flush 内后写覆盖先写；FlushToSSTable 内部按序 insert 去重
		}
		if err := mt.FlushToSSTable(entries); err != nil {
			t.Fatal(err)
		}
		mt.CompactSSTable(0)
	}

	// 模拟重启。
	sst2 := NewSSTable()
	sst2.LoadSSTableMetaList()
	time.Sleep(200 * time.Millisecond)
	mt2 := newBareMemTable(sst2)

	stale := 0
	for k, want := range ref {
		v, ok := mt2.getFromSSTables([]byte(k))
		if !ok {
			t.Fatalf("post-restart key %s lost", k)
		}
		if string(v) != want {
			stale++
			if stale <= 5 {
				t.Errorf("post-restart key %s = %q, want %q (stale)", k, v, want)
			}
		}
	}
	if stale > 0 {
		t.Fatalf("%d/%d keys returned stale values after restart", stale, len(ref))
	}
}
