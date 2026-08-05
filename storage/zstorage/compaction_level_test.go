package zstorage

import (
	"fmt"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

// TestCompaction_LevelPersistedAcrossRestart 是「重启不塌缩」的回归守卫：
// 造出跨多个 level 的文件后，模拟重启（新建 SSTable + LoadSSTableMetaList），
// 断言 per-level 分布被保留，而非全部塌缩到 L0（后者会在重启后触发全量重写）。
func TestCompaction_LevelPersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	oldPath, oldComp := config.G.SSTablePath, config.G.MaxCompactionSize
	config.G.SSTablePath = dir
	config.G.MaxCompactionSize = 4
	defer func() { config.G.SSTablePath, config.G.MaxCompactionSize = oldPath, oldComp }()

	mt := newBareMemTable(NewSSTable())
	val := make([]byte, 32)
	global := 0
	for f := 0; f < 40; f++ {
		entries := make([]istorage.LogEntry, 100)
		for i := range entries {
			entries[i] = istorage.LogEntry{Key: []byte(fmt.Sprintf("key%08d", global)), Value: val}
			global++
		}
		if err := mt.FlushToSSTable(entries); err != nil {
			t.Fatal(err)
		}
		mt.CompactSSTable(0)
	}

	before, totalBefore := levelDistribution(mt.sst)
	// 必须真的产生了高于 L0 的 level，否则本测试没意义。
	if before == fmt.Sprintf("L0=%d ", totalBefore) || totalBefore == 0 {
		t.Fatalf("precondition: expected multi-level files, got %q", before)
	}

	// 模拟重启：全新 SSTable 从磁盘恢复。
	sst2 := NewSSTable()
	sst2.LoadSSTableMetaList()
	time.Sleep(150 * time.Millisecond)
	after, totalAfter := levelDistribution(sst2)

	if totalAfter != totalBefore {
		t.Fatalf("file count changed across restart: before=%d after=%d", totalBefore, totalAfter)
	}
	if after != before {
		t.Fatalf("level structure collapsed across restart: before=%q after=%q", before, after)
	}
}
