package zstorage

import (
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

// TestRecency_OverwriteAcrossCompactionSurvivesRestart 是「newest-wins 跨重启不倒挂」的
// 判别测试：x=A 落进已合并（compacted）文件，再写 x=B（留在新 L0），模拟重启后 GET x 必须是 B。
//
// 风险点：读路径 getFromSSTables 按 mata 逆序判定「新胜旧」（内存里 = 创建序）；但重启时
// LoadSSTableMetaList 按文件名字符串排序重建 mata，而非真实时间序——若排序把旧的 merged 文件
// 排到新的 L0 文件之后，逆序就会先命中旧 merged，返回陈旧值 A。
func TestRecency_OverwriteAcrossCompactionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	oldPath, oldComp := config.G.SSTablePath, config.G.MaxCompactionSize
	config.G.SSTablePath = dir
	config.G.MaxCompactionSize = 2
	defer func() { config.G.SSTablePath, config.G.MaxCompactionSize = oldPath, oldComp }()

	mt := &MemTable{sst: NewSSTable()}

	// x=A 与另一个 key 一起落 L0，再补一个 L0，触发 compaction 把它们并成 L1 merged 文件。
	if err := mt.FlushToSSTable([]istorage.LogEntry{
		{Key: []byte("x"), Value: []byte("A")},
		{Key: []byte("a"), Value: []byte("1")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := mt.FlushToSSTable([]istorage.LogEntry{{Key: []byte("m"), Value: []byte("2")}}); err != nil {
		t.Fatal(err)
	}
	mt.CompactSSTable(0) // L0 两文件 >=2 → 合并到 L1；x=A 现位于 merged 文件

	// 覆盖写 x=B，留在新的 L0 文件（更新）。
	if err := mt.FlushToSSTable([]istorage.LogEntry{{Key: []byte("x"), Value: []byte("B")}}); err != nil {
		t.Fatal(err)
	}

	if v, ok := mt.getFromSSTables([]byte("x")); !ok || string(v) != "B" {
		t.Fatalf("pre-restart GET x = %q (found=%v), want B", v, ok)
	}

	// 模拟重启：全新 SSTable 从磁盘恢复。
	sst2 := NewSSTable()
	sst2.LoadSSTableMetaList()
	time.Sleep(150 * time.Millisecond)
	mt2 := &MemTable{sst: sst2}

	v, ok := mt2.getFromSSTables([]byte("x"))
	if !ok {
		t.Fatalf("post-restart GET x not found")
	}
	if string(v) != "B" {
		t.Fatalf("post-restart GET x = %q, want B —— 陈旧值倒挂 bug（重启后 mata 按文件名排序，旧 merged 盖过新 L0）", v)
	}
}
