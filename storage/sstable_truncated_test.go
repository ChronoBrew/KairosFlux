package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeverENG/BanDB/config"
)

// TestTruncatedTailStillReadable 验证尾部残缺的 SSTable 仍能读到其数据段里的 key。
//
// 为什么必须成立：尾部（块索引 + 布隆 + footer）可能因磁盘写满、外部截断或旧版本的
// 未检查写入而缺失。此时 footer magic 校验失败，MaxKey 无从得知。若读路径此时去
// 顺序扫描数据段来「推导」MaxKey，就会把之后的索引/布隆字节当记录读出，得到错乱的
// 上界，从而把命中的 key 整段跳过——数据看似凭空消失，且无任何错误上报。
//
// 现在的约定是：MaxKey 不可信就不施加上界。多扫一个文件是可接受的代价，漏读不是。
func TestTruncatedTailStillReadable(t *testing.T) {
	dir := t.TempDir()
	oldSST := config.G.SSTablePath
	config.G.SSTablePath = dir
	t.Cleanup(func() { config.G.SSTablePath = oldSST })

	// 写一个正常的 SSTable（含完整尾部）。
	ss := NewSSTable()
	entries := make([]LogEntry, 0, 200)
	for i := 0; i < 200; i++ {
		entries = append(entries, LogEntry{
			Key:   []byte(fmt.Sprintf("key%04d", i)),
			Value: []byte(fmt.Sprintf("val%04d", i)),
		})
	}
	if err := ss.WriteToSSTable(entries); err != nil {
		t.Fatalf("WriteToSSTable: %v", err)
	}
	path := ss.Metas()[0].Filepath

	// 截掉尾部：footer 与部分布隆段一起消失，模拟写尾中断。
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-int64(indexFooterSize)-8); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// 模拟重启：重新加载元信息（此时读不到 footer）。
	fresh := NewSSTable()
	fresh.LoadSSTableMetaList()
	metas := fresh.Metas()
	if len(metas) != 1 {
		t.Fatalf("应加载到 1 个 SSTable, 实际 %d", len(metas))
	}
	if metas[0].MaxKeyKnown {
		t.Fatal("尾部残缺的文件不应声称 MaxKey 可信")
	}

	// 数据段完好，故其中的 key 必须仍能读到——尤其是排序上靠后的那些：
	// 若上界被错误地推导为某个偏小的值，正是它们会被跳过。
	for _, i := range []int{0, 1, 99, 150, 198, 199} {
		key := []byte(fmt.Sprintf("key%04d", i))
		got, found := fresh.ReadFromSSTable(path, key)
		if !found {
			t.Fatalf("key %s 应仍可读到（数据段完好）", key)
		}
		want := fmt.Sprintf("val%04d", i)
		if string(got) != want {
			t.Fatalf("key %s = %q, want %q", key, got, want)
		}
	}
}

// TestTruncatedTailNotSkippedByRangeFilter 从引擎层验证同一件事：尾部残缺的文件不会被
// 范围过滤整段跳过。这是上一个用例的端到端版本——ReadFromSSTable 直接按路径读，绕过了
// 范围过滤，而真正出问题的正是过滤那一步。
func TestTruncatedTailNotSkippedByRangeFilter(t *testing.T) {
	dir := t.TempDir()
	oldSST, oldWAL, oldMax := config.G.SSTablePath, config.G.WALPath, config.G.MaxMemTableSize
	config.G.SSTablePath = dir
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.MaxMemTableSize = 4 // 极小阈值，逼迫尽快 flush 出 SSTable
	t.Cleanup(func() {
		config.G.SSTablePath, config.G.WALPath, config.G.MaxMemTableSize = oldSST, oldWAL, oldMax
	})

	ss := NewSSTable()
	entries := []LogEntry{
		{Key: []byte("aaa"), Value: []byte("v-aaa")},
		{Key: []byte("mmm"), Value: []byte("v-mmm")},
		{Key: []byte("zzz"), Value: []byte("v-zzz")},
	}
	if err := ss.WriteToSSTable(entries); err != nil {
		t.Fatalf("WriteToSSTable: %v", err)
	}
	path := ss.Metas()[0].Filepath
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-int64(indexFooterSize)-4); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// 引擎重启：其内部会 LoadSSTableMetaList，随后 Get 走 [MinKey,MaxKey] 过滤。
	e := NewEngine()
	t.Cleanup(func() { e.Close() })

	// zzz 是排序最靠后的 key：上界一旦被猜小，它必然第一个被漏掉。
	for _, kv := range entries {
		got, err := e.Get(kv.Key)
		if err != nil {
			t.Fatalf("Get %s: %v（尾部残缺不应导致数据不可读）", kv.Key, err)
		}
		if string(got) != string(kv.Value) {
			t.Fatalf("Get %s = %q, want %q", kv.Key, got, kv.Value)
		}
	}
}
