package zstorage

import (
	"github.com/NeverENG/BanDB/config"
	"os"
	"testing"
)

func TestMemTable_PutAndDelete(t *testing.T) {
	// 配置临时WAL
	oldWALPath := config.G.WALPath
	testWAL := "test_memtable_wal.log"
	config.G.WALPath = testWAL
	os.Remove(testWAL) // 先删除旧的
	defer func() {
		os.Remove(testWAL)
		config.G.WALPath = oldWALPath
	}()

	memTable := NewMemTable()
	t.Log("MemTable created")

	err := memTable.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	t.Logf("Put key1 success, size: %d", memTable.Size())

	val, err := memTable.Get([]byte("key1"))
	if err != nil || string(val) != "value1" {
		t.Fatalf("Get failed: val='%s', err=%v", string(val), err)
	}
	t.Logf("Get key1 success: %s", string(val))

	err = memTable.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	t.Log("Delete key1 success")

	val, err = memTable.Get([]byte("key1"))
	if err == nil && val != nil {
		t.Errorf("Expected key1 to be deleted, but got: %s", string(val))
	}
}

// setupMemTableTempEnv 为 MemTable 测试配置隔离的 WAL/SSTable 路径并关闭自动刷盘。
func setupMemTableTempEnv(t *testing.T, walName string) {
	t.Helper()
	oldWAL := config.G.WALPath
	oldSST := config.G.SSTablePath
	oldMax := config.G.MaxMemTableSize
	config.G.WALPath = walName
	config.G.SSTablePath = t.TempDir()
	config.G.MaxMemTableSize = 1 << 20
	os.Remove(walName)
	t.Cleanup(func() {
		os.Remove(walName)
		config.G.WALPath = oldWAL
		config.G.SSTablePath = oldSST
		config.G.MaxMemTableSize = oldMax
	})
}

// TestMemTableDeleteFlushedKeyNoResurrect 删除一个已 flush 到 SSTable 的 key：
// 墓碑必须在 active 与落盘后都 shadow 旧值，且后续 Put 可复活该 key。
func TestMemTableDeleteFlushedKeyNoResurrect(t *testing.T) {
	setupMemTableTempEnv(t, "test_tombstone_wal.log")
	m := NewMemTable()

	if err := m.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	m.Flush() // v 落到 SSTable，active 清空

	// active 墓碑应 shadow SSTable 中的旧值
	if err := m.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if val, err := m.Get([]byte("k")); err == nil && val != nil {
		t.Errorf("after delete (active tombstone) expected miss, got %q", string(val))
	}

	m.Flush() // 墓碑落到新 SSTable
	if val, err := m.Get([]byte("k")); err == nil && val != nil {
		t.Errorf("after delete+flush (SSTable tombstone) expected miss, got %q", string(val))
	}

	// 复活：删除后再写入应可读到新值
	if err := m.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("re-put: %v", err)
	}
	if val, err := m.Get([]byte("k")); err != nil || string(val) != "v2" {
		t.Errorf("resurrect: got %q err=%v, want v2", string(val), err)
	}
}

// TestMemTableEmptyValueNotTombstone 空值是真实值, 不能被当作墓碑：
// Put(k, []byte{}) 经 flush 落盘后, Get 必须返回 found+空, 而非未找到。
func TestMemTableEmptyValueNotTombstone(t *testing.T) {
	setupMemTableTempEnv(t, "test_emptyval_wal.log")
	m := NewMemTable()

	if err := m.Put([]byte("e"), []byte{}); err != nil {
		t.Fatalf("put empty: %v", err)
	}
	// active 中
	if val, err := m.Get([]byte("e")); err != nil || val == nil {
		t.Fatalf("empty value in active read as missing: val=%v err=%v", val, err)
	}
	m.Flush() // 空值落盘
	val, err := m.Get([]byte("e"))
	if err != nil {
		t.Fatalf("empty value after flush read as missing (tombstone): %v", err)
	}
	if len(val) != 0 {
		t.Errorf("expected empty value after flush, got %q", string(val))
	}
}

// TestGetReturnsNewestAcrossSSTables 覆盖写后再 flush：同一 key 分布在新旧两个
// SSTable 中，Get 必须返回最新版本。当前读路径按文件最旧在前取首个命中，会返回
// 陈旧值——本测试即该正确性 bug 的回归门。
func TestGetReturnsNewestAcrossSSTables(t *testing.T) {
	oldWAL := config.G.WALPath
	oldSST := config.G.SSTablePath
	oldMax := config.G.MaxMemTableSize
	config.G.WALPath = "test_newest_wal.log"
	config.G.SSTablePath = t.TempDir()
	config.G.MaxMemTableSize = 1 << 20 // 避免 Put 触发自动刷盘，由测试显式 Flush 控制
	os.Remove(config.G.WALPath)
	defer func() {
		os.Remove(config.G.WALPath)
		config.G.WALPath = oldWAL
		config.G.SSTablePath = oldSST
		config.G.MaxMemTableSize = oldMax
	}()

	m := NewMemTable()

	if err := m.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	m.Flush() // v1 落到 SSTable #1，active 清空

	if err := m.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	m.Flush() // v2 落到 SSTable #2

	val, err := m.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get after overwrite+flush: %v", err)
	}
	if string(val) != "v2" {
		t.Errorf("expected newest value 'v2', got %q (stale read across SSTables)", string(val))
	}
}
