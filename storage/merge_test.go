package storage

import (
	"path/filepath"
	"testing"
)

func entry(k, v string) LogEntry {
	return LogEntry{Key: []byte(k), Value: []byte(v)}
}

func withTempSSTDir(t *testing.T) Options {
	t.Helper()
	return Options{Dir: t.TempDir()}
}

// TestSSTableIteratorStopsAtDataEnd 迭代器只读数据区, 不把块索引/布隆当作条目。
func TestSSTableIteratorStopsAtDataEnd(t *testing.T) {
	opts := withTempSSTDir(t)
	ss := NewSSTable(opts)
	if err := ss.WriteToSSTable([]LogEntry{
		entry("k1", "v1"), entry("k2", "v2"), entry("k3", "v3"),
	}); err != nil {
		t.Fatal(err)
	}
	path := ss.Metas()[0].Filepath

	it, err := newSSTableIterator(path)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	want := []string{"k1=v1", "k2=v2", "k3=v3"}
	var got []string
	for it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	if it.Err() != nil {
		t.Fatalf("iterator err: %v", it.Err())
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestMergeBasic 多个不相交源合并: 全部 key 有序保留、可经现有读路径读取、布隆可用。
func TestMergeBasic(t *testing.T) {
	opts := withTempSSTDir(t)
	ss := NewSSTable(opts)
	if err := ss.WriteToSSTable([]LogEntry{entry("a", "1"), entry("c", "1"), entry("e", "1")}); err != nil {
		t.Fatal(err)
	}
	if err := ss.WriteToSSTable([]LogEntry{entry("b", "2"), entry("d", "2"), entry("f", "2")}); err != nil {
		t.Fatal(err)
	}
	files := ss.LevelFiles(0)
	if len(files) != 2 {
		t.Fatalf("want 2 source files, got %d", len(files))
	}

	merged := ss.MergeSSTable(files, 1)
	if merged == nil {
		t.Fatal("MergeSSTable returned nil")
	}
	if merged.Level != 1 {
		t.Errorf("merged level = %d, want 1", merged.Level)
	}

	entries, err := ss.ReadAllFromSSTable(merged.Filepath)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c", "d", "e", "f"}
	if len(entries) != len(want) {
		t.Fatalf("merged has %d entries, want %d", len(entries), len(want))
	}
	for i, e := range entries {
		if string(e.Key) != want[i] {
			t.Errorf("entry %d key = %q, want %q", i, e.Key, want[i])
		}
	}

	// 经块索引读取
	if v, ok := ss.ReadFromSSTable(merged.Filepath, []byte("d")); !ok || string(v) != "2" {
		t.Errorf("read d: ok=%v v=%q", ok, v)
	}
	// 布隆已写入且能否决缺失 key
	if NewSSTable(opts).getBloom(merged.Filepath) == nil {
		t.Error("merged file should carry a bloom filter")
	}
	if _, ok := ss.ReadFromSSTable(merged.Filepath, []byte("zzz")); ok {
		t.Error("absent key reported present")
	}
}

// TestMergeDedupKeepNewest 同 key 出现在多个源时, srcIdx 最大(最新)者胜出且只出现一次。
func TestMergeDedupKeepNewest(t *testing.T) {
	opts := withTempSSTDir(t)
	ss := NewSSTable(opts)
	// 三个文件按加入顺序 srcIdx = 0,1,2; "dup" 的最新值应为 v2
	ss.WriteToSSTable([]LogEntry{entry("a", "a0"), entry("dup", "v0")})
	ss.WriteToSSTable([]LogEntry{entry("dup", "v1"), entry("m", "m1")})
	ss.WriteToSSTable([]LogEntry{entry("dup", "v2"), entry("z", "z2")})

	merged := ss.MergeSSTable(ss.LevelFiles(0), 1)
	if merged == nil {
		t.Fatal("merge returned nil")
	}
	entries, err := ss.ReadAllFromSSTable(merged.Filepath)
	if err != nil {
		t.Fatal(err)
	}

	dupCount, dupVal := 0, ""
	for _, e := range entries {
		if string(e.Key) == "dup" {
			dupCount++
			dupVal = string(e.Value)
		}
	}
	if dupCount != 1 {
		t.Errorf("dup appeared %d times, want exactly 1", dupCount)
	}
	if dupVal != "v2" {
		t.Errorf("dup value = %q, want v2 (newest source)", dupVal)
	}
	if len(entries) != 4 { // a, dup, m, z
		t.Errorf("merged unique count = %d, want 4", len(entries))
	}
	// 直接点查也应拿到最新值
	if v, ok := ss.ReadFromSSTable(merged.Filepath, []byte("dup")); !ok || string(v) != "v2" {
		t.Errorf("read dup: ok=%v v=%q, want v2", ok, v)
	}
}

// TestMergeRejectsUnsortedSource 源非升序时归并应失败(返回 nil), 而非静默产出错误结果。
func TestMergeRejectsUnsortedSource(t *testing.T) {
	opts := withTempSSTDir(t)
	path := filepath.Join(opts.Dir, "bad.sst")
	// 降序 key — 违反归并前提
	writeV1SSTable(t, path, []LogEntry{entry("c", "1"), entry("b", "1"), entry("a", "1")})

	ss := NewSSTable(opts)
	merged := ss.MergeSSTable([]*SSTableMeta{{Filepath: path}}, 1)
	if merged != nil {
		t.Error("merge should fail on a non-ascending source, got non-nil")
	}
}

// tomb 构造一个墓碑条目 (Value==nil)。
func tomb(k string) LogEntry { return LogEntry{Key: []byte(k), Value: nil} }

// TestSSTableTombstoneRoundTrip 墓碑经 SSTable 写→点查→ReadAll→迭代器全链路：
// 哨兵长度不触发巨型分配，墓碑还原为 found+nil，真实值不受影响。
func TestSSTableTombstoneRoundTrip(t *testing.T) {
	opts := withTempSSTDir(t)
	ss := NewSSTable(opts)
	if err := ss.WriteToSSTable([]LogEntry{
		entry("a", "1"), tomb("del"), entry("z", "3"),
	}); err != nil {
		t.Fatal(err)
	}
	path := ss.Metas()[0].Filepath

	if v, ok := ss.ReadFromSSTable(path, []byte("del")); !ok || v != nil {
		t.Errorf("point-read tombstone: ok=%v v=%q, want ok=true v=nil", ok, v)
	}
	if v, ok := ss.ReadFromSSTable(path, []byte("a")); !ok || string(v) != "1" {
		t.Errorf("point-read a: ok=%v v=%q, want 1", ok, v)
	}

	entries, err := ss.ReadAllFromSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if string(e.Key) == "del" && e.Value != nil {
			t.Errorf("ReadAll tombstone value = %q, want nil", e.Value)
		}
	}

	it, err := newSSTableIterator(path)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	for it.Next() {
		if string(it.Key()) == "del" && it.Value() != nil {
			t.Errorf("iterator tombstone value = %q, want nil", it.Value())
		}
	}
	if it.Err() != nil {
		t.Fatalf("iterator err: %v", it.Err())
	}
}

// TestMergePreservesTombstone 旧文件含值、新文件含同 key 墓碑：归并须保留墓碑
// (newest 胜出)，compaction 后点查仍为已删除，而不是把旧值复活。
func TestMergePreservesTombstone(t *testing.T) {
	opts := withTempSSTDir(t)
	ss := NewSSTable(opts)
	ss.WriteToSSTable([]LogEntry{entry("k", "v")}) // srcIdx 0（旧）
	ss.WriteToSSTable([]LogEntry{tomb("k")})       // srcIdx 1（新，墓碑）

	merged := ss.MergeSSTable(ss.LevelFiles(0), 1)
	if merged == nil {
		t.Fatal("merge returned nil")
	}
	if v, ok := ss.ReadFromSSTable(merged.Filepath, []byte("k")); !ok || v != nil {
		t.Errorf("merged k: ok=%v v=%q, want ok=true v=nil (tombstone wins over older value)", ok, v)
	}
}
