package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// replayWALFile 把指定路径的 WAL 全部记录读成切片，便于断言。
func replayWALFile(t *testing.T, path string) []WALRecord {
	t.Helper()
	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	var out []WALRecord
	if err := w.Replay(func(op uint8, key, value []byte) error {
		out = append(out, WALRecord{Op: op, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return out
}

// TestRewriteFailureKeepsOldWALIntact 验证 checkpoint 重写失败后，旧 WAL 依然完整可重放。
//
// 这是 Rewrite 存在的全部意义：它要把 WAL 换成一份更小的快照，而快照里的 active+dirty
// 尚无 SSTable 副本。一旦重写中途失败又把旧 WAL 破坏掉，这些已 ack 的写就没有任何副本了。
//
// 故障注入方式：在临时文件的路径上放一个目录，使 os.OpenFile 必然失败——这是可移植的
// 注入手段，不依赖磁盘写满或权限细节。
func TestRewriteFailureKeepsOldWALIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := w.Append(WALOpPut, []byte(fmt.Sprintf("k%02d", i)), []byte("v")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	before := replayWALFile(t, path)
	if len(before) != 20 {
		t.Fatalf("重写前应有 20 条, 实际 %d", len(before))
	}

	// 占住 .tmp 路径，让重写必然失败。
	if err := os.Mkdir(path+".tmp", 0o755); err != nil {
		t.Fatalf("mkdir tmp: %v", err)
	}

	w2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	err = w2.Rewrite([]WALRecord{{Op: WALOpPut, Key: []byte("only"), Value: []byte("one")}})
	if err == nil {
		t.Fatal("临时文件不可创建时 Rewrite 必须报错，不能静默当作成功")
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 关键断言：失败的重写不得动到旧 WAL。
	after := replayWALFile(t, path)
	if len(after) != len(before) {
		t.Fatalf("重写失败后旧 WAL 应完整保留 %d 条, 实际 %d 条——已 ack 的写被丢掉了",
			len(before), len(after))
	}
	for i := range before {
		if string(after[i].Key) != string(before[i].Key) {
			t.Fatalf("第 %d 条 key = %q, want %q", i, after[i].Key, before[i].Key)
		}
	}
}

// TestRewriteLeftoverTmpIsIgnored 验证「重写完成前崩溃」这一状态可安全恢复：
// 残留的 .tmp 不参与重放，旧 WAL 照常读出。
//
// rename 之前崩溃时磁盘上就是这个样子——一份完整的旧 WAL 加一份写了一半的 .tmp。
func TestRewriteLeftoverTmpIsIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	if err := w.Append(WALOpPut, []byte("kept"), []byte("v")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 伪造一份「写了一半」的 .tmp：内容故意是残缺记录。
	if err := os.WriteFile(path+".tmp", []byte{WALOpPut, 0xFF, 0xFF}, 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	got := replayWALFile(t, path)
	if len(got) != 1 || string(got[0].Key) != "kept" {
		t.Fatalf("残留的 .tmp 不应影响重放, 实际 %+v", got)
	}
}

// TestRewriteReplacesContentAtomically 验证成功路径：重写后只剩新内容，且立即可重放。
func TestRewriteReplacesContentAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := w.Append(WALOpPut, []byte(fmt.Sprintf("old%02d", i)), []byte("v")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	snapshot := []WALRecord{
		{Op: WALOpPut, Key: []byte("hot1"), Value: []byte("v1")},
		{Op: WALOpDelete, Key: []byte("hot2")}, // 墓碑必须原样保留，否则被删的 key 会复活
	}
	if err := w.Rewrite(snapshot); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	// 重写后仍应能继续追加（内部已重新打开新 inode）。
	if err := w.Append(WALOpPut, []byte("after"), []byte("v")); err != nil {
		t.Fatalf("重写后 Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := replayWALFile(t, path)
	want := []string{"hot1", "hot2", "after"}
	if len(got) != len(want) {
		t.Fatalf("重写后应有 %d 条, 实际 %d 条: %+v", len(want), len(got), got)
	}
	for i, k := range want {
		if string(got[i].Key) != k {
			t.Fatalf("第 %d 条 key = %q, want %q", i, got[i].Key, k)
		}
	}
	if got[1].Op != WALOpDelete {
		t.Fatalf("墓碑的 op 应为 WALOpDelete, 实际 %d", got[1].Op)
	}

	// .tmp 不应残留。
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Fatal("成功重写后不应残留 .tmp")
	}
}
