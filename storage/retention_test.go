package storage

import (
	"fmt"
	"os"
	"testing"
)

// writeOneSSTable 写出一个含给定 key 区间的 SSTable，返回其元信息。
func writeOneSSTable(t *testing.T, ss *SSTable, keys ...string) *SSTableMeta {
	t.Helper()
	entries := make([]LogEntry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, LogEntry{Key: []byte(k), Value: []byte("v-" + k)})
	}
	if err := ss.WriteToSSTable(entries); err != nil {
		t.Fatalf("WriteToSSTable: %v", err)
	}
	metas := ss.Metas()
	return metas[len(metas)-1]
}

// TestReclaimUpTo_DropsOnlyFullyDeliveredFiles 是保留期回收的核心契约：
// 只丢弃整份都落在游标之前的文件，跨越游标的文件必须留下。
//
// 判据用严格小于：恰好含 bound 的文件要保留，因为 bound 本身尚未被投递消费——它是「下一批
// 的起点」，不是「已完成的位置」。
func TestReclaimUpTo_DropsOnlyFullyDeliveredFiles(t *testing.T) {
	opts := testOptions(t)
	e := NewEngine(opts)
	t.Cleanup(func() { e.Close() })

	below := writeOneSSTable(t, e.sst, "a01", "a02", "a03")  // 整份在游标之前
	across := writeOneSSTable(t, e.sst, "a09", "b01", "b02") // 跨越游标
	above := writeOneSSTable(t, e.sst, "c01", "c02")         // 整份在游标之后

	// 游标定在 "b00"：a0x 已全部投递；across 含 b01/b02 尚未投递；above 更在其后。
	got := e.ReclaimUpTo([]byte("b00"))
	if got != 1 {
		t.Fatalf("应只回收 1 个文件, 实际 %d", got)
	}

	if _, err := os.Stat(below.Filepath); !os.IsNotExist(err) {
		t.Fatal("整份已投递的文件应被删除")
	}
	for _, m := range []*SSTableMeta{across, above} {
		if _, err := os.Stat(m.Filepath); err != nil {
			t.Fatalf("未整体投递的文件必须保留: %s: %v", m.Filepath, err)
		}
	}

	// 保留文件里的数据仍可读——回收不得影响未投递数据的可见性。
	for _, k := range []string{"a09", "b01", "b02", "c01", "c02"} {
		if _, err := e.Get([]byte(k)); err != nil {
			t.Fatalf("未投递的 key %s 应仍可读: %v", k, err)
		}
	}
	// 被回收文件里的数据不再可读，这正是「缓冲」语义。
	if _, err := e.Get([]byte("a01")); err == nil {
		t.Fatal("已回收的数据不应仍可读——否则说明文件没真正被丢弃")
	}
}

// TestReclaimUpTo_SkipsFilesWithUnknownMaxKey 验证 MaxKey 不可信时一律跳过。
//
// 尾部残缺（或老格式）的文件读不到块索引，MaxKey 无从得知。此时若按猜测的上界回收，
// 可能删掉尚未投递的数据——宁可少回收，不可误删。
func TestReclaimUpTo_SkipsFilesWithUnknownMaxKey(t *testing.T) {
	opts := testOptions(t)
	ss := NewSSTable(opts)
	meta := writeOneSSTable(t, ss, "a01", "a02")

	// 截掉尾部，使 footer 不可读；重新加载后 MaxKey 不可信。
	info, err := os.Stat(meta.Filepath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(meta.Filepath, info.Size()-int64(indexFooterSize)-4); err != nil {
		t.Fatal(err)
	}

	e := NewEngine(opts) // 构造时同步加载元信息
	t.Cleanup(func() { e.Close() })
	metas := e.sst.Metas()
	if len(metas) != 1 || metas[0].MaxKeyKnown {
		t.Fatalf("前提失效：应加载到 1 个 MaxKey 不可信的文件, 实际 %+v", metas)
	}

	if got := e.ReclaimUpTo([]byte("zzz")); got != 0 {
		t.Fatalf("MaxKey 不可信的文件不应被回收, 实际回收 %d 个", got)
	}
	if _, err := os.Stat(metas[0].Filepath); err != nil {
		t.Fatal("文件应保留")
	}
}

// TestReclaimUpTo_EmptyBoundReclaimsNothing 验证尚无已提交游标时不回收任何文件。
func TestReclaimUpTo_EmptyBoundReclaimsNothing(t *testing.T) {
	e := NewEngine(testOptions(t))
	t.Cleanup(func() { e.Close() })
	writeOneSSTable(t, e.sst, "a01", "a02")

	if got := e.ReclaimUpTo(nil); got != 0 {
		t.Fatalf("游标为空时不应回收, 实际 %d", got)
	}
	if got := e.ReclaimUpTo([]byte{}); got != 0 {
		t.Fatalf("游标为空时不应回收, 实际 %d", got)
	}
}

// TestReclaimUpTo_ConcurrentWithCompaction 回收与 compaction 并发时不得互相破坏。
//
// 两者都会删除 SSTable 文件。若交错，compaction 正在读的源文件可能被回收删除——POSIX 下
// 已打开的 fd 仍可读，那批已回收的数据会被写进合并输出，即「已回收的数据复活」。二者共用
// fileMu 串行化。本用例在 -race 下反复交叉调用，守护该互斥。
func TestReclaimUpTo_ConcurrentWithCompaction(t *testing.T) {
	opts := testOptions(t)
	opts.MaxCompactionSize = 2
	e := NewEngine(opts)
	t.Cleanup(func() { e.Close() })

	for f := 0; f < 8; f++ {
		writeOneSSTable(t, e.sst, fmt.Sprintf("k%02d0", f), fmt.Sprintf("k%02d1", f))
	}

	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		for i := 0; i < 20; i++ {
			e.CompactSSTable(0)
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for i := 0; i < 20; i++ {
			e.ReclaimUpTo([]byte("k040"))
		}
	}()
	<-done
	<-done

	// 未被回收范围内的数据必须仍可读。
	for f := 4; f < 8; f++ {
		for _, suffix := range []string{"0", "1"} {
			k := fmt.Sprintf("k%02d%s", f, suffix)
			if _, err := e.Get([]byte(k)); err != nil {
				t.Fatalf("游标之后的 key %s 应仍可读: %v", k, err)
			}
		}
	}
}
