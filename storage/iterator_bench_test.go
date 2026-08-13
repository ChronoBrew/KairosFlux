package storage

import (
	"fmt"
	"testing"
	"time"
)

// TestIteratorScanThroughput 量化 SSTable 顺序迭代的吞吐。
//
// 这条路径同时服务 compaction（K 路归并的每个源）与范围扫描，是全量读的热点。
// 用法：go test -run IteratorScanThroughput -v ./storage/
func TestIteratorScanThroughput(t *testing.T) {
	opts := testOptions(t)
	ss := NewSSTable(opts)

	const n = 100_000
	entries := make([]LogEntry, 0, n)
	value := make([]byte, 200)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	for i := 0; i < n; i++ {
		entries = append(entries, LogEntry{Key: []byte(fmt.Sprintf("key%012d", i)), Value: value})
	}
	if err := ss.WriteToSSTable(entries); err != nil {
		t.Fatalf("WriteToSSTable: %v", err)
	}
	path := ss.Metas()[0].Filepath

	// 全量迭代三次取最好成绩，减少页缓存冷热的影响。
	best := time.Duration(1 << 62)
	for round := 0; round < 3; round++ {
		it, err := newSSTableIterator(path)
		if err != nil {
			t.Fatalf("newSSTableIterator: %v", err)
		}
		t0 := time.Now()
		count := 0
		for it.Next() {
			count++
		}
		elapsed := time.Since(t0)
		if err := it.Err(); err != nil {
			t.Fatalf("iterate: %v", err)
		}
		it.Close()
		if count != n {
			t.Fatalf("应迭代 %d 条, 实际 %d", n, count)
		}
		if elapsed < best {
			best = elapsed
		}
	}
	t.Logf("迭代 %d 条耗时 %v（%.0f 万条/秒）", n, best.Round(time.Millisecond),
		float64(n)/best.Seconds()/10000)
}

// TestMergeThroughput 量化 compaction 的归并吞吐：K 路归并逐条从各源迭代器取数，
// 故其成本直接受迭代器读取效率支配。
func TestMergeThroughput(t *testing.T) {
	opts := testOptions(t)
	ss := NewSSTable(opts)

	const files, perFile = 8, 12_500
	value := make([]byte, 200)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	for f := 0; f < files; f++ {
		entries := make([]LogEntry, 0, perFile)
		for i := 0; i < perFile; i++ {
			// 各文件 key 不相交且整体有序，构成典型的整层合并输入。
			entries = append(entries, LogEntry{
				Key:   []byte(fmt.Sprintf("key%02d%010d", f, i)),
				Value: value,
			})
		}
		if err := ss.WriteToSSTable(entries); err != nil {
			t.Fatalf("WriteToSSTable: %v", err)
		}
	}

	srcs := ss.LevelFiles(0)
	if len(srcs) != files {
		t.Fatalf("应有 %d 个源文件, 实际 %d", files, len(srcs))
	}
	t0 := time.Now()
	merged := ss.MergeSSTable(srcs, 1)
	elapsed := time.Since(t0)
	if merged == nil {
		t.Fatal("合并失败")
	}
	total := files * perFile
	t.Logf("归并 %d 个文件共 %d 条耗时 %v（%.0f 万条/秒）",
		files, total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds()/10000)
}
