package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/NeverENG/BanDB/config"
)

// writeSSTables 写出 n 个各含若干 key 的 SSTable，返回它们的路径。
func writeSSTables(t *testing.T, ss *SSTable, n, perFile int) []string {
	t.Helper()
	var paths []string
	for f := 0; f < n; f++ {
		entries := make([]LogEntry, 0, perFile)
		for i := 0; i < perFile; i++ {
			entries = append(entries, LogEntry{
				Key:   []byte(fmt.Sprintf("f%d-k%04d", f, i)),
				Value: []byte(fmt.Sprintf("f%d-v%04d", f, i)),
			})
		}
		if err := ss.WriteToSSTable(entries); err != nil {
			t.Fatalf("WriteToSSTable: %v", err)
		}
		metas := ss.Metas()
		paths = append(paths, metas[len(metas)-1].Filepath)
	}
	return paths
}

// TestCompactionFailureKeepsSourceFiles 验证 compaction 失败时源文件不被删除。
//
// 这是 compaction 唯一不能出错的地方：CompactSSTable 在 MergeSSTable 返回非 nil 后就
// 删除全部源文件。若合并实际上失败了却报告成功，源文件被删、合并结果又不可用，数据即
// 永久丢失——没有第三份副本。
//
// 故障注入方式：把 SSTable 目录改为只读，使合并输出文件无法创建。这依赖文件权限，
// 故 root 下会失效——检测到仍能创建文件时直接跳过，而不是给出假绿。
func TestCompactionFailureKeepsSourceFiles(t *testing.T) {
	dir := t.TempDir()
	oldSST := config.G.SSTablePath
	config.G.SSTablePath = dir
	t.Cleanup(func() { config.G.SSTablePath = oldSST })

	ss := NewSSTable()
	paths := writeSSTables(t, ss, 3, 50)

	if err := os.Chmod(dir, 0o500); err != nil { // r-x：可读可遍历，不可创建新文件
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })

	// 确认注入确实生效（root 或某些文件系统下权限不起作用）。
	if probe, err := os.Create(dir + "/probe"); err == nil {
		probe.Close()
		os.Remove(dir + "/probe")
		os.Chmod(dir, 0o755)
		t.Skip("当前环境下只读目录仍可创建文件（可能以 root 运行），跳过该故障注入")
	}

	files := ss.LevelFiles(0)
	if len(files) != 3 {
		t.Fatalf("应有 3 个 L0 文件, 实际 %d", len(files))
	}

	if got := ss.MergeSSTable(files, 1); got != nil {
		t.Fatal("输出文件无法创建时 MergeSSTable 必须返回 nil，否则调用方会据此删除源文件")
	}

	// 关键断言：源文件仍在，且内容可读。
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("合并失败后源文件应保留: %s: %v", p, err)
		}
	}
	os.Chmod(dir, 0o755)
	for f, p := range paths {
		key := []byte(fmt.Sprintf("f%d-k0000", f))
		if _, found := ss.ReadFromSSTable(p, key); !found {
			t.Fatalf("合并失败后源文件的数据应仍可读: %s", key)
		}
	}
}

// TestCompactSSTableKeepsSourcesOnMergeFailure 从引擎层验证同一不变量：CompactSSTable
// 在合并失败时不得删除任何源文件，且全部 key 仍可读。
func TestCompactSSTableKeepsSourcesOnMergeFailure(t *testing.T) {
	dir := t.TempDir()
	oldSST, oldCompact := config.G.SSTablePath, config.G.MaxCompactionSize
	config.G.SSTablePath = dir
	config.G.MaxCompactionSize = 2 // 两个文件即触发合并
	t.Cleanup(func() {
		config.G.SSTablePath, config.G.MaxCompactionSize = oldSST, oldCompact
	})

	e := NewEngine()
	t.Cleanup(func() { e.Close() })
	paths := writeSSTables(t, e.sst, 3, 30)

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
	if probe, err := os.Create(dir + "/probe"); err == nil {
		probe.Close()
		os.Remove(dir + "/probe")
		os.Chmod(dir, 0o755)
		t.Skip("当前环境下只读目录仍可创建文件（可能以 root 运行），跳过该故障注入")
	}

	e.CompactSSTable(0) // 必然失败，但不得删源文件

	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("compaction 失败后源文件应保留: %s: %v", p, err)
		}
	}
	if got := len(e.sst.LevelFiles(0)); got != 3 {
		t.Fatalf("compaction 失败后 L0 仍应有 3 个文件, 实际 %d", got)
	}

	os.Chmod(dir, 0o755)
	for f := range paths {
		for _, i := range []int{0, 15, 29} {
			key := []byte(fmt.Sprintf("f%d-k%04d", f, i))
			if _, err := e.Get(key); err != nil {
				t.Fatalf("compaction 失败后 %s 应仍可读: %v", key, err)
			}
		}
	}
}
