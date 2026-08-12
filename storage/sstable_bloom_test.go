package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeverENG/BanDB/config"
)

// makeSortedEntries 生成跨多个仓库、整体有序的 entries。
func makeSortedEntries(n int) []LogEntry {
	es := make([]LogEntry, 0, n*2)
	for i := 0; i < n; i++ {
		es = append(es, LogEntry{
			Key:   []byte(fmt.Sprintf("log:%05d", i)),
			Value: []byte(fmt.Sprintf("lval-%d", i)),
		})
	}
	for i := 0; i < n; i++ {
		es = append(es, LogEntry{
			Key:   []byte(fmt.Sprintf("order:%05d", i)),
			Value: []byte(fmt.Sprintf("oval-%d", i)),
		})
	}
	return es // "log:" < "order:" 已整体有序
}

// writeV1SSTable 手写旧格式(v1, 无布隆)文件，用于验证向后兼容读取。
func writeV1SSTable(t *testing.T, path string, entries []LogEntry) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create v1 sstable: %v", err)
	}
	defer file.Close()

	type blk struct {
		lastKey     []byte
		blockOffset int64
	}
	var blockIdx []blk
	var buf bytes.Buffer
	for i, e := range entries {
		bi := i / SSTableBlockSize
		if i%SSTableBlockSize == 0 {
			blockIdx = append(blockIdx, blk{blockOffset: int64(buf.Len())})
		}
		blockIdx[bi].lastKey = e.Key
		binary.Write(&buf, binary.BigEndian, uint32(len(e.Key)))
		buf.Write(e.Key)
		binary.Write(&buf, binary.BigEndian, uint32(len(e.Value)))
		buf.Write(e.Value)
	}
	file.Write(buf.Bytes())
	indexStart, _ := file.Seek(0, io.SeekCurrent)
	for _, b := range blockIdx {
		binary.Write(file, binary.BigEndian, uint32(len(b.lastKey)))
		file.Write(b.lastKey)
		binary.Write(file, binary.BigEndian, b.blockOffset)
	}
	binary.Write(file, binary.BigEndian, uint32(len(blockIdx)))
	binary.Write(file, binary.BigEndian, indexStart)
	binary.Write(file, binary.BigEndian, indexFooterMagic)
	file.Sync()
}

// TestSSTableBloomRoundTrip 写入(含布隆)→读取：存在的 key 命中，
// 不存在的 key 返回 false，且布隆过滤器被正确写入并加载。
func TestSSTableBloomRoundTrip(t *testing.T) {
	old := config.G.SSTablePath
	config.G.SSTablePath = t.TempDir()
	defer func() { config.G.SSTablePath = old }()

	entries := makeSortedEntries(200)
	ss := NewSSTable()
	if err := ss.WriteToSSTable(entries); err != nil {
		t.Fatalf("write: %v", err)
	}
	path := ss.Metas()[0].Filepath

	// 布隆过滤器已写入（从磁盘重新加载验证，绕过写入缓存）
	fresh := NewSSTable()
	if bloom := fresh.getBloom(path); bloom == nil {
		t.Fatal("expected bloom filter in v2 file, got nil")
	}

	for _, e := range entries {
		v, ok := ss.ReadFromSSTable(path, e.Key)
		if !ok || !bytes.Equal(v, e.Value) {
			t.Fatalf("read %q: ok=%v val=%q", e.Key, ok, v)
		}
	}
	// 不存在的 key
	if _, ok := ss.ReadFromSSTable(path, []byte("log:99999")); ok {
		t.Error("absent key log:99999 reported present")
	}
	if _, ok := ss.ReadFromSSTable(path, []byte("nosuchns:1")); ok {
		t.Error("absent namespace reported present")
	}
}

// TestSSTableBackwardCompatV1 旧格式文件(无布隆)仍能被正确读取，
// 且 getBloom 返回 nil（不误判为有布隆）。
func TestSSTableBackwardCompatV1(t *testing.T) {
	old := config.G.SSTablePath
	dir := t.TempDir()
	config.G.SSTablePath = dir
	defer func() { config.G.SSTablePath = old }()

	entries := makeSortedEntries(80)
	path := filepath.Join(dir, "sstable_v1.sst")
	writeV1SSTable(t, path, entries)

	ss := NewSSTable()
	if bloom := ss.getBloom(path); bloom != nil {
		t.Fatal("v1 file must have no bloom, got non-nil")
	}
	for _, e := range entries {
		v, ok := ss.ReadFromSSTable(path, e.Key)
		if !ok || !bytes.Equal(v, e.Value) {
			t.Fatalf("v1 read %q: ok=%v val=%q", e.Key, ok, v)
		}
	}
	// v1 缺失 key 必须经块索引/全量 fallback 正确返回 (nil,false)：
	for _, absent := range []string{
		"order:99999", // 命名空间存在但 key 不存在
		"log:00100",   // 落在已存在 key 范围内的空洞(仅插入 0..79)
		"zzz:beyond",  // 超出最大 key
		"aaa:before",  // 小于最小 key
		"nosuchns:1",  // 命名空间不存在
	} {
		if _, ok := ss.ReadFromSSTable(path, []byte(absent)); ok {
			t.Errorf("v1 absent key %q reported present", absent)
		}
	}
}
