package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/NeverENG/BanDB/config"
)

// TestGetMissingKeyIsErrKeyNotFound 固定「key 不存在」的错误契约：调用方须能以
// errors.Is 判别，从而与读盘失败等真实故障区分。此前该错误在四处各自 errors.New
// 一个新对象，任何调用方都无法判别，只能一律当作失败处理。
func TestGetMissingKeyIsErrKeyNotFound(t *testing.T) {
	dir := t.TempDir()
	oldSST := config.G.SSTablePath
	config.G.SSTablePath = dir
	t.Cleanup(func() { config.G.SSTablePath = oldSST })

	mt := NewEngine()
	t.Cleanup(func() { mt.Close() })

	if err := mt.Put([]byte("present"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, err := mt.Get([]byte("absent"))
	if err == nil {
		t.Fatal("读取不存在的 key 应返回错误")
	}
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("errors.Is(err, ErrKeyNotFound) 应为真, 实际错误: %v", err)
	}

	// 墓碑（已删除）与不存在同属 ErrKeyNotFound，二者对调用方语义一致。
	if err := mt.Delete([]byte("present")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := mt.Get([]byte("present")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("读取墓碑应为 ErrKeyNotFound, 实际: %v", err)
	}
}

// TestWriteEmptySSTableIsErrNoEntries 固定空条目集落盘的错误契约。
func TestWriteEmptySSTableIsErrNoEntries(t *testing.T) {
	oldSST := config.G.SSTablePath
	config.G.SSTablePath = filepath.Join(t.TempDir(), "sst")
	t.Cleanup(func() { config.G.SSTablePath = oldSST })

	ss := NewSSTable()
	if err := ss.WriteToSSTable(nil); !errors.Is(err, ErrNoEntries) {
		t.Fatalf("写入空条目集应为 ErrNoEntries, 实际: %v", err)
	}
}
