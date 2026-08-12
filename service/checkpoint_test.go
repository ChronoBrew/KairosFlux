package service

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
)

// setupStandalone 配置一个 standalone KVServer，指定 memtable 刷盘阈值，返回实例与清理函数。
func setupStandalone(t *testing.T, maxMemTableSize int) (*KVServer, string, func()) {
	t.Helper()
	oldWALPath := config.G.WALPath
	oldSSTablePath := config.G.SSTablePath
	oldMode := config.G.Mode
	oldMaxSize := config.G.MaxMemTableSize

	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	config.G.Mode = config.ModeStandalone
	config.G.WALPath = walPath
	config.G.SSTablePath = dir
	config.G.MaxMemTableSize = maxMemTableSize

	kv := NewKVServer()
	cleanup := func() {
		config.G.WALPath = oldWALPath
		config.G.SSTablePath = oldSSTablePath
		config.G.Mode = oldMode
		config.G.MaxMemTableSize = oldMaxSize
	}
	return kv, walPath, cleanup
}

// TestCheckpoint_RecoverAcrossSSTableAndTombstone 验证 WAL checkpoint 自清洁的安全性：
// checkpoint 把 WAL 重写为未刷盘热数据快照后，已落 SSTable 的历史数据被移出 WAL，
// 但重启仍能从 SSTable 恢复；删除墓碑跨 checkpoint 不复活；checkpoint 之后的新写也在。
func TestCheckpoint_RecoverAcrossSSTableAndTombstone(t *testing.T) {
	kv, _, cleanup := setupStandalone(t, 4) // 阈值极小，逼迫大量数据刷到 SSTable
	defer cleanup()

	// 写入 50 个不同 key，触发多轮 active→dirty→SSTable 刷盘。
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := kv.Write(Command{Type: CommandPut, Key: key, Value: []byte(fmt.Sprintf("v%04d", i))}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// 删除一个较早的 key，其墓碑需跨 checkpoint 存活。
	if err := kv.Write(Command{Type: CommandDelete, Key: []byte("k0000")}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// 等待异步 FlushWorker 把 dirty 落到 SSTable。
	time.Sleep(300 * time.Millisecond)

	// checkpoint：把 WAL 重写为当前未刷盘热数据快照，回收已落 SSTable 的历史 WAL。
	kv.Checkpoint()

	// checkpoint 之后再写一个新 key——必须落在重写后的 WAL 里。
	if err := kv.Write(Command{Type: CommandPut, Key: []byte("after"), Value: []byte("cp")}); err != nil {
		t.Fatalf("post-checkpoint write: %v", err)
	}
	// 完整停机：必须停掉后台 FlushWorker/Compaction，否则它们会与 kv2 抢同一 SSTable 目录。
	_ = kv.Close()

	// 模拟重启：复用同一 config（WAL/SSTable 路径不变）新建实例，从 SSTable + 重写后的
	// WAL 恢复。不能再调 setupStandalone——那会换一个新的临时目录。
	kv2 := NewKVServer()
	defer kv2.Close()
	time.Sleep(100 * time.Millisecond) // 等 SSTable 元数据异步加载

	// 全部 50 个 key（除被删的 k0000）都应恢复——多数只可能来自 SSTable，因为 checkpoint
	// 已把它们移出 WAL。
	for i := 1; i < 50; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		want := fmt.Sprintf("v%04d", i)
		if v, err := kv2.Get(key); err != nil || string(v) != want {
			t.Fatalf("recover %s: v=%q err=%v (want %q)", key, v, err, want)
		}
	}
	// 删除墓碑跨 checkpoint 存活，k0000 不得复活。
	if _, err := kv2.Get([]byte("k0000")); err == nil {
		t.Fatal("k0000 resurrected after checkpoint+recover")
	}
	// checkpoint 之后写入的新 key 存在。
	if v, err := kv2.Get([]byte("after")); err != nil || string(v) != "cp" {
		t.Fatalf("post-checkpoint key lost: v=%q err=%v", v, err)
	}
}

// TestCheckpoint_BoundsWALGrowth 验证周期 checkpoint 把 WAL 稳态大小约束在未刷盘热数据
// 量级：对小 key 集反复覆盖写 N≫阈值 次，WAL 不应线性膨胀到 N 条记录。
func TestCheckpoint_BoundsWALGrowth(t *testing.T) {
	kv, walPath, cleanup := setupStandalone(t, 100) // checkpoint 间隔 = 2×100 = 200 次写
	defer cleanup()
	defer kv.wal.Close()

	const (
		uniqueKeys = 10
		writes     = 5000 // 25 个 checkpoint 周期
		workers    = 20
	)
	value := make([]byte, 8) // 全部写同一值，最终态确定，避免并发覆盖的定序问题
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < writes; i += workers {
				key := []byte(fmt.Sprintf("k%02d", i%uniqueKeys))
				if err := kv.Write(Command{Type: CommandPut, Key: key, Value: value}); err != nil {
					t.Errorf("write %d: %v", i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	// 每条记录 ~9+3+8=20B。有界应在几百条以内(~数 KB)；无界则 5000 条 ~100KB。
	const bound = 20 * 1024
	if fi.Size() > bound {
		t.Fatalf("WAL grew unbounded: %d bytes (> %d); checkpoint not reclaiming", fi.Size(), bound)
	}

	// 有界的同时数据仍正确：每个 key 最终值可读。
	for k := 0; k < uniqueKeys; k++ {
		key := []byte(fmt.Sprintf("k%02d", k))
		if _, err := kv.Get(key); err != nil {
			t.Fatalf("key %s lost after bounded checkpoints: %v", key, err)
		}
	}
}
