package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
)

// TestReloadRecoversFlushedKeys 守护 SSTable 重载恢复：写入远超单表阈值的数据逼其 flush 到
// SSTable，停机后用新 MemTable 在同一目录重新加载，所有已 flush key 都应能读回。
//
// 回归目标：LoadSSTableMetaList 曾用 EnsureMeta 顺序扫描算 MaxKey，扫过了数据段之后的
// 块索引/布隆/footer，MaxKey 退化成空串，使 getFromSSTables 的 [MinKey,MaxKey] 过滤把所有
// 命中 key 跳过 → 重启后已 flush 数据全部丢失。修复后应 50/50 恢复。
func TestReloadRecoversFlushedKeys(t *testing.T) {
	oldWAL := config.G.WALPath
	oldSST := config.G.SSTablePath
	oldMax := config.G.MaxMemTableSize
	dir := t.TempDir()
	config.G.WALPath = dir + "/wal.log"
	config.G.SSTablePath = dir
	config.G.MaxMemTableSize = 4 // 极小阈值，逼迫多轮 active→dirty→SSTable flush
	defer func() {
		config.G.WALPath = oldWAL
		config.G.SSTablePath = oldSST
		config.G.MaxMemTableSize = oldMax
	}()

	const n = 50
	mt := NewMemTable()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := mt.Put(key, []byte(fmt.Sprintf("v%04d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond) // 等异步 FlushWorker 把数据落到 SSTable
	_ = mt.Close()                     // 停后台协程，避免与重载实例抢同一目录

	// 模拟重启：同一目录新建 MemTable，从 SSTable 重新加载。
	mt2 := NewMemTable()
	defer mt2.Close()
	time.Sleep(200 * time.Millisecond) // 等 SSTable 元数据/索引异步加载

	miss := 0
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		want := fmt.Sprintf("v%04d", i)
		if v, err := mt2.Get(key); err != nil || string(v) != want {
			miss++
			if miss <= 5 {
				t.Errorf("recover %s: v=%q err=%v (want %q)", key, v, err, want)
			}
		}
	}
	if miss > 0 {
		t.Fatalf("SSTable reload lost %d/%d flushed keys", miss, n)
	}
}
