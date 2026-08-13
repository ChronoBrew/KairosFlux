package storage

import (
	"fmt"
	"testing"
	"time"
)

// TestReloadRecoversFlushedKeys 守护 SSTable 重载恢复：写入远超单表阈值的数据逼其 flush 到
// SSTable，停机后用新 MemTable 在同一目录重新加载，所有已 flush key 都应能读回。
//
// 回归目标：MaxKey 曾由顺序扫描数据段推导，扫过了数据段之后的块索引/布隆/footer，
// 退化成空串，使 [MinKey,MaxKey] 过滤把所有命中 key 跳过 → 重启后已 flush 数据全部丢失。
// 现在 MaxKey 只从块索引取，取不到则不施加上界。
func TestReloadRecoversFlushedKeys(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxMemTableSize: 4} // 极小阈值，逼迫多轮 active→dirty→SSTable flush

	const n = 50
	mt := NewEngine(opts)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := mt.Put(key, []byte(fmt.Sprintf("v%04d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond) // 等异步 FlushWorker 把数据落到 SSTable
	_ = mt.Close()                     // 停后台协程，避免与重载实例抢同一目录

	// 模拟重启：同一目录新建 MemTable，从 SSTable 重新加载。
	mt2 := NewEngine(opts)
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

// TestNewEngineSeesExistingSSTablesImmediately 固定引擎的启动契约：NewEngine 返回后，
// 磁盘上已有的 SSTable 必须立即可见——不需要调用方 sleep 等待任何后台加载。
//
// 这条契约此前不成立：元信息扫描是 goroutine，且它整体替换 metas，于是与并发的 AddMeta
// 相争。启动时 WAL 重放触发的 flush 刚登记好元信息，就可能被随后完成的扫描抹掉，那个
// SSTable 的数据直到下次重启前都读不到——恰好发生在崩溃恢复路径上。
//
// 本用例故意不 sleep：一旦加载退回异步，它就会失败。
func TestNewEngineSeesExistingSSTablesImmediately(t *testing.T) {
	// 阈值取大：本用例自行写出 SSTable，不依赖自动 flush。
	opts := Options{Dir: t.TempDir(), MaxMemTableSize: 1 << 20}

	// 先在目录里放好一个 SSTable。
	seed := NewSSTable(opts)
	entries := make([]LogEntry, 0, 64)
	for i := 0; i < 64; i++ {
		entries = append(entries, LogEntry{
			Key:   []byte(fmt.Sprintf("pre%04d", i)),
			Value: []byte(fmt.Sprintf("val%04d", i)),
		})
	}
	if err := seed.WriteToSSTable(entries); err != nil {
		t.Fatalf("WriteToSSTable: %v", err)
	}

	e := NewEngine(opts)
	t.Cleanup(func() { e.Close() })

	// 立即读，不给后台加载留任何时间窗口。
	for _, i := range []int{0, 31, 63} {
		key := []byte(fmt.Sprintf("pre%04d", i))
		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("NewEngine 返回后 %s 应立即可读: %v", key, err)
		}
		if want := fmt.Sprintf("val%04d", i); string(got) != want {
			t.Fatalf("Get %s = %q, want %q", key, got, want)
		}
	}
}
