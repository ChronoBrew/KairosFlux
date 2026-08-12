package storage

import (
	"github.com/NeverENG/BanDB/storage/istorage"
)

// Engine 存储引擎，作为 MemTable 的薄封装
// MemTable 内部已使用 RWMutex 自同步，Engine 不再持有自己的锁
type Engine struct {
	memTable istorage.IMemTable
}

func NewEngine(memTable istorage.IMemTable) *Engine {
	return &Engine{memTable: memTable}
}

func (e *Engine) Put(key []byte, value []byte) error {
	// MemTable.Put 内部已包含同步和刷盘触发逻辑
	return e.memTable.Put(key, value)
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	// MemTable.Get 内部已包含同步逻辑
	return e.memTable.Get(key)
}

func (e *Engine) Delete(key []byte) error {
	// MemTable.Delete 内部已包含同步逻辑
	return e.memTable.Delete(key)
}

// Scan 在 [start,end] 闭区间升序遍历最新可见键值，跳过墓碑；fn 返回 false 提前停止。
// 当前仅覆盖 MemTable（未刷盘热数据），已刷盘到 SSTable 的历史数据待后续接入。
func (e *Engine) Scan(start, end []byte, fn func(key, value []byte) bool) {
	e.memTable.ScanRange(start, end, fn)
}

// SnapshotLive 返回未刷盘热数据（active+dirty，含墓碑）的快照，供 WAL checkpoint 重写。
func (e *Engine) SnapshotLive() []istorage.LogEntry {
	return e.memTable.SnapshotLive()
}

// Close 停止底层 MemTable 的后台协程，用于优雅停机。
func (e *Engine) Close() error {
	return e.memTable.Close()
}

// FlushToSSTable 快照重放到 SSTable（不经过 active 表，走临时表 → Flush → SSTable 路径）
func (e *Engine) FlushToSSTable(entries []istorage.LogEntry) error {
	return e.memTable.FlushToSSTable(entries)
}
