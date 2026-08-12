package storage

type IMemTable interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	// ScanRange 在 [start,end] 闭区间升序遍历最新可见键值，跳过墓碑；
	// fn 返回 false 提前停止。start/end 为空表示该侧不限。
	ScanRange(start, end []byte, fn func(key, value []byte) bool)
	// SnapshotLive 返回 active+dirty 合并后的全部键值（含墓碑 value==nil），
	// 供 WAL checkpoint 重写；底层字节已拷贝。
	SnapshotLive() []LogEntry
	Size() int
	StartFlush()
	// Close 停止后台 FlushWorker/Compaction 协程，用于优雅停机。
	Close() error
	// FlushToSSTable 将 entries 写入临时表并立即 Flush 到 SSTable
	// 不经过 active 表，不阻塞正常读写
	FlushToSSTable(entries []LogEntry) error
}
type ISSTable interface {
	LoadSSTableMetaList()
	AddMata(meta *SSTableMata)
	RemoveMata(target *SSTableMata)
	GetLevelFiles(level int) []*SSTableMata
	GetAllMata() []*SSTableMata

	WriteToSSTable(entry []LogEntry) error
	ReadFromSSTable(filepath string, key []byte) ([]byte, bool)
	ReadAllFromSSTable(filepath string) ([]*LogEntry, error)
	MergeSSTable(files []*SSTableMata, targetLevel int) *SSTableMata
	DeleteSSTable(meta *SSTableMata)
}
