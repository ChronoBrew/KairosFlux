package zstorage

import "sync/atomic"

// 写放大观测：累计「刷盘(flush → L0)」与「合并(compaction)」写出的字节数。
// 写放大 = (flush + compaction) / 刷盘，反映一条数据平均被 compaction 重写多少遍。
// 零成本原子计数，供 benchmark 量化，也可后续接入 metrics。
var (
	flushBytesWritten      atomic.Int64
	compactionBytesWritten atomic.Int64
)

// CompactionStats 是某一刻的写放大统计。
type CompactionStats struct {
	FlushBytes      int64 // 刷盘（memtable → L0 SSTable）写出的字节
	CompactionBytes int64 // 合并（compaction）写出的字节
}

// ReadCompactionStats 读取当前写放大计数快照。
func ReadCompactionStats() CompactionStats {
	return CompactionStats{
		FlushBytes:      flushBytesWritten.Load(),
		CompactionBytes: compactionBytesWritten.Load(),
	}
}

// ResetCompactionStats 清零计数（供 benchmark 隔离测量）。
func ResetCompactionStats() {
	flushBytesWritten.Store(0)
	compactionBytesWritten.Store(0)
}
