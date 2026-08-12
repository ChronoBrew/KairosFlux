package storage

import (
	"strconv"
	"strings"
	"sync/atomic"
)

// parseLevelFromName 从 SSTable 文件名解析其 level。文件名形如 `sstable_L0_<ts>.sst`
// 或 `sstable_merged_L2_<ts>.sst`——找 `_L<digits>_` 段取 level。老格式文件名不含该段
// （如 `sstable_<ts>.sst`），返回 0 保持向后兼容。level 编码进文件名而非只存内存，是为了
// 让重启（LoadSSTableMetaList）能恢复 level 结构，避免所有文件塌缩到 L0 触发全量重写。
func parseLevelFromName(name string) int {
	i := strings.Index(name, "_L")
	if i < 0 {
		return 0
	}
	rest := name[i+2:] // "_L" 之后
	j := strings.IndexByte(rest, '_')
	if j <= 0 {
		return 0
	}
	level, err := strconv.Atoi(rest[:j])
	if err != nil || level < 0 {
		return 0
	}
	return level
}

// parseCreateTsFromName 从文件名解析创建时间戳（末尾的 UnixNano），作为 SSTable 的
// 真实创建序。文件名形如 `sstable_L0_<ts>.sst` / `sstable_merged_L2_<ts>.sst` / 老格式
// `sstable_<ts>.sst`——ts 恒为最后一个 `_` 之后、`.sst` 之前的数字段。解析失败返回 0。
//
// 用途（关键正确性）：读路径 getFromSSTables 按 metas 逆序判定 newest-wins（内存中 metas =
// 创建序）。重启时若按文件名字符串排序重建 metas，`L0` 会排在 `merged` 之前，导致旧的
// merged 文件在逆序中盖过新的 L0 文件、返回陈旧值。按创建时间戳排序才等价于内存创建序。
func parseCreateTsFromName(name string) int64 {
	base := strings.TrimSuffix(name, ".sst")
	i := strings.LastIndexByte(base, '_')
	if i < 0 {
		return 0
	}
	ts, err := strconv.ParseInt(base[i+1:], 10, 64)
	if err != nil || ts < 0 {
		return 0
	}
	return ts
}

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
