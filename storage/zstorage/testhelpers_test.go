package zstorage

import "github.com/NeverENG/BanDB/config"

// newBareMemTable 构造一个不启动后台 goroutine 的 MemTable（供确定性测试直接驱动
// FlushToSSTable/CompactSSTable），并从当前 config 快照 flush/compaction 阈值——
// 与生产 NewMemTable 一致，使 CompactSSTable 的阈值判断走 m.maxCompaction 而非全局。
func newBareMemTable(sst *SSTable) *MemTable {
	return &MemTable{
		sst:           sst,
		maxSize:       config.G.MaxMemTableSize,
		maxCompaction: config.G.MaxCompactionSize,
	}
}
