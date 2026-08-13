// 本文件是 SSTable 的类型与共用定义：磁盘布局常量、块索引、以及 SSTable 本身。
// 读路径见 sstable_read.go，写路径与 compaction 见 sstable_write.go，元信息管理见 sstable_meta.go。
package storage

import (
	"os"
	"sync"
	"sync/atomic"
)

const (
	SSTableBlockSize        = 64
	indexFooterMagic uint32 = 0x49445846 // "IDXF"
	indexFooterSize  int64  = 16         // BlockCount(4) + IndexOffset(8) + Magic(4)
	// 布隆过滤器段位于块索引之后、索引 Footer 之前，向后兼容：
	// 旧格式(v1)无此段，索引 Footer 布局与位置不变。
	bloomTrailerMagic  uint32  = 0x424c4d46 // "BLMF"
	bloomTrailerSize   int64   = 12         // BloomLen(8) + Magic(4)
	defaultBloomFPRate float64 = 0.01
	// maxBloomSectionBytes 限制单文件布隆段大小，防止损坏的 bloomLen 触发
	// 超大分配或 int64 溢出导致的非法负偏移。
	maxBloomSectionBytes uint64 = 1 << 30 // 1 GiB
	// tombstoneValLen 作为 value 长度哨兵标记墓碑（删除标记）：磁盘上仅写该长度、
	// 不写 value 字节，读侧据此还原为 nil。正常 value 长度不可能取此值，且老格式
	// 文件永不含此哨兵，向后兼容。约定：内存与磁盘均以 Value==nil 表示墓碑。
	tombstoneValLen uint32 = 0xFFFFFFFF
)

// blockMeta 是构建期的单块元信息：该块最后一个 key 与块起始偏移。
// 写入 SSTable 尾部的块索引即由它序列化而来。
type blockMeta struct {
	lastKey     []byte
	blockOffset int64
}

type BlockIndexEntry struct {
	LastKey     []byte
	BlockOffset int64
}

// blockIndex 单个 SSTable 文件的块索引及数据区结束偏移。
//
// 磁盘上的索引只记录每块起始偏移，故块长度由「下一块起始 − 本块起始」推出，最后一块
// 的结束即数据区结束（footer 中的 indexOffset）。缓存 dataEnd 使块长度完全可计算，
// 从而支持按精确长度单次读取整块，无需变更磁盘格式。
type blockIndex struct {
	entries []BlockIndexEntry
	dataEnd int64
}

// blockExtent 返回第 i 块在文件中的 [start,end) 字节范围。
type SSTable struct {
	// dir 是 SSTable 文件目录，构造时从 config 快照一份。构造在主 goroutine 完成，之后
	// 后台 goroutine（如 LoadSSTableMetaList、Flush、Merge）读 ss.dir 而非全局 config.G，
	// 避免与（测试中）并发修改全局配置形成数据竞争。
	dir string

	// metas 是元数据主副本，仅在持 mu 时修改；每次修改后即时发布一份不可变快照到 snapshot。
	// 读路径（每次点查都要遍历元数据）从 snapshot 无锁读取，避免逐次加锁与整表拷贝。
	// 写侧为 copy-on-write：变更频率为 flush / compaction 级，远低于读。
	// 快照按值不可变——调用方只可读取，不得原地修改其元素顺序或内容。
	metas      []*SSTableMeta
	snapshot   atomic.Pointer[[]*SSTableMeta]
	mu         sync.RWMutex
	indexCache map[string]*blockIndex
	idxMu      sync.RWMutex
	bloomCache map[string]*PartitionedBloom // 值为 nil 表示老格式(已确认无布隆)
	bloomMu    sync.RWMutex

	// fdCache 按路径复用的常驻只读文件句柄，避免每次点查都 open/close 一次文件。
	// 读路径一律使用 ReadAt：其不依赖文件内偏移，故同一句柄可被任意多个并发读者共享。
	// 缓存规模由 SSTable 文件数界定（compaction 持续收敛文件数）；句柄在 DeleteSSTable
	// 中关闭并剔除，否则每轮 compaction 泄漏一个 fd。
	fdCache map[string]*os.File
	fdMu    sync.RWMutex

	// blocks 缓存已读取的数据块，使热数据点查不再触达磁盘、也不再逐次分配整块缓冲。
	// 预算取自 config.BlockCacheBytes（<=0 关闭，此时为 nil，读路径退化为每次 ReadAt）。
	blocks *blockCache
}

// NewSSTable 按 opts 构造 SSTable 集合的管理者。
func NewSSTable(opts Options) *SSTable {
	opts = opts.withDefaults()
	ss := &SSTable{
		dir:        opts.Dir,
		metas:      make([]*SSTableMeta, 0),
		indexCache: make(map[string]*blockIndex),
		bloomCache: make(map[string]*PartitionedBloom),
		fdCache:    make(map[string]*os.File),
		blocks:     newBlockCache(opts.BlockCacheBytes),
	}
	ss.publishMetas()
	return ss
}

// publishMetas 发布 metas 的不可变快照供读路径无锁获取。调用方须持 ss.mu。
