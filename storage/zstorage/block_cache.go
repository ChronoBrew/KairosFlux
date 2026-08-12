package zstorage

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// blockCacheShards 是缓存分片数（须为 2 的幂）。分片以降低高并发点查下的锁竞争：
// 每次命中只锁住 1/N 的缓存。
const blockCacheShards = 16

// blockCache 按字节预算限容的分片 LRU 数据块缓存。
//
// SSTable 一经写入即只读，数据块内容因此不可变，缓存条目可被并发读者共享而无需拷贝。
// 调用方只可读取所得切片，不得原地修改。
//
// 零值不可用；nil 接收者上的方法均安全（视作缓存关闭），故调用方无需分支判断。
type blockCache struct {
	shards [blockCacheShards]blockCacheShard
}

// blockCacheKey 唯一标识一个数据块：文件路径 + 块起始偏移。
// SSTable 文件名含纳秒创建时间戳，故路径不会被复用，键不会跨文件混淆。
type blockCacheKey struct {
	path   string
	offset int64
}

type blockCacheEntry struct {
	key  blockCacheKey
	data []byte
}

// blockCacheShard 单个分片：LRU 链表（front 为最近使用）+ 索引 + 字节预算。
type blockCacheShard struct {
	mu     sync.Mutex
	budget int64
	used   int64
	lru    *list.List // 元素值为 *blockCacheEntry
	items  map[blockCacheKey]*list.Element
}

// 命中率观测：与 compaction 写放大计数同规格的零成本原子计数。
var (
	blockCacheHits   atomic.Int64
	blockCacheMisses atomic.Int64
	blockCacheEvicts atomic.Int64
)

// BlockCacheStats 是某一刻的数据块缓存计数。
type BlockCacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// ReadBlockCacheStats 读取当前数据块缓存计数快照。
func ReadBlockCacheStats() BlockCacheStats {
	return BlockCacheStats{
		Hits:      blockCacheHits.Load(),
		Misses:    blockCacheMisses.Load(),
		Evictions: blockCacheEvicts.Load(),
	}
}

// ResetBlockCacheStats 清零计数（供 benchmark 隔离测量）。
func ResetBlockCacheStats() {
	blockCacheHits.Store(0)
	blockCacheMisses.Store(0)
	blockCacheEvicts.Store(0)
}

// newBlockCache 按总字节预算构造缓存，预算均分到各分片。预算 <=0 返回 nil（缓存关闭）。
func newBlockCache(budgetBytes int64) *blockCache {
	if budgetBytes <= 0 {
		return nil
	}
	c := &blockCache{}
	per := budgetBytes / blockCacheShards
	if per < 1 {
		per = 1
	}
	for i := range c.shards {
		c.shards[i] = blockCacheShard{
			budget: per,
			lru:    list.New(),
			items:  make(map[blockCacheKey]*list.Element),
		}
	}
	return c
}

func (c *blockCache) shard(key blockCacheKey) *blockCacheShard {
	h := fnv.New64a()
	h.Write([]byte(key.path))
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(key.offset >> (8 * i))
	}
	h.Write(b[:])
	return &c.shards[h.Sum64()&(blockCacheShards-1)]
}

// get 取一个已缓存的数据块。返回的切片不可修改。
func (c *blockCache) get(path string, offset int64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	key := blockCacheKey{path: path, offset: offset}
	return c.getShard(c.shard(key), key)
}

// put 写入一个数据块；超出分片预算时按 LRU 淘汰。data 写入后归缓存所有，调用方不得再修改。
func (c *blockCache) put(path string, offset int64, data []byte) {
	if c == nil {
		return
	}
	key := blockCacheKey{path: path, offset: offset}
	c.putShard(c.shard(key), key, data)
}

// getShard 在指定分片内取块，并提升其 LRU 热度。
func (c *blockCache) getShard(s *blockCacheShard, key blockCacheKey) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[key]
	if !ok {
		blockCacheMisses.Add(1)
		return nil, false
	}
	s.lru.MoveToFront(el)
	blockCacheHits.Add(1)
	return el.Value.(*blockCacheEntry).data, true
}

// putShard 在指定分片内写入块，并按 LRU 淘汰至预算之内。
// 单块大于整个分片预算时不缓存——否则它会反复挤掉该分片其余全部条目。
func (c *blockCache) putShard(s *blockCacheShard, key blockCacheKey, data []byte) {
	size := int64(len(data))
	if size == 0 || size > s.budget {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok { // 并发读者可能已填入：仅提升热度
		s.lru.MoveToFront(el)
		return
	}
	s.items[key] = s.lru.PushFront(&blockCacheEntry{key: key, data: data})
	s.used += size

	for s.used > s.budget {
		back := s.lru.Back()
		if back == nil {
			break
		}
		victim := back.Value.(*blockCacheEntry)
		s.lru.Remove(back)
		delete(s.items, victim.key)
		s.used -= int64(len(victim.data))
		blockCacheEvicts.Add(1)
	}
}

// dropFile 剔除该文件的全部缓存块，在 SSTable 被删除时调用。
// 逐分片全扫，成本与缓存条目数成正比，但仅在 compaction 频率上发生。
func (c *blockCache) dropFile(path string) {
	if c == nil {
		return
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for el := s.lru.Front(); el != nil; {
			next := el.Next()
			entry := el.Value.(*blockCacheEntry)
			if entry.key.path == path {
				s.lru.Remove(el)
				delete(s.items, entry.key)
				s.used -= int64(len(entry.data))
			}
			el = next
		}
		s.mu.Unlock()
	}
}
