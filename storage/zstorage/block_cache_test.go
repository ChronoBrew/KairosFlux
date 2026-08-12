package zstorage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestBlockCacheHitAndMiss 验证写入后可命中，未写入的键为 miss。
func TestBlockCacheHitAndMiss(t *testing.T) {
	c := newBlockCache(1 << 20)
	data := []byte("block-payload")

	if _, ok := c.get("a.sst", 0); ok {
		t.Fatal("空缓存不应命中")
	}

	c.put("a.sst", 0, data)
	got, ok := c.get("a.sst", 0)
	if !ok {
		t.Fatal("写入后应命中")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("命中内容不符: got %q want %q", got, data)
	}

	// 同路径不同偏移是不同的块。
	if _, ok := c.get("a.sst", 64); ok {
		t.Fatal("不同偏移不应命中")
	}
	// 同偏移不同路径是不同的块。
	if _, ok := c.get("b.sst", 0); ok {
		t.Fatal("不同路径不应命中")
	}
}

// TestBlockCacheNilReceiverIsDisabled 验证 nil 缓存（预算 <=0）上的操作安全且恒 miss，
// 使读路径无需分支判断缓存是否启用。
func TestBlockCacheNilReceiverIsDisabled(t *testing.T) {
	if c := newBlockCache(0); c != nil {
		t.Fatal("预算 0 应返回 nil（缓存关闭）")
	}
	var c *blockCache // 关闭态
	c.put("a.sst", 0, []byte("x"))
	if _, ok := c.get("a.sst", 0); ok {
		t.Fatal("关闭态不应命中")
	}
	c.dropFile("a.sst") // 不应 panic
}

// TestBlockCacheEvictsUnderBudget 验证超出预算时按 LRU 淘汰，且最近使用的块被保留。
func TestBlockCacheEvictsUnderBudget(t *testing.T) {
	// 每分片预算 = 总预算/分片数。把总预算设为 shards×100，使单分片仅容 100 字节。
	c := newBlockCache(blockCacheShards * 100)
	blk := func(n int) []byte { return bytes.Repeat([]byte("x"), n) }

	// 全部落到同一分片：直接对同一分片操作，避免依赖哈希分布。
	s := c.shard(blockCacheKey{path: "a.sst", offset: 0})
	if s.budget != 100 {
		t.Fatalf("分片预算应为 100, 得到 %d", s.budget)
	}

	// 逐个塞入 40 字节的块直到超预算：容量 100 → 最多同时容纳 2 块。
	keys := []int64{0, 1, 2}
	for _, off := range keys {
		c.putShard(s, blockCacheKey{path: "a.sst", offset: off}, blk(40))
	}

	live := 0
	for _, off := range keys {
		if _, ok := c.getShard(s, blockCacheKey{path: "a.sst", offset: off}); ok {
			live++
		}
	}
	if live != 2 {
		t.Fatalf("预算 100/块 40 应只留 2 块, 实际留 %d 块", live)
	}
	// 最早写入的块应已被淘汰。
	if _, ok := c.getShard(s, blockCacheKey{path: "a.sst", offset: 0}); ok {
		t.Fatal("最久未使用的块应被淘汰")
	}
}

// TestBlockCacheSkipsOversizedBlock 验证单块超过分片预算时不入缓存——否则它会反复
// 挤掉该分片其余全部条目。
func TestBlockCacheSkipsOversizedBlock(t *testing.T) {
	c := newBlockCache(blockCacheShards * 100)
	c.put("a.sst", 0, bytes.Repeat([]byte("x"), 200))
	if _, ok := c.get("a.sst", 0); ok {
		t.Fatal("超预算的单块不应入缓存")
	}
}

// TestBlockCacheDropFile 验证文件被删除后其全部块被剔除，其它文件不受影响。
func TestBlockCacheDropFile(t *testing.T) {
	c := newBlockCache(1 << 20)
	for i := int64(0); i < 32; i++ {
		c.put("gone.sst", i*64, []byte(fmt.Sprintf("v%d", i)))
		c.put("kept.sst", i*64, []byte(fmt.Sprintf("v%d", i)))
	}

	c.dropFile("gone.sst")

	for i := int64(0); i < 32; i++ {
		if _, ok := c.get("gone.sst", i*64); ok {
			t.Fatalf("已删文件的块 offset=%d 仍在缓存", i*64)
		}
		if _, ok := c.get("kept.sst", i*64); !ok {
			t.Fatalf("其它文件的块 offset=%d 被误删", i*64)
		}
	}
}

// TestBlockCacheConcurrent 并发读写同一批块，配合 -race 守卫分片锁的正确性。
func TestBlockCacheConcurrent(t *testing.T) {
	c := newBlockCache(1 << 20)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := int64(0); i < 200; i++ {
				path := fmt.Sprintf("f%d.sst", i%4)
				c.put(path, i*64, []byte("payload"))
				c.get(path, i*64)
				if i%50 == 0 {
					c.dropFile(path)
				}
			}
		}(w)
	}
	wg.Wait()
}
