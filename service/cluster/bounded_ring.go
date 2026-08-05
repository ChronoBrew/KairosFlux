package cluster

import (
	"math"
	"sync"
)

// BoundedRing 是「有界负载一致性哈希」（Google, Consistent Hashing with Bounded Loads）：
// 在一致性哈希的局部性基础上，给每个节点设容量上限 ⌈(1+ε)·总负载/节点数⌉；分配 key 时从其
// 哈希落点顺时针找第一个「未满」的节点。于是每个节点的负载被限制在均值的 (1+ε) 倍内、消除
// 热点，同时仍保持一致性哈希「增删节点移动最少」的性质。
//
// 用途定位（诚实）：这是**请求/副本负载均衡**原语——带局部性偏好、但负载有界。它允许同一 key
// 的不同请求在主节点满载时溢出到邻居，故适合无状态请求分发（如把转发请求均衡到一组副本），
// **不适合有状态数据放置**（数据放置要求 key 恒定映射到同一节点，见 NodeFor/ShardOf）。
//
// 并发安全。鸽巢原理保证：因容量之和 ≥ (1+ε)·总负载 ≥ 总负载，任一时刻必有未满节点。
type BoundedRing struct {
	mu      sync.Mutex
	ring    *HashRing
	nodes   []string
	epsilon float64
	load    map[string]int
	total   int
}

// NewBoundedRing 构造有界负载环。epsilon<=0 取默认 0.25（即负载上限为均值的 1.25 倍）。
func NewBoundedRing(nodes []string, vnodes int, epsilon float64) *BoundedRing {
	if epsilon <= 0 {
		epsilon = 0.25
	}
	load := make(map[string]int, len(nodes))
	for _, n := range nodes {
		load[n] = 0
	}
	return &BoundedRing{
		ring:    NewHashRing(nodes, vnodes),
		nodes:   append([]string(nil), nodes...),
		epsilon: epsilon,
		load:    load,
	}
}

// capacityLocked 返回当前每节点容量上限（调用方持锁）。
func (b *BoundedRing) capacityLocked() int {
	n := len(b.nodes)
	if n == 0 {
		return 0
	}
	return int(math.Ceil((1 + b.epsilon) * float64(b.total) / float64(n)))
}

// Assign 为 key 选一个节点：从其哈希落点顺时针找第一个未达容量上限的节点，并计一次负载。
// 完成后须以同一 node 调 Release 归还。空环返回 ("", false)。
func (b *BoundedRing) Assign(key []byte) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.nodes) == 0 {
		return "", false
	}
	b.total++
	capLimit := b.capacityLocked()
	for _, node := range b.ring.walkNodesFrom(key) {
		if b.load[node] < capLimit {
			b.load[node]++
			return node, true
		}
	}
	// 理论到不了（鸽巢原理）；防御性回退主节点。
	primary := b.ring.NodeFor(key)
	b.load[primary]++
	return primary, true
}

// Release 归还一次负载。
func (b *BoundedRing) Release(node string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.load[node] > 0 {
		b.load[node]--
		b.total--
	}
}

// Loads 返回各节点当前负载快照。
func (b *BoundedRing) Loads() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]int, len(b.load))
	for k, v := range b.load {
		out[k] = v
	}
	return out
}

// Capacity 返回当前容量上限（供观测）。
func (b *BoundedRing) Capacity() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.capacityLocked()
}
