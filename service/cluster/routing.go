// Package cluster 实现分片分布式集群的路由与控制面骨架。
//
// 设计动机：BanDB 的定位是「数仓写入前置缓冲引擎」，向分片集群演进时需要
// 一层与存储解耦的「放置与路由控制面」（借鉴 dubbo-go / PD 的思路）。本包
// 只承担控制面职责——决定「一个 key 归属哪个物理节点 / 哪个分片」，以及节点
// 存活的注册发现；真实的跨节点数据迁移与传输属于传输层重写范围，本包以桩标注。
//
// 零第三方依赖：一致性哈希仅使用标准库 hash/crc32。
package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// defaultVNodes 是每个物理节点默认的虚拟节点数。
//
// 为什么要虚拟节点：一致性哈希若每个物理节点只在环上占一个点，节点数较少时
// 分布会严重不均。给每个物理节点在环上放置多个「虚拟节点」，可让 key 更均匀地
// 散落到各物理节点，并使增删节点时被迁移的 key 更接近理论下界 1/N。
const defaultVNodes = 128

// HashRing 是带虚拟节点的一致性哈希环。
//
// 环上每个哈希点（vnode）映射回一个物理节点名。查询时对 key 取哈希，在环上
// 顺时针找到第一个 vnode，其对应的物理节点即为归属。增删物理节点只影响相邻
// 弧段上的 key，从而满足一致性哈希「移动最少 key」的核心性质。
//
// 并发安全：所有读操作持 RLock，增删节点持 Lock。
type HashRing struct {
	mu     sync.RWMutex
	vnodes int // 每个物理节点的虚拟节点数

	// sortedHashes 是所有 vnode 哈希值的升序切片，用于二分查找归属点。
	sortedHashes []uint32
	// hashToNode 将 vnode 哈希值映射回物理节点名。
	hashToNode map[uint32]string
	// nodes 记录已加入环的物理节点，避免重复加入。
	nodes map[string]struct{}
}

// NewHashRing 构造一致性哈希环。vnodes<=0 时取默认值 defaultVNodes。
func NewHashRing(nodes []string, vnodes int) *HashRing {
	if vnodes <= 0 {
		vnodes = defaultVNodes
	}
	r := &HashRing{
		vnodes:     vnodes,
		hashToNode: make(map[uint32]string),
		nodes:      make(map[string]struct{}),
	}
	for _, n := range nodes {
		r.addNodeLocked(n)
	}
	r.rebuildLocked()
	return r
}

// vnodeKey 构造第 i 个虚拟节点的哈希输入。用 "#" 分隔避免不同节点名拼接后碰撞。
func vnodeKey(node string, i int) string {
	return node + "#" + strconv.Itoa(i)
}

// hashKey 计算字节 key 在环上的哈希位置。使用 crc32 IEEE 表，标准库实现、无依赖。
func hashKey(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}

// addNodeLocked 把一个物理节点的所有 vnode 写入映射（不重建排序切片，调用方负责）。
// 调用方必须已持有写锁。
func (r *HashRing) addNodeLocked(node string) {
	if _, ok := r.nodes[node]; ok {
		return
	}
	r.nodes[node] = struct{}{}
	for i := 0; i < r.vnodes; i++ {
		h := hashKey([]byte(vnodeKey(node, i)))
		r.hashToNode[h] = node
	}
}

// rebuildLocked 依据当前 hashToNode 重建升序哈希切片。调用方必须已持有写锁。
func (r *HashRing) rebuildLocked() {
	r.sortedHashes = r.sortedHashes[:0]
	for h := range r.hashToNode {
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// AddNode 动态加入一个物理节点。只有落在其 vnode 相邻弧段的 key 会改变归属。
func (r *HashRing) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	before := len(r.nodes)
	r.addNodeLocked(node)
	if len(r.nodes) != before {
		r.rebuildLocked()
	}
}

// RemoveNode 动态摘除一个物理节点。其 vnode 上的 key 顺时针改归下一个节点，
// 其余 key 归属不变，满足一致性哈希性质。
func (r *HashRing) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; !ok {
		return
	}
	delete(r.nodes, node)
	for i := 0; i < r.vnodes; i++ {
		h := hashKey([]byte(vnodeKey(node, i)))
		// 仅当该哈希点确实归属被删节点时才删除，避免误删哈希碰撞下他人的点。
		if r.hashToNode[h] == node {
			delete(r.hashToNode, h)
		}
	}
	r.rebuildLocked()
}

// NodeFor 返回 key 归属的物理节点：环上顺时针第一个 vnode 对应的节点。
// 空环返回 ""。
func (r *HashRing) NodeFor(key []byte) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return ""
	}
	idx := r.searchLocked(hashKey(key))
	return r.hashToNode[r.sortedHashes[idx]]
}

// searchLocked 二分查找 h 顺时针落点的 sortedHashes 下标（超过最大值则环回 0）。
// 调用方必须已持有读锁或写锁。
func (r *HashRing) searchLocked(h uint32) int {
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= h
	})
	if idx == len(r.sortedHashes) {
		idx = 0 // 环回到最小哈希点
	}
	return idx
}

// walkNodesFrom 从 key 的哈希落点开始，按顺时针顺序返回去重后的物理节点序列。
//
// 为什么需要它：一个物理节点在环上有多个 vnode，故障转移读侧（Placement.OwnerOf）
// 需要「跳过不存活的归属节点，顺环找下一个存活节点」。该方法给出稳定的顺时针
// 节点访问顺序，且每个物理节点只出现一次。空环返回 nil。
//
// 保持未导出：导出面严格等于任务约定，此助手仅供同包 Placement 使用。
func (r *HashRing) walkNodesFrom(key []byte) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := len(r.sortedHashes)
	if n == 0 {
		return nil
	}
	start := r.searchLocked(hashKey(key))
	out := make([]string, 0, len(r.nodes))
	seen := make(map[string]struct{}, len(r.nodes))
	for i := 0; i < n; i++ {
		node := r.hashToNode[r.sortedHashes[(start+i)%n]]
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	return out
}

// ShardReplicas 返回分片 shardID 的副本集：以分片锚点在环上顺时针取前 rf 个物理节点。
//
// 为什么走环而非取模：这让「分片 → 承载节点子集」的放置与一致性哈希环统一——增删节点时
// 副本集按环的最小变动性质迁移，且各节点用同一环独立算出完全一致的副本集与顺序（无需协调），
// 从而 Raft 组的 peers 顺序、每节点在组内的下标（me）在各副本上天然一致。
//
// rf<=0 视为 1；rf 超过物理节点数则取全部。空环返回 nil。
func ShardReplicas(ring *HashRing, shardID, rf int) []string {
	nodes := ring.walkNodesFrom(shardAnchor(shardID))
	if len(nodes) == 0 {
		return nil
	}
	if rf <= 0 {
		rf = 1
	}
	if rf > len(nodes) {
		rf = len(nodes)
	}
	return nodes[:rf]
}

// shardAnchor 构造分片在环上的锚点 key。用 "分片#" 前缀与普通数据 key 区分命名空间。
func shardAnchor(shardID int) []byte {
	return []byte("shard#" + strconv.Itoa(shardID))
}

// ShardOf 把 key 映射到分片编号 [0, shardCount)。shardCount<=0 返回 0。
//
// 注意分层：这是纯取模的「分片编号」，与 NodeFor 的「节点归属」是两层概念——
// 分片是逻辑数据切分单位，节点是物理承载单位，两者由控制面分别决定、可独立演进。
func ShardOf(key []byte, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	return int(hashKey(key) % uint32(shardCount))
}
