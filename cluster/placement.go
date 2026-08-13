package cluster

import (
	"log/slog"
)

// 这些能力依赖传输层重写（当前 Raft 走 net/rpc 静态传输），属 stretch 范围，
// 见架构文档。桩实现统一返回此错误，避免调用方误以为已生效。
// Placement 是放置控制面：组合一致性哈希环与节点注册表，回答「某 key 当前的
// 属主是谁」，并提供故障转移与再平衡的入口。
//
// 分工：HashRing 负责静态归属计算，Registry 负责存活视图，Placement 把二者
// 合成为「考虑存活的属主查询」——这正是读侧故障转移的体现。
type Placement struct {
	ring *HashRing
	reg  *Registry
}

// NewPlacement 组合哈希环与注册表构造放置控制面。
func NewPlacement(ring *HashRing, reg *Registry) *Placement {
	return &Placement{ring: ring, reg: reg}
}

// OwnerOf 返回 key 当前的存活属主。
//
// 先由环计算归属节点；若该节点在注册表中不存活，则沿环顺时针跳到下一个存活
// 节点（读侧故障转移）。当所有节点都不存活（或环为空）时返回 ""。
func (p *Placement) OwnerOf(key []byte) string {
	for _, node := range p.ring.walkNodesFrom(key) {
		if p.reg.IsAlive(node) {
			return node
		}
	}
	return ""
}

// Failover 处理节点下线：把 deadNode 从环上摘除，使其 key 顺时针重新归属。
// 这是控制面对故障的「写侧」响应——改变归属拓扑本身，而非仅在读时跳过。
func (p *Placement) Failover(deadNode string) {
	p.ring.RemoveNode(deadNode)
	slog.Info("[cluster] failover: node removed from ring", "node", deadNode)
}

// IsLocal 判定 key 的属主是否为 self（本节点），供网关决定本地处理还是转发。
func (p *Placement) IsLocal(key []byte, self string) bool {
	return p.OwnerOf(key) == self
}
