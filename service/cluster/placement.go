package cluster

import (
	"errors"
	"log/slog"
)

// errNotImplemented 标记尚未落地的控制面能力（数据迁移、跨节点转发等）。
// 这些能力依赖传输层重写（当前 Raft 走 net/rpc 静态传输），属 stretch 范围，
// 见架构文档。桩实现统一返回此错误，避免调用方误以为已生效。
var errNotImplemented = errors.New("cluster: not implemented")

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

// Rebalance 是再平衡桩（stretch）。
//
// 真正的再平衡需要在成员变更后执行真实的数据迁移（把 key 的实际数据从旧属主
// 搬到新属主），这依赖跨节点数据传输通道——当前 Raft 使用 net/rpc 静态传输，
// 无法承载分片迁移，属传输层重写范围（见架构文档）。此处仅记录 TODO 并返回
// errNotImplemented，保留接口与调用点，待传输层就绪后填充。
func (p *Placement) Rebalance() error {
	slog.Warn("[cluster] rebalance: TODO, requires data migration over a new transport (stretch)")
	return errNotImplemented
}
