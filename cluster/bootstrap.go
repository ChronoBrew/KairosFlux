package cluster

import "time"

// NewClusterFromPeers 用节点地址列表构建一个 Placement：一致性哈希环 + 注册表（初始
// 全部存活）。vnodes<=0 取默认；aliveTTL 为注册表存活判定窗口——「先路由」阶段无心跳，
// 取较大 TTL 让所有节点视为存活（健康探测/故障转移属后续工作）。
func NewClusterFromPeers(peers []string, vnodes int, aliveTTL time.Duration) *Placement {
	ring := NewHashRing(peers, vnodes)
	reg := NewRegistry(peers, aliveTTL)
	return NewPlacement(ring, reg)
}
