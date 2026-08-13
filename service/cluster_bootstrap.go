package service

import (
	"log/slog"
	"time"

	"github.com/NeverENG/BanDB/cluster"
	"github.com/NeverENG/BanDB/config"
)

// assumeAliveTTL 让所有节点恒被视为存活。
//
// 集群目前没有心跳——cluster.Registry.Heartbeat 无任何调用方，故存活视图不会被刷新。
// 此时若取一个有限 TTL，所有节点会在该窗口后被判为失联，Placement.OwnerOf 返回空串，
// 路由随即整体中断。取远超进程寿命的值，是把「尚无心跳」这一事实显式固定下来，
// 而不是伪装成一个会过期的存活窗口。接入心跳后应改为真实的判活窗口。
const assumeAliveTTL = 100 * 365 * 24 * time.Hour

// EnableShardRoutingFromConfig 按配置在 router 上开启分片路由（默认关闭时直接返回）。
// 开启时以 config.Peers 为节点地址构建一致性哈希放置，self = Peers[Me]，不属本节点的
// key 经 BanNet 转发到 owner。健康探测/故障转移属后续工作，这里所有节点视为存活。
func EnableShardRoutingFromConfig(r *Router) {
	if !config.G.ShardRoutingEnabled {
		return
	}
	peers := config.G.Peers
	if config.G.Me < 0 || config.G.Me >= len(peers) {
		slog.Error("shard routing: invalid Me/Peers, routing disabled", "me", config.G.Me, "peers", len(peers))
		return
	}
	self := peers[config.G.Me]
	placement := cluster.NewClusterFromPeers(peers, config.G.VNodes, assumeAliveTTL)
	pool := cluster.NewPeerPool(5 * time.Second)
	r.SetRouting(placement, self, pool)
	slog.Info("shard routing enabled", "self", self, "peers", peers)
}
