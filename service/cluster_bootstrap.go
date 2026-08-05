package service

import (
	"log/slog"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/service/cluster"
)

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
	placement := cluster.NewClusterFromPeers(peers, config.G.VNodes, 100*365*24*time.Hour)
	pool := cluster.NewPeerPool(5 * time.Second)
	r.SetRouting(placement, self, pool)
	slog.Info("shard routing enabled", "self", self, "peers", peers)
}
