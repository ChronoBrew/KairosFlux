//go:build experimental

// 隔离说明见同包 command.go 顶部注释。
package shardkv

import (
	"log/slog"
	"net"
	"net/rpc"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/NeverENG/BanDB/cluster"
	"github.com/NeverENG/BanDB/raft"
)

// Shard 是一个分片：一个 Raft 组 + 该分片的 FSM store + 一个排空 ApplyCh 的 apply 循环。
type Shard struct {
	id     int
	raft   *raft.Raft
	store  KVStore
	stopCh chan struct{}
}

// applyLoop 排空该分片 Raft 的 ApplyCh，把已提交命令应用到 store。
//
// 必须从构造起就运行：Raft 的 `ApplyCh <- entry` 是持锁的阻塞发送（缓冲 100）；若无人排空，
// 累计 100 条后发送会在锁内阻塞、卡死整个组。故每分片构造即起本循环，Stop 时退出。
func (s *Shard) applyLoop() {
	ch := s.raft.ApplyCh
	for {
		select {
		case <-s.stopCh:
			return
		case entry := <-ch:
			if entry.IsSnapshot {
				continue // v1 不处理快照重放
			}
			cmd, err := decodeCommand(entry.Command)
			if err != nil {
				slog.Error("shardkv: decode command failed", "shard", s.id, "error", err)
				continue
			}
			switch cmd.Op {
			case "Put":
				s.store.Put(cmd.Key, cmd.Value)
			case "Delete":
				s.store.Delete(cmd.Key)
			}
		}
	}
}

// Node 是集群中的一个节点。真分片下每个分片只复制到 rf 个节点的副本子集（rf<节点数），故
// 本节点只托管「自己在其副本集内」的那些分片（每分片一个 Raft 组，peers=该分片副本子集）。
// replicas 记录全部分片的副本地址（含本节点不托管的），供写/读路由与跨节点转发。
type Node struct {
	self       string
	me         int
	addrs      []string
	shardCount int
	rf         int
	shards     map[int]*Shard               // 仅本节点托管的分片
	replicas   map[int][]string             // 每个分片的副本节点地址（全部分片）
	readLB     map[int]*cluster.P2CBalancer // 本节点不托管的分片：转发读的副本 LB（延迟感知）
	groupSrv   *raft.RaftGroupServer

	served atomic.Int64 // 本节点作为副本服务的转发读次数（观测 P2C 在副本间的分布）
}

// NewNode 构造节点：按副本因子 rf 用一致性哈希环算出各分片的副本集，只为「本节点属于其副本集」
// 的分片建 Raft 组、内存 store 并起 apply 循环；Raft 组的 peers 即该分片副本子集，me 为本节点在
// 子集内的下标（各副本用同一环独立算出一致的子集顺序，故下标天然一致）。rf<=0 或 >=节点数时退化
// 为全副本（每节点托管每分片）。dataDir 下每分片一个子目录持久化 Raft 状态。
func NewNode(addrs []string, me, shardCount, rf int, dataDir string) *Node {
	self := addrs[me]
	ring := cluster.NewHashRing(addrs, 0)
	n := &Node{
		self:       self,
		me:         me,
		addrs:      addrs,
		shardCount: shardCount,
		rf:         rf,
		shards:     make(map[int]*Shard),
		replicas:   make(map[int][]string, shardCount),
		readLB:     make(map[int]*cluster.P2CBalancer),
		groupSrv:   raft.NewRaftGroupServer(),
	}
	for sid := 0; sid < shardCount; sid++ {
		reps := cluster.ShardReplicas(ring, sid, rf)
		n.replicas[sid] = reps
		meInSet := indexOf(reps, self)
		if meInSet < 0 {
			// 本节点不托管该分片：建 P2C 均衡器，读请求按延迟在副本间择优转发。
			n.readLB[sid] = cluster.NewP2CBalancer(reps, 0)
			continue
		}
		r := raft.NewRaftGroup(sid, reps, meInSet, filepath.Join(dataDir, "shard"+strconv.Itoa(sid)))
		sh := &Shard{id: sid, raft: r, store: newMemStore(), stopCh: make(chan struct{})}
		go sh.applyLoop()
		n.shards[sid] = sh
		n.groupSrv.AddGroup(sid, r)
	}
	return n
}

// indexOf 返回 s 在 ss 中的下标，不存在返回 -1。
func indexOf(ss []string, s string) int {
	for i, v := range ss {
		if v == s {
			return i
		}
	}
	return -1
}

// Serve 在给定 rpc.Server / 监听器上暴露本节点的分片 RPC 端点。
func (n *Node) Serve(server *rpc.Server, ln net.Listener) error {
	if err := n.groupSrv.RegisterRPC(server); err != nil {
		return err
	}
	// ShardRead 服务转发读的落地端：从本节点托管的分片副本读取。与 RaftRPC 共享同一连接
	// （net/rpc 一条连接可服务多个已注册服务），故转发读复用 Raft 的连接池、不额外建连。
	if err := server.RegisterName("ShardRead", &readService{node: n}); err != nil {
		return err
	}
	go server.Accept(ln)
	return nil
}

// Put 按 key 路由到分片组 leader 提交（leader-aware，换届自动重定向重试）。
func (n *Node) Put(key, value []byte) error {
	return n.propose(command{Op: "Put", Key: key, Value: value})
}

// Delete 按 key 路由到分片组 leader 提交删除。
func (n *Node) Delete(key []byte) error {
	return n.propose(command{Op: "Delete", Key: key})
}

func (n *Node) propose(c command) error {
	sid := cluster.ShardOf(c.Key, n.shardCount)
	b, err := encodeCommand(c)
	if err != nil {
		return err
	}
	// 路由到该分片的副本集组 leader；本节点是否为副本无关——ProposeToGroup 直接拨向副本地址。
	return raft.ProposeToGroup(n.replicas[sid], sid, b, time.Second, 5*time.Second)
}

// LocalGet 从本节点的分片副本读取（最终一致：apply 异步，可能落后于最新提交）。
// 本节点若非该分片副本则返回 (nil,false)。
func (n *Node) LocalGet(key []byte) ([]byte, bool) {
	sid := cluster.ShardOf(key, n.shardCount)
	sh, ok := n.shards[sid]
	if !ok {
		return nil, false
	}
	return sh.store.Get(key)
}

// ShardLeader 返回 分片 组在本节点视角的 leader 地址（供观测/调试）。
func (n *Node) ShardLeader(shardID int) (string, bool) {
	sh, ok := n.shards[shardID]
	if !ok {
		return "", false
	}
	return sh.raft.LeaderHint()
}

// Stop 停止所有分片的 Raft 与 apply 循环。
func (n *Node) Stop() {
	for _, sh := range n.shards {
		close(sh.stopCh)
		sh.raft.Stop()
	}
}

// ShardOf 暴露本节点的 key→分片映射（供测试断言 key 落在哪个分片）。
func (n *Node) ShardOf(key []byte) int {
	return cluster.ShardOf(key, n.shardCount)
}
