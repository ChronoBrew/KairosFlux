package shardkv

import (
	"log/slog"
	"net"
	"net/rpc"
	"path/filepath"
	"strconv"
	"time"

	"github.com/NeverENG/BanDB/Raft"
	"github.com/NeverENG/BanDB/service/cluster"
)

// Shard 是一个分片：一个 Raft 组 + 该分片的 FSM store + 一个排空 ApplyCh 的 apply 循环。
type Shard struct {
	id     int
	raft   *Raft.Raft
	store  KVStore
	stopCh chan struct{}
}

// applyLoop 排空该分片 Raft 的 ApplyCh，把已提交命令应用到 store。
//
// 【必须从构造起就运行】Raft 的 `ApplyCh <- entry` 是持锁的阻塞发送（缓冲 100）；若无人排空，
// 累计 100 条后发送会在锁内阻塞、卡死整个组。故每分片构造即起本循环，Stop 时退出。
func (s *Shard) applyLoop() {
	ch := s.raft.GetApplyCh()
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

// Node 是集群中的一个节点：托管所有分片（每分片一个 Raft 组），共享一个 RPC 端点（RaftGroupServer
// 按 GroupID=shardID 分发）。写按 key 路由到分片组 leader 提交，各节点 apply 循环复制应用。
type Node struct {
	self       string
	me         int
	addrs      []string
	shardCount int
	shards     map[int]*Shard
	groupSrv   *Raft.RaftGroupServer
}

// NewNode 构造节点：为每个分片建 Raft 组、内存 store，并起 apply 循环；注册进共享的分发器。
// 调用方随后用 Serve 绑定监听器。dataDir 下每分片一个子目录持久化 Raft 状态。
func NewNode(addrs []string, me, shardCount int, dataDir string) *Node {
	n := &Node{
		self:       addrs[me],
		me:         me,
		addrs:      addrs,
		shardCount: shardCount,
		shards:     make(map[int]*Shard),
		groupSrv:   Raft.NewRaftGroupServer(),
	}
	for sid := 0; sid < shardCount; sid++ {
		r := Raft.NewRaftGroup(sid, addrs, me, filepath.Join(dataDir, "shard"+strconv.Itoa(sid)))
		sh := &Shard{id: sid, raft: r, store: newMemStore(), stopCh: make(chan struct{})}
		go sh.applyLoop()
		n.shards[sid] = sh
		n.groupSrv.AddGroup(sid, r)
	}
	return n
}

// Serve 在给定 rpc.Server / 监听器上暴露本节点的分片 RPC 端点。
func (n *Node) Serve(server *rpc.Server, ln net.Listener) error {
	if err := n.groupSrv.RegisterRPC(server); err != nil {
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
	return Raft.ProposeToGroup(n.addrs, sid, b, time.Second, 5*time.Second)
}

// LocalGet 从本节点的分片副本读取（最终一致：apply 异步，可能落后于最新提交）。
func (n *Node) LocalGet(key []byte) ([]byte, bool) {
	sid := cluster.ShardOf(key, n.shardCount)
	return n.shards[sid].store.Get(key)
}

// ShardLeader 返回 shard 组在本节点视角的 leader 地址（供观测/调试）。
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
