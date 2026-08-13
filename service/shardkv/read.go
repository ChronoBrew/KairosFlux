package shardkv

import (
	"fmt"
	"time"

	"github.com/NeverENG/BanDB/cluster"
	"github.com/NeverENG/BanDB/raft"
)

// ShardReadArgs / ShardReadReply 是转发读 RPC 的报文：向某分片副本读取一个 key。
type ShardReadArgs struct {
	Shard int
	Key   []byte
}

type ShardReadReply struct {
	Value []byte
	Found bool
}

// readService 是转发读的服务端：从本节点托管的分片副本读取。注册名 "ShardRead"。
type readService struct {
	node *Node
}

// Get 从本节点托管的分片 store 读取 key。本节点若不托管该分片返回错误（路由错误，正常不发生）。
func (s *readService) Get(args *ShardReadArgs, reply *ShardReadReply) error {
	sh, ok := s.node.shards[args.Shard]
	if !ok {
		return fmt.Errorf("shardkv: node does not host shard %d", args.Shard)
	}
	s.node.served.Add(1)
	v, found := sh.store.Get(args.Key)
	reply.Value, reply.Found = v, found
	return nil
}

// Get 读取 key（最终一致）：
//   - 本节点托管该分片 → 本地读（最快，无网络）。
//   - 否则 → 用 P2C 在该分片副本集间按延迟择优，经共享连接池转发读到被选中的副本。这是副本读
//     LB 的真实价值点：调用方本身不是该分片副本，必须跨节点，P2C 把读导向更快/更空闲的副本。
//
// 转发 RPC 失败时顺序回退其余副本，避免单副本抖动导致读失败。
func (n *Node) Get(key []byte) ([]byte, bool, error) {
	sid := cluster.ShardOf(key, n.shardCount)
	if _, hosted := n.shards[sid]; hosted {
		v, found := n.LocalGet(key)
		return v, found, nil
	}

	reps := n.replicas[sid]
	lb := n.readLB[sid]
	if lb == nil || len(reps) == 0 {
		return nil, false, fmt.Errorf("shardkv: no replicas for shard %d", sid)
	}

	args := &ShardReadArgs{Shard: sid, Key: key}
	addr, done := lb.Pick()
	start := time.Now()
	var reply ShardReadReply
	err := raft.PooledCall(addr, "ShardRead.Get", args, &reply)
	done(time.Since(start)) // 回填 EWMA/释放在途——P2C 据此持续把读导向更快的副本
	if err == nil {
		return reply.Value, reply.Found, nil
	}

	// 回退：顺序尝试其余副本（不计入 P2C，避免把故障副本的坏样本当作正常延迟）。
	for _, r := range reps {
		if r == addr {
			continue
		}
		var rep ShardReadReply
		if e := raft.PooledCall(r, "ShardRead.Get", args, &rep); e == nil {
			return rep.Value, rep.Found, nil
		}
	}
	return nil, false, err
}
