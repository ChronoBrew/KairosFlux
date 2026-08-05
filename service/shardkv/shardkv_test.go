package shardkv

import (
	"fmt"
	"net"
	"net/rpc"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// bindListeners 预绑 n 个临时端口（:0），返回监听器与地址，避免固定端口 TIME_WAIT flaky。
func bindListeners(t *testing.T, n int) ([]net.Listener, []string) {
	t.Helper()
	lns := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}
	return lns, addrs
}

// TestShardKV_MultiRaftShardedReplicated 端到端验证 Multi-Raft 分片 KV：
// 起 3 节点、每节点托管 shardCount 个分片组；经一个节点写入跨 ≥2 个分片的 key，各自经其
// 分片组 leader 提交、复制到全部节点，读回一致。分片组各自独立选主/提交。
func TestShardKV_MultiRaftShardedReplicated(t *testing.T) {
	const nNodes, shardCount = 3, 6
	base := t.TempDir()
	lns, addrs := bindListeners(t, nNodes)

	nodes := make([]*Node, nNodes)
	for i := 0; i < nNodes; i++ {
		nd := NewNode(addrs, i, shardCount, nNodes, filepath.Join(base, "node"+strconv.Itoa(i)))
		nodes[i] = nd
		if err := nd.Serve(rpc.NewServer(), lns[i]); err != nil {
			t.Fatalf("serve: %v", err)
		}
	}
	// 收尾（在 base 删除前）：先关监听停入站 RPC，再停节点，再 drain。
	t.Cleanup(func() {
		for _, ln := range lns {
			ln.Close()
		}
		for _, nd := range nodes {
			nd.Stop()
		}
		time.Sleep(150 * time.Millisecond)
	})

	// 挑选跨 ≥2 个分片的一组 key。
	kv := map[string]string{}
	shardsSeen := map[int]bool{}
	for i := 0; (len(shardsSeen) < 2 || len(kv) < 6) && i < 1000; i++ {
		k := fmt.Sprintf("obj-%d", i)
		kv[k] = fmt.Sprintf("val-%d", i)
		shardsSeen[nodes[0].ShardOf([]byte(k))] = true
	}
	if len(shardsSeen) < 2 {
		t.Fatalf("need keys spanning >=2 shards, got %d", len(shardsSeen))
	}

	// 经 node 0 写入（leader-aware 把每个 key 路由到其分片组 leader，可能不是 node 0）。
	for k, v := range kv {
		if err := nodes[0].Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// 验证复制：所有节点本地副本都收敛到全部键值。
	deadline := time.Now().Add(5 * time.Second)
	converged := false
	for time.Now().Before(deadline) && !converged {
		time.Sleep(50 * time.Millisecond)
		converged = true
		for _, nd := range nodes {
			for k, v := range kv {
				got, ok := nd.LocalGet([]byte(k))
				if !ok || string(got) != v {
					converged = false
					break
				}
			}
			if !converged {
				break
			}
		}
	}
	if !converged {
		t.Fatal("shards did not converge across all nodes")
	}

	// 观测：各分片组的 leader 分布，证明是相互独立的 Raft 组。
	leaders := map[string]bool{}
	for sid := 0; sid < shardCount; sid++ {
		if l, ok := nodes[0].ShardLeader(sid); ok {
			leaders[l] = true
			t.Logf("shard %d leader=%s", sid, l)
		}
	}
	t.Logf("distinct shard leaders across %d shards: %d (independent Raft groups)", shardCount, len(leaders))
}
