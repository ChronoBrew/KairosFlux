package shardkv

import (
	"fmt"
	"net"
	"net/rpc"
	"path/filepath"
	"strconv"
	"sync"
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

// TestShardKV_RealSharding_ForwardedReads 端到端验证「真分片 + P2C 转发读」：
// 3 节点、rf=2 → 每个分片只落在 2 个节点上（数据跨节点分区）。对每个 key 特意选一个「不托管
// 该分片」的节点做写与读——写经 ProposeToGroup 路由到副本组 leader，读经 P2C 在副本集间择优
// 转发（真跨节点读，调用方非副本）。最后验证 P2C 把同一分片的转发读分布到了两个副本上。
func TestShardKV_RealSharding_ForwardedReads(t *testing.T) {
	const nNodes, shardCount, rf = 3, 6, 2
	base := t.TempDir()
	lns, addrs := bindListeners(t, nNodes)

	nodes := make([]*Node, nNodes)
	for i := 0; i < nNodes; i++ {
		nd := NewNode(addrs, i, shardCount, rf, filepath.Join(base, "node"+strconv.Itoa(i)))
		nodes[i] = nd
		if err := nd.Serve(rpc.NewServer(), lns[i]); err != nil {
			t.Fatalf("serve: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, ln := range lns {
			ln.Close()
		}
		for _, nd := range nodes {
			nd.Stop()
		}
		time.Sleep(150 * time.Millisecond)
	})

	// 放置断言：每个分片恰好 rf 个副本，且存在分片未被某节点托管（真分区，非全副本）。
	partitioned := false
	for sid := 0; sid < shardCount; sid++ {
		if len(nodes[0].replicas[sid]) != rf {
			t.Fatalf("shard %d: 副本数 %d，期望 %d", sid, len(nodes[0].replicas[sid]), rf)
		}
		hosts := 0
		for _, nd := range nodes {
			if _, ok := nd.shards[sid]; ok {
				hosts++
			}
		}
		if hosts != rf {
			t.Fatalf("shard %d 被 %d 个节点托管，期望 %d", sid, hosts, rf)
		}
		if hosts < nNodes {
			partitioned = true
		}
	}
	if !partitioned {
		t.Fatal("期望真分区（某分片未被某节点托管），却是全副本")
	}

	// nonReplica 返回一个不托管 sid 的节点（转发读的调用方）。
	nonReplica := func(sid int) *Node {
		for _, nd := range nodes {
			if _, ok := nd.shards[sid]; !ok {
				return nd
			}
		}
		return nil
	}

	// 对每个 key，经「非副本节点」写 + 转发读，验证正确性。
	for i := 0; i < 40; i++ {
		k := []byte(fmt.Sprintf("obj-%d", i))
		v := []byte(fmt.Sprintf("val-%d", i))
		fwd := nonReplica(nodes[0].ShardOf(k))
		if fwd == nil {
			t.Fatalf("key %s 的分片无非副本节点（rf 应 < nNodes）", k)
		}
		if err := fwd.Put(k, v); err != nil {
			t.Fatalf("经非副本节点 Put %s: %v", k, err)
		}
		deadline := time.Now().Add(5 * time.Second)
		var got []byte
		var ok bool
		var err error
		for time.Now().Before(deadline) {
			got, ok, err = fwd.Get(k)
			if err == nil && ok && string(got) == string(v) {
				break
			}
			time.Sleep(30 * time.Millisecond)
		}
		if err != nil || !ok || string(got) != string(v) {
			t.Fatalf("转发读 key %s: got=%q ok=%v err=%v", k, got, ok, err)
		}
	}

	// P2C 分布：从非副本节点【并发】连发转发读。串行读时 P2C 会正确收敛到最快副本（这是设计
	// 使然，不是分布），唯有并发负载下 inflight 项才让繁忙副本变得不划算、把读摊到其余副本——
	// 这正是网关同时转发大量读的真实场景。验证：读只落在该分片副本集内（路由正确、绝不落到非
	// 副本调用方），且并发下两个副本都分到了读（inflight 感知的负载均衡，无需注入延迟）。
	probe := []byte("obj-0")
	sid := nodes[0].ShardOf(probe)
	fwd := nonReplica(sid)
	reps := fwd.replicas[sid]
	replicaSet := map[string]bool{}
	for _, r := range reps {
		replicaSet[r] = true
	}
	before := map[string]int64{}
	for _, nd := range nodes {
		before[nd.self] = nd.served.Load()
	}
	const goroutines, perG = 8, 40
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, _, err := fwd.Get(probe); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatalf("并发转发读: %v", err)
	}
	servedBy := 0
	for _, nd := range nodes {
		delta := nd.served.Load() - before[nd.self]
		if delta > 0 {
			if !replicaSet[nd.self] {
				t.Fatalf("转发读落到非副本节点 %s（路由错误）", nd.self)
			}
			t.Logf("副本 %s 服务了 %d 次并发转发读", nd.self, delta)
			servedBy++
		}
	}
	if servedBy < len(reps) {
		t.Fatalf("并发下 P2C 只用了 %d 个副本，期望摊到全部 %d 个", servedBy, len(reps))
	}
}
