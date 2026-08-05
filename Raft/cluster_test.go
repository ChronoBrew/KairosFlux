package Raft

import (
	"net/rpc"
	"testing"
	"time"
)

// startCluster 在进程内起 n 个 Raft 节点（同一组 groupID），各自监听独立端口、经 net/rpc
// 互联。返回各节点的 *Raft（调用方 defer Stop）。
func startCluster(t *testing.T, addrs []string, groupID int) []*Raft {
	t.Helper()
	rafts := make([]*Raft, len(addrs))
	for i := range addrs {
		r := NewRaftGroup(groupID, addrs, i, t.TempDir())
		t.Cleanup(r.Stop)
		rafts[i] = r

		gs := NewRaftGroupServer()
		gs.AddGroup(groupID, r)
		server := rpc.NewServer()
		if err := gs.RegisterRPC(server); err != nil {
			t.Fatalf("register: %v", err)
		}
		go server.Accept(mustListen(t, addrs[i]))
	}
	return rafts
}

// waitLeader 轮询直到出现一个 Leader，返回它；超时则 fail。
func waitLeader(t *testing.T, rafts []*Raft, timeout time.Duration) *Raft {
	t.Helper()
	steps := int(timeout / (50 * time.Millisecond))
	for i := 0; i < steps; i++ {
		time.Sleep(50 * time.Millisecond)
		leaders := 0
		var leader *Raft
		for _, r := range rafts {
			if s, _ := r.GetState(); s == Leader {
				leaders++
				leader = r
			}
		}
		if leaders == 1 {
			return leader
		}
	}
	t.Fatal("no single leader elected within timeout")
	return nil
}

// TestRaftCluster_ElectReplicateCommit 端到端验证多节点：选出唯一 Leader → 在 Leader 上
// propose 一条命令 → 复制到 follower → 各节点日志收敛包含该命令。
func TestRaftCluster_ElectReplicateCommit(t *testing.T) {
	addrs := []string{"localhost:8020", "localhost:8021", "localhost:8022"}
	rafts := startCluster(t, addrs, 0)

	leader := waitLeader(t, rafts, 5*time.Second)

	index, err := leader.AppendEntry([]byte("hello-cluster"))
	if err != nil {
		t.Fatalf("propose on leader failed: %v", err)
	}
	if index < 0 {
		t.Fatalf("expected valid log index, got %d", index)
	}

	// 等复制到全部节点。
	deadline := 3 * time.Second
	steps := int(deadline / (50 * time.Millisecond))
	converged := false
	for i := 0; i < steps && !converged; i++ {
		time.Sleep(50 * time.Millisecond)
		converged = true
		for _, r := range rafts {
			log := r.GetLog()
			if len(log) == 0 || string(log[len(log)-1].Command) != "hello-cluster" {
				converged = false
				break
			}
		}
	}
	if !converged {
		for i, r := range rafts {
			t.Logf("node %d log len=%d", i, len(r.GetLog()))
		}
		t.Fatal("command did not replicate to all nodes")
	}
}
