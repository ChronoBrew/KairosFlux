package raft

import (
	"net"
	"net/rpc"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// startCluster 在进程内起 n 个 Raft 节点（同一组 groupID）。用临时端口（:0）预绑监听器再
// 据其真实地址构建集群，避免固定端口在连续测试运行间因 TIME_WAIT 冲突而 flaky。
// 返回各节点 *Raft 与其地址（下标对应）。
//
// 清理顺序很关键：所有节点的 WAL 数据放在同一个 base 临时目录（最先注册 → 最后清理）；
// 单独注册的收尾（先关监听器停止入站 RPC、再 Stop 各节点、再 drain）在 base 删除前运行，
// 避免「节点仍在写 WAL 而临时目录已被 RemoveAll」导致的 directory not empty flaky。
func startCluster(t *testing.T, n, groupID int) ([]*Raft, []string) {
	t.Helper()
	base := t.TempDir() // 最先注册，LIFO 下最后清理

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

	rafts := make([]*Raft, n)
	for i := 0; i < n; i++ {
		r := NewRaftGroup(groupID, addrs, i, filepath.Join(base, "node"+strconv.Itoa(i)))
		rafts[i] = r

		gs := NewRaftGroupServer()
		gs.AddGroup(groupID, r)
		server := rpc.NewServer()
		if err := gs.RegisterRPC(server); err != nil {
			t.Fatalf("register: %v", err)
		}
		go server.Accept(lns[i])
	}

	// 收尾（在 base 删除前运行）：先关监听器停止入站 RPC，再停各节点，再 drain 在途处理。
	t.Cleanup(func() {
		for _, ln := range lns {
			ln.Close()
		}
		for _, r := range rafts {
			r.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	})

	return rafts, addrs
}

// waitLeader 轮询直到出现唯一 Leader，返回它；超时则 fail。
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

// allHaveLast 报告是否所有节点最后一条日志的命令都等于 want。
func allHaveLast(rafts []*Raft, want string) bool {
	for _, r := range rafts {
		log := r.GetLog()
		if len(log) == 0 || string(log[len(log)-1].Command) != want {
			return false
		}
	}
	return true
}

// waitConverge 轮询直到所有节点最后一条日志命令为 want。
func waitConverge(rafts []*Raft, want string, timeout time.Duration) bool {
	steps := int(timeout / (50 * time.Millisecond))
	for i := 0; i < steps; i++ {
		time.Sleep(50 * time.Millisecond)
		if allHaveLast(rafts, want) {
			return true
		}
	}
	return false
}

// TestRaftCluster_ElectReplicateCommit 端到端验证多节点：选出唯一 Leader → 在 Leader 上
// propose 一条命令 → 复制到 follower → 各节点日志收敛包含该命令。
func TestRaftCluster_ElectReplicateCommit(t *testing.T) {
	rafts, addrs := startCluster(t, 3, 0)
	waitLeader(t, rafts, 5*time.Second)

	// 经 leader-aware propose 提交（等待 commit，且换届时自动重试），比直接 AppendEntry
	// 到某个可能随即下台的 leader 更可靠。
	if err := ProposeToGroup(addrs, 0, []byte("hello-cluster"), time.Second, 5*time.Second); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	if !waitConverge(rafts, "hello-cluster", 5*time.Second) {
		for i, r := range rafts {
			t.Logf("node %d log len=%d", i, len(r.GetLog()))
		}
		t.Fatal("command did not replicate to all nodes")
	}
}

// TestRaftCluster_LeaderAwarePropose 验证 leader-aware 路由/重定向：只把请求发给一个
// follower，ProposeToGroup 应据 LeaderHint 重定向到 leader、提交成功，并复制到全部节点。
func TestRaftCluster_LeaderAwarePropose(t *testing.T) {
	rafts, addrs := startCluster(t, 3, 0)
	leader := waitLeader(t, rafts, 5*time.Second)
	leaderAddr, _ := leader.LeaderHint()

	// 等心跳传播，让 follower 学到 leader（使重定向走 LeaderHint 而非盲试）。
	time.Sleep(300 * time.Millisecond)

	follower := ""
	for _, a := range addrs {
		if a != leaderAddr {
			follower = a
			break
		}
	}
	if follower == "" {
		t.Fatal("no follower address found")
	}

	// 只从 follower 提交：必须经重定向命中 leader 才能成功。
	if err := ProposeToGroup([]string{follower}, 0, []byte("via-follower"), time.Second, 5*time.Second); err != nil {
		t.Fatalf("leader-aware propose failed: %v", err)
	}

	if !waitConverge(rafts, "via-follower", 3*time.Second) {
		t.Fatal("redirected command did not replicate to all nodes")
	}
}
