package raft

import (
	"net/rpc"
	"testing"
	"time"
)

// TestRaftGroupServer_DispatchesByGroupID 验证 Multi-Raft 传输多路复用：一个节点在同一个
// RPC 端点上托管两个 Raft 组，发往 GroupID=1 的 AppendEntries 只落进组 1、不落进组 0。
func TestRaftGroupServer_DispatchesByGroupID(t *testing.T) {
	host := "localhost:8010"
	peers := []string{"localhost:8009", host} // 索引 1 为托管节点，索引 0 为发送方

	// 托管节点（me=1）在一个端点上跑两个组。
	g0 := NewRaftGroup(0, peers, 1, t.TempDir())
	defer g0.Stop()
	g1 := NewRaftGroup(1, peers, 1, t.TempDir())
	defer g1.Stop()

	gs := NewRaftGroupServer()
	gs.AddGroup(0, g0)
	gs.AddGroup(1, g1)

	server := rpc.NewServer()
	if err := gs.RegisterRPC(server); err != nil {
		t.Fatalf("register: %v", err)
	}
	go server.Accept(mustListen(t, host))
	time.Sleep(100 * time.Millisecond)

	// 组 1 的发送方（me=0）；其 SendAppendEntries 会打上 GroupID=1。
	sender1 := NewRaftGroup(1, peers, 0, t.TempDir())
	defer sender1.Stop()

	args := &AppendEntriesArgs{
		GroupID:      1, // 显式发往组 1，验证服务端按 GroupID 分发
		Term:         5, // 取较高 term，避免与托管组自身选举 term 竞争
		LeaderID:     0,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Index: 0, Term: 5, Command: []byte("g1-entry")}},
		LeaderCommit: -1,
	}
	reply, err := sender1.SendAppendEntries(host, args)
	if err != nil {
		t.Fatalf("send append: %v", err)
	}
	if !reply.Success {
		t.Fatal("append to group 1 should succeed")
	}

	// 条目只应落进组 1。
	if got := len(g1.GetLog()); got != 1 {
		t.Fatalf("group 1 should have 1 entry, got %d", got)
	}
	if string(g1.GetLog()[0].Command) != "g1-entry" {
		t.Fatalf("group 1 wrong command: %q", g1.GetLog()[0].Command)
	}
	if got := len(g0.GetLog()); got != 0 {
		t.Fatalf("group 0 must stay empty (dispatch isolation), got %d", got)
	}
}
