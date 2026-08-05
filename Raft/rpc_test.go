package Raft

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

// mustListen 监听 addr 并在测试结束时关闭，避免固定端口在 -count=2 下被占用。
func mustListen(t *testing.T, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

func TestRequestVoteRPC(t *testing.T) {
	peers := []string{"localhost:8000", "localhost:8001"}
	r1 := NewRaftWithDataDir(peers, 0, t.TempDir())
	defer r1.Stop()
	r2 := NewRaftWithDataDir(peers, 1, t.TempDir())
	defer r2.Stop()

	server1 := rpc.NewServer()
	NewRaftRPC(r1).RegisterRPC(server1)

	server2 := rpc.NewServer()
	NewRaftRPC(r2).RegisterRPC(server2)

	go server1.Accept(mustListen(t, "localhost:8000"))
	go server2.Accept(mustListen(t, "localhost:8001"))

	time.Sleep(100 * time.Millisecond)

	args := &RequestVoteArgs{
		Term:         5, // higher term to win the vote
		CandidateID:  0,
		LastLogIndex: -1,
		LastLogTerm:  0,
	}

	reply, err := r1.SendRequestVote("localhost:8001", args)
	if err != nil {
		t.Fatalf("SendRequestVote failed: %v", err)
	}

	if !reply.VoteGranted {
		t.Error("Expected vote to be granted")
	}

	if reply.Term != 5 {
		t.Errorf("Expected term 5, got %d", reply.Term)
	}
}

func TestAppendEntriesRPC(t *testing.T) {
	peers := []string{"localhost:8002", "localhost:8003"}
	r1 := NewRaftWithDataDir(peers, 0, t.TempDir())
	defer r1.Stop()
	r2 := NewRaftWithDataDir(peers, 1, t.TempDir())
	defer r2.Stop()

	server1 := rpc.NewServer()
	NewRaftRPC(r1).RegisterRPC(server1)

	server2 := rpc.NewServer()
	NewRaftRPC(r2).RegisterRPC(server2)

	go server1.Accept(mustListen(t, "localhost:8002"))
	go server2.Accept(mustListen(t, "localhost:8003"))

	time.Sleep(100 * time.Millisecond)

	args := &AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 0, Term: 1, Command: []byte("test command")},
		},
		LeaderCommit: -1,
	}

	reply, err := r1.SendAppendEntries("localhost:8003", args)
	if err != nil {
		t.Fatalf("SendAppendEntries failed: %v", err)
	}

	if !reply.Success {
		t.Error("Expected append to be successful")
	}

	if reply.Term != 1 {
		t.Errorf("Expected term 1, got %d", reply.Term)
	}

	log := r2.GetLog()
	if len(log) != 1 {
		t.Errorf("Expected 1 log entry, got %d", len(log))
	}

	if string(log[0].Command) != "test command" {
		t.Errorf("Expected command 'test command', got '%s'", string(log[0].Command))
	}
}
