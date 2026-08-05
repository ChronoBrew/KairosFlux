package Raft

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestNewRaft(t *testing.T) {
	peers := []string{"localhost:8000", "localhost:8001", "localhost:8002"}
	r := NewRaftWithDataDir(peers, 0, t.TempDir())

	if r == nil {
		t.Fatal("NewRaft returned nil")
	}

	if r.me != 0 {
		t.Errorf("Expected me to be 0, got %d", r.me)
	}

	if r.state != Follower {
		t.Errorf("Expected initial state to be Follower, got %v", r.state)
	}

	if r.Term != 0 {
		t.Errorf("Expected initial term to be 0, got %d", r.Term)
	}

	if len(r.peers) != 3 {
		t.Errorf("Expected 3 peers, got %d", len(r.peers))
	}
}

func TestGetState(t *testing.T) {
	peers := []string{"localhost:8000"}
	r := NewRaftWithDataDir(peers, 0, t.TempDir())

	state, term := r.GetState()
	if state != Follower {
		t.Errorf("Expected state to be Follower, got %v", state)
	}
	if term != 0 {
		t.Errorf("Expected term to be 0, got %d", term)
	}
}

func TestGetLog(t *testing.T) {
	peers := []string{"localhost:8000"}
	r := NewRaftWithDataDir(peers, 0, t.TempDir())

	log := r.GetLog()
	if len(log) != 0 {
		t.Errorf("Expected empty log, got %d entries", len(log))
	}
}

func TestAppendEntry(t *testing.T) {
	peers := []string{"localhost:8000"}
	r := NewRaftWithDataDir(peers, 0, t.TempDir())

	index, _ := r.AppendEntry([]byte("test command"))
	if index != -1 {
		t.Errorf("Expected -1 for non-leader, got %d", index)
	}
}

func TestElectionTimeout(t *testing.T) {
	peers := []string{"localhost:8000"}
	r := NewRaftWithDataDir(peers, 0, t.TempDir())

	time.Sleep(400 * time.Millisecond)

	state, term := r.GetState()
	if state == Follower && term == 0 {
		t.Error("Expected election to start after timeout")
	}
}

func TestLeaderAppendsLog(t *testing.T) {
	peers := []string{"localhost:8000"}
	r := NewRaftWithDataDir(peers, 0, t.TempDir())

	time.Sleep(400 * time.Millisecond)

	state, _ := r.GetState()
	if state != Leader {
		t.Skip("Not leader, skipping log append test")
	}

	index, _ := r.AppendEntry([]byte("test command"))
	if index != 0 {
		t.Errorf("Expected index 0, got %d", index)
	}

	log := r.GetLog()
	if len(log) != 1 {
		t.Errorf("Expected 1 log entry, got %d", len(log))
	}

	if string(log[0].Command) != "test command" {
		t.Errorf("Expected command 'test command', got '%s'", string(log[0].Command))
	}
}

func TestLeaderSendsHeartbeats(t *testing.T) {
	peers := []string{"localhost:8000"}
	r := NewRaftWithDataDir(peers, 0, t.TempDir())

	time.Sleep(400 * time.Millisecond)

	state, _ := r.GetState()
	if state != Leader {
		t.Skip("Not leader, skipping heartbeat test")
	}

	time.Sleep(100 * time.Millisecond)

	state, _ = r.GetState()
	if state != Leader {
		t.Error("Expected state to remain Leader after heartbeats")
	}
}

// TestPersistenceTermAndVotedFor 测试 Term 和 votedFor 的持久化
func TestPersistenceTermAndVotedFor(t *testing.T) {
	peers := []string{"localhost:8000"}
	datadir := "raft_test_data_term"
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)

	r := NewRaftWithDataDir(peers, 0, datadir)

	// 模拟选举：增加 Term
	r.mu.Lock()
	r.Term = 5
	r.votedFor = 0
	r.persistLocked()
	r.mu.Unlock()

	// 创建新的 Raft 实例，应该从磁盘加载状态
	r2 := NewRaftWithDataDir(peers, 0, datadir)

	if r2.Term != 5 {
		t.Errorf("Expected Term to be 5 after reload, got %d", r2.Term)
	}

	if r2.votedFor != 0 {
		t.Errorf("Expected votedFor to be 0 after reload, got %d", r2.votedFor)
	}

	fmt.Println("✓ Term and votedFor persistence test passed")
}

// TestPersistenceLog 测试日志持久化
func TestPersistenceLog(t *testing.T) {
	peers := []string{"localhost:8000"}
	datadir := "raft_test_data_log"
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)

	r := NewRaftWithDataDir(peers, 0, datadir)

	// 添加一些日志条目
	r.mu.Lock()
	r.log = append(r.log, LogEntry{Index: 0, Term: 1, Command: []byte("cmd1")})
	r.log = append(r.log, LogEntry{Index: 1, Term: 1, Command: []byte("cmd2")})
	r.log = append(r.log, LogEntry{Index: 2, Term: 2, Command: []byte("cmd3")})
	r.persistLocked()
	r.mu.Unlock()

	// 创建新的 Raft 实例，应该从磁盘加载日志
	r2 := NewRaftWithDataDir(peers, 0, datadir)

	if len(r2.log) != 3 {
		t.Errorf("Expected 3 log entries after reload, got %d", len(r2.log))
	}

	if string(r2.log[0].Command) != "cmd1" {
		t.Errorf("Expected first command to be 'cmd1', got '%s'", string(r2.log[0].Command))
	}

	if string(r2.log[2].Command) != "cmd3" {
		t.Errorf("Expected third command to be 'cmd3', got '%s'", string(r2.log[2].Command))
	}

	fmt.Println("✓ Log persistence test passed")
}

// TestSnapshotCreation 测试快照创建
func TestSnapshotCreation(t *testing.T) {
	peers := []string{"localhost:8000"}
	datadir := "raft_test_data_snap_create"
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)

	r := NewRaftWithDataDir(peers, 0, datadir)

	// 添加日志并设置 commitIndex
	r.mu.Lock()
	r.log = append(r.log, LogEntry{Index: 0, Term: 1, Command: []byte("cmd1")})
	r.log = append(r.log, LogEntry{Index: 1, Term: 1, Command: []byte("cmd2")})
	r.commitIndex = 1
	r.mu.Unlock()

	// 创建快照
	err := r.TakeSnapshot(1)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// 验证快照元数据
	if r.LastIncludedIndex != 1 {
		t.Errorf("Expected LastIncludedIndex to be 1, got %d", r.LastIncludedIndex)
	}

	if r.lastSnapshotIndex != 1 {
		t.Errorf("Expected lastSnapshotIndex to be 1, got %d", r.lastSnapshotIndex)
	}

	fmt.Println("✓ Snapshot creation test passed")
}

// TestSnapshotPersistence 测试快照持久化和恢复
func TestSnapshotPersistence(t *testing.T) {
	peers := []string{"localhost:8000"}
	datadir := "raft_test_data_snap_persist"
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)

	r := NewRaftWithDataDir(peers, 0, datadir)

	// 添加日志并设置 commitIndex
	r.mu.Lock()
	r.log = append(r.log, LogEntry{Index: 0, Term: 1, Command: []byte("cmd1")})
	r.log = append(r.log, LogEntry{Index: 1, Term: 1, Command: []byte("cmd2")})
	r.log = append(r.log, LogEntry{Index: 2, Term: 2, Command: []byte("cmd3")})
	r.commitIndex = 2
	r.mu.Unlock()

	// 创建快照（包含索引 0 和 1）
	err := r.TakeSnapshot(1)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// 验证内存日志被截断
	r.mu.Lock()
	logLen := len(r.log)
	r.mu.Unlock()

	if logLen != 1 {
		t.Errorf("Expected 1 log entry after snapshot (index 2), got %d", logLen)
	}

	// 创建新的 Raft 实例，应该从磁盘加载快照
	r2 := NewRaftWithDataDir(peers, 0, datadir)

	if r2.LastIncludedIndex != 1 {
		t.Errorf("Expected LastIncludedIndex to be 1 after reload, got %d", r2.LastIncludedIndex)
	}

	if r2.commitIndex != 1 {
		t.Errorf("Expected commitIndex to be 1 after reload, got %d", r2.commitIndex)
	}

	if r2.lastApplied != 1 {
		t.Errorf("Expected lastApplied to be 1 after reload, got %d", r2.lastApplied)
	}

	fmt.Println("✓ Snapshot persistence and recovery test passed")
}

// TestInstallSnapshotRPC 测试 InstallSnapshot RPC 处理
func TestInstallSnapshotRPC(t *testing.T) {
	peers := []string{"localhost:8000", "localhost:8001"}
	datadir := "raft_test_data_install_snap"
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)

	r := NewRaftWithDataDir(peers, 0, datadir)

	// 添加一些日志
	r.mu.Lock()
	r.log = append(r.log, LogEntry{Index: 0, Term: 1, Command: []byte("old1")})
	r.log = append(r.log, LogEntry{Index: 1, Term: 1, Command: []byte("old2")})
	r.commitIndex = 1
	r.mu.Unlock()

	// 模拟接收 InstallSnapshot RPC
	args := &InstallSnapshotArgs{
		Term:             2,
		LeaderID:         1,
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:             []byte("new snapshot"),
	}

	rpc := &RaftRPC{raft: r}
	reply := &InstallSnapshotReply{}
	err := rpc.InstallSnapshot(args, reply)

	if err != nil {
		t.Fatalf("InstallSnapshot failed: %v", err)
	}

	if !reply.Success {
		t.Error("InstallSnapshot should succeed")
	}

	// 验证状态更新
	if r.Term != 2 {
		t.Errorf("Expected Term to be 2, got %d", r.Term)
	}

	if r.LastIncludedIndex != 1 {
		t.Errorf("Expected LastIncludedIndex to be 1, got %d", r.LastIncludedIndex)
	}

	if r.commitIndex != 1 {
		t.Errorf("Expected commitIndex to be 1, got %d", r.commitIndex)
	}

	fmt.Println("✓ InstallSnapshot RPC test passed")
}

// TestPersistAfterElection 测试选举后的持久化
func TestPersistAfterElection(t *testing.T) {
	peers := []string{"localhost:8000"}
	datadir := "raft_test_data_elect_persist"
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)

	r := NewRaftWithDataDir(peers, 0, datadir)

	// 等待成为 Leader
	time.Sleep(400 * time.Millisecond)

	state, term := r.GetState()
	if state != Leader {
		t.Skip("Not leader, skipping election persistence test")
	}

	originalTerm := term

	// 重新加载，验证 Term 已持久化
	r2 := NewRaftWithDataDir(peers, 0, datadir)
	_, newTerm := r2.GetState()

	if newTerm < originalTerm {
		t.Errorf("Expected Term >= %d after reload, got %d", originalTerm, newTerm)
	}

	fmt.Printf("✓ Election persistence test passed (Term: %d -> %d)\n", originalTerm, newTerm)
}

// TestPersistAfterAppendEntry 测试 AppendEntry 后的持久化
func TestPersistAfterAppendEntry(t *testing.T) {
	peers := []string{"localhost:8000"}
	datadir := "raft_test_data_append_persist"
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)

	r := NewRaftWithDataDir(peers, 0, datadir)

	// 等待成为 Leader
	time.Sleep(400 * time.Millisecond)

	state, _ := r.GetState()
	if state != Leader {
		t.Skip("Not leader, skipping append entry persistence test")
	}

	// 添加条目
	index, _ := r.AppendEntry([]byte("persistent command"))
	if index == -1 {
		t.Fatal("AppendEntry failed")
	}

	// 重新加载，验证日志已持久化
	r2 := NewRaftWithDataDir(peers, 0, datadir)
	log := r2.GetLog()

	if len(log) == 0 {
		t.Error("Expected log to persist after reload")
	} else if string(log[len(log)-1].Command) != "persistent command" {
		t.Errorf("Expected last command to be 'persistent command', got '%s'", string(log[len(log)-1].Command))
	}

	fmt.Println("✓ AppendEntry persistence test passed")
}
