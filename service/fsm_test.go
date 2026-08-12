package service

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/raft"
)

func setupTest(t *testing.T) (*KVServer, func()) {
	oldWALPath := config.G.WALPath
	oldMaxSize := config.G.MaxMemTableSize
	oldPeers := config.G.Peers
	oldMe := config.G.Me
	oldMode := config.G.Mode

	// 每个测试用唯一的文件名，避免测试间干扰
	testWALPath := "test_service_wal_" + time.Now().Format("20060102150405.000000") + ".log"

	// 这些测试覆盖 Raft FSM 路径（Apply/AppendEntry/WaitUntilReady），固定 raft 模式
	config.G.Mode = config.ModeRaft
	config.G.WALPath = testWALPath
	config.G.MaxMemTableSize = 100
	config.G.Peers = []string{"localhost:9000"}
	config.G.Me = 0

	// 确保文件不存在（如果之前有残留）
	os.Remove(testWALPath)

	fsm := NewKVServer()

	cleanup := func() {
		// 清理文件
		os.Remove(testWALPath)
		config.G.WALPath = oldWALPath
		config.G.MaxMemTableSize = oldMaxSize
		config.G.Peers = oldPeers
		config.G.Me = oldMe
		config.G.Mode = oldMode
	}

	return fsm, cleanup
}

// TestFSM_EmptyValueNotTombstone 验证空值经 EncodeCommand→Apply 全链路后仍是
// 一个真实的空值(found)，而非被误当作墓碑(Value==nil 删除)。这是墓碑约定
// "真实值永不为 nil" 的边界：若 []byte{} 在 JSON 往返中退化为 nil，空值会被
// 静默删除——本测试守住该不变量。
func TestFSM_EmptyValueNotTombstone(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	cmdBytes, err := EncodeCommand(Command{Type: "Put", Key: []byte("ek"), Value: []byte{}})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	fsm.Apply(raft.LogEntry{Index: 0, Term: 1, Command: cmdBytes})

	val, err := fsm.Get([]byte("ek"))
	if err != nil {
		t.Fatalf("empty value read back as missing (treated as tombstone): %v", err)
	}
	if len(val) != 0 {
		t.Errorf("expected empty value, got %q", string(val))
	}
}

func TestFSM_BasicOperation(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	cmd := Command{
		Type:  "Put",
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}

	cmdBytes, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}

	entry := raft.LogEntry{
		Index:   0,
		Term:    1,
		Command: cmdBytes,
	}

	fsm.Apply(entry)

	val, err := fsm.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("fsm.Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(val))
	}
}

func TestFSM_DeleteOperation(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	putCmd := Command{
		Type:  "Put",
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	putBytes, _ := EncodeCommand(putCmd)
	fsm.Apply(raft.LogEntry{Index: 0, Term: 1, Command: putBytes})

	val, _ := fsm.Get([]byte("key1"))
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(val))
	}

	delCmd := Command{
		Type: "Delete",
		Key:  []byte("key1"),
	}
	delBytes, _ := EncodeCommand(delCmd)
	fsm.Apply(raft.LogEntry{Index: 1, Term: 1, Command: delBytes})

	val, err := fsm.Get([]byte("key1"))
	if err == nil && val != nil {
		t.Errorf("Expected nil after delete, got '%s'", string(val))
	}
}

func TestFSM_UpdateOperation(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	cmd1 := Command{Type: "Put", Key: []byte("key1"), Value: []byte("value1")}
	cmdBytes1, _ := EncodeCommand(cmd1)
	fsm.Apply(raft.LogEntry{Index: 0, Term: 1, Command: cmdBytes1})

	cmd2 := Command{Type: "Put", Key: []byte("key1"), Value: []byte("value2")}
	cmdBytes2, _ := EncodeCommand(cmd2)
	fsm.Apply(raft.LogEntry{Index: 1, Term: 1, Command: cmdBytes2})

	val, _ := fsm.Get([]byte("key1"))
	if string(val) != "value2" {
		t.Errorf("Expected 'value2', got '%s'", string(val))
	}
}

// TestStandalone_WriteAndRecover 验证 standalone 模式：写经 WAL+存储且不启动 Raft，
// 重启后新实例能从同一 WAL 重放恢复（含删除墓碑）。
func TestStandalone_WriteAndRecover(t *testing.T) {
	oldWALPath := config.G.WALPath
	oldSSTablePath := config.G.SSTablePath
	oldMode := config.G.Mode
	oldMaxSize := config.G.MaxMemTableSize

	dir := t.TempDir()
	config.G.Mode = config.ModeStandalone
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.MaxMemTableSize = 1 << 20 // 足够大，避免本测试触发刷盘
	defer func() {
		config.G.WALPath = oldWALPath
		config.G.SSTablePath = oldSSTablePath
		config.G.Mode = oldMode
		config.G.MaxMemTableSize = oldMaxSize
	}()

	kv := NewKVServer()
	if kv.raft != nil {
		t.Fatal("standalone mode must not start Raft")
	}
	for _, c := range []Command{
		{Type: "Put", Key: []byte("k1"), Value: []byte("v1")},
		{Type: "Put", Key: []byte("k2"), Value: []byte("v2")},
		{Type: "Delete", Key: []byte("k1")},
	} {
		if err := kv.Write(c); err != nil {
			t.Fatalf("write %+v: %v", c, err)
		}
	}
	_ = kv.wal.Close()

	// 模拟重启：新实例从同一 WAL 恢复
	kv2 := NewKVServer()
	defer kv2.wal.Close()

	if v, err := kv2.Get([]byte("k2")); err != nil || string(v) != "v2" {
		t.Fatalf("recover k2: v=%q err=%v", v, err)
	}
	if _, err := kv2.Get([]byte("k1")); err == nil {
		t.Fatal("k1 should remain deleted after recovery")
	}
}

// TestWaitUntilReady_SingleNodeAcceptsImmediateWrite 验证 #86 的修复：
// 新建的单节点在选主前不是 Leader（写会被拒），WaitUntilReady 返回后必为 Leader，
// 此时立即写入应当成功。
func TestWaitUntilReady_SingleNodeAcceptsImmediateWrite(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	go fsm.Run()

	// 选主超时下界 150ms，故刚创建时必为非 Leader —— 这正是 #86 的失败窗口。
	if state, _ := fsm.Raft().State(); state == raft.Leader {
		t.Fatalf("expected non-Leader before election window elapses, got Leader")
	}

	fsm.WaitUntilReady()

	if state, _ := fsm.Raft().State(); state != raft.Leader {
		t.Fatalf("after WaitUntilReady expected Leader, got %v", state)
	}

	idx, err := fsm.AppendEntry(Command{Type: "Put", Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("write immediately after ready failed: %v", err)
	}
	if err := fsm.WaitForCommit(idx); err != nil {
		t.Fatalf("WaitForCommit failed: %v", err)
	}
}
