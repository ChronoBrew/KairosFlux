package offset

import (
	"bytes"
	"testing"
)

// fakeCommitter 是内存 map 实现的假 Committer，用于隔离测 KVOffsetStore：
// key 不存在时 Get 返回 (nil, nil)，与真实 KV 约定一致。
type fakeCommitter struct {
	m map[string][]byte
}

func newFakeCommitter() *fakeCommitter {
	return &fakeCommitter{m: map[string][]byte{}}
}

func (c *fakeCommitter) Put(key, value []byte) error {
	// 拷贝一份，避免调用方后续复用底层数组污染已存值。
	c.m[string(key)] = append([]byte(nil), value...)
	return nil
}

func (c *fakeCommitter) Get(key []byte) ([]byte, error) {
	v, ok := c.m[string(key)]
	if !ok {
		return nil, nil // key 不存在
	}
	return v, nil
}

// TestLoadBeforeCommitReturnsNil 验证未提交时 Load 返回 (nil, nil)，表示从头开始。
func TestLoadBeforeCommitReturnsNil(t *testing.T) {
	store := NewKVOffsetStore(newFakeCommitter())
	got, err := store.Load("ch")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil cursor before commit, got %q", got)
	}
}

// TestCommitThenLoad 验证 Commit 后 Load 读回同一游标。
func TestCommitThenLoad(t *testing.T) {
	store := NewKVOffsetStore(newFakeCommitter())
	cursor := []byte("k9\x00")
	if err := store.Commit("ch", cursor); err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	got, err := store.Load("ch")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !bytes.Equal(got, cursor) {
		t.Fatalf("Load = %q, want %q", got, cursor)
	}
}

// TestCommitIsPerSink 验证不同 sink 的游标互不干扰（key 由 sink 名派生并隔离）。
func TestCommitIsPerSink(t *testing.T) {
	store := NewKVOffsetStore(newFakeCommitter())
	if err := store.Commit("ch", []byte("c1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit("doris", []byte("d1")); err != nil {
		t.Fatal(err)
	}
	ch, _ := store.Load("ch")
	doris, _ := store.Load("doris")
	if !bytes.Equal(ch, []byte("c1")) || !bytes.Equal(doris, []byte("d1")) {
		t.Fatalf("per-sink cursors crossed: ch=%q doris=%q", ch, doris)
	}
}
