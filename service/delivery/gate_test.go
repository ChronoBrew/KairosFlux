package delivery

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestDeliverer_GateBlocksDelivery 验证 gate 返回 false 时不投递、游标不推进（模拟 raft
// Follower），gate 转 true 后恢复投递——这是 raft 模式「只在 Leader 投递」的核心保证。
func TestDeliverer_GateBlocksDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	sink, err := NewFileSink("file", path)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	src := &sliceSource{recs: []Record{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}}
	d := NewDeliverer(src, sink, 10, time.Millisecond)

	allow := false
	d.SetGate(func() bool { return allow })

	// gate=false：多轮都不应投递。
	for i := 0; i < 3; i++ {
		if err := d.deliverOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := countLines(t, path); n != 0 {
		t.Fatalf("gate closed: expected 0 delivered, got %d", n)
	}

	// gate=true：恢复投递。
	allow = true
	if err := d.deliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := countLines(t, path); n != 2 {
		t.Fatalf("gate open: expected 2 delivered, got %d", n)
	}
}
