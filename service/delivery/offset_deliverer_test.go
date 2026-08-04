package delivery

import (
	"context"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/service/delivery/offset"
)

// recordingCommitter 是内存 map 实现的假 Committer，模拟持久化 offset 存储，
// 以便「重启」（造新 Deliverer 复用同一 store）后仍读到已提交游标。
type recordingCommitter struct {
	m map[string][]byte
}

func (c *recordingCommitter) Put(key, value []byte) error {
	c.m[string(key)] = append([]byte(nil), value...)
	return nil
}

func (c *recordingCommitter) Get(key []byte) ([]byte, error) {
	return c.m[string(key)], nil
}

// countingSink 记录收到的每条 Record.Value，用于断言不重投已提交批。
type countingSink struct {
	got []string
}

func (s *countingSink) Name() string { return "counting" }

func (s *countingSink) Send(_ context.Context, batch []Record) error {
	for _, r := range batch {
		s.got = append(s.got, string(r.Value))
	}
	return nil
}

func (s *countingSink) Health() SinkHealth { return SinkHealth{Healthy: true} }

// TestDeliverer_ResumesFromCommittedOffset 验证崩溃续传：第一个 Deliverer 投递若干批并
// Commit 游标；用同一 offsetStore 造新 Deliverer 模拟「重启」，应从已提交游标续投，
// 且不重投已提交批。
func TestDeliverer_ResumesFromCommittedOffset(t *testing.T) {
	recs := []Record{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("d"), Value: []byte("4")},
	}
	store := offset.NewKVOffsetStore(&recordingCommitter{m: map[string][]byte{}})

	// 第一段：投递前两条（两批，batchSize=1），游标提交到 offset store。
	sink1 := &countingSink{}
	src1 := &sliceSource{recs: recs}
	d1 := NewDelivererWithOffset(src1, sink1, "counting", store, 1, time.Millisecond)
	// Run 会 Load offset（此时为 nil，从头开始）。
	if cursor, err := store.Load("counting"); err != nil {
		t.Fatal(err)
	} else {
		_ = cursor
	}
	// 手动驱动两次 deliverOnce（等价 Run 里两轮 tick），但需先注入 Load 后的游标。
	d1.cursor, _ = store.Load("counting")
	for i := 0; i < 2; i++ {
		if err := d1.deliverOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink1.got) != 2 {
		t.Fatalf("first deliverer sent %d recs, want 2: %v", len(sink1.got), sink1.got)
	}

	// 第二段：模拟重启——复用同一 store 造新 Deliverer，从已提交游标续投剩余两条。
	sink2 := &countingSink{}
	src2 := &sliceSource{recs: recs}
	d2 := NewDelivererWithOffset(src2, sink2, "counting", store, 1, time.Millisecond)
	d2.cursor, _ = store.Load("counting") // 等价 Run 启动时的 Load
	for i := 0; i < 4; i++ {
		if err := d2.deliverOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	// 续投应只发剩余的 "3","4"，不重投已提交的 "1","2"。
	if len(sink2.got) != 2 || sink2.got[0] != "3" || sink2.got[1] != "4" {
		t.Fatalf("resumed deliverer sent %v, want [3 4] (no re-delivery of committed batches)", sink2.got)
	}
}
