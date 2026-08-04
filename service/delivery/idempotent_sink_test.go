package delivery

import (
	"context"
	"path/filepath"
	"testing"
)

func TestIdempotentFileSink_SkipsAlreadyDelivered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	sink, err := NewIdempotentFileSink("file", path)
	if err != nil {
		t.Fatal(err)
	}

	batch := []Record{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	if err := sink.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	// 重投同一批：应被 HWM 全部跳过，文件行数不变。
	if err := sink.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if n := countLines(t, path); n != 2 {
		t.Fatalf("re-send should be idempotent: expected 2 lines, got %d", n)
	}
}

func TestIdempotentFileSink_RecoversHWMAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")

	// 第一个 sink 实例投递两批后「崩溃」（不 Close 也应已 fsync）。
	s1, err := NewIdempotentFileSink("file", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = s1.Send(context.Background(), []Record{{Key: []byte("a"), Value: []byte("1")}})
	_ = s1.Send(context.Background(), []Record{{Key: []byte("b"), Value: []byte("2")}})

	// 重开：HWM 应从文件恢复到 "b"，重投 [a,b] 全跳过，只有新 key "c" 落地。
	s2, err := NewIdempotentFileSink("file", path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	err = s2.Send(context.Background(), []Record{
		{Key: []byte("a"), Value: []byte("1")}, // 已投递
		{Key: []byte("b"), Value: []byte("2")}, // 已投递
		{Key: []byte("c"), Value: []byte("3")}, // 新
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := countLines(t, path); n != 3 {
		t.Fatalf("expected 3 unique lines after recover (a,b,c), got %d", n)
	}
}
