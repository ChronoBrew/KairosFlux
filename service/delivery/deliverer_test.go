package delivery

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// sliceSource 是一个内存投递源，按 cursor 之后的顺序返回记录，用于隔离测 deliverer。
type sliceSource struct {
	recs []Record
}

func (s *sliceSource) Fetch(cursor []byte, limit int) ([]Record, []byte, error) {
	// cursor 用「已消费条数」编码为单字节即可满足测试（记录数很小）。
	start := 0
	if len(cursor) == 1 {
		start = int(cursor[0])
	}
	end := start + limit
	if end > len(s.recs) {
		end = len(s.recs)
	}
	if start >= len(s.recs) {
		return nil, cursor, nil
	}
	return s.recs[start:end], []byte{byte(end)}, nil
}

func TestFileSink_SendWritesJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	sink, err := NewFileSink("file", path)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	batch := []Record{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	if err := sink.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	lines := countLines(t, path)
	if lines != 2 {
		t.Fatalf("expected 2 JSONL lines, got %d", lines)
	}
}

func TestDeliverer_DeliversAllInBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	sink, err := NewFileSink("file", path)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	src := &sliceSource{recs: []Record{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}}
	d := NewDeliverer(src, sink, 2, time.Millisecond)

	// 手动驱动直到投递干净，避免依赖 ticker 时序。
	for i := 0; i < 5; i++ {
		if err := d.deliverOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	if got := countLines(t, path); got != 3 {
		t.Fatalf("expected 3 records delivered, got %d", got)
	}
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		n++
	}
	return n
}
