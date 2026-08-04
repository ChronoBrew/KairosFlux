package delivery

import (
	"bytes"
	"testing"

	"github.com/NeverENG/BanDB/pkg/predicate"
	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/service/delivery/offset"
)

// fakeScanner 返回预置的有序条目，忽略范围（测试只关心保留 key 过滤与游标推进）。
type fakeScanner struct{ entries []proto.ScanEntry }

func (f *fakeScanner) Scan(start, end []byte, _ predicate.Predicate) []proto.ScanEntry {
	out := make([]proto.ScanEntry, 0, len(f.entries))
	for _, e := range f.entries {
		if start != nil && bytes.Compare(e.Key, start) < 0 {
			continue
		}
		out = append(out, e)
	}
	return out
}

func TestKVSource_SkipsReservedOffsetKeys(t *testing.T) {
	sc := &fakeScanner{entries: []proto.ScanEntry{
		{Key: []byte(offset.ReservedPrefix + "file"), Value: []byte("cursor")},
		{Key: []byte("order:1"), Value: []byte("v1")},
		{Key: []byte("order:2"), Value: []byte("v2")},
	}}
	src := NewKVSource(sc, nil)

	batch, next, err := src.Fetch(nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 business records (reserved key skipped), got %d", len(batch))
	}
	for _, r := range batch {
		if bytes.HasPrefix(r.Key, []byte(offset.ReservedPrefix)) {
			t.Fatalf("reserved key leaked into batch: %q", r.Key)
		}
	}
	// 游标推进到最后一条扫描 key 之后。
	if !bytes.Equal(next, append([]byte("order:2"), 0x00)) {
		t.Fatalf("unexpected next cursor: %q", next)
	}
}
