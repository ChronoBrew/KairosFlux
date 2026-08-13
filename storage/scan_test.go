package storage

import (
	"reflect"
	"testing"
)

// collect 把 [start,end] 扫描结果收成 key:value 串对，便于断言顺序与内容。
func collect(m *Engine, start, end []byte) [][2]string {
	var out [][2]string
	m.ScanRange(start, end, func(k, v []byte) bool {
		out = append(out, [2]string{string(k), string(v)})
		return true
	})
	return out
}

func newMemWith(active, dirty *SkipList) *Engine {
	return &Engine{active: active, dirty: dirty}
}

func sl(pairs ...[2]string) *SkipList {
	s := newSkipList(32, 0.5)
	for _, p := range pairs {
		var val []byte
		if p[1] != "" {
			val = []byte(p[1])
		} // 空串表示墓碑(nil value)
		s.insert([]byte(p[0]), val)
	}
	return s
}

func TestScanRange_Bounds(t *testing.T) {
	m := newMemWith(sl(
		[2]string{"k1", "a"}, [2]string{"k2", "b"}, [2]string{"k3", "c"},
		[2]string{"k4", "d"}, [2]string{"k5", "e"},
	), nil)

	got := collect(m, []byte("k2"), []byte("k4"))
	want := [][2]string{{"k2", "b"}, {"k3", "c"}, {"k4", "d"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("闭区间扫描错误\n got=%v\nwant=%v", got, want)
	}
}

func TestScanRange_Unbounded(t *testing.T) {
	m := newMemWith(sl([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"}), nil)
	got := collect(m, nil, nil)
	want := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("无界扫描应返回全部升序\n got=%v\nwant=%v", got, want)
	}
}

func TestScanRange_SkipTombstone(t *testing.T) {
	m := newMemWith(sl(
		[2]string{"k1", "a"}, [2]string{"k2", ""}, [2]string{"k3", "c"},
	), nil)
	got := collect(m, nil, nil)
	want := [][2]string{{"k1", "a"}, {"k3", "c"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("墓碑应被跳过\n got=%v\nwant=%v", got, want)
	}
}

func TestScanRange_EarlyStop(t *testing.T) {
	m := newMemWith(sl([2]string{"k1", "a"}, [2]string{"k2", "b"}, [2]string{"k3", "c"}), nil)
	var seen int
	m.ScanRange(nil, nil, func(k, v []byte) bool {
		seen++
		return false // 立即停止
	})
	if seen != 1 {
		t.Fatalf("fn 返回 false 应在第一条后停止，实际遍历 %d 条", seen)
	}
}

func TestScanRange_ActiveOverridesDirty(t *testing.T) {
	// active 最新：k2 覆盖 dirty 的旧值，k4 的墓碑遮蔽 dirty 的值。
	active := sl([2]string{"k2", "A"}, [2]string{"k4", ""})
	dirty := sl([2]string{"k2", "old"}, [2]string{"k3", "C"}, [2]string{"k4", "D"})
	m := newMemWith(active, dirty)

	got := collect(m, nil, nil)
	want := [][2]string{{"k2", "A"}, {"k3", "C"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active 应覆盖 dirty 且墓碑遮蔽\n got=%v\nwant=%v", got, want)
	}
}
