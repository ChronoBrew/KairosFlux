package storage

import (
	"fmt"
	"reflect"
	"testing"
	"time"
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

// newMemWith 构造只有内存表的引擎。仍需给出一个空的 SSTable 集合：ScanRange 现在会
// 归并 SSTable，nil 的 sst 会在取元信息时解引用空指针。空 Dir 不触碰磁盘。
func newMemWith(active, dirty *SkipList) *Engine {
	return &Engine{active: active, dirty: dirty, sst: NewSSTable(Options{})}
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

// TestScanRange_CoversFlushedData 守护扫描的覆盖范围：已 flush 到 SSTable 的数据必须
// 仍在扫描结果中。
//
// 这条契约此前不成立：ScanRange 只遍历 active 与 dirty 两张内存表，一旦 flush 发生，
// 那批数据对扫描完全不可见。后果不止 SCAN 命令少返回结果——下游投递按游标调用 Scan 取数，
// 会直接跳过所有已落盘的记录，即静默漏投。而 Get 能读到同样的数据，两条读路径的可见范围
// 不一致，问题因此更难察觉。
func TestScanRange_CoversFlushedData(t *testing.T) {
	// 极小阈值：写入过程中必然多次 flush。
	e := NewEngine(Options{Dir: t.TempDir(), MaxMemTableSize: 5})
	t.Cleanup(func() { e.Close() })

	const n = 200
	for i := 0; i < n; i++ {
		if err := e.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	waitFlushed(t, e)

	seen := map[string]string{}
	e.ScanRange(nil, nil, func(k, v []byte) bool {
		seen[string(k)] = string(v)
		return true
	})

	if len(seen) != n {
		t.Fatalf("扫描应覆盖全部 %d 条（含已 flush），实际 %d 条", n, len(seen))
	}
	for i := 0; i < n; i++ {
		k, want := fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i)
		if got := seen[k]; got != want {
			t.Fatalf("扫描到 %s = %q, want %q", k, got, want)
		}
	}
}

// TestScanRange_NewestWinsAcrossSSTables 覆盖写落在不同 SSTable 时，扫描须返回最新值；
// 删除墓碑须压住更旧的已落盘版本。
func TestScanRange_NewestWinsAcrossSSTables(t *testing.T) {
	e := NewEngine(Options{Dir: t.TempDir(), MaxMemTableSize: 1 << 20})
	t.Cleanup(func() { e.Close() })

	if err := e.Put([]byte("dup"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put([]byte("gone"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	e.Flush() // 落到第一个 SSTable

	if err := e.Put([]byte("dup"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	e.Flush() // 落到第二个 SSTable：覆盖写与墓碑都在更新的文件里

	got := map[string]string{}
	e.ScanRange(nil, nil, func(k, v []byte) bool { got[string(k)] = string(v); return true })

	if got["dup"] != "new" {
		t.Fatalf("覆盖写应返回最新值, 实际 %q", got["dup"])
	}
	if v, ok := got["gone"]; ok {
		t.Fatalf("已删除的 key 不应出现在扫描结果中, 实际 %q", v)
	}
}

// waitFlushed 等后台 FlushWorker 把 dirty 落盘。
func waitFlushed(t *testing.T, e *Engine) {
	t.Helper()
	for i := 0; i < 200; i++ {
		e.mu.RLock()
		pending := e.dirty != nil
		e.mu.RUnlock()
		if !pending && len(e.sst.Metas()) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
