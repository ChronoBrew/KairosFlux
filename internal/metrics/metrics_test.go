package metrics

import "testing"

// 计数器为进程级全局，本测试以「增量」断言，避免依赖其它测试是否先跑过。
func TestCountersAndSnapshot(t *testing.T) {
	before := Take()

	FramesDroppedMalformed.Add(2)
	FramesDroppedNonMonotonic.Add(1)
	Writes.Add(5)
	WriteErrors.Add(1)
	BackpressureStalls.Add(3)

	after := Take()

	if d := after.DroppedMalformed - before.DroppedMalformed; d != 2 {
		t.Fatalf("DroppedMalformed 增量应为 2，得到 %d", d)
	}
	if d := after.DroppedNonMonotonic - before.DroppedNonMonotonic; d != 1 {
		t.Fatalf("DroppedNonMonotonic 增量应为 1，得到 %d", d)
	}
	if d := after.Writes - before.Writes; d != 5 {
		t.Fatalf("Writes 增量应为 5，得到 %d", d)
	}
	if d := after.WriteErrors - before.WriteErrors; d != 1 {
		t.Fatalf("WriteErrors 增量应为 1，得到 %d", d)
	}
	if d := after.BackpressureStalls - before.BackpressureStalls; d != 3 {
		t.Fatalf("BackpressureStalls 增量应为 3，得到 %d", d)
	}
}

func TestMemTableGauges(t *testing.T) {
	// 未注册回调时仪表读 0，不应 panic。
	s := Take()
	if s.MemTableInflightBytes != 0 {
		t.Fatalf("未注册回调时 inflight 应为 0，得到 %d", s.MemTableInflightBytes)
	}

	inflight := int64(8192)
	SetMemTableGauges(func() int64 { return inflight }, 16384)

	s = Take()
	if s.MemTableInflightBytes != 8192 {
		t.Fatalf("inflight 应实时读回调值 8192，得到 %d", s.MemTableInflightBytes)
	}
	if s.MemTableBudgetBytes != 16384 {
		t.Fatalf("budget 应为 16384，得到 %d", s.MemTableBudgetBytes)
	}

	// 回调实时反映变化。
	inflight = 12288
	if got := Take().MemTableInflightBytes; got != 12288 {
		t.Fatalf("inflight 应实时反映为 12288，得到 %d", got)
	}
}
