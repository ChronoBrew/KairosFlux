package storage

import (
	"bytes"
	"container/heap"
	"fmt"
)

// entryIterator 是参与归并的有序源：按 key 升序逐条产出 (key, value)。
// SSTable 文件与内存表快照都实现它，从而可在同一次归并中混合。
type entryIterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
	Close() error
}

// mergeSource 一个参与归并的源迭代器及其 srcIdx（约定：越大越新）。
type mergeSource struct {
	it     entryIterator
	srcIdx int
	key    []byte
	value  []byte
	prev   []byte // 上一条 key，用于校验源严格升序
}

// mergeHeap 最小堆：先按 key 升序；key 相同则 srcIdx 大者（更新）先弹出，
// 从而在去重时被保留。
type mergeHeap []*mergeSource

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].key, h[j].key); c != 0 {
		return c < 0
	}
	return h[i].srcIdx > h[j].srcIdx
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*mergeSource)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	s := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return s
}

// mergeIterator 对多个**有序**源做 K 路归并，输出 key 升序且去重
// （同 key 仅保留 srcIdx 最大者，即最新版本）。内存占用 O(K)，不物化全部条目。
//
// 非 goroutine-safe：所有方法（Next/Key/Value/Err/Close）须在同一 goroutine 中
// 串行调用。堆操作与 pull/advance 都假定独占 mergeSource，并发调用会与堆内部的
// Less 比较产生数据竞争（container/heap 不保证比较的原子性）。merge 仅用于后台
// compaction 单 goroutine 路径，故不内置锁。
type mergeIterator struct {
	h       mergeHeap
	sources []entryIterator // 持有以便 Close
	curKey  []byte
	curVal  []byte
	err     error
}

func newMergeIterator(iters []entryIterator) (*mergeIterator, error) {
	m := &mergeIterator{sources: iters}
	h := make(mergeHeap, 0, len(iters))
	for idx, it := range iters {
		s := &mergeSource{it: it, srcIdx: idx}
		if err := m.pull(s); err != nil {
			m.err = err
			return m, err
		}
		if s.key != nil {
			h = append(h, s)
		}
	}
	m.h = h
	heap.Init(&m.h)
	return m, nil
}

// pull 从源读下一条到 s；遇到非升序返回错误。s.key==nil 表示该源耗尽。
func (m *mergeIterator) pull(s *mergeSource) error {
	if s.it.Next() {
		k := s.it.Key()
		if s.prev != nil && bytes.Compare(k, s.prev) <= 0 {
			return fmt.Errorf("merge: source %d not sorted ascending (%q after %q)", s.srcIdx, k, s.prev)
		}
		s.prev = k
		s.key = k
		s.value = s.it.Value()
		return nil
	}
	if err := s.it.Err(); err != nil {
		return err
	}
	s.key = nil
	s.value = nil
	return nil
}

// advance 从源取下一条并按需重新入堆。
func (m *mergeIterator) advance(s *mergeSource) error {
	if err := m.pull(s); err != nil {
		return err
	}
	if s.key != nil {
		heap.Push(&m.h, s)
	}
	return nil
}

// Next 输出下一个去重后的最小 key。返回 false 表示结束或出错（用 Err 区分）。
func (m *mergeIterator) Next() bool {
	if m.err != nil || m.h.Len() == 0 {
		return false
	}
	top := heap.Pop(&m.h).(*mergeSource)
	m.curKey = top.key
	m.curVal = top.value
	dupKey := top.key
	if err := m.advance(top); err != nil {
		m.err = err
		return false
	}
	// 丢弃所有同 key 的较旧版本（源内严格升序，故不会再次出现 dupKey）
	for m.h.Len() > 0 && bytes.Equal(m.h[0].key, dupKey) {
		dup := heap.Pop(&m.h).(*mergeSource)
		if err := m.advance(dup); err != nil {
			m.err = err
			return false
		}
	}
	return true
}

func (m *mergeIterator) Key() []byte   { return m.curKey }
func (m *mergeIterator) Value() []byte { return m.curVal }
func (m *mergeIterator) Err() error    { return m.err }

// Close 关闭所有源句柄，返回首个错误。
func (m *mergeIterator) Close() error {
	var first error
	for _, it := range m.sources {
		if err := it.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
