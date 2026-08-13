// 本文件是跳表本身——一个有序的内存数据结构，不涉及 flush、compaction 或 SSTable。
// LSM 引擎（Engine）在 engine.go，它持有若干张跳表与一组 SSTable。

package storage

import (
	"bytes"
	"math/rand"
)

// SkipList 是一张有序跳表，Engine 用它承载内存中的活跃表与待 flush 的不可变表。
// 它自身不加锁——并发由持有者（Engine）统一同步。
type SkipList struct {
	size     int
	level    int
	head     *SkipNode
	byteSize int64

	// maxLevel 与 p 是本表的层高上限与升层概率，构造时固定。此前它们是包级变量且在
	// import 时取自全局配置，既无法按实例配置，也让测试改配置成为空操作。
	maxLevel int
	p        float64 // 当前表内 key+value 的累计字节数（覆盖写按增量维护）
}

// SkipNode 是跳表节点。Next 的长度即该节点的层高。
type SkipNode struct {
	Next  []*SkipNode
	Key   []byte
	Value []byte
}

// newSkipList 创建一个新的空跳表
func newSkipList(maxLevel int, p float64) *SkipList {
	return &SkipList{
		head:     newSkipNode(maxLevel, nil, nil),
		maxLevel: maxLevel,
		p:        p,
	}
}

// NewEngine 创建新的 MemTable
func newSkipNode(level int, key []byte, value []byte) *SkipNode {
	return &SkipNode{
		Next:  make([]*SkipNode, level),
		Key:   key,
		Value: value,
	}
}

// randomLevel 生成新节点的随机层级。
func (sl *SkipList) randomLevel() int {
	level := 1
	for rand.Float64() < sl.p && level < sl.maxLevel {
		level++
	}
	return level
}

// Size 返回 active 表中的元素个数
func (sl *SkipList) firstGTE(key []byte) *SkipNode {
	p := sl.head
	if p == nil {
		return nil
	}
	for i := sl.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
	}
	return p.Next[0]
}

// ScanRange 在 [start,end] 闭区间内升序遍历 active+dirty 合并后的最新可见键值，
// 跳过墓碑(value==nil)，对每条命中调用 fn；fn 返回 false 可提前停止。
// start/end 为空分别表示下界/上界不限。
//
// 全程持读锁：热窗口范围扫描有界且短，期间写入与 flush 会等待。fn 内若需在调用
// 返回后继续持有 key/value，应自行拷贝——底层切片归 MemTable 所有。
func (sl *SkipList) search(key []byte) ([]byte, bool) {
	p := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
	}
	p = p.Next[0]
	if p != nil && bytes.Compare(p.Key, key) == 0 {
		return p.Value, true
	}
	return nil, false
}

// Put 插入或更新键值对，始终操作 active 表
func (sl *SkipList) insert(key []byte, value []byte) int64 {
	update := make([]*SkipNode, sl.maxLevel)
	p := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
		update[i] = p
	}

	// 检查 key 是否已存在
	p = p.Next[0]
	if p != nil && bytes.Compare(p.Key, key) == 0 {
		// key 已存在，更新值：byteSize 按新旧 value 差值调整
		delta := int64(len(value)) - int64(len(p.Value))
		p.Value = value
		sl.byteSize += delta
		return delta
	}

	// 生成新节点的随机层级
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	// 创建新节点并插入每一层
	newNode := newSkipNode(newLevel, key, value)
	for i := 0; i < newLevel; i++ {
		newNode.Next[i] = update[i].Next[i]
		update[i].Next[i] = newNode
	}

	sl.size++
	delta := int64(len(key)) + int64(len(value))
	sl.byteSize += delta
	return delta
}

// Delete 删除指定 key 的节点，始终操作 active 表
func (sl *SkipList) delete(key []byte) bool {
	update := make([]*SkipNode, sl.maxLevel)
	p := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
		update[i] = p
	}

	p = p.Next[0]
	if p == nil || bytes.Compare(p.Key, key) != 0 {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].Next[i] != p {
			break
		}
		update[i].Next[i] = p.Next[i]
	}

	for sl.level > 0 && sl.head.Next[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return true
}

func collectAllEntry(sl *SkipList) []LogEntry {
	logEntries := make([]LogEntry, 0, sl.size)

	p := sl.head.Next[0]
	for p != nil {
		logEntries = append(logEntries, LogEntry{
			Key:   p.Key,
			Value: p.Value,
		})
		p = p.Next[0]
	}
	return logEntries
}
