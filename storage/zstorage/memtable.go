package zstorage

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/pkg/credit"
	"github.com/NeverENG/BanDB/pkg/metrics"
	"github.com/NeverENG/BanDB/storage/istorage"
)

var _ istorage.IMemTable = &MemTable{}

var (
	maxLevel    = config.G.MaxMemTableLevel
	probability = config.G.MaxMemTableP
)

// SkipList 跳表数据结构，封装跳表的 size、level 和 head 指针
type SkipList struct {
	size     int
	level    int
	head     *SkipNode
	byteSize int64 // 当前表内 key+value 的累计字节数（覆盖写按增量维护）
}

// MemTable 基于跳表的双表内存表实现
// active:  当前写入表，接收所有 Put/Delete 操作
// dirty:   正在刷盘的不可变快照，刷盘完成后置 nil
// 采用双表模式避免 Flush 与写入之间的数据竞争
type MemTable struct {
	active *SkipList
	dirty  *SkipList // 正在刷盘中的不可变表，可能仍包含未刷盘的旧数据（供 Get 回退查询）
	mu     sync.RWMutex

	FlushChan chan bool
	compactCh chan bool
	stopCh    chan struct{}

	sst *SSTable

	credits *credit.Pool // 字节级令牌桶背压：限制未刷盘数据的内存占用
}

// SkipNode 跳表节点
type SkipNode struct {
	Next  []*SkipNode
	Key   []byte
	Value []byte
}

// newSkipList 创建一个新的空跳表
func newSkipList() *SkipList {
	return &SkipList{
		head: newSkipNode(maxLevel, nil, nil),
	}
}

// NewMemTable 创建新的 MemTable
func NewMemTable() *MemTable {
	mt := &MemTable{
		active:    newSkipList(),
		FlushChan: make(chan bool, 1),
		compactCh: make(chan bool, 1),
		stopCh:    make(chan struct{}),
		sst:       NewSSTable(),
		credits:   credit.New(config.G.MemTableMaxInflightBytes),
	}
	// 注册未刷盘字节数仪表，供周期性指标快照实时读取。
	metrics.SetMemTableGauges(mt.InflightBytes, config.G.MemTableMaxInflightBytes)

	go mt.FlushWorker()
	go mt.ListenCompactCh()

	go mt.sst.LoadSSTableMetaList()
	return mt
}

// newSkipNode 创建新的跳表节点
func newSkipNode(level int, key []byte, value []byte) *SkipNode {
	return &SkipNode{
		Next:  make([]*SkipNode, level),
		Key:   key,
		Value: value,
	}
}

// randomLevel 生成随机层级
func randomLevel() int {
	level := 1
	for rand.Float64() < probability && level < maxLevel {
		level++
	}
	return level
}

// Size 返回 active 表中的元素个数
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.active == nil {
		return 0
	}
	return m.active.size
}

// Get 获取指定 key 的值
// 查找顺序：active → dirty → SSTable
func (m *MemTable) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	active := m.active
	dirty := m.dirty
	m.mu.RUnlock()

	if active == nil || active.head == nil {
		return nil, errors.New("NO DATA IN MEM")
	}

	// 先在 active 中查找（最新数据）。命中墓碑(val==nil)即已删除，不再下穿。
	if val, found := active.search(key); found {
		if val == nil {
			return nil, errors.New("Key not found")
		}
		return val, nil
	}

	if dirty != nil && dirty.head != nil {
		if val, found := dirty.search(key); found {
			if val == nil {
				return nil, errors.New("Key not found")
			}
			return val, nil
		}
	}

	if val, found := m.getFromSSTables(key); found {
		if val == nil { // SSTable 中的墓碑
			return nil, errors.New("Key not found")
		}
		return val, nil
	}

	return nil, errors.New("Key not found")
}

// firstGTE 返回第一个 Key >= key 的节点；key 为空表示从头开始。无则返回 nil。
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
// 全程持读锁：热窗口范围扫描有界且短，期间写入与刷盘会等待。fn 内若需在调用
// 返回后继续持有 key/value，应自行拷贝——底层切片归 MemTable 所有。
func (m *MemTable) ScanRange(start, end []byte, fn func(key, value []byte) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var a, d *SkipNode
	if m.active != nil {
		a = m.active.firstGTE(start)
	}
	if m.dirty != nil {
		d = m.dirty.firstGTE(start)
	}

	for a != nil || d != nil {
		// 选更小的 key；相等时 active 为最新版本，用 active 值并同步推进 dirty。
		var key, val []byte
		switch {
		case d == nil || (a != nil && bytes.Compare(a.Key, d.Key) < 0):
			key, val = a.Key, a.Value
			a = a.Next[0]
		case a == nil || bytes.Compare(d.Key, a.Key) < 0:
			key, val = d.Key, d.Value
			d = d.Next[0]
		default: // a.Key == d.Key：active 覆盖 dirty
			key, val = a.Key, a.Value
			a = a.Next[0]
			d = d.Next[0]
		}

		if len(end) > 0 && bytes.Compare(key, end) > 0 {
			return // 升序遍历已越过上界，可停
		}
		if val == nil {
			continue // 墓碑：跳过
		}
		if !fn(key, val) {
			return
		}
	}
}

// SnapshotLive 返回 active+dirty 合并后的全部键值快照（active 覆盖 dirty），
// 与 ScanRange 不同：保留墓碑(value==nil)。用于 WAL checkpoint 重写——未刷盘的
// 热数据（含删除墓碑）必须完整保留，否则重放时被删的 key 会从 SSTable 复活。
// 拷贝底层字节，返回后可安全持有。
func (m *MemTable) SnapshotLive() []istorage.LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var a, d *SkipNode
	if m.active != nil {
		a = m.active.head.Next[0]
	}
	if m.dirty != nil {
		d = m.dirty.head.Next[0]
	}

	out := make([]istorage.LogEntry, 0)
	for a != nil || d != nil {
		var key, val []byte
		switch {
		case d == nil || (a != nil && bytes.Compare(a.Key, d.Key) < 0):
			key, val = a.Key, a.Value
			a = a.Next[0]
		case a == nil || bytes.Compare(d.Key, a.Key) < 0:
			key, val = d.Key, d.Value
			d = d.Next[0]
		default: // a.Key == d.Key：active 为最新版本
			key, val = a.Key, a.Value
			a = a.Next[0]
			d = d.Next[0]
		}
		e := istorage.LogEntry{Key: append([]byte(nil), key...)}
		if val != nil { // 保留 nil-vs-空切片语义：nil=墓碑，非 nil=普通写
			e.Value = append([]byte(nil), val...)
		}
		out = append(out, e)
	}
	return out
}

// search 在跳表中查找指定 key，返回值和是否找到
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
func (m *MemTable) Put(key []byte, value []byte) error {
	full := int64(len(key)) + int64(len(value))
	m.acquireCredit(full) // 字节级令牌桶背压：信用不足则阻塞

	m.mu.Lock()
	if m.active == nil || m.active.head == nil {
		m.mu.Unlock()
		m.credits.Release(full) // 写入未发生，归还信用
		return errors.New("NO DATA IN MEMTABLE")
	}

	delta := m.active.insert(key, value)

	// 检查 active 表大小是否超过阈值，触发刷盘
	if m.active.size > config.G.MaxMemTableSize {
		m.StartFlush()
	}
	m.mu.Unlock()

	// 覆盖写实际增量 < 预占的 full，归还多占部分，避免信用单调泄漏
	if over := full - delta; over != 0 {
		m.credits.Release(over)
	}
	return nil
}

// acquireCredit 为本次写入预占 n 字节信用；不足时先触发刷盘以归还信用，再阻塞等待。
func (m *MemTable) acquireCredit(n int64) {
	if m.credits.TryAcquire(n) {
		return
	}
	metrics.BackpressureStalls.Add(1) // 快路径未命中，将触发刷盘并阻塞等待信用
	m.StartFlush()                     // 确保有 flush 在路上来归还信用，避免永久阻塞
	m.credits.Acquire(n)
}

// InflightBytes 返回当前未刷盘（active + 正在刷的 dirty）占用的字节信用，供观测/压测使用。
func (m *MemTable) InflightBytes() int64 {
	return m.credits.Used()
}

// insert 在跳表中插入键值对（无锁版本，由调用者保证线程安全）
// insert 插入或覆盖 key，并返回本次操作使 byteSize 变化的增量（覆盖写可能为负）。
// 调用方用该增量做背压信用对账。
func (sl *SkipList) insert(key []byte, value []byte) int64 {
	update := make([]*SkipNode, maxLevel)
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
	newLevel := randomLevel()
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
func (m *MemTable) Delete(key []byte) error {
	full := int64(len(key)) // 墓碑 value 为 nil
	m.acquireCredit(full)

	m.mu.Lock()
	if m.active == nil || m.active.head == nil {
		m.mu.Unlock()
		m.credits.Release(full)
		return errors.New("NO DATA IN MEMTABLE")
	}

	// 写入墓碑(Value==nil)而非物理删除：物理删除只能去掉 active 中的节点，
	// 无法 shadow 已 flush 到 SSTable 的同名旧值——读路径会从 SSTable 把它「复活」。
	// 删除因此变为幂等盲写，不再返回 key not found。
	delta := m.active.insert(key, nil)

	if m.active.size > config.G.MaxMemTableSize {
		m.StartFlush()
	}
	m.mu.Unlock()

	if over := full - delta; over != 0 {
		m.credits.Release(over)
	}
	return nil
}

// delete 从跳表中删除节点，返回是否成功删除
func (sl *SkipList) delete(key []byte) bool {
	update := make([]*SkipNode, maxLevel)
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

func (m *MemTable) Close() error {
	close(m.stopCh)
	return nil
}

func (m *MemTable) StartFlush() {
	select {
	case m.FlushChan <- true:
	default:
	}
}

// Flush 将 dirty 表数据刷入 SSTable
// 流程：
//  1. 持锁交换 active → dirty（active 变为 dirty 的不可变快照）
//  2. 创建新的空 active 表用于接受后续写入
//  3. 释放锁，在锁外将 dirty 数据写入 SSTable
//  4. 刷盘完成后将 dirty 置 nil
func (m *MemTable) Flush() {
	// 步骤 1-2: 持锁进行交换（快速操作）
	m.mu.Lock()
	// dirty 非 nil 说明上一次刷盘失败遗留，本次直接重试它，不再交换（避免覆盖丢数据）
	if m.dirty == nil {
		if m.active.size == 0 {
			m.mu.Unlock()
			return
		}
		m.dirty = m.active
		m.active = newSkipList()
	}
	dirty := m.dirty
	m.mu.Unlock()

	slog.Debug("flushing memtable", "entries", dirty.size)

	allEntries := collectAllEntry(dirty)
	err := m.sst.WriteToSSTable(allEntries)
	if err != nil {
		slog.Error("flush error", "error", err)
		m.StartFlush() // 重试；dirty 保留不丢数据，信用待重试成功后再释放
		return
	}

	m.mu.Lock()
	freed := dirty.byteSize
	m.dirty = nil
	m.mu.Unlock()

	m.credits.Release(freed) // 归还信用，唤醒被背压阻塞的写
	slog.Debug("flush completed")
}

func (m *MemTable) FlushWorker() {
	for {
		select {
		case <-m.FlushChan:
			m.Flush()
		case <-m.stopCh:
			return
		}
	}
}

// collectAllEntry 收集跳表中的所有 entry（从第 0 层按序遍历）
func collectAllEntry(sl *SkipList) []istorage.LogEntry {
	logEntries := make([]istorage.LogEntry, 0, sl.size)

	p := sl.head.Next[0]
	for p != nil {
		logEntries = append(logEntries, istorage.LogEntry{
			Key:   p.Key,
			Value: p.Value,
		})
		p = p.Next[0]
	}
	return logEntries
}

func (m *MemTable) getFromSSTables(key []byte) ([]byte, bool) {
	// 新→旧遍历：mata 按落盘先后追加（最旧在前），故逆序取首个命中即为最新版本，
	// 避免旧 SSTable 的陈旧值盖过新 SSTable 中对同一 key 的覆盖写。
	//
	// 【正确性不变量 / 慎改】此处判定 newest-wins 只看 mata 顺序、不看 level。它依赖
	// 「同一 key 的最新值所在文件的创建 ts 最大」——这是当前「compaction 合并整层」机制
	// 的涌现性质（每次 L0 compaction 卷走全部 L0，新写不会被落在浅层而其它数据继续下沉），
	// 不是构造保证。重启由 LoadSSTableMetaList 按创建 ts 重建 mata 维持之（见其注释与
	// recency_test.go / recency_fuzz_test.go 守卫）。
	// 若改为 overlap-aware / 部分选择 compaction，该不变量会被打破（新写可滞留浅层、旧值
	// 经深层合并获得更大 ts 而倒挂），必须改用 per-key 序号等显式 recency，否则会无声重新
	// 引入「重启后读到陈旧值」。
	metas := m.sst.GetAllMata()
	for i := len(metas) - 1; i >= 0; i-- {
		meta := metas[i]
		// 首次访问时自动加载 MaxKey
		meta.EnsureMeta()

		// 用 MinKey 和 MaxKey 过滤
		if bytes.Compare(key, meta.MinKey) < 0 ||
			bytes.Compare(key, meta.MaxKey) > 0 {
			continue
		}

		// 在文件中查找
		if value, found := m.sst.ReadFromSSTable(meta.Filepath, key); found {
			return value, true
		}
	}
	return nil, false
}

// FlushToSSTable 将 entries 写入临时跳表并立即 Flush 到 SSTable
// 不经过 active 表，不影响正常读写，专用于快照重放等场景
func (m *MemTable) FlushToSSTable(entries []istorage.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// 创建临时跳表，按序插入（同 key 自动去重/更新）。
	// Value==nil 为墓碑，按墓碑插入而非物理删除：快照中的删除需写入墓碑以 shadow
	// 旧 SSTable 中的同名值，否则该 key 会在读路径被「复活」。
	tmp := newSkipList()
	for _, entry := range entries {
		tmp.insert(entry.Key, entry.Value)
	}

	// 从临时跳表收集有序条目
	sorted := collectAllEntry(tmp)
	if len(sorted) == 0 {
		return nil
	}

	// 写入 SSTable（SSTable 内部有锁保护元数据并发安全）
	if err := m.sst.WriteToSSTable(sorted); err != nil {
		return fmt.Errorf("FlushToSSTable write error: %w", err)
	}

	// 触发 Compaction 检查
	select {
	case m.compactCh <- true:
	default:
	}

	slog.Info("FlushToSSTable completed", "entries", len(sorted))
	return nil
}

func (m *MemTable) WriteSSTable() error {
	m.mu.RLock()
	active := m.active
	m.mu.RUnlock()

	err := m.sst.WriteToSSTable(collectAllEntry(active))
	select {
	case m.compactCh <- true:
	default:
	}
	return err
}

func (m *MemTable) ListenCompactCh() {
	for {
		select {
		case <-m.compactCh:
			m.CompactSSTable(0)
		case <-m.stopCh:
			return
		}
	}
}

func (m *MemTable) CompactSSTable(startLevel int) {
	maxLevel := 10

	for level := startLevel; level < maxLevel; level++ {
		files := m.sst.GetLevelFiles(level)

		if len(files) < config.G.MaxCompactionSize {
			continue
		}

		slog.Info("compacting level", "level", level, "files", len(files))

		newMeta := m.sst.MergeSSTable(files, level+1)
		if newMeta == nil {
			slog.Error("failed to merge level", "level", level)
			continue
		}

		for _, meta := range files {
			m.sst.DeleteSSTable(meta)
			m.sst.RemoveMata(meta)
		}

		slog.Info("level compaction completed", "level", level)
	}
}
