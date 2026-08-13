package storage

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"

	"github.com/NeverENG/BanDB/internal/credit"
	"github.com/NeverENG/BanDB/internal/metrics"
)

// Engine 是 LSM 存储引擎：它同时持有内存中的表与磁盘上的 SSTable 集合，并驱动二者
// 之间的流转。
//
// 注意区分：这里的「内存表」是 SkipList（见 skiplist.go），Engine 是持有它们并连同
// SSTable 一起管理的整条链路。
//
// 内存侧采用双表：
//   - active 接收所有 Put/Delete；
//   - dirty 是正在 flush 的不可变快照，flush 完成后置 nil。
//
// 双表使 flush 能在锁外进行：交换后 dirty 不再被写入，Get 仍可回退查询它，
// 从而避免 flush 与写入相互阻塞。
//
// 读顺序 active → dirty → SSTable；写入受字节级信用背压约束，超预算即阻塞等待 flush
// 归还信用。后台两个协程分别负责 flush 与 compaction。
type Engine struct {
	active *SkipList
	dirty  *SkipList // 正在 flush 中的不可变表，可能仍包含未 flush 的旧数据（供 Get 回退查询）
	mu     sync.RWMutex

	// flushCh 与 compactCh 是纯信号：值不携带信息，故用 struct{}。缓冲 1 + 非阻塞发送
	// 使重复触发被合并为一次待处理信号。
	flushCh   chan struct{}
	compactCh chan struct{}
	stopCh    chan struct{}

	sst *SSTable

	credits *credit.Pool // 字节级令牌桶背压：限制未 flush 数据的内存占用

	// 构造时从 config 快照的阈值：flush 触发大小与 compaction 触发文件数。快照而非在
	// 后台 goroutine（FlushWorker/ListenCompactCh）里读全局 config.G，避免与（测试中）
	// 并发修改全局配置形成数据竞争。
	maxSize       int
	maxCompaction int

	// opts 是构造时传入的参数，引擎此后只认它，不再读全局配置。
	opts Options
}

// SkipNode 跳表节点
// NewEngine 按 opts 构造存储引擎。参数在此固定，此后不再读全局配置。
// 需要沿用进程配置时传 DefaultOptions()。
func NewEngine(opts Options) *Engine {
	opts = opts.withDefaults()
	mt := &Engine{
		active:        newSkipList(opts.SkipListMaxLevel, opts.SkipListP),
		flushCh:       make(chan struct{}, 1),
		compactCh:     make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		sst:           NewSSTable(opts),
		credits:       credit.New(opts.MaxInflightBytes),
		maxSize:       opts.MaxMemTableSize,
		maxCompaction: opts.MaxCompactionSize,
		opts:          opts,
	}
	// 注册未 flush 字节数仪表，供周期性指标快照实时读取。
	metrics.SetMemTableGauges(mt.InflightBytes, opts.MaxInflightBytes)

	go mt.FlushWorker()
	go mt.ListenCompactCh()

	// 元信息扫描必须同步完成：引擎在知道磁盘上有哪些 SSTable 之前不能对外服务。
	// 此前它是 goroutine，且 LoadSSTableMetaList 会整体替换 metas，于是与并发的 AddMeta
	// 相争——启动时 WAL 重放触发的 flush 刚登记好元信息，就可能被随后完成的扫描抹掉，
	// 那个 SSTable 的数据直到下次重启前都读不到。文件级的块索引/布隆预热仍是异步的，
	// 那只是缓存预热，与顺序无关。
	mt.sst.LoadSSTableMetaList()
	return mt
}

// newSkipNode 创建新的跳表节点
func (m *Engine) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.active == nil {
		return 0
	}
	return m.active.size
}

// Get 获取指定 key 的值
// 查找顺序：active → dirty → SSTable
func (m *Engine) Get(key []byte) ([]byte, error) {
	// active 的查找必须持锁：Put/Delete 会并发改写其节点指针，无锁遍历可能跟到只连了
	// 一半的新节点，读出错值甚至解引用空指针。SSTable 查找不持锁——它要读磁盘，持锁会让
	// 写入停等 I/O；dirty 一经交换便不再被写入，故也无需持锁。
	m.mu.RLock()
	if m.active == nil || m.active.head == nil {
		m.mu.RUnlock()
		return nil, ErrMemTableUnavailable
	}
	// 先在 active 中查找（最新数据）。命中墓碑(val==nil)即已删除，不再下穿。
	val, found := m.active.search(key)
	dirty := m.dirty
	m.mu.RUnlock()

	if found {
		if val == nil {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	if dirty != nil && dirty.head != nil {
		if val, found := dirty.search(key); found {
			if val == nil {
				return nil, ErrKeyNotFound
			}
			return val, nil
		}
	}

	if val, found := m.getFromSSTables(key); found {
		if val == nil { // SSTable 中的墓碑
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	return nil, ErrKeyNotFound
}

// firstGTE 返回第一个 Key >= key 的节点；key 为空表示从头开始。无则返回 nil。
func (m *Engine) ScanRange(start, end []byte, fn func(key, value []byte) bool) {
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
// 与 ScanRange 不同：保留墓碑(value==nil)。用于 WAL checkpoint 重写——未 flush 的
// 热数据（含删除墓碑）必须完整保留，否则重放时被删的 key 会从 SSTable 复活。
// 拷贝底层字节，返回后可安全持有。
func (m *Engine) SnapshotLive() []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var a, d *SkipNode
	if m.active != nil {
		a = m.active.head.Next[0]
	}
	if m.dirty != nil {
		d = m.dirty.head.Next[0]
	}

	out := make([]LogEntry, 0)
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
		e := LogEntry{Key: append([]byte(nil), key...)}
		if val != nil { // 保留 nil-vs-空切片语义：nil=墓碑，非 nil=普通写
			e.Value = append([]byte(nil), val...)
		}
		out = append(out, e)
	}
	return out
}

// search 在跳表中查找指定 key，返回值和是否找到
func (m *Engine) Put(key []byte, value []byte) error {
	full := int64(len(key)) + int64(len(value))
	m.acquireCredit(full) // 字节级令牌桶背压：信用不足则阻塞

	m.mu.Lock()
	if m.active == nil || m.active.head == nil {
		m.mu.Unlock()
		m.credits.Release(full) // 写入未发生，归还信用
		return ErrMemTableUnavailable
	}

	delta := m.active.insert(key, value)

	// 检查 active 表大小是否超过阈值，触发 flush
	if m.active.size > m.maxSize {
		m.StartFlush()
	}
	m.mu.Unlock()

	// 覆盖写实际增量 < 预占的 full，归还多占部分，避免信用单调泄漏
	if over := full - delta; over != 0 {
		m.credits.Release(over)
	}
	return nil
}

// acquireCredit 为本次写入预占 n 字节信用；不足时先触发 flush 以归还信用，再阻塞等待。
func (m *Engine) acquireCredit(n int64) {
	if m.credits.TryAcquire(n) {
		return
	}
	metrics.BackpressureStalls.Add(1) // 快路径未命中，将触发 flush 并阻塞等待信用
	m.StartFlush()                    // 确保有 flush 在路上来归还信用，避免永久阻塞
	m.credits.Acquire(n)
}

// InflightBytes 返回当前未 flush（active + 正在刷的 dirty）占用的字节信用，供观测/压测使用。
func (m *Engine) InflightBytes() int64 {
	return m.credits.Used()
}

// insert 在跳表中插入键值对（无锁版本，由调用者保证线程安全）
// insert 插入或覆盖 key，并返回本次操作使 byteSize 变化的增量（覆盖写可能为负）。
// 调用方用该增量做背压信用对账。
func (m *Engine) Delete(key []byte) error {
	full := int64(len(key)) // 墓碑 value 为 nil
	m.acquireCredit(full)

	m.mu.Lock()
	if m.active == nil || m.active.head == nil {
		m.mu.Unlock()
		m.credits.Release(full)
		return ErrMemTableUnavailable
	}

	// 写入墓碑(Value==nil)而非物理删除：物理删除只能去掉 active 中的节点，
	// 无法 shadow 已 flush 到 SSTable 的同名旧值——读路径会从 SSTable 把它「复活」。
	// 删除因此变为幂等盲写，不再返回 key not found。
	delta := m.active.insert(key, nil)

	if m.active.size > m.maxSize {
		m.StartFlush()
	}
	m.mu.Unlock()

	if over := full - delta; over != 0 {
		m.credits.Release(over)
	}
	return nil
}

// delete 从跳表中删除节点，返回是否成功删除
func (m *Engine) Close() error {
	close(m.stopCh)
	return nil
}

func (m *Engine) StartFlush() {
	select {
	case m.flushCh <- struct{}{}:
	default:
	}
}

// Flush 将 dirty 表数据刷入 SSTable
// 流程：
//  1. 持锁交换 active → dirty（active 变为 dirty 的不可变快照）
//  2. 创建新的空 active 表用于接受后续写入
//  3. 释放锁，在锁外将 dirty 数据写入 SSTable
//  4. flush 完成后将 dirty 置 nil
func (m *Engine) Flush() {
	// 步骤 1-2: 持锁进行交换（快速操作）
	m.mu.Lock()
	// dirty 非 nil 说明上一次 flush 失败遗留，本次直接重试它，不再交换（避免覆盖丢数据）
	if m.dirty == nil {
		if m.active.size == 0 {
			m.mu.Unlock()
			return
		}
		m.dirty = m.active
		m.active = newSkipList(m.opts.SkipListMaxLevel, m.opts.SkipListP)
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

func (m *Engine) FlushWorker() {
	for {
		select {
		case <-m.flushCh:
			m.Flush()
		case <-m.stopCh:
			return
		}
	}
}

// collectAllEntry 收集跳表中的所有 entry（从第 0 层按序遍历）
func (m *Engine) getFromSSTables(key []byte) ([]byte, bool) {
	// 新→旧遍历：metas 按落盘先后追加（最旧在前），故逆序取首个命中即为最新版本，
	// 避免旧 SSTable 的陈旧值盖过新 SSTable 中对同一 key 的覆盖写。
	//
	// 正确性不变量（慎改）：此处判定 newest-wins 只看 metas 顺序、不看 level。它依赖
	// 「同一 key 的最新值所在文件的创建 ts 最大」——这是当前「一次 compaction 卷走整层」机制
	// 的涌现性质（每次 L0 compaction 卷走全部 L0，新写不会被落在浅层而其它数据继续下沉），
	// 不是构造保证。重启由 LoadSSTableMetaList 按创建 ts 重建 metas 维持之（见其注释与
	// recency_test.go / recency_fuzz_test.go 守卫）。
	// 若改为 overlap-aware / 部分选择 compaction，该不变量会被打破（新写可滞留浅层、旧值
	// 经深层 compaction 获得更大 ts 而倒挂），必须改用 per-key 序号等显式 recency，否则会无声重新
	// 引入「重启后读到陈旧值」。
	metas := m.sst.Metas()
	for i := len(metas) - 1; i >= 0; i-- {
		meta := metas[i]
		// 用 [MinKey, MaxKey] 快速排除不可能命中的文件。MinKey 取自文件头部，恒可信；
		// MaxKey 仅在取自块索引时可信，否则跳过上界判断（宁可多扫一个文件，也不能因为
		// 猜错上界而漏掉命中的 key）。
		if bytes.Compare(key, meta.MinKey) < 0 {
			continue
		}
		if meta.MaxKeyKnown && bytes.Compare(key, meta.MaxKey) > 0 {
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
func (m *Engine) FlushToSSTable(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// 创建临时跳表，按序插入（同 key 自动去重/更新）。
	// Value==nil 为墓碑，按墓碑插入而非物理删除：快照中的删除需写入墓碑以 shadow
	// 旧 SSTable 中的同名值，否则该 key 会在读路径被「复活」。
	tmp := newSkipList(m.opts.SkipListMaxLevel, m.opts.SkipListP)
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
		return fmt.Errorf("storage: flush snapshot to sstable: %w", err)
	}

	// 触发 Compaction 检查
	select {
	case m.compactCh <- struct{}{}:
	default:
	}

	slog.Info("FlushToSSTable completed", "entries", len(sorted))
	return nil
}

func (m *Engine) ListenCompactCh() {
	for {
		select {
		case <-m.compactCh:
			m.CompactSSTable(0)
		case <-m.stopCh:
			return
		}
	}
}

func (m *Engine) CompactSSTable(startLevel int) {
	maxLevel := 10

	for level := startLevel; level < maxLevel; level++ {
		files := m.sst.LevelFiles(level)

		if len(files) < m.maxCompaction {
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
			m.sst.RemoveMeta(meta)
		}

		slog.Info("level compaction completed", "level", level)
	}
}
