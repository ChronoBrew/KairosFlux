package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeverENG/BanDB/config"
)

const (
	SSTableBlockSize        = 64
	indexFooterMagic uint32 = 0x49445846 // "IDXF"
	indexFooterSize  int64  = 16         // BlockCount(4) + IndexOffset(8) + Magic(4)
	// 布隆过滤器段位于块索引之后、索引 Footer 之前，向后兼容：
	// 旧格式(v1)无此段，索引 Footer 布局与位置不变。
	bloomTrailerMagic  uint32  = 0x424c4d46 // "BLMF"
	bloomTrailerSize   int64   = 12         // BloomLen(8) + Magic(4)
	defaultBloomFPRate float64 = 0.01
	// maxBloomSectionBytes 限制单文件布隆段大小，防止损坏的 bloomLen 触发
	// 超大分配或 int64 溢出导致的非法负偏移。
	maxBloomSectionBytes uint64 = 1 << 30 // 1 GiB
	// tombstoneValLen 作为 value 长度哨兵标记墓碑（删除标记）：磁盘上仅写该长度、
	// 不写 value 字节，读侧据此还原为 nil。正常 value 长度不可能取此值，且老格式
	// 文件永不含此哨兵，向后兼容。约定：内存与磁盘均以 Value==nil 表示墓碑。
	tombstoneValLen uint32 = 0xFFFFFFFF
)

type BlockIndexEntry struct {
	LastKey     []byte
	BlockOffset int64
}

// blockIndex 单个 SSTable 文件的块索引及数据区结束偏移。
//
// 磁盘上的索引只记录每块起始偏移，故块长度由「下一块起始 − 本块起始」推出，最后一块
// 的结束即数据区结束（footer 中的 indexOffset）。缓存 dataEnd 使块长度完全可计算，
// 从而支持按精确长度单次读取整块，无需变更磁盘格式。
type blockIndex struct {
	entries []BlockIndexEntry
	dataEnd int64
}

// blockExtent 返回第 i 块在文件中的 [start,end) 字节范围。
func (bi *blockIndex) blockExtent(i int) (int64, int64) {
	start := bi.entries[i].BlockOffset
	if i+1 < len(bi.entries) {
		return start, bi.entries[i+1].BlockOffset
	}
	return start, bi.dataEnd
}

var _ ISSTable = &SSTable{}

type SSTable struct {
	// dir 是 SSTable 文件目录，构造时从 config 快照一份。构造在主 goroutine 完成，之后
	// 后台 goroutine（如 LoadSSTableMetaList、Flush、Merge）读 ss.dir 而非全局 config.G，
	// 避免与（测试中）并发修改全局配置形成数据竞争。
	dir string

	// metas 是元数据主副本，仅在持 mu 时修改；每次修改后即时发布一份不可变快照到 snapshot。
	// 读路径（每次点查都要遍历元数据）从 snapshot 无锁读取，避免逐次加锁与整表拷贝。
	// 写侧为 copy-on-write：变更频率为刷盘/合并级，远低于读。
	// 快照按值不可变——调用方只可读取，不得原地修改其元素顺序或内容。
	metas      []*SSTableMeta
	snapshot   atomic.Pointer[[]*SSTableMeta]
	mu         sync.RWMutex
	indexCache map[string]*blockIndex
	idxMu      sync.RWMutex
	bloomCache map[string]*PartitionedBloom // 值为 nil 表示老格式(已确认无布隆)
	bloomMu    sync.RWMutex

	// fdCache 按路径复用的常驻只读文件句柄，避免每次点查都 open/close 一次文件。
	// 读路径一律使用 ReadAt：其不依赖文件内偏移，故同一句柄可被任意多个并发读者共享。
	// 缓存规模由 SSTable 文件数界定（compaction 持续收敛文件数）；句柄在 DeleteSSTable
	// 中关闭并剔除，否则每轮 compaction 泄漏一个 fd。
	fdCache map[string]*os.File
	fdMu    sync.RWMutex

	// blocks 缓存已读取的数据块，使热数据点查不再触达磁盘、也不再逐次分配整块缓冲。
	// 预算取自 config.BlockCacheBytes（<=0 关闭，此时为 nil，读路径退化为每次 ReadAt）。
	blocks *blockCache
}

func NewSSTable() *SSTable {
	ss := &SSTable{
		dir:        config.G.SSTablePath,
		metas:      make([]*SSTableMeta, 0),
		indexCache: make(map[string]*blockIndex),
		bloomCache: make(map[string]*PartitionedBloom),
		fdCache:    make(map[string]*os.File),
		blocks:     newBlockCache(config.G.BlockCacheBytes),
	}
	ss.publishMetas()
	return ss
}

// publishMetas 发布 metas 的不可变快照供读路径无锁获取。调用方须持 ss.mu。
func (ss *SSTable) publishMetas() {
	snap := make([]*SSTableMeta, len(ss.metas))
	copy(snap, ss.metas)
	ss.snapshot.Store(&snap)
}

// openFile 取该路径的常驻只读句柄，未缓存则打开并缓存。返回 nil 表示打开失败。
func (ss *SSTable) openFile(path string) *os.File {
	ss.fdMu.RLock()
	f, ok := ss.fdCache[path]
	ss.fdMu.RUnlock()
	if ok {
		return f
	}

	ss.fdMu.Lock()
	defer ss.fdMu.Unlock()
	if f, ok := ss.fdCache[path]; ok { // 双检：并发者可能已填入
		return f
	}
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	ss.fdCache[path] = f
	return f
}

// closeFile 关闭并剔除该路径的常驻句柄（文件被删除时调用）。
func (ss *SSTable) closeFile(path string) {
	ss.fdMu.Lock()
	f, ok := ss.fdCache[path]
	delete(ss.fdCache, path)
	ss.fdMu.Unlock()
	if ok {
		f.Close()
	}
}

func (ss *SSTable) LoadSSTableMetaList() {
	dir := ss.dir

	if err := os.MkdirAll(dir, dirPerm); err != nil {
		slog.Error("cannot create SSTable directory", "error", err)
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("cannot read SSTable directory", "error", err)
		return
	}

	metas := make([]*SSTableMeta, 0)
	count := 0

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue
		}

		fullPath := filepath.Join(dir, entry.Name())

		file, err := os.Open(fullPath)
		if err != nil {
			slog.Warn("failed to open SSTable", "file", entry.Name(), "error", err)
			continue
		}

		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			slog.Warn("failed to read key length", "file", entry.Name(), "error", err)
			file.Close()
			continue
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			slog.Warn("failed to read key", "file", entry.Name(), "error", err)
			file.Close()
			continue
		}

		file.Close()

		info, err := os.Stat(fullPath)
		if err != nil {
			slog.Warn("failed to stat SSTable", "file", entry.Name(), "error", err)
			continue
		}

		meta := &SSTableMeta{
			Level:        parseLevelFromName(entry.Name()), // 从文件名恢复 level，避免重启塌缩到 L0
			Filepath:     fullPath,
			MinKey:       keyBytes,
			MaxKey:       nil,
			Size:         info.Size(),
			MaxKeyLoaded: false,
		}

		// 从块索引末项直接取 MaxKey（新格式）。不能用 EnsureMeta 的顺序扫描：它把数据段
		// 之后的块索引/布隆/footer 也当记录读，导致 MaxKey 错乱（实测退化成空串），
		// 使 getFromSSTables 的 [MinKey,MaxKey] 范围过滤把命中 key 整段跳过 → 重启后
		// 已刷盘数据全部读不到。老格式无 footer，loadBlockIndexFromFile 返回 nil，
		// 保留 MaxKeyLoaded=false 由 EnsureMeta 顺序扫描兜底（对纯数据文件正确）。
		if idx := ss.loadBlockIndexFromFile(fullPath); idx != nil && len(idx.entries) > 0 {
			meta.MaxKey = idx.entries[len(idx.entries)-1].LastKey
			meta.MaxKeyLoaded = true
		}

		metas = append(metas, meta)
		count++
	}

	// 按创建时间戳升序重建 metas，等价于内存中的创建（append）序——读路径靠 metas 逆序判定
	// newest-wins，按文件名字符串排序会让旧 merged 盖过新 L0 而返回陈旧值。时间戳相同时
	// 退回文件名比较以保证确定性。
	sort.Slice(metas, func(i, j int) bool {
		ti := parseCreateTsFromName(filepath.Base(metas[i].Filepath))
		tj := parseCreateTsFromName(filepath.Base(metas[j].Filepath))
		if ti != tj {
			return ti < tj
		}
		return metas[i].Filepath < metas[j].Filepath
	})

	ss.mu.Lock()
	ss.metas = metas
	ss.publishMetas()
	ss.mu.Unlock()

	for _, meta := range metas {
		go meta.EnsureMeta()
		go ss.getBlockIndex(meta.Filepath) // 异步预热块索引
		go ss.getBloom(meta.Filepath)      // 异步预热布隆过滤器
	}

	slog.Info("SSTable index loaded", "files", count, "dir", dir)
}

// WriteToSSTable 将有序 entries 写入 SSTable 文件（含块索引）
func (ss *SSTable) WriteToSSTable(entries []LogEntry) error {
	if len(entries) == 0 {
		return ErrNoEntries
	}

	// L0：memtable 刷盘落到 level 0；level 编进文件名以便重启恢复。
	filename := fmt.Sprintf("sstable_L0_%d.sst", time.Now().UnixNano())
	dir := ss.dir
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("create data directory failed: %v", err)
	}
	fullPath := filepath.Join(dir, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("create SSTable file failed: %v", err)
	}
	defer file.Close()

	// 构建数据 buffer + 块索引
	type blk struct {
		lastKey     []byte
		blockOffset int64
	}
	var blockIdx []blk

	var buf bytes.Buffer
	for i, entry := range entries {
		bi := i / SSTableBlockSize
		if i%SSTableBlockSize == 0 {
			blockIdx = append(blockIdx, blk{blockOffset: int64(buf.Len())})
		}
		blockIdx[bi].lastKey = entry.Key

		binary.Write(&buf, binary.BigEndian, uint32(len(entry.Key)))
		buf.Write(entry.Key)
		if entry.Value == nil { // 墓碑：仅写哨兵长度，无 value 字节
			binary.Write(&buf, binary.BigEndian, tombstoneValLen)
		} else {
			binary.Write(&buf, binary.BigEndian, uint32(len(entry.Value)))
			buf.Write(entry.Value)
		}
	}

	// 写数据
	if _, err := file.Write(buf.Bytes()); err != nil {
		return err
	}

	// 写块索引: [LastKeyLen(4B)][LastKey][BlockOffset(8B)] × N
	indexStart, _ := file.Seek(0, io.SeekCurrent)
	for _, b := range blockIdx {
		binary.Write(file, binary.BigEndian, uint32(len(b.lastKey)))
		file.Write(b.lastKey)
		binary.Write(file, binary.BigEndian, b.blockOffset)
	}

	// 写布隆过滤器段（块索引之后、Footer 之前）
	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = entries[i].Key
	}
	pb, err := writeBloomSection(file, keys)
	if err != nil {
		return err
	}

	// 写 Footer: BlockCount(4B) + IndexOffset(8B) + Magic(4B)
	binary.Write(file, binary.BigEndian, uint32(len(blockIdx)))
	binary.Write(file, binary.BigEndian, indexStart)
	binary.Write(file, binary.BigEndian, indexFooterMagic)

	// 缓存块索引
	cache := make([]BlockIndexEntry, len(blockIdx))
	for i, b := range blockIdx {
		cache[i] = BlockIndexEntry{LastKey: b.lastKey, BlockOffset: b.blockOffset}
	}
	ss.idxMu.Lock()
	ss.indexCache[fullPath] = &blockIndex{entries: cache, dataEnd: indexStart}
	ss.idxMu.Unlock()

	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync SSTable file failed: %v", err)
	}
	ss.cacheBloom(fullPath, pb) // 落盘后再缓存

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat SSTable file failed: %v", err)
	}
	meta := &SSTableMeta{
		Level:        0,
		Filepath:     fullPath,
		MinKey:       entries[0].Key,
		MaxKey:       entries[len(entries)-1].Key,
		Size:         info.Size(),
		MaxKeyLoaded: true,
	}
	ss.AddMeta(meta)
	flushBytesWritten.Add(info.Size())
	return nil
}

// GetAllMetas 返回元数据的不可变快照，按落盘先后升序（最旧在前）。
// 无锁零拷贝；调用方只可读取，不得原地修改。切片元素是共享指针，非对象副本。
func (ss *SSTable) GetAllMetas() []*SSTableMeta {
	if snap := ss.snapshot.Load(); snap != nil {
		return *snap
	}
	return nil
}

func (ss *SSTable) ReadAllFromSSTable(filepath string) ([]*LogEntry, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 尝试读 Footer，确定数据结束位置（新格式有块索引在末尾）
	dataEnd := ss.readDataEndOffset(file)
	if dataEnd > 0 {
		file.Seek(0, io.SeekStart)
	}

	entries := make([]*LogEntry, 0)
	for {
		if dataEnd > 0 {
			pos, _ := file.Seek(0, io.SeekCurrent)
			if pos >= dataEnd {
				break
			}
		}

		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read key length: %v", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %v", err)
		}

		var valueLen uint32
		if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("failed to read value length: %v", err)
		}

		var valueBytes []byte // 墓碑(哨兵长度)还原为 nil，无 value 字节
		if valueLen != tombstoneValLen {
			valueBytes = make([]byte, valueLen)
			if _, err := io.ReadFull(file, valueBytes); err != nil {
				return nil, fmt.Errorf("failed to read value: %v", err)
			}
		}

		entry := &LogEntry{
			Key:   keyBytes,
			Value: valueBytes,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// readDataEndOffset 读 Footer 返回数据结束偏移，老格式返回 -1
func (ss *SSTable) readDataEndOffset(f *os.File) int64 {
	return sstableDataEnd(f)
}

func (ss *SSTable) ReadFromSSTable(filepath string, key []byte) ([]byte, bool) {
	// 布隆过滤器快速否决：明确不存在则直接返回，省去磁盘读
	if bloom := ss.getBloom(filepath); bloom != nil && !bloom.MayContain(key) {
		return nil, false
	}
	if idx := ss.getBlockIndex(filepath); idx != nil {
		return ss.searchBlock(filepath, key, idx)
	}
	// 老格式 fallback
	return ss.readFromSSTableFull(filepath, key)
}

// writeBloomSection 在块索引之后写入分区布隆过滤器及 trailer，返回构建的
// 过滤器供调用方在 file.Sync() 之后再写入缓存——避免崩溃时出现「缓存说有
// 但文件未落盘」的不一致。
// 文件布局: ...[BlockIndex][BloomBlob][BloomLen(8B)][BloomMagic(4B)][Footer]
func writeBloomSection(file *os.File, keys [][]byte) (*PartitionedBloom, error) {
	pb := BuildPartitionedBloom(keys, DefaultNamespaceSep, defaultBloomFPRate)
	blob := pb.Encode()
	if _, err := file.Write(blob); err != nil {
		return nil, err
	}
	if err := binary.Write(file, binary.BigEndian, uint64(len(blob))); err != nil {
		return nil, err
	}
	if err := binary.Write(file, binary.BigEndian, bloomTrailerMagic); err != nil {
		return nil, err
	}
	return pb, nil
}

// cacheBloom 将过滤器写入缓存（应在 file.Sync() 成功后调用）。
func (ss *SSTable) cacheBloom(fullPath string, pb *PartitionedBloom) {
	ss.bloomMu.Lock()
	ss.bloomCache[fullPath] = pb
	ss.bloomMu.Unlock()
}

// getBloom 从缓存取布隆过滤器；miss 时从文件加载（老格式返回并缓存 nil）。
func (ss *SSTable) getBloom(filepath string) *PartitionedBloom {
	ss.bloomMu.RLock()
	pb, ok := ss.bloomCache[filepath]
	ss.bloomMu.RUnlock()
	if ok {
		return pb
	}
	pb = ss.loadBloomFromFile(filepath)
	ss.bloomMu.Lock()
	ss.bloomCache[filepath] = pb // 可能为 nil(老格式)，缓存避免重复读盘
	ss.bloomMu.Unlock()
	return pb
}

// loadBloomFromFile 读取紧邻索引 Footer 之前的布隆 trailer 与 blob。
// 全程用 SeekEnd 负偏移定位，不依赖 Stat().Size()，从而消除
// 「Stat 取大小 → Seek 读内容」之间文件被改写/截断的竞争窗口。
// 老格式(无 trailer，magic 不匹配)或 bloomLen 越界返回 nil。
func (ss *SSTable) loadBloomFromFile(filepath string) *PartitionedBloom {
	f, err := os.Open(filepath)
	if err != nil {
		return nil
	}
	defer f.Close()

	// 布隆 trailer 紧邻 16B 索引 Footer 之前
	if _, err := f.Seek(-(indexFooterSize + bloomTrailerSize), io.SeekEnd); err != nil {
		return nil // 文件比 footer+trailer 还短(含老格式小文件)
	}
	var bloomLen uint64
	var magic uint32
	if err := binary.Read(f, binary.BigEndian, &bloomLen); err != nil {
		return nil
	}
	if err := binary.Read(f, binary.BigEndian, &magic); err != nil {
		return nil
	}
	if magic != bloomTrailerMagic || bloomLen == 0 || bloomLen > maxBloomSectionBytes {
		return nil // 老格式 magic 不匹配，或 bloomLen 损坏/越界
	}

	// blob 紧邻 trailer 之前，同样用 SeekEnd 负偏移定位
	if _, err := f.Seek(-(indexFooterSize + bloomTrailerSize + int64(bloomLen)), io.SeekEnd); err != nil {
		return nil
	}
	blob := make([]byte, bloomLen)
	if _, err := io.ReadFull(f, blob); err != nil {
		return nil
	}
	pb, err := DecodePartitionedBloom(blob, 0, defaultBloomFPRate)
	if err != nil {
		return nil
	}
	return pb
}

// getBlockIndex 从缓存取块索引，miss 时从文件加载
func (ss *SSTable) getBlockIndex(filepath string) *blockIndex {
	ss.idxMu.RLock()
	idx, ok := ss.indexCache[filepath]
	ss.idxMu.RUnlock()
	if ok {
		return idx
	}
	idx = ss.loadBlockIndexFromFile(filepath)
	if idx == nil {
		return nil
	}
	ss.idxMu.Lock()
	ss.indexCache[filepath] = idx
	ss.idxMu.Unlock()
	return idx
}

// loadBlockIndexFromFile 从文件末尾读取块索引及数据区结束偏移
func (ss *SSTable) loadBlockIndexFromFile(filepath string) *blockIndex {
	f, err := os.Open(filepath)
	if err != nil {
		return nil
	}
	defer f.Close()

	if _, err := f.Seek(-indexFooterSize, io.SeekEnd); err != nil {
		return nil
	}
	var blockCount uint32
	var indexOffset int64
	var magic uint32
	binary.Read(f, binary.BigEndian, &blockCount)
	binary.Read(f, binary.BigEndian, &indexOffset)
	binary.Read(f, binary.BigEndian, &magic)

	if magic != indexFooterMagic || blockCount == 0 || indexOffset <= 0 {
		return nil
	}

	if _, err := f.Seek(indexOffset, io.SeekStart); err != nil {
		return nil
	}
	entries := make([]BlockIndexEntry, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		var keyLen uint32
		if err := binary.Read(f, binary.BigEndian, &keyLen); err != nil {
			return nil
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			return nil
		}
		var offset int64
		if err := binary.Read(f, binary.BigEndian, &offset); err != nil {
			return nil
		}
		entries[i] = BlockIndexEntry{LastKey: key, BlockOffset: offset}
	}
	// indexOffset 既是块索引起点，也是数据区终点——即最后一块的结束偏移。
	return &blockIndex{entries: entries, dataEnd: indexOffset}
}

// searchBlock 二分块索引定位目标块，单次 ReadAt 读入整块后在内存中扫描。
// 每次点查恒定一次 read syscall，与块内条目数无关。
func (ss *SSTable) searchBlock(filepath string, key []byte, bi *blockIndex) ([]byte, bool) {
	idx := bi.entries
	lo, hi := 0, len(idx)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(key, idx[mid].LastKey) <= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo >= len(idx) {
		return nil, false
	}

	start, end := bi.blockExtent(lo)
	if end <= start {
		return nil, false
	}

	// 命中缓存的块由多个读者共享，故下方扫描只读、且命中时对 value 另行拷贝。
	buf, cached := ss.blocks.get(filepath, start)
	if !cached {
		f := ss.openFile(filepath)
		if f == nil {
			return nil, false
		}
		buf = make([]byte, end-start)
		// ReadAt 不使用文件内偏移，故共享句柄可被并发读者安全复用。
		if _, err := f.ReadAt(buf, start); err != nil {
			return nil, false
		}
		ss.blocks.put(filepath, start, buf)
	}

	for off := 0; off+4 <= len(buf); {
		kLen := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		if kLen < 0 || off+kLen+4 > len(buf) {
			break
		}
		k := buf[off : off+kLen]
		off += kLen
		vLen := binary.BigEndian.Uint32(buf[off:])
		off += 4

		tomb := vLen == tombstoneValLen
		var v []byte
		if !tomb { // 墓碑无 value 字节
			if off+int(vLen) > len(buf) {
				break
			}
			v = buf[off : off+int(vLen)]
			off += int(vLen)
		}

		cmp := bytes.Compare(k, key)
		if cmp == 0 {
			if tomb { // 命中墓碑：found 但已删除
				return nil, true
			}
			// 必须拷贝：v 是整块 buf 的子切片，直接返回会让整块常驻内存。
			// 用 make+copy 而非 append([]byte(nil), ...)：后者在 v 为空时返回 nil，
			// 会把「空 value」误变成「墓碑」（约定 nil=墓碑）。
			out := make([]byte, len(v))
			copy(out, v)
			return out, true
		}
		if cmp > 0 {
			break
		}
	}
	return nil, false
}

// readFromSSTableFull 老格式文件全量读取（兼容）
func (ss *SSTable) readFromSSTableFull(filepath string, key []byte) ([]byte, bool) {
	entries, _ := ss.ReadAllFromSSTable(filepath)
	for _, entry := range entries {
		if bytes.Equal(entry.Key, key) {
			return entry.Value, true
		}
	}
	return nil, false
}

// 合并多个 SSTable 文件。
//
// 【正确性不变量 / 慎改】同 key 的 tie-break 由 files 的 srcIdx 决定（越大越新，见下），
// 合并输出文件用 time.Now() 打新 ts。这依赖调用方 CompactSSTable「合并整层全部文件」：
// 因此同 key 的多个版本要么都在本次输入里（srcIdx 定新旧），要么更新的版本在更浅层的
// 更晚 ts 文件里——读路径据此按创建 ts 判 newest-wins（见 memtable.go getFromSSTables）。
// 若引入 overlap-aware / 部分选择或跨层合并，必须同时确保「低 level = 更新」进入 tie-break，
// 并把 recency 显式化（per-key 序号），否则合并会静默选中陈旧值。
func (ss *SSTable) MergeSSTable(files []*SSTableMeta, targetLevel int) *SSTableMeta {
	if len(files) == 0 {
		return nil
	}

	slog.Info("merging SSTable files", "files", len(files), "targetLevel", targetLevel)

	// 为每个源文件打开流式迭代器（srcIdx = 在 files 中的序号，越大越新）
	iters := make([]*sstableIterator, 0, len(files))
	for _, meta := range files {
		it, err := newSSTableIterator(meta.Filepath)
		if err != nil {
			slog.Error("failed to open SSTable iterator for merge", "file", meta.Filepath, "error", err)
			for _, opened := range iters {
				opened.Close()
			}
			return nil
		}
		iters = append(iters, it)
	}

	mi, err := newMergeIterator(iters)
	if err != nil {
		mi.Close()
		slog.Error("failed to init merge iterator", "error", err)
		return nil
	}
	defer mi.Close()

	// targetLevel 编进文件名，使重启（LoadSSTableMetaList）能恢复该文件的 level。
	filename := fmt.Sprintf("sstable_merged_L%d_%d.sst", targetLevel, time.Now().UnixNano())
	dir := ss.dir
	fullPath := filepath.Join(dir, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		slog.Error("failed to create merged SSTable", "error", err)
		return nil
	}
	defer file.Close()

	// K 路归并流式写出：value 直接落盘，仅累积块索引(每块一条)与 key(供布隆)，
	// 不再把全部源条目读入内存。
	type blk struct {
		lastKey     []byte
		blockOffset int64
	}
	var blockIdx []blk
	var keys [][]byte
	var minKey, maxKey []byte
	var dataOffset int64
	bw := bufio.NewWriter(file)

	count := 0
	for mi.Next() {
		k := mi.Key()
		v := mi.Value()
		bi := count / SSTableBlockSize
		if count%SSTableBlockSize == 0 {
			blockIdx = append(blockIdx, blk{blockOffset: dataOffset})
		}
		blockIdx[bi].lastKey = k

		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(len(k)))
		bw.Write(hdr[:])
		bw.Write(k)
		if v == nil { // 墓碑：写哨兵长度，无 value 字节
			binary.BigEndian.PutUint32(hdr[:], tombstoneValLen)
			bw.Write(hdr[:])
			dataOffset += int64(8 + len(k))
		} else {
			binary.BigEndian.PutUint32(hdr[:], uint32(len(v)))
			bw.Write(hdr[:])
			if _, werr := bw.Write(v); werr != nil {
				slog.Error("failed to write merged entry", "error", werr)
				return nil
			}
			dataOffset += int64(8 + len(k) + len(v))
		}

		keys = append(keys, k) // k 为迭代器新分配，安全持有
		if count == 0 {
			minKey = k
		}
		maxKey = k
		count++
	}
	if err := mi.Err(); err != nil {
		slog.Error("merge iteration failed", "error", err)
		return nil
	}
	if count == 0 {
		slog.Warn("no entries to merge")
		return nil
	}
	if err := bw.Flush(); err != nil {
		slog.Error("failed to flush merged data", "error", err)
		return nil
	}

	// 以下写尾与 WriteToSSTable 完全一致，保证字节布局相同、可被现有读路径读取
	indexStart, _ := file.Seek(0, io.SeekCurrent)
	for _, b := range blockIdx {
		binary.Write(file, binary.BigEndian, uint32(len(b.lastKey)))
		file.Write(b.lastKey)
		binary.Write(file, binary.BigEndian, b.blockOffset)
	}

	pb, err := writeBloomSection(file, keys)
	if err != nil {
		slog.Error("failed to write bloom for merged SSTable", "error", err)
		return nil
	}

	binary.Write(file, binary.BigEndian, uint32(len(blockIdx)))
	binary.Write(file, binary.BigEndian, indexStart)
	binary.Write(file, binary.BigEndian, indexFooterMagic)

	cache := make([]BlockIndexEntry, len(blockIdx))
	for i, b := range blockIdx {
		cache[i] = BlockIndexEntry{LastKey: b.lastKey, BlockOffset: b.blockOffset}
	}
	ss.idxMu.Lock()
	ss.indexCache[fullPath] = &blockIndex{entries: cache, dataEnd: indexStart}
	ss.idxMu.Unlock()

	if err := file.Sync(); err != nil {
		slog.Error("failed to sync merged SSTable", "error", err)
		return nil
	}
	ss.cacheBloom(fullPath, pb) // 落盘后再缓存

	info, err := file.Stat()
	if err != nil {
		slog.Error("failed to stat merged SSTable", "error", err)
		return nil
	}

	newMeta := &SSTableMeta{
		Level:        targetLevel,
		Filepath:     fullPath,
		MinKey:       minKey,
		MaxKey:       maxKey,
		Size:         info.Size(),
		MaxKeyLoaded: true,
	}

	ss.AddMeta(newMeta)
	compactionBytesWritten.Add(info.Size())
	slog.Info("SSTable merged", "level", targetLevel, "file", filename, "keys", count, "size", info.Size())

	return newMeta
}
func (ss *SSTable) AddMeta(meta *SSTableMeta) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.metas = append(ss.metas, meta)
	ss.publishMetas()
}

func (ss *SSTable) RemoveMeta(target *SSTableMeta) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for i, meta := range ss.metas {
		if meta == target {
			ss.metas = append(ss.metas[:i], ss.metas[i+1:]...)
			ss.publishMetas()
			return
		}
	}
}

// GetLevelFiles 获取指定层级的文件列表
func (ss *SSTable) GetLevelFiles(level int) []*SSTableMeta {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	var result []*SSTableMeta
	for _, meta := range ss.metas {
		if meta.Level == level {
			result = append(result, meta)
		}
	}
	return result
}

func (ss *SSTable) DeleteSSTable(meta *SSTableMeta) {
	if err := os.Remove(meta.Filepath); err != nil {
		slog.Warn("failed to delete SSTable", "file", meta.Filepath, "error", err)
	}
	// 与索引/布隆缓存同时剔除常驻句柄与已缓存数据块，否则每轮 compaction 泄漏一个 fd、
	// 并让已删文件的块长期占用缓存预算。
	ss.closeFile(meta.Filepath)
	ss.blocks.dropFile(meta.Filepath)
	ss.idxMu.Lock()
	delete(ss.indexCache, meta.Filepath)
	ss.idxMu.Unlock()
	ss.bloomMu.Lock()
	delete(ss.bloomCache, meta.Filepath)
	ss.bloomMu.Unlock()
}
