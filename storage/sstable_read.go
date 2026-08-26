// 本文件是 SSTable 的读路径：常驻句柄、块索引与布隆缓存、点查与全量读。
package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync/atomic"
)

func (bi *blockIndex) blockExtent(i int) (int64, int64) {
	start := bi.entries[i].BlockOffset
	if i+1 < len(bi.entries) {
		return start, bi.entries[i+1].BlockOffset
	}
	return start, bi.dataEnd
}

func (ss *SSTable) openFile(path string) *os.File {
	ss.fdMu.RLock()
	e, ok := ss.fdCache[path]
	if ok && !e.removed {
		ss.fdMu.RUnlock()
		return e.f
	}
	ss.fdMu.RUnlock()

	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	ss.fdMu.Lock()
	defer ss.fdMu.Unlock()
	if e, ok := ss.fdCache[path]; ok { // 双检：并发者可能已填入
		if e.removed {
			f.Close() // 打开期间文件被删：丢弃新句柄，行为同 ENOENT
			return nil
		}
		f.Close()
		return e.f
	}
	ss.fdCache[path] = &fdEntry{f: f}
	return f
}

// openRef 取该路径的常驻句柄并增加一个引用（扫描/归并迭代器持有）。返回错误表示
// 打开失败（含文件已删除的情形）。调用方必须保证每个成功返回都对应一次 closeRef，
// 错误路径同样——句柄生命周期下沉到 fdCache 的引用计数，正是 #52 的修复点。
func (ss *SSTable) openRef(path string) (*os.File, error) {
	ss.fdMu.RLock()
	e, ok := ss.fdCache[path]
	if ok && !e.removed {
		atomic.AddInt32(&e.refs, 1) // 快路径只持 RLock：refs 必须是原子（见 fdEntry 注释）
		ss.fdMu.RUnlock()
		return e.f, nil
	}
	ss.fdMu.RUnlock()

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	ss.fdMu.Lock()
	defer ss.fdMu.Unlock()
	if e, ok := ss.fdCache[path]; ok { // 双检：并发者可能已填入
		if e.removed {
			f.Close() // 打开期间文件被删：与 os.Open 的 ENOENT 行为一致
			return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
		}
		atomic.AddInt32(&e.refs, 1)
		return e.f, nil
	}
	ss.fdCache[path] = &fdEntry{f: f, refs: 1}
	return f, nil
}

// closeRef 释放一次引用。引用归零且文件已标记删除时关闭句柄并剔除缓存；否则句柄
// 继续常驻（供点读与后续迭代复用）。
func (ss *SSTable) closeRef(path string) {
	ss.fdMu.Lock()
	defer ss.fdMu.Unlock()
	e, ok := ss.fdCache[path]
	if !ok {
		return // 早已在 refs 归零时被关闭剔除
	}
	n := atomic.AddInt32(&e.refs, -1)
	if n <= 0 && e.removed {
		delete(ss.fdCache, path)
		e.f.Close()
	}
}

// closeFile 关闭并剔除该路径的常驻句柄（文件被删除时调用）。若仍有迭代器引用
// （并发扫描/归并正在读该文件），仅标记 removed，待最后一个引用释放后才真正关闭
// ——保持「unlink 后已打开的 fd 仍可读」的 POSIX 语义，与修复前各迭代器持独立
// 句柄时的行为等价。
func (ss *SSTable) closeFile(path string) {
	ss.fdMu.Lock()
	e, ok := ss.fdCache[path]
	if !ok {
		ss.fdMu.Unlock()
		return
	}
	e.removed = true
	if atomic.LoadInt32(&e.refs) == 0 {
		delete(ss.fdCache, path)
		e.f.Close()
	}
	ss.fdMu.Unlock()
}

func (ss *SSTable) ReadAllFromSSTable(filepath string) ([]*LogEntry, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 有 footer 时数据区的终点是已知的；没有 footer（老格式，或尾部残缺）则读到 EOF。
	//
	// 无论探测结果如何都必须把偏移移回开头：readDataEndOffset 为读 footer 已经 seek 到了
	// 文件末尾附近。此前这一步写在 dataEnd > 0 的条件里，于是无 footer 的文件从末尾开始
	// 解析，恒返回 0 条记录——老格式的全量读回退路径实际从未生效。
	dataEnd := ss.readDataEndOffset(file)
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// 解析不下去时就地停止，返回已解出的条目，而不是让整个读取失败。
	//
	// 这与 WAL.Replay 对撕裂尾写的处理一致，理由也相同：数据区总是先于尾部写出，故无法
	// 解析的字节只可能出现在有效数据之后。此前这里 return nil, err，而唯一的调用方
	// readFromSSTableFull 丢弃该错误后遍历 nil 切片——尾部一旦残缺，整个文件的 key 全部
	// 读成「不存在」，且不报错。
	entries := make([]*LogEntry, 0)
	for {
		if dataEnd > 0 {
			pos, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			if pos >= dataEnd {
				break
			}
		}

		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			break
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			break
		}

		var valueLen uint32
		if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
			break
		}
		var valueBytes []byte // 墓碑(哨兵长度)还原为 nil，无 value 字节
		if valueLen != tombstoneValLen {
			valueBytes = make([]byte, valueLen)
			if _, err := io.ReadFull(file, valueBytes); err != nil {
				break
			}
		}

		entries = append(entries, &LogEntry{Key: keyBytes, Value: valueBytes})
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

// MergeSSTable 把 files 归并为 targetLevel 上的一个新 SSTable，即 compaction 的落地实现。
//
// 正确性不变量（慎改）：同 key 的 tie-break 由 files 的 srcIdx 决定（越大越新，见下），
// compaction 输出文件用 time.Now() 打新 ts。这依赖调用方 CompactSSTable「一次 compaction 卷走整层全部文件」：
// 因此同 key 的多个版本要么都在本次输入里（srcIdx 定新旧），要么更新的版本在更浅层的
// 更晚 ts 文件里——读路径据此按创建 ts 判 newest-wins（见 memtable.go getFromSSTables）。
// 若引入 overlap-aware / 部分选择或跨层 compaction，必须同时确保「低 level = 更新」进入 tie-break，
// 并把 recency 显式化（per-key 序号），否则 compaction 会静默选中陈旧值。
