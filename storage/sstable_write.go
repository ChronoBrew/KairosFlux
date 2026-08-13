// 本文件是 SSTable 的写路径：flush 落盘、写尾（块索引 + 布隆 + Footer）与 compaction 合并。
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
	"time"
)

func (ss *SSTable) WriteToSSTable(entries []LogEntry) error {
	if len(entries) == 0 {
		return ErrNoEntries
	}

	// L0：memtable flush 落到 level 0；level 编进文件名以便重启恢复。
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
	var blockIdx []blockMeta

	// 数据段先攒在内存 buffer 里再一次写出。此处的 binary.Write 不检查错误是安全的：
	// 目标是 bytes.Buffer，其 Write 按文档永不返回错误（容量不足时直接 panic）。
	var buf bytes.Buffer
	for i, entry := range entries {
		bi := i / SSTableBlockSize
		if i%SSTableBlockSize == 0 {
			blockIdx = append(blockIdx, blockMeta{blockOffset: int64(buf.Len())})
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

	// 数据区已写完，此处的偏移即块索引起点，也是读路径推算末块长度的依据。
	indexStart, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("locate index offset failed: %w", err)
	}

	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = entries[i].Key
	}
	pb, err := writeTail(file, blockIdx, keys, indexStart)
	if err != nil {
		return fmt.Errorf("write SSTable tail failed: %w", err)
	}

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
		Level:       0,
		Filepath:    fullPath,
		MinKey:      entries[0].Key,
		MaxKey:      entries[len(entries)-1].Key,
		Size:        info.Size(),
		MaxKeyKnown: true,
	}
	ss.AddMeta(meta)
	flushBytesWritten.Add(info.Size())
	return nil
}

// Metas 返回元数据的不可变快照，按落盘先后升序（最旧在前）。
// 无锁零拷贝；调用方只可读取，不得原地修改。切片元素是共享指针，非对象副本。
func writeBloomSection(w io.Writer, keys [][]byte) (*PartitionedBloom, error) {
	pb := BuildPartitionedBloom(keys, DefaultNamespaceSep, defaultBloomFPRate)
	blob := pb.Encode()
	if _, err := w.Write(blob); err != nil {
		return nil, err
	}
	if err := binary.Write(w, binary.BigEndian, uint64(len(blob))); err != nil {
		return nil, err
	}
	if err := binary.Write(w, binary.BigEndian, bloomTrailerMagic); err != nil {
		return nil, err
	}
	return pb, nil
}

// writeTail 写出 SSTable 的尾部三段：块索引、布隆过滤器、Footer。
//
// 由 WriteToSSTable 与 MergeSSTable 共用，二者的字节布局因此不可能漂移——读路径只有
// 一份解析实现，写路径也应只有一份。
//
// 每一次写入都检查错误。此前这里的 binary.Write/Write 返回值被丢弃，后果并非「少写了
// 几个字节」而是静默的数据丢失：尾部残缺的文件会让 footer magic 校验失败，重启时
// EnsureMeta 在新格式文件上算出错误的 MaxKey，[MinKey,MaxKey] 过滤随即跳过整个文件；
// 而调用方以为落盘成功——Flush 会丢弃内存副本，compaction 会删除源文件。
func writeTail(w io.Writer, blocks []blockMeta, keys [][]byte, indexStart int64) (*PartitionedBloom, error) {
	// 块索引: [LastKeyLen(4B)][LastKey][BlockOffset(8B)] × N
	for _, b := range blocks {
		if err := binary.Write(w, binary.BigEndian, uint32(len(b.lastKey))); err != nil {
			return nil, fmt.Errorf("write block index key length: %w", err)
		}
		if _, err := w.Write(b.lastKey); err != nil {
			return nil, fmt.Errorf("write block index key: %w", err)
		}
		if err := binary.Write(w, binary.BigEndian, b.blockOffset); err != nil {
			return nil, fmt.Errorf("write block index offset: %w", err)
		}
	}

	pb, err := writeBloomSection(w, keys)
	if err != nil {
		return nil, fmt.Errorf("write bloom section: %w", err)
	}

	// Footer: BlockCount(4B) + IndexOffset(8B) + Magic(4B)
	if err := binary.Write(w, binary.BigEndian, uint32(len(blocks))); err != nil {
		return nil, fmt.Errorf("write footer block count: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, indexStart); err != nil {
		return nil, fmt.Errorf("write footer index offset: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, indexFooterMagic); err != nil {
		return nil, fmt.Errorf("write footer magic: %w", err)
	}
	return pb, nil
}

// cacheBloom 将过滤器写入缓存（应在 file.Sync() 成功后调用）。
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
	var blockIdx []blockMeta
	var keys [][]byte
	var minKey, maxKey []byte
	var dataOffset int64
	// 数据段经 bufio 写出。循环内的 Write 不逐个检查错误是安全的：bufio.Writer 记住
	// 首个错误并在其后所有 Write 与 Flush 上返回它，而下方 bw.Flush() 的错误是被检查的。
	bw := bufio.NewWriter(file)

	count := 0
	for mi.Next() {
		k := mi.Key()
		v := mi.Value()
		bi := count / SSTableBlockSize
		if count%SSTableBlockSize == 0 {
			blockIdx = append(blockIdx, blockMeta{blockOffset: dataOffset})
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

	indexStart, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		slog.Error("failed to locate index offset for merged SSTable", "error", err)
		return nil
	}
	pb, err := writeTail(file, blockIdx, keys, indexStart)
	if err != nil {
		slog.Error("failed to write merged SSTable tail", "error", err)
		return nil
	}

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
		Level:       targetLevel,
		Filepath:    fullPath,
		MinKey:      minKey,
		MaxKey:      maxKey,
		Size:        info.Size(),
		MaxKeyKnown: true,
	}

	ss.AddMeta(newMeta)
	compactionBytesWritten.Add(info.Size())
	slog.Info("SSTable merged", "level", targetLevel, "file", filename, "keys", count, "size", info.Size())

	return newMeta
}
