package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// sstableIterator 顺序流式读取单个 SSTable 文件数据区的 (key,value)，key 升序。
//
// 不变量：每次 Next 都为 key/value 分配**新**切片（不复用缓冲区），因此调用方
// 可以安全持有上一次 Next 返回的 key/value——K 路归并的最小堆依赖此性质。
// 若未来为了性能改成复用单个缓冲区，会静默破坏堆中已入队的元素。
type sstableIterator struct {
	f *os.File
	// r 是带缓冲的读取器。此前逐字段直接从 *os.File 读（键长、键、值长、值），每条记录
	// 约四次 read syscall；一层缓冲即把它摊薄到每若干条一次内核往返。
	r *bufio.Reader
	// pos 是已从文件消费的字节偏移。加了缓冲后不能再用 Seek(0, SeekCurrent) 判断位置——
	// 那返回的是底层文件偏移，已被预读推到缓冲区末尾，会让迭代提前判定越过 dataEnd。
	pos int64
	// exhausted 表示已确定该文件不含目标范围内的条目，Next 直接返回 false。
	exhausted bool
	dataEnd   int64 // 数据区结束偏移；<=0 表示读到 EOF（兼容无 Footer 的老文件）
	key       []byte
	value     []byte
	err       error
}

// iterReadBufSize 是迭代器的读缓冲大小。取值需覆盖若干条记录，使内核往返次数与记录数解耦。
const iterReadBufSize = 64 << 10

// newSSTableIteratorFrom 打开文件并借块索引直接跳到可能含 start 的那一块，避免从文件头
// 顺序跳过。
//
// 这对按游标推进的扫描是决定性的：投递每取一批就以新游标再扫一次，若每次都从头顺序跳到
// 游标处，总代价随数据量平方增长。二分块索引后每次只从目标块开始读。
//
// 无块索引（老格式或尾部残缺）时退回从头读，由调用方的范围裁剪保证正确性。
func newSSTableIteratorFrom(ss *SSTable, path string, start []byte) (*sstableIterator, error) {
	it, err := newSSTableIterator(path)
	if err != nil {
		return nil, err
	}
	if len(start) == 0 {
		return it, nil
	}
	bi := ss.getBlockIndex(path)
	if bi == nil || len(bi.entries) == 0 {
		return it, nil
	}
	// 二分找第一个 lastKey >= start 的块：start 只可能落在该块或其后。
	lo, hi := 0, len(bi.entries)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(start, bi.entries[mid].LastKey) <= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if bytes.Compare(start, bi.entries[lo].LastKey) > 0 {
		// 越过最后一块的末键：该文件不含 >= start 的条目。
		it.exhausted = true
		return it, nil
	}
	off := bi.entries[lo].BlockOffset
	if _, err := it.f.Seek(off, io.SeekStart); err != nil {
		it.Close()
		return nil, err
	}
	it.r.Reset(it.f) // 缓冲里可能已预读了 seek 前的字节，必须丢弃
	it.pos = off
	return it, nil
}

// newSSTableIterator 打开文件并定位到数据区起点。
func newSSTableIterator(path string) (*sstableIterator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	dataEnd := sstableDataEnd(f)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &sstableIterator{f: f, r: bufio.NewReaderSize(f, iterReadBufSize), dataEnd: dataEnd}, nil
}

// sstableDataEnd 读 Footer 返回数据区结束偏移；老格式或异常返回 -1（读到 EOF）。
func sstableDataEnd(f *os.File) int64 {
	info, err := f.Stat()
	if err != nil || info.Size() < indexFooterSize {
		return -1
	}
	if _, err := f.Seek(-indexFooterSize, io.SeekEnd); err != nil {
		return -1
	}
	var blockCount uint32
	var indexOffset int64
	var magic uint32
	binary.Read(f, binary.BigEndian, &blockCount)
	binary.Read(f, binary.BigEndian, &indexOffset)
	binary.Read(f, binary.BigEndian, &magic)
	if magic == indexFooterMagic && blockCount > 0 && indexOffset > 0 {
		return indexOffset
	}
	return -1
}

// Next 前进到下一条；返回 false 表示已耗尽或出错（用 Err 区分）。
func (it *sstableIterator) Next() bool {
	if it.err != nil || it.exhausted {
		return false
	}
	if it.dataEnd > 0 && it.pos >= it.dataEnd {
		return false
	}

	var hdr [4]byte
	if _, err := io.ReadFull(it.r, hdr[:]); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			it.err = err
		}
		return false
	}
	keyLen := binary.BigEndian.Uint32(hdr[:])
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(it.r, key); err != nil {
		it.err = err
		return false
	}
	if _, err := io.ReadFull(it.r, hdr[:]); err != nil {
		it.err = err
		return false
	}
	valLen := binary.BigEndian.Uint32(hdr[:])

	if valLen == tombstoneValLen { // 墓碑：无 value 字节，value 还原为 nil
		it.pos += int64(8) + int64(keyLen)
		it.key = key
		it.value = nil
		return true
	}
	val := make([]byte, valLen)
	if _, err := io.ReadFull(it.r, val); err != nil {
		it.err = err
		return false
	}
	it.pos += int64(8) + int64(keyLen) + int64(valLen)
	it.key = key
	it.value = val
	return true
}

func (it *sstableIterator) Key() []byte   { return it.key }
func (it *sstableIterator) Value() []byte { return it.value }
func (it *sstableIterator) Err() error    { return it.err }
func (it *sstableIterator) Close() error  { return it.f.Close() }

// sliceIterator 在一份已排序的条目切片上迭代，供内存表参与归并。
//
// 为何先拷贝成切片而不直接遍历跳表：跳表由写入方并发改写，无锁遍历会跟到只连了一半的
// 节点（这正是 Get 曾经的缺陷）。而归并过程要读磁盘，若全程持锁则写入会停等 I/O。
// 折中是在锁内把范围内的条目拷出来，锁外做归并——拷贝量由内存表大小天然有界。
type sliceIterator struct {
	entries []LogEntry
	pos     int
}

func newSliceIterator(entries []LogEntry) *sliceIterator {
	return &sliceIterator{entries: entries, pos: -1}
}

func (it *sliceIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

func (it *sliceIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return nil
	}
	return it.entries[it.pos].Key
}

func (it *sliceIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return nil
	}
	return it.entries[it.pos].Value
}

func (it *sliceIterator) Err() error   { return nil }
func (it *sliceIterator) Close() error { return nil }

// rangeIterator 把底层源裁剪到 [start,end] 闭区间：跳过小于 start 的条目，越过 end 即结束。
// start/end 为 nil 表示该侧不限。
type rangeIterator struct {
	src   entryIterator
	start []byte
	end   []byte
	done  bool
}

func newRangeIterator(src entryIterator, start, end []byte) *rangeIterator {
	return &rangeIterator{src: src, start: start, end: end}
}

func (it *rangeIterator) Next() bool {
	if it.done {
		return false
	}
	for it.src.Next() {
		k := it.src.Key()
		if len(it.start) > 0 && bytes.Compare(k, it.start) < 0 {
			continue // 尚未到下界
		}
		if len(it.end) > 0 && bytes.Compare(k, it.end) > 0 {
			it.done = true // 源为升序，越过上界即可停
			return false
		}
		return true
	}
	it.done = true
	return false
}

func (it *rangeIterator) Key() []byte   { return it.src.Key() }
func (it *rangeIterator) Value() []byte { return it.src.Value() }
func (it *rangeIterator) Err() error    { return it.src.Err() }
func (it *rangeIterator) Close() error  { return it.src.Close() }
