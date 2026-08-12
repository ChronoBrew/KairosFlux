package storage

import (
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
	f       *os.File
	dataEnd int64 // 数据区结束偏移；<=0 表示读到 EOF（兼容无 Footer 的老文件）
	key     []byte
	value   []byte
	err     error
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
	return &sstableIterator{f: f, dataEnd: dataEnd}, nil
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
	if it.err != nil {
		return false
	}
	if it.dataEnd > 0 {
		pos, err := it.f.Seek(0, io.SeekCurrent)
		if err != nil {
			it.err = err
			return false
		}
		if pos >= it.dataEnd {
			return false
		}
	}

	var keyLen uint32
	if err := binary.Read(it.f, binary.BigEndian, &keyLen); err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(it.f, key); err != nil {
		it.err = err
		return false
	}
	var valLen uint32
	if err := binary.Read(it.f, binary.BigEndian, &valLen); err != nil {
		it.err = err
		return false
	}
	if valLen == tombstoneValLen { // 墓碑：无 value 字节，value 还原为 nil
		it.key = key
		it.value = nil
		return true
	}
	val := make([]byte, valLen)
	if _, err := io.ReadFull(it.f, val); err != nil {
		it.err = err
		return false
	}
	it.key = key
	it.value = val
	return true
}

func (it *sstableIterator) Key() []byte   { return it.key }
func (it *sstableIterator) Value() []byte { return it.value }
func (it *sstableIterator) Err() error    { return it.err }
func (it *sstableIterator) Close() error  { return it.f.Close() }
