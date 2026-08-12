package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// DefaultNamespaceSep 是默认的命名空间分隔符。
// 数据仓库场景下 key 常带"仓库"前缀，如 "log:2026-05-31"、"order:10086"。
const DefaultNamespaceSep = ':'

// PartitionedBloom 按 key 的命名空间前缀（"仓库"）分区，每个仓库一个
// 独立的 BloomFilter，例如 "log"、"order" 各自一份，互不污染。
//
// 分区内部采用「前缀删除」：把仓库前缀去掉后只对区分性后缀做哈希。
// 数据仓库里同一仓库的 key 高度相似（共享长前缀），删除冗余前缀既省去
// 无意义的哈希输入，又让命名空间本身成为第一道过滤——查 "order:x" 时
// 若 order 仓库根本不存在，可立即返回 false，不会被 log 仓库的 key 干扰。
type PartitionedBloom struct {
	sep        byte
	n          int     // 每个分区的预期元素数
	p          float64 // 每个分区的目标误判率
	partitions map[string]*BloomFilter
}

// NewPartitionedBloom 创建分区布隆过滤器。
// nPerPartition / p 用于按需创建各分区的子过滤器（最优 m、k 计算）。
func NewPartitionedBloom(sep byte, nPerPartition int, p float64) *PartitionedBloom {
	return &PartitionedBloom{
		sep:        sep,
		n:          nPerPartition,
		p:          p,
		partitions: make(map[string]*BloomFilter),
	}
}

// splitNamespace 以首个 sep 为界拆出 (命名空间, 后缀)。
// 无 sep 时命名空间为空串（默认仓库），后缀为整个 key。
func splitNamespace(key []byte, sep byte) (string, []byte) {
	if i := bytes.IndexByte(key, sep); i >= 0 {
		return string(key[:i]), key[i+1:]
	}
	return "", key
}

// BuildPartitionedBloom 根据全部 key 一次性构建：先按仓库预统计元素数，
// 再为每个仓库按其各自的 n 计算最优 m、k，最后前缀删除后插入。
// 适合 SSTable 这类 key 集合已知且构建后不再变更的场景，sizing 比 Add
// 逐个懒创建（共用一个 n）更精确。
func BuildPartitionedBloom(keys [][]byte, sep byte, p float64) *PartitionedBloom {
	counts := make(map[string]int)
	for _, k := range keys {
		ns, _ := splitNamespace(k, sep)
		counts[ns]++
	}
	pb := &PartitionedBloom{
		sep:        sep,
		n:          len(keys), // 后续若再 Add 的兜底容量
		p:          p,
		partitions: make(map[string]*BloomFilter),
	}
	for ns, c := range counts {
		pb.partitions[ns] = NewBloomFilter(c, p)
	}
	for _, k := range keys {
		ns, suf := splitNamespace(k, sep)
		pb.partitions[ns].Add(suf)
	}
	return pb
}

// Add 按命名空间路由到对应分区，前缀删除后插入后缀。
func (pb *PartitionedBloom) Add(key []byte) {
	ns, suffix := splitNamespace(key, pb.sep)
	bf := pb.partitions[ns]
	if bf == nil {
		bf = NewBloomFilter(pb.n, pb.p)
		pb.partitions[ns] = bf
	}
	bf.Add(suffix)
}

// MayContain 命名空间不存在直接返回 false；否则在对应分区查后缀。
func (pb *PartitionedBloom) MayContain(key []byte) bool {
	ns, suffix := splitNamespace(key, pb.sep)
	bf := pb.partitions[ns]
	if bf == nil {
		return false
	}
	return bf.MayContain(suffix)
}

// Namespaces 返回当前已建立的所有命名空间（仓库）。
func (pb *PartitionedBloom) Namespaces() []string {
	out := make([]string, 0, len(pb.partitions))
	for ns := range pb.partitions {
		out = append(out, ns)
	}
	return out
}

// Encode 序列化：[sep(1B)][partCount(4B)] 然后每个分区
// [nsLen(4B)][ns][bloomLen(4B)][bloomBytes]，大端。
func (pb *PartitionedBloom) Encode() []byte {
	var buf bytes.Buffer
	buf.WriteByte(pb.sep)
	binary.Write(&buf, binary.BigEndian, uint32(len(pb.partitions)))
	for ns, bf := range pb.partitions {
		binary.Write(&buf, binary.BigEndian, uint32(len(ns)))
		buf.WriteString(ns)
		enc := bf.Encode()
		binary.Write(&buf, binary.BigEndian, uint32(len(enc)))
		buf.Write(enc)
	}
	return buf.Bytes()
}

// DecodePartitionedBloom 从 Encode 的字节还原。n/p 仅用于后续新增分区，
// 已有分区从字节恢复其原始 m、k。
func DecodePartitionedBloom(data []byte, nPerPartition int, p float64) (*PartitionedBloom, error) {
	if len(data) < 5 {
		return nil, errors.New("partitioned bloom: data too short")
	}
	pb := NewPartitionedBloom(data[0], nPerPartition, p)
	r := bytes.NewReader(data[1:])
	var partCount uint32
	if err := binary.Read(r, binary.BigEndian, &partCount); err != nil {
		return nil, err
	}
	for i := uint32(0); i < partCount; i++ {
		var nsLen uint32
		if err := binary.Read(r, binary.BigEndian, &nsLen); err != nil {
			return nil, err
		}
		nsBytes := make([]byte, nsLen)
		if _, err := io.ReadFull(r, nsBytes); err != nil {
			return nil, err
		}
		var bloomLen uint32
		if err := binary.Read(r, binary.BigEndian, &bloomLen); err != nil {
			return nil, err
		}
		bloomBytes := make([]byte, bloomLen)
		if _, err := io.ReadFull(r, bloomBytes); err != nil {
			return nil, err
		}
		bf, err := DecodeBloomFilter(bloomBytes)
		if err != nil {
			return nil, err
		}
		pb.partitions[string(nsBytes)] = bf
	}
	return pb, nil
}
