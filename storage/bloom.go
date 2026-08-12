package storage

import (
	"encoding/binary"
	"errors"
	"math"
)

// BloomFilter 标准布隆过滤器。
// 位数组大小 m 与哈希函数个数 k 由预期元素数 n 和目标误判率 p
// 按最优公式计算得出，避免拍脑袋取值导致空间浪费或误判率失控。
type BloomFilter struct {
	bits []uint64 // 位数组，每字 64 bit
	m    uint64   // 位数
	k    uint64   // 哈希函数个数
}

const (
	bloomMinBits        = 64
	fnvOffset64  uint64 = 14695981039346656037
	fnvPrime64   uint64 = 1099511628211
	// bloomSeed2 用于派生第二个独立哈希（Kirsch-Mitzenmacher 双哈希）
	bloomSeed2 uint64 = 0x9e3779b97f4a7c15
)

// NewBloomFilter 按 n 个元素、目标误判率 p 计算最优 m、k：
//
//	m = ceil(-n * ln(p) / (ln2)^2)   位数
//	k = round((m/n) * ln2)           哈希函数个数
//
// n<=0 视为 1；p 不在 (0,1) 区间时回退到 0.01。
func NewBloomFilter(n int, p float64) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	ln2 := math.Ln2
	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (ln2 * ln2)))
	if m < bloomMinBits {
		m = bloomMinBits
	}
	k := uint64(math.Round(float64(m) / float64(n) * ln2))
	if k < 1 {
		k = 1
	}
	return &BloomFilter{
		bits: make([]uint64, (m+63)/64),
		m:    m,
		k:    k,
	}
}

// fnv1a 带种子的 FNV-1a 64 位哈希，无内存分配。
func fnv1a(data []byte, seed uint64) uint64 {
	h := fnvOffset64 ^ seed
	for _, c := range data {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// hashPair 用两个独立种子的 FNV-1a 派生 (h1, h2)，
// 后续位置由 h1 + i*h2 给出（双哈希模拟 k 个独立哈希）。
func hashPair(key []byte) (uint64, uint64) {
	return fnv1a(key, 0), fnv1a(key, bloomSeed2)
}

// Add 插入一个 key。
func (b *BloomFilter) Add(key []byte) {
	h1, h2 := hashPair(key)
	for i := uint64(0); i < b.k; i++ {
		pos := (h1 + i*h2) % b.m
		b.bits[pos/64] |= 1 << (pos % 64)
	}
}

// MayContain 判断 key 是否可能存在。
// 返回 false 一定不存在；返回 true 可能存在（存在误判率 p）。
func (b *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := hashPair(key)
	for i := uint64(0); i < b.k; i++ {
		pos := (h1 + i*h2) % b.m
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// Encode 序列化为字节：[m(8B)][k(8B)][bits...]，大端。
func (b *BloomFilter) Encode() []byte {
	out := make([]byte, 16+len(b.bits)*8)
	binary.BigEndian.PutUint64(out[0:8], b.m)
	binary.BigEndian.PutUint64(out[8:16], b.k)
	for i, w := range b.bits {
		binary.BigEndian.PutUint64(out[16+i*8:], w)
	}
	return out
}

// DecodeBloomFilter 从 Encode 的字节还原。
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, errors.New("bloom: data too short")
	}
	m := binary.BigEndian.Uint64(data[0:8])
	k := binary.BigEndian.Uint64(data[8:16])
	words := (m + 63) / 64
	if k == 0 || m == 0 || uint64(len(data)-16) != words*8 {
		return nil, errors.New("bloom: corrupt data")
	}
	bits := make([]uint64, words)
	for i := range bits {
		bits[i] = binary.BigEndian.Uint64(data[16+i*8:])
	}
	return &BloomFilter{bits: bits, m: m, k: k}, nil
}
