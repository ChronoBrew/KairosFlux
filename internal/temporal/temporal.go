// Package temporal 定义 KairosFlux 时态内核（M0）的版本化记录语义。
//
// 核心思想（M0）：
//   - 一条逻辑记录用 logical key 标识，例如 "quote:2026-08-17:600000"。
//   - 每次写入产生一条不可变版本，落盘到版本化存储键：
//     "quote:2026-08-17:600000:v0000000000000003"（seq 定宽 20 位十进制）。
//   - seq 最大（最新写入）的版本即当前版本；`:current` 指针键记录当前版本号与
//     负载指纹，供快速 GET 与对账。
//   - as-of 查询返回“写入时间 <= as_of”的版本中 seq 最大的那一条——回答
//     “在某个时刻，系统当时知道什么”，不带任何未来信息。
//   - 任何状态都可从版本集合重放出来，并用本包提供的确定性指纹校验（自校验）。
//
// 本包刻意保持纯净：不碰存储、协议、IO。语义先立住，再由 router/storage/客户端
// 共享同一份实现，避免“同一套语义在多个地方各写一遍”的分叉。
package temporal

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// versionSep 分隔 logical key 与版本号。用 ":v" 而非 ":"，避免与业务 key 中的
// ":" 混淆，也便于 ParseVersionStorageKey 用 LastIndex 精确切分。
const versionSep = ":v"

// versionSeqWidth 版本号定宽位数：20 位十进制足够 10^20 次写入，且保证版本键的
// 字典序 = 数值序（LSM 扫描天然按版本升序返回）。
const versionSeqWidth = 20

// Version 是逻辑记录的一个不可变版本。
type Version struct {
	LogicalKey string // 如 "quote:2026-08-17:600000"
	Seq        uint64 // 写入序号，同一 logical key 内严格递增（总序）
	WriteNanos int64  // 写入时刻（unix nanos），as-of 判定用
	Payload    []byte // 该版本的内容
}

// VersionStorageKey 返回某版本在存储层的键。
func VersionStorageKey(logical string, seq uint64) string {
	return logical + versionSep + fmt.Sprintf("%0*d", versionSeqWidth, seq)
}

// ParseVersionStorageKey 从存储键切回 logical key 与版本号。不合法返回 ok=false。
func ParseVersionStorageKey(storageKey string) (logical string, seq uint64, ok bool) {
	i := strings.LastIndex(storageKey, versionSep)
	if i < 0 {
		return "", 0, false
	}
	seqStr := storageKey[i+len(versionSep):]
	if len(seqStr) != versionSeqWidth {
		return "", 0, false
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return storageKey[:i], seq, true
}

// currentSuffix 是 :current 指针键相对逻辑键的后缀。与 versionSep(":v") 刻意
// 首字符不同（'c' vs 'v'），保证 :current 键在字典序上排在同一逻辑键的所有
// 版本键之前（见 VersionStorageKeyLowerBound/UpperBound 的范围扫描）。
const currentSuffix = ":current"

// CurrentStorageKey 返回逻辑记录的“当前版本指针”存储键。
func CurrentStorageKey(logical string) string {
	return logical + currentSuffix
}

// IsCurrentStorageKey 报告 storageKey 是否是某个逻辑键的 :current 指针键。
// 供 SCAN 过滤内部键使用（版本化记录对业务 SCAN 不可见，见 M0 RFC）。
func IsCurrentStorageKey(storageKey string) bool {
	return strings.HasSuffix(storageKey, currentSuffix) && len(storageKey) > len(currentSuffix)
}

// VersionStorageKeyLowerBound/VersionStorageKeyUpperBound 返回某逻辑键全部版本
// 存储键的闭区间扫描边界（seq 从 0 到 uint64 最大值），供 LIST_VERSIONS/
// GET_AS_OF/REPLAY_FINGERPRINT 按 [start,end] 精确圈定该逻辑键的版本键、不
// 依赖裸前缀匹配（裸前缀在逻辑键本身含冒号时会有歧义，定宽区间不会）。
func VersionStorageKeyLowerBound(logical string) string {
	return VersionStorageKey(logical, 0)
}

func VersionStorageKeyUpperBound(logical string) string {
	return VersionStorageKey(logical, math.MaxUint64)
}

// CurrentValue 是 :current 指针的内容：当前版本号 + 负载指纹。
// 指纹用于对账：从版本集合重放出的最新负载指纹必须与指针一致。
type CurrentValue struct {
	Seq         uint64
	PayloadHash string
}

// PayloadHash 返回负载的 sha256 十六进制摘要。
func (v Version) PayloadHash() string {
	sum := sha256.Sum256(v.Payload)
	return hex.EncodeToString(sum[:])
}

// Latest 返回 seq 最大的版本（当前版本）。空集合返回 false。
func Latest(versions []Version) (Version, bool) {
	if len(versions) == 0 {
		return Version{}, false
	}
	best := versions[0]
	for _, v := range versions[1:] {
		if v.Seq > best.Seq {
			best = v
		}
	}
	return best, true
}

// AsOf 返回“写入时间 <= asOfNanos”的版本中 seq 最大的那一条。
// 这是 PIT 查询的核心语义：回答“当时系统知道什么”，绝不返回未来写入。
func AsOf(versions []Version, asOfNanos int64) (Version, bool) {
	var best Version
	found := false
	for _, v := range versions {
		if v.WriteNanos <= asOfNanos && (!found || v.Seq > best.Seq) {
			best = v
			found = true
		}
	}
	return best, found
}

// Entry 是参与状态指纹的规范条目。
type Entry struct {
	LogicalKey string
	Seq        uint64
	Payload    []byte
}

// Fingerprint 返回一组条目的确定性 sha256 摘要：
//   - 按 (LogicalKey, Seq) 排序，与输入顺序无关；
//   - 每条写成 "logical|seq|payloadLen|" + payload + "\n"，长度前缀消除
//     “键/负载边界模糊”导致的碰撞歧义。
//
// 用途：重放全量版本后对最新状态做指纹，与 :current 指针比对；也用于跨进程/
// 跨机器验证“同一份账本是否产生同一状态”。
func Fingerprint(entries []Entry) string {
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].LogicalKey != sorted[j].LogicalKey {
			return sorted[i].LogicalKey < sorted[j].LogicalKey
		}
		return sorted[i].Seq < sorted[j].Seq
	})
	h := sha256.New()
	for _, e := range sorted {
		fmt.Fprintf(h, "%s|%d|%d|", e.LogicalKey, e.Seq, len(e.Payload))
		h.Write(e.Payload)
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// EncodeVersionValue 编码某版本存储键落盘的 value：[writeNanos u64 LE][payload]。
// 版本键只有 payload 是业务负责的内容，写入时刻必须与它一起落盘才能支持
// AsOf 判定（AsOf 语义定义在 WriteNanos 上，见 AsOf 函数），而存储层是无结构
// 的字节 KV，故由本函数把两者打包进一份 value。
func EncodeVersionValue(writeNanos int64, payload []byte) []byte {
	buf := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(writeNanos))
	copy(buf[8:], payload)
	return buf
}

// DecodeVersionValue 是 EncodeVersionValue 的逆操作。长度不足 8 字节（不可能
// 是本函数编码的结果）返回 ok=false。
func DecodeVersionValue(raw []byte) (writeNanos int64, payload []byte, ok bool) {
	if len(raw) < 8 {
		return 0, nil, false
	}
	writeNanos = int64(binary.LittleEndian.Uint64(raw[0:8]))
	return writeNanos, raw[8:], true
}

// EncodeCurrentValue 编码 :current 指针落盘的 value：
// [seq u64 LE][hashLen u16 LE][payloadHash bytes]。hashLen 前缀而非定长，是
// 因为 CurrentValue.PayloadHash 的类型是十六进制字符串（与 Version.PayloadHash()
// 返回值一致，两处不做 hex↔raw 的额外换算），长度本应恒为 64，但显式前缀比
// "硬编码 64 且未来换指纹算法时静默错位" 更安全。
func EncodeCurrentValue(cv CurrentValue) []byte {
	buf := make([]byte, 8+2+len(cv.PayloadHash))
	binary.LittleEndian.PutUint64(buf[0:8], cv.Seq)
	binary.LittleEndian.PutUint16(buf[8:10], uint16(len(cv.PayloadHash)))
	copy(buf[10:], cv.PayloadHash)
	return buf
}

// DecodeCurrentValue 是 EncodeCurrentValue 的逆操作。
func DecodeCurrentValue(raw []byte) (CurrentValue, bool) {
	if len(raw) < 10 {
		return CurrentValue{}, false
	}
	seq := binary.LittleEndian.Uint64(raw[0:8])
	n := int(binary.LittleEndian.Uint16(raw[8:10]))
	if len(raw) < 10+n {
		return CurrentValue{}, false
	}
	return CurrentValue{Seq: seq, PayloadHash: string(raw[10 : 10+n])}, true
}
