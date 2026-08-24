package proto

import (
	"encoding/binary"
	"fmt"

	"github.com/ChronoBrew/KairosFlux/predicate"
)

// SCAN 请求负载布局（小端）：
//
//	[startLen u32][endLen u32][fieldLen u32][op u8][start][end][field][operand=剩余]
//
// SCAN 响应负载布局：
//
//	[statusLen u8][status][count u32]{ [keyLen u32][key][valueLen u32][value] }×count
const scanReqHeaderLen = 13 // 3×u32 + 1×u8

// ScanRequest 是一次边缘范围查询的参数。Start/End 为空表示该侧不限。
type ScanRequest struct {
	Start []byte
	End   []byte
	Pred  predicate.Predicate
}

// ScanEntry 是一条命中结果。
type ScanEntry struct {
	Key   []byte
	Value []byte
}

// EncodeScanRequest 编码 SCAN 请求负载。
func EncodeScanRequest(r ScanRequest) []byte {
	field := []byte(r.Pred.Field)
	operand := []byte(r.Pred.Operand)

	buf := make([]byte, scanReqHeaderLen+len(r.Start)+len(r.End)+len(field)+len(operand))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(r.Start)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(r.End)))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(field)))
	buf[12] = byte(r.Pred.Op)

	off := scanReqHeaderLen
	off += copy(buf[off:], r.Start)
	off += copy(buf[off:], r.End)
	off += copy(buf[off:], field)
	copy(buf[off:], operand)
	return buf
}

// DecodeScanRequest 解码 SCAN 请求负载。
func DecodeScanRequest(data []byte) (ScanRequest, error) {
	if len(data) < scanReqHeaderLen {
		return ScanRequest{}, fmt.Errorf("scan request too short: %d", len(data))
	}
	startLen := int(binary.LittleEndian.Uint32(data[0:4]))
	endLen := int(binary.LittleEndian.Uint32(data[4:8]))
	fieldLen := int(binary.LittleEndian.Uint32(data[8:12]))
	op := predicate.Op(data[12])

	off := scanReqHeaderLen
	if startLen < 0 || endLen < 0 || fieldLen < 0 || off+startLen+endLen+fieldLen > len(data) {
		return ScanRequest{}, fmt.Errorf("scan request length fields exceed payload")
	}
	start := data[off : off+startLen]
	off += startLen
	end := data[off : off+endLen]
	off += endLen
	field := data[off : off+fieldLen]
	off += fieldLen
	operand := data[off:]

	return ScanRequest{
		Start: start,
		End:   end,
		Pred:  predicate.Predicate{Field: string(field), Op: op, Operand: string(operand)},
	}, nil
}

// EncodeScanResponse 编码 SCAN 响应负载。
func EncodeScanResponse(status string, entries []ScanEntry) []byte {
	size := 1 + len(status) + 4
	for _, e := range entries {
		size += 8 + len(e.Key) + len(e.Value)
	}
	buf := make([]byte, size)
	buf[0] = byte(len(status))
	off := 1
	off += copy(buf[off:], status)
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(entries)))
	off += 4
	for _, e := range entries {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(e.Key)))
		off += 4
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(e.Value)))
		off += 4
		off += copy(buf[off:], e.Key)
		off += copy(buf[off:], e.Value)
	}
	return buf
}

// DecodeScanResponse 解码 SCAN 响应负载，返回状态与命中条目。
func DecodeScanResponse(payload []byte) (status string, entries []ScanEntry, err error) {
	if len(payload) < 1 {
		return "", nil, fmt.Errorf("scan response empty")
	}
	statusLen := int(payload[0])
	off := 1
	if off+statusLen+4 > len(payload) {
		return "", nil, fmt.Errorf("scan response too short for status+count")
	}
	status = string(payload[off : off+statusLen])
	off += statusLen
	count := int(binary.LittleEndian.Uint32(payload[off : off+4]))
	off += 4

	// count 是攻击者/故障对端可控的 u32，最大 ~42 亿。在验证到底有没有那么多字节之前
	// 就按 count 原样 make([]ScanEntry, 0, count) 预分配容量，是 TLV 解析器的经典
	// 内存放大漏洞：ScanEntry 在 64 位平台上是 48 字节（两个切片头），count 撑到
	// 0xFFFFFFFF 时这一行会尝试预留约 206GiB 容量。每个条目最少占 8 字节
	// （keyLen+valueLen 头），故 count 不可能超过剩余负载字节数/8——按这个真实
	// 上界夹住预分配容量，不管声明的 count 有多离谱，预分配都不会超过 payload 本身
	// 的大小量级。
	maxPossibleEntries := (len(payload) - off) / 8
	preAlloc := count
	if preAlloc > maxPossibleEntries {
		preAlloc = maxPossibleEntries
	}
	if preAlloc < 0 {
		preAlloc = 0
	}
	entries = make([]ScanEntry, 0, preAlloc)
	for i := 0; i < count; i++ {
		if off+8 > len(payload) {
			return "", nil, fmt.Errorf("scan response truncated at entry %d header", i)
		}
		keyLen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		valueLen := int(binary.LittleEndian.Uint32(payload[off+4 : off+8]))
		off += 8
		if keyLen < 0 || valueLen < 0 || off+keyLen+valueLen > len(payload) {
			return "", nil, fmt.Errorf("scan response truncated at entry %d body", i)
		}
		key := payload[off : off+keyLen]
		off += keyLen
		value := payload[off : off+valueLen]
		off += valueLen
		entries = append(entries, ScanEntry{Key: key, Value: value})
	}
	return status, entries, nil
}
