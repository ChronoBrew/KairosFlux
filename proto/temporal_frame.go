package proto

import "encoding/binary"

// Kair v2 时态内核新增 opcode（PUT_VERSIONED/GET_AS_OF/LIST_VERSIONS/
// REPLAY_FINGERPRINT，见 docs/rfc/时态内核-M0-版本化与as-of.md）的请求/响应
// 负载编解码。放在 proto 包而非 service：与 put_frame.go/scan.go 同一先例——
// codec 包只认 v2 帧 envelope，负载内部布局归 proto 包。

// DecodeAsOfFrame 解析 GET_AS_OF 请求负载：[keyLen u32 LE][key][asOfNanos u64 LE]。
// asOfNanos 是有符号 unix 纳秒时间戳，按位重新解释为 u64 存放（与
// EncodeAsOfFrame 互为逆操作）。
func DecodeAsOfFrame(data []byte) (key []byte, asOfNanos int64, ok bool) {
	if len(data) < 4 {
		return nil, 0, false
	}
	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if keyLen < 0 || 4+keyLen+8 > len(data) {
		return nil, 0, false
	}
	key = data[4 : 4+keyLen]
	asOfNanos = int64(binary.LittleEndian.Uint64(data[4+keyLen : 4+keyLen+8]))
	return key, asOfNanos, true
}

// EncodeAsOfFrame 是 DecodeAsOfFrame 的逆操作。
func EncodeAsOfFrame(key []byte, asOfNanos int64) []byte {
	buf := make([]byte, 4+len(key)+8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:], key)
	binary.LittleEndian.PutUint64(buf[4+len(key):4+len(key)+8], uint64(asOfNanos))
	return buf
}

// EncodeVersionEntry 编码单条版本记录：
// [seq u64 LE][writeNanos u64 LE][payloadLen u32 LE][payload]。
// GET_AS_OF 的 OK 响应体、LIST_VERSIONS 的 OK 响应体（重复此结构）共享同一
// 编码，两处只是"一条"与"若干条"的区别，不重复定义两套格式。
func EncodeVersionEntry(seq uint64, writeNanos int64, payload []byte) []byte {
	buf := make([]byte, 8+8+4+len(payload))
	binary.LittleEndian.PutUint64(buf[0:8], seq)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(writeNanos))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(payload)))
	copy(buf[20:], payload)
	return buf
}

// DecodeVersionEntry 解析 EncodeVersionEntry 编码的一条记录，返回消费的字节数
// consumed，供 DecodeListVersionsResponse 在同一个缓冲区里依次解出多条记录。
func DecodeVersionEntry(data []byte) (seq uint64, writeNanos int64, payload []byte, consumed int, ok bool) {
	if len(data) < 20 {
		return 0, 0, nil, 0, false
	}
	seq = binary.LittleEndian.Uint64(data[0:8])
	writeNanos = int64(binary.LittleEndian.Uint64(data[8:16]))
	n := int(binary.LittleEndian.Uint32(data[16:20]))
	if n < 0 || 20+n > len(data) {
		return 0, 0, nil, 0, false
	}
	return seq, writeNanos, data[20 : 20+n], 20 + n, true
}

// EncodeListVersionsResponse 编码 LIST_VERSIONS 的 OK 响应体：
// [count u32 LE][entry...]（entry 为 EncodeVersionEntry 编码）。空列表是合法
// 结果（count=0），不是错误——"这个逻辑键还没有任何版本"与"请求本身出错"是
// 两件事。
func EncodeListVersionsResponse(entries [][]byte) []byte {
	total := 4
	for _, e := range entries {
		total += len(e)
	}
	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(entries)))
	off := 4
	for _, e := range entries {
		copy(buf[off:], e)
		off += len(e)
	}
	return buf
}

// VersionEntryView 是 DecodeListVersionsResponse 解出的一条版本记录，供调用方
// （kairosflux-cli 等 v2 客户端）按字段读取，避免在调用点重复手写多返回值解构。
type VersionEntryView struct {
	Seq        uint64
	WriteNanos int64
	Payload    []byte
}

// DecodeListVersionsResponse 是 EncodeListVersionsResponse 的逆操作。
func DecodeListVersionsResponse(body []byte) ([]VersionEntryView, bool) {
	if len(body) < 4 {
		return nil, false
	}
	count := int(binary.LittleEndian.Uint32(body[0:4]))
	off := 4
	out := make([]VersionEntryView, 0, count)
	for i := 0; i < count; i++ {
		seq, writeNanos, payload, consumed, ok := DecodeVersionEntry(body[off:])
		if !ok {
			return nil, false
		}
		out = append(out, VersionEntryView{Seq: seq, WriteNanos: writeNanos, Payload: payload})
		off += consumed
	}
	return out, true
}

// EncodeReplayFingerprintResponse 编码 REPLAY_FINGERPRINT 的 OK 响应体：
// [keyCount u32 LE][mismatchCount u32 LE][fingerprintLen u16 LE][fingerprint]
// [mismatchLogicalKey...]（每条 [len u16 LE][bytes]，共 mismatchCount 条）。
// fingerprint 是 internal/temporal.Fingerprint 对"重放出的最新状态集合"算出的
// 十六进制摘要（跨进程对比用，见该函数文档）；mismatchCount/mismatch 列表是
// 与 :current 指针逐一对账后的不一致清单（验收三问第 2 问的实体：一致=0）。
func EncodeReplayFingerprintResponse(keyCount, mismatchCount uint32, fingerprint string, mismatchKeys []string) []byte {
	total := 4 + 4 + 2 + len(fingerprint)
	for _, k := range mismatchKeys {
		total += 2 + len(k)
	}
	buf := make([]byte, total)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:off+4], keyCount)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], mismatchCount)
	off += 4
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(fingerprint)))
	off += 2
	copy(buf[off:], fingerprint)
	off += len(fingerprint)
	for _, k := range mismatchKeys {
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(k)))
		off += 2
		copy(buf[off:], k)
		off += len(k)
	}
	return buf
}

// ReplayFingerprintView 是 DecodeReplayFingerprintResponse 解出的结果视图。
type ReplayFingerprintView struct {
	KeyCount      uint32
	MismatchCount uint32
	Fingerprint   string
	MismatchKeys  []string
}

// DecodeReplayFingerprintResponse 是 EncodeReplayFingerprintResponse 的逆操作。
func DecodeReplayFingerprintResponse(body []byte) (ReplayFingerprintView, bool) {
	if len(body) < 10 {
		return ReplayFingerprintView{}, false
	}
	keyCount := binary.LittleEndian.Uint32(body[0:4])
	mismatchCount := binary.LittleEndian.Uint32(body[4:8])
	fpLen := int(binary.LittleEndian.Uint16(body[8:10]))
	off := 10
	if off+fpLen > len(body) {
		return ReplayFingerprintView{}, false
	}
	fingerprint := string(body[off : off+fpLen])
	off += fpLen
	keys := make([]string, 0, mismatchCount)
	for i := uint32(0); i < mismatchCount; i++ {
		if off+2 > len(body) {
			return ReplayFingerprintView{}, false
		}
		n := int(binary.LittleEndian.Uint16(body[off : off+2]))
		off += 2
		if off+n > len(body) {
			return ReplayFingerprintView{}, false
		}
		keys = append(keys, string(body[off:off+n]))
		off += n
	}
	return ReplayFingerprintView{
		KeyCount:      keyCount,
		MismatchCount: mismatchCount,
		Fingerprint:   fingerprint,
		MismatchKeys:  keys,
	}, true
}
