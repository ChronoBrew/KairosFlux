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

// DecodePutVersionedFrame 解析 PUT_VERSIONED 请求负载：
// [keyLen u32 LE][valueLen u32 LE][key][value]([sourceLen u32 LE][source])?。
// source 段是 M2 新增的操作元数据（写入方标识，任务书第 2 项"操作元数据
// 信封"），按精确剩余长度判断是否存在——省略时 source=""，兼容 M0 时期只有
// key/value 的老调用（proto.EncodePutFrame + OpcodePutVersioned），不是一种
// "错误但容忍"的兜底，是"这个字段协议上就是可选的"这条设计的直接体现。
func DecodePutVersionedFrame(data []byte) (key, value []byte, source string, ok bool) {
	k, v, decodeOK := DecodePutFrame(data)
	if !decodeOK {
		return nil, nil, "", false
	}
	consumed := 8 + len(k) + len(v)
	rest := data[consumed:]
	if len(rest) == 0 {
		return k, v, "", true
	}
	if len(rest) < 4 {
		return nil, nil, "", false
	}
	sourceLen := int(binary.LittleEndian.Uint32(rest[0:4]))
	if sourceLen < 0 || 4+sourceLen != len(rest) {
		return nil, nil, "", false
	}
	return k, v, string(rest[4 : 4+sourceLen]), true
}

// EncodePutVersionedFrame 编码 PUT_VERSIONED 请求负载。source==""时省略尾部
// 字段，生成与 M0 时期 proto.EncodePutFrame 逐字节相同的请求（老调用方/老
// 向量零回归）；source!=""时追加它。
func EncodePutVersionedFrame(key, value []byte, source string) []byte {
	if source == "" {
		return EncodePutFrame(key, value)
	}
	base := EncodePutFrame(key, value)
	buf := make([]byte, len(base)+4+len(source))
	copy(buf, base)
	binary.LittleEndian.PutUint32(buf[len(base):len(base)+4], uint32(len(source)))
	copy(buf[len(base)+4:], source)
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
// Bounded 是 M2 新增字段，见 EncodeReplayFingerprintResponseV2 的文档。
type ReplayFingerprintView struct {
	KeyCount      uint32
	MismatchCount uint32
	Fingerprint   string
	MismatchKeys  []string
	Bounded       bool
}

// DecodeReplayFingerprintResponse 是 EncodeReplayFingerprintResponse /
// EncodeReplayFingerprintResponseV2 的逆操作。两种响应体只在"是否多 1 个
// bounded 尾字节"上有区别（M2 新增，见 EncodeReplayFingerprintResponseV2
// 的文档）——按精确剩余长度判断是否存在，不猜内容；旧响应（没有尾字节）
// 解出 Bounded=false，与 M0 时期的实际语义（"这次调用没有 asOf 上界，
// mismatch 就是核对过的结果"）一致。
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
	bounded := false
	if off < len(body) {
		bounded = body[off] != 0
		off++
	}
	return ReplayFingerprintView{
		KeyCount:      keyCount,
		MismatchCount: mismatchCount,
		Fingerprint:   fingerprint,
		MismatchKeys:  keys,
		Bounded:       bounded,
	}, true
}

// EncodeReplayFingerprintResponseV2 是 EncodeReplayFingerprintResponse 的 M2
// 扩展：在既有编码末尾追加 1 字节 bounded 标记（0/1）——附加而非替换既有
// 布局，是 M2 任务书"跨语言向量字节不变红线：新信封/新字段只追加"这条原则
// 在响应体这一侧的体现。bounded=true 时调用方必须知道 mismatchKeys 恒为空
// 不代表"核对通过"，而是"没有核对"（见 service.ReplayResult.Bounded 的
// 文档），CLI 展示层必须显式区分这两种情况，不能只看 mismatchCount==0。
func EncodeReplayFingerprintResponseV2(keyCount, mismatchCount uint32, fingerprint string, mismatchKeys []string, bounded bool) []byte {
	base := EncodeReplayFingerprintResponse(keyCount, mismatchCount, fingerprint, mismatchKeys)
	out := make([]byte, len(base)+1)
	copy(out, base)
	if bounded {
		out[len(base)] = 1
	}
	return out
}

// DecodeReplayFingerprintRequest 解析 REPLAY_FINGERPRINT 请求负载：
// [prefixLen u32 LE][prefix]([asOfNanos i64 LE])?。asOfNanos 段是 M2 新增的
// 可选时间上界（任务书第 1 项："升级为按数据集/时间范围扫描"），按精确长度
// 判断是否存在——旧调用（EncodeKeyOnlyFrame 格式，只有 prefix、没有尾部
// 8 字节）与新调用（多 8 字节）在总长度上正好相差 8，不依赖对内容做任何
// 猜测（对照 QuantBrew 侧"禁止字符串匹配承载语义"的教训）。
func DecodeReplayFingerprintRequest(data []byte) (prefix []byte, asOfNanos int64, ok bool) {
	if len(data) < 4 {
		return nil, 0, false
	}
	prefixLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if prefixLen < 0 || 4+prefixLen > len(data) {
		return nil, 0, false
	}
	prefix = data[4 : 4+prefixLen]
	rest := len(data) - (4 + prefixLen)
	switch rest {
	case 0:
		return prefix, 0, true
	case 8:
		asOfNanos = int64(binary.LittleEndian.Uint64(data[4+prefixLen : 4+prefixLen+8]))
		return prefix, asOfNanos, true
	default:
		return nil, 0, false
	}
}

// EncodeReplayFingerprintRequest 编码 REPLAY_FINGERPRINT 请求负载。
// asOfNanos<=0 时省略尾部 8 字节（生成与 M0 时期 EncodeKeyOnlyFrame 逐字节
// 相同的旧格式请求，保持老调用方零回归）；asOfNanos>0 时追加它。
func EncodeReplayFingerprintRequest(prefix []byte, asOfNanos int64) []byte {
	if asOfNanos <= 0 {
		return EncodeKeyOnlyFrame(prefix)
	}
	buf := make([]byte, 4+len(prefix)+8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(prefix)))
	copy(buf[4:], prefix)
	binary.LittleEndian.PutUint64(buf[4+len(prefix):4+len(prefix)+8], uint64(asOfNanos))
	return buf
}

// LIST_WRITES（Kair v2 opcode 0x0D，时态内核 M2，方案 §M2 第 2 项）请求/响应
// 负载编解码——全新 opcode，没有 M0 遗留格式要兼容，字段设计不需要"省略尾部
// 可选字段"这类向后兼容技巧。

// DecodeListWritesRequest 解析 LIST_WRITES 请求负载：
// [prefixLen u32 LE][prefix][tFromNanos i64 LE][tToNanos i64 LE]
// [sourceLen u32 LE][source]。tFromNanos<=0 表示无下界，tToNanos<=0 表示无
// 上界——真实的 write_ts（time.Now().UnixNano()）恒为正数，用非正数做"无界"
// 哨兵不会与任何真实写入的时刻混淆（与 internal/temporal 包
// envelopeMarkerBit 判据同一类"基于真实取值范围的确定性约定"，不是猜内容）。
// sourceLen=0 表示不按来源过滤。
func DecodeListWritesRequest(data []byte) (prefix []byte, tFromNanos, tToNanos int64, source []byte, ok bool) {
	if len(data) < 4 {
		return nil, 0, 0, nil, false
	}
	prefixLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if prefixLen < 0 || 4+prefixLen+8+8+4 > len(data) {
		return nil, 0, 0, nil, false
	}
	off := 4
	prefix = data[off : off+prefixLen]
	off += prefixLen
	tFromNanos = int64(binary.LittleEndian.Uint64(data[off : off+8]))
	off += 8
	tToNanos = int64(binary.LittleEndian.Uint64(data[off : off+8]))
	off += 8
	sourceLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	if sourceLen < 0 || off+sourceLen != len(data) {
		return nil, 0, 0, nil, false
	}
	source = data[off : off+sourceLen]
	return prefix, tFromNanos, tToNanos, source, true
}

// EncodeListWritesRequest 是 DecodeListWritesRequest 的逆操作。
func EncodeListWritesRequest(prefix []byte, tFromNanos, tToNanos int64, source []byte) []byte {
	buf := make([]byte, 4+len(prefix)+8+8+4+len(source))
	off := 0
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(prefix)))
	off += 4
	copy(buf[off:], prefix)
	off += len(prefix)
	binary.LittleEndian.PutUint64(buf[off:off+8], uint64(tFromNanos))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:off+8], uint64(tToNanos))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(source)))
	off += 4
	copy(buf[off:], source)
	return buf
}

// EncodeWriteEnvelopeEntry 编码 LIST_WRITES 响应体里一条命中记录：
// [logicalKeyLen u32 LE][logicalKey][seq u64 LE][writeNanos i64 LE]
// [sourceLen u32 LE][source][schemaVer u32 LE][hashLen u16 LE][payloadHash]
// [payloadLen u32 LE][payload][hashOK u8]。hashOK 是 service.WriteEnvelope.
// HashOK 的直接编码（1=完整性自检通过或无历史哈希可比对，0=persisted_hash
// 与现算 sha256(payload) 不符——数据在写入之后发生过静默漂移，见
// service.WriteEnvelope 的文档）。
func EncodeWriteEnvelopeEntry(logicalKey string, seq uint64, writeNanos int64, source string, schemaVer uint32, payloadHash string, payload []byte, hashOK bool) []byte {
	buf := make([]byte, 4+len(logicalKey)+8+8+4+len(source)+4+2+len(payloadHash)+4+len(payload)+1)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(logicalKey)))
	off += 4
	copy(buf[off:], logicalKey)
	off += len(logicalKey)
	binary.LittleEndian.PutUint64(buf[off:off+8], seq)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:off+8], uint64(writeNanos))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(source)))
	off += 4
	copy(buf[off:], source)
	off += len(source)
	binary.LittleEndian.PutUint32(buf[off:off+4], schemaVer)
	off += 4
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(payloadHash)))
	off += 2
	copy(buf[off:], payloadHash)
	off += len(payloadHash)
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(payload)))
	off += 4
	copy(buf[off:], payload)
	off += len(payload)
	if hashOK {
		buf[off] = 1
	}
	return buf
}

// WriteEnvelopeView 是 DecodeWriteEnvelopeEntry/DecodeListWritesResponse 解出
// 的一条记录，供调用方（kairosflux-cli 的 list-writes/export-writes）按字段
// 读取。
type WriteEnvelopeView struct {
	LogicalKey  string
	Seq         uint64
	WriteNanos  int64
	Source      string
	SchemaVer   uint32
	PayloadHash string
	Payload     []byte
	HashOK      bool
}

// DecodeWriteEnvelopeEntry 是 EncodeWriteEnvelopeEntry 的逆操作，返回消费的
// 字节数 consumed，供 DecodeListWritesResponse 依次解出多条记录。
func DecodeWriteEnvelopeEntry(data []byte) (WriteEnvelopeView, int, bool) {
	if len(data) < 4 {
		return WriteEnvelopeView{}, 0, false
	}
	off := 0
	logicalKeyLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	if logicalKeyLen < 0 || off+logicalKeyLen+8+8+4 > len(data) {
		return WriteEnvelopeView{}, 0, false
	}
	logicalKey := string(data[off : off+logicalKeyLen])
	off += logicalKeyLen
	seq := binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	writeNanos := int64(binary.LittleEndian.Uint64(data[off : off+8]))
	off += 8
	sourceLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	if sourceLen < 0 || off+sourceLen+4+2 > len(data) {
		return WriteEnvelopeView{}, 0, false
	}
	source := string(data[off : off+sourceLen])
	off += sourceLen
	schemaVer := binary.LittleEndian.Uint32(data[off : off+4])
	off += 4
	hashLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
	off += 2
	if hashLen < 0 || off+hashLen+4 > len(data) {
		return WriteEnvelopeView{}, 0, false
	}
	payloadHash := string(data[off : off+hashLen])
	off += hashLen
	payloadLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	if payloadLen < 0 || off+payloadLen+1 > len(data) {
		return WriteEnvelopeView{}, 0, false
	}
	payload := data[off : off+payloadLen]
	off += payloadLen
	hashOK := data[off] != 0
	off++
	return WriteEnvelopeView{
		LogicalKey:  logicalKey,
		Seq:         seq,
		WriteNanos:  writeNanos,
		Source:      source,
		SchemaVer:   schemaVer,
		PayloadHash: payloadHash,
		Payload:     payload,
		HashOK:      hashOK,
	}, off, true
}

// SourceCountView 是 DecodeListWritesResponse 解出的一条按来源聚合计数。
type SourceCountView struct {
	Source string
	Count  uint32
}

// EncodeListWritesResponse 编码 LIST_WRITES 的 OK 响应体：
// [matchCount u32 LE][entry...]（entry 为 EncodeWriteEnvelopeEntry 编码）
// [sourceCountN u32 LE]([sourceLen u32 LE][source][count u32 LE])...。
// entries/sourceNames+sourceCounts 均由调用方（service.TemporalStore.
// ListWrites）保证按 (LogicalKey,Seq)/Source 升序排好，本函数只负责编码、
// 不重新排序——排序是业务层的确定性保证，不是编解码层的职责。sourceNames
// 与 sourceCounts 用等长的平行切片表达"按来源聚合计数"，而不是在 proto 包
// 里新定义一个与 service.SourceCount 同形状的结构体——proto 包不依赖 service
// 包（避免反向依赖边），调用方（RouterV2.handleListWrites）直接拆出两个
// 切片传进来即可，不需要额外的转换类型。
func EncodeListWritesResponse(entries [][]byte, sourceNames []string, sourceCounts []uint32) []byte {
	total := 4
	for _, e := range entries {
		total += len(e)
	}
	total += 4
	for _, name := range sourceNames {
		total += 4 + len(name) + 4
	}
	buf := make([]byte, total)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(entries)))
	off += 4
	for _, e := range entries {
		copy(buf[off:], e)
		off += len(e)
	}
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(sourceNames)))
	off += 4
	for i, name := range sourceNames {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(name)))
		off += 4
		copy(buf[off:], name)
		off += len(name)
		binary.LittleEndian.PutUint32(buf[off:off+4], sourceCounts[i])
		off += 4
	}
	return buf
}

// DecodeListWritesResponse 是 EncodeListWritesResponse 的逆操作。
func DecodeListWritesResponse(body []byte) ([]WriteEnvelopeView, []SourceCountView, bool) {
	if len(body) < 4 {
		return nil, nil, false
	}
	off := 0
	matchCount := int(binary.LittleEndian.Uint32(body[off : off+4]))
	off += 4
	entries := make([]WriteEnvelopeView, 0, matchCount)
	for i := 0; i < matchCount; i++ {
		e, consumed, ok := DecodeWriteEnvelopeEntry(body[off:])
		if !ok {
			return nil, nil, false
		}
		entries = append(entries, e)
		off += consumed
	}
	if off+4 > len(body) {
		return nil, nil, false
	}
	sourceCountN := int(binary.LittleEndian.Uint32(body[off : off+4]))
	off += 4
	counts := make([]SourceCountView, 0, sourceCountN)
	for i := 0; i < sourceCountN; i++ {
		if off+4 > len(body) {
			return nil, nil, false
		}
		srcLen := int(binary.LittleEndian.Uint32(body[off : off+4]))
		off += 4
		if srcLen < 0 || off+srcLen+4 > len(body) {
			return nil, nil, false
		}
		src := string(body[off : off+srcLen])
		off += srcLen
		count := binary.LittleEndian.Uint32(body[off : off+4])
		off += 4
		counts = append(counts, SourceCountView{Source: src, Count: count})
	}
	return entries, counts, true
}
