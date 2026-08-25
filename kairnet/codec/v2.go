package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Kair v2 帧格式（docs/rfc/Kair-2.md §2/§3）。本文件是纯新增：不修改本包
// 已有的 v1 DataPack/Message 的任何字段或行为——v1 与 v2 是两套完全独立的
// 头部结构与类型，只共享 EffectiveMaxSize/ErrFrameTooLarge 这类与协议版本
// 无关的帧长防护策略（见 Decode 的注释），避免为帧长上限另开一套语义。
//
//	[magic+ver u16 LE][flags u8][opcode u8][type u16 LE][corr_id u32 LE][dataLen u32 LE][data bytes]
//
// 定长头部 14 字节，不再有 v1 那样的变长 msgID——opcode 是定长数字。
// magic+ver 字段数值 = magic<<8 | version（高字节 magic、低字节 version），
// 按 LE 存储后 wire 上第 0 字节是 version、第 1 字节才是 magic——这个字节序
// 容易被两侧实现"一致地搞反"而无法被自洽的向量文件发现，故额外有
// TestVectorsV2_MagicByteOrderIsVersionThenMagic 锁定绝对字节位置，不只是
// 通过 Pack/UnPack 往返自洽。见 docs/kair/vectors-v2.json 的
// v2_magic_byte_order_lock 向量与本文件 EncodeMagicVer 的文档。

// MagicV2 是 v2 帧的魔数（RFC §2 草案值 0xBA，"BanDB" 的缩写谐音）。
const MagicV2 byte = 0xBA

// VersionV2 是本实现支持的 Kair 协议版本号（v2 起始值）。
const VersionV2 byte = 0x02

// HeaderV2Len 是 v2 定长头部字节数：magic+ver(2) + flags(1) + opcode(1) +
// type(2) + corr_id(4) + dataLen(4) = 14。
const HeaderV2Len = 14

// opcode（RFC §3.1）。0x00-0x7F 是请求，0x80-0xFF 是响应。
const (
	OpcodePut   uint8 = 0x01
	OpcodeGet   uint8 = 0x02
	OpcodeDel   uint8 = 0x03
	OpcodeScan  uint8 = 0x04
	OpcodeHello uint8 = 0x05 // v2 起第一次被赋予行为（协商，见 kairnet/negotiate 包）
	// OpcodeBye: ack=every 连接上仍是保留位（语义不变）；ack=window/none 连接上
	// 第二阶段被赋予"收尾对账"行为，见 RFC §11.4、service.RouterV2。
	OpcodeBye uint8 = 0x06

	// OpcodeFlush/OpcodeStat: RFC §11 交互模式（ack=window/none 场景）新增
	// opcode，第二阶段实现，见 service.RouterV2。
	OpcodeFlush uint8 = 0x07
	OpcodeStat  uint8 = 0x08

	// OpcodePutVersioned/OpcodeGetAsOf/OpcodeListVersions/OpcodeReplayFingerprint：
	// 时态内核 M0 接线新增 opcode（docs/rfc/时态内核-M0-版本化与as-of.md），紧接在
	// 已有编号之后顺延分配。与既有写路径的刻意区分：v1 OpcodePut/OpcodeDel 与 v2
	// 该二者保留原有"覆盖写"语义不变（老客户端零影响），版本化只属于这四个新
	// opcode，不静默改变任何既有行为。四者均不参与 §11.2.2 的 ack 三档窗口/累计
	// 记账——它们是数据模型层的新增能力而非既有写路径的批处理优化，简化起见统一
	// 按"请求-即时响应"处理，与 GET/SCAN/STAT 同类（响应用既有 OpcodeOK/OpcodeErr）。
	OpcodePutVersioned      uint8 = 0x09
	OpcodeGetAsOf           uint8 = 0x0A
	OpcodeListVersions      uint8 = 0x0B
	OpcodeReplayFingerprint uint8 = 0x0C

	// OpcodeListWrites：时态内核 M2 新增的审计查询 opcode（docs/方案-BanDB-
	// 时态内核与AI数据平面.md §M2 第 2 项），紧接在上面四个 M0 opcode 之后
	// 顺延分配。与它们同类：不参与 ack 三档窗口/累计记账，按"请求-即时响应"
	// 处理，响应用既有 OpcodeOK/OpcodeErr。
	OpcodeListWrites uint8 = 0x0D

	OpcodeOK  uint8 = 0x80
	OpcodeErr uint8 = 0x81

	// OpcodeWindowAck/OpcodeStatAck: 同上，RFC §11 交互模式，第二阶段实现，
	// 响应体格式见 RFC §11.2.2/§11.2.3，编解码在 service.RouterV2。
	OpcodeWindowAck uint8 = 0x82
	OpcodeStatAck   uint8 = 0x83
)

// 机读错误码（RFC §10，第二阶段最小可用子集）：§10 本身只是设计，仓库里
// 至今没有代码实现过这套分类学——本阶段只实现 WINDOW_ACK/STAT_ACK 的
// firstErrCode 字段真正需要的最小子集，不是 §10 全表的落地，子码分配等
// 治理问题仍然开放（RFC §7/§10.3）。
const (
	// ErrCodeNone 表示"没有错误"，用于 firstErrCode 字段在窗口/连接尚未
	// 出现任何拒绝时的零值——0 不与下面任何一个真实错误码冲突。
	ErrCodeNone uint16 = 0x0000
	// ErrCodeMalformedFrame 对应 §10.3 的 0x1xxx 段（帧/传输层）：PUT 负载
	// 解不出 key/value（keyLen/valueLen 与实际字节不符）。
	ErrCodeMalformedFrame uint16 = 0x1001
	// ErrCodeResultTooLarge 同属 §10.3 的 0x1xxx 段（帧/传输层，不是内容
	// 校验失败）：服务端算出的响应体超过帧长上限（EffectiveMaxSize），无法
	// 安全打包成一帧发送——时态内核 M2 的 LIST_WRITES 在无过滤/大数据集场景
	// 下可能命中这条（一个前缀下的写入历史可以任意长），见
	// service.RouterV2.handleListWrites 的文档：结构化拒绝比"服务端直接发送
	// 一个客户端解码时会因超限而拒绝的巨帧、让调用方对着超时干等"更诚实。
	ErrCodeResultTooLarge uint16 = 0x1002
	// ErrCodeSchemaValidation 对应 §10.3 的 0x3xxx 段（schema 校验）：value
	// 未通过已注册类型的校验器。本阶段未实现 §9 Schema Descriptor 的按类型
	// 子码分配，所有 schema 校验失败共用这一个码，具体原因仍由人读的
	// reason 字段承载。
	ErrCodeSchemaValidation uint16 = 0x3001
	// ErrCodeUnauthorizedRole 是新增的 0x4xxx 段（角色/权限校验，§10.3
	// 表里此前只分配到 0x3xxx，本码是这套分类学的第一个 0x4xxx 段落地）：
	// PUT_VERSIONED 请求帧解出的 source 按 internal/identity.SourceRole
	// 判定为 RoleAgent，但目标 key 不在 internal/identity.IsProposalKey
	// 判定的 Proposal 键空间内——"agent 身份只能写 Proposal 对象"这条规则
	// 由 service/router_v2.go 的 handlePutVersioned 在协议层强制（M4
	// WriteAsAgent 此前只是应用层 API 闸门，见 internal/aiplane/doc.go）。
	ErrCodeUnauthorizedRole uint16 = 0x4001
)

// type（RFC §3.2），与 service/ingesthook/schema 的注册表对齐。
const (
	// TypeUnspecified 是"未声明类型"，向后兼容默认值——不做类型相关的
	// 校验/单调性分派，退化为 v1 行为（RFC §6）。
	TypeUnspecified uint16 = 0
	// TypeQuote 对应 service/ingesthook/schema 的 "quote:" 前缀校验器。
	TypeQuote uint16 = 1
)

// EncodeMagicVer 组装 magic+ver 字段的 u16 数值：数值 = magic<<8 | version
// （高字节 magic、低字节 version，RFC §2 原文措辞）。调用方仍需用
// binary.LittleEndian.PutUint16 写入两字节缓冲区——LE 存储会让 version 出现
// 在 wire 的第 0 字节、magic 出现在第 1 字节，这是本函数刻意不自己做字节
// 切片的原因：调用方应该始终通过 encoding/binary 的 LE 函数完成最后一步，
// 不要在这之上再手写字节顺序，避免出现"两处都写、两处不一致"的分歧。
func EncodeMagicVer(magic, version byte) uint16 {
	return uint16(magic)<<8 | uint16(version)
}

// DecodeMagicVer 从 magic+ver 字段还原出的 u16 数值里拆出 (magic, version)。
func DecodeMagicVer(v uint16) (magic byte, version byte) {
	return byte(v >> 8), byte(v)
}

// SniffResult 是 RFC §6 双栈判据的判定结果：给定帧最前面 2 字节，判断这是
// v2 帧、还是应该整体回退按 v1 头部重新解释、还是 magic 匹配但版本号不受
// 支持（这第三种情形不应该被误判为"这是 v1 帧"——未来的 v3 帧和 v1 帧的
// 处置方式必须不同：前者是"版本不兼容"的错误，后者是"这本来就是另一种
// 协议"的正常回退）。
type SniffResult int

const (
	// SniffV1 表示这 2 字节不携带 v2 magic，调用方应整体回退按 v1 的 6 字节
	// 头部重新解释这两个字节（RFC §6）。
	SniffV1 SniffResult = iota
	// SniffV2 表示 magic 与本实现支持的 VersionV2 都匹配，可以继续按 v2
	// 头部解析剩余 12 字节。
	SniffV2
	// SniffUnsupportedVersion 表示 magic 匹配但 version 不是本实现认识的
	// 版本——这是一个协议不兼容错误，不是"退回 v1"，调用方应拒绝该连接/帧
	// 而不是静默当作 v1 处理。
	SniffUnsupportedVersion
)

// SniffVersion 判断帧头最前 2 字节（必须恰好 2 字节）属于上述哪一种情形。
// 只在 magic 字节匹配的前提下才检查 version，未匹配 magic 时不关心 version
// 取值——这保证了"magic 不对 → 明确判 v1"与"magic 对、version 不对 →
// 明确判版本不兼容"两条路径不会互相塌缩成一条 if 语句（那样会在未来出现
// v3 时把"确实是新版本"的帧误判成"这是 v1 帧"）。
//
// 已知、未解决的碰撞窗口（RFC §7"仍然开放"，本阶段不解决，只记录）：这个
// 函数只看 2 字节，一个 v1 帧的 dataLen 低 16 位如果恰好等于 0xBA02（LE 存储
// 后与 v2 magic+ver 完全一样），会被误判为 SniffV2。生产读循环的服务端一侧
// 目前不受这个风险影响——RFC §5.1 约定 v2 客户端的探测帧本身就是 v1 格式
// （见 kairnet/negotiate 包），服务端识别"这是不是协商探测帧"走的是"先按 v1
// 解出完整帧、再比对 MsgID=='HELLO'"这条路径（kairnet/transport.Connection.
// StartReader），根本不调用本函数；本函数目前只在客户端一侧
// （negotiate.ClientNegotiate）用来判断"服务端的响应是不是 v2 格式"，那里
// 的碰撞窗口是"v1 服务端恰好回了一个满足这个字节模式的响应"，实践中 v1
// 服务端对 HELLO 根本不响应（见 RFC §5），窗口目前只是理论风险。真正需要
// 治理时应该和 §7"type 号治理"一并设计。
func SniffVersion(first2Bytes []byte) SniffResult {
	magicVer := binary.LittleEndian.Uint16(first2Bytes[0:2])
	magic, version := DecodeMagicVer(magicVer)
	if magic != MagicV2 {
		return SniffV1
	}
	if version != VersionV2 {
		return SniffUnsupportedVersion
	}
	return SniffV2
}

// HeaderV2 是 v2 帧定长头部（去掉 magic+ver 之后剩余字段）的内存表示。
type HeaderV2 struct {
	Flags   uint8
	Opcode  uint8
	Type    uint16
	CorrID  uint32
	DataLen uint32
}

// MessageV2 是一帧 v2 消息的内存表示：头部 + 负载字节。
type MessageV2 struct {
	Header  HeaderV2
	Payload []byte
}

// NewMessageV2 构造一个 MessageV2，DataLen 由 payload 长度派生（调用方不需
// 要、也不应该自己维护这个冗余字段）。
func NewMessageV2(h HeaderV2, payload []byte) *MessageV2 {
	h.DataLen = uint32(len(payload))
	return &MessageV2{Header: h, Payload: payload}
}

// ErrNotV2Magic 标记待解析的 2/14 字节不携带 v2 magic——调用方（如 RFC §6
// 描述的双栈嗅探逻辑）应据此回退按 v1 头部重新解释，而不是把这当作一个
// "这一帧坏了"的错误上抛。
var ErrNotV2Magic = errors.New("codec: header does not carry v2 magic byte")

// ErrUnsupportedV2Version 标记 magic 匹配但版本号不是本实现支持的 VersionV2
// ——不同于 ErrNotV2Magic，这确实是一个协议错误，不应被当作"回退 v1"处理。
var ErrUnsupportedV2Version = errors.New("codec: unsupported Kair v2 minor version")

// DataPackV2 是 v2 帧的编解码器，与 v1 的 DataPack 对称但完全独立（不同的
// 头部布局、不同的类型），实现同样的"Pack 编码整帧 / UnPack 只解头部 /
// Decode 从 io.Reader 读出完整帧"三段式接口形状，方便调用方按同一心智模型
// 使用两个协议版本。
type DataPackV2 struct{}

// NewDataPackV2 构造一个 DataPackV2。
func NewDataPackV2() *DataPackV2 { return &DataPackV2{} }

// HeadLen 返回 v2 定长头部字节数（14）。
func (dp *DataPackV2) HeadLen() uint32 { return HeaderV2Len }

// Pack 编码一帧完整的 v2 帧：14 字节定长头 + 负载，一次性分配。
func (dp *DataPackV2) Pack(msg *MessageV2) ([]byte, error) {
	payload := msg.Payload
	buf := make([]byte, HeaderV2Len+len(payload))
	binary.LittleEndian.PutUint16(buf[0:2], EncodeMagicVer(MagicV2, VersionV2))
	buf[2] = msg.Header.Flags
	buf[3] = msg.Header.Opcode
	binary.LittleEndian.PutUint16(buf[4:6], msg.Header.Type)
	binary.LittleEndian.PutUint32(buf[6:10], msg.Header.CorrID)
	binary.LittleEndian.PutUint32(buf[10:14], uint32(len(payload)))
	copy(buf[HeaderV2Len:], payload)
	return buf, nil
}

// UnPack 只解析 14 字节定长头部（不读负载），校验 magic/version，返回
// HeaderV2（含 DataLen）。与 v1 DataPack.UnPack 对称：负载读取交给 Decode。
func (dp *DataPackV2) UnPack(head []byte) (*HeaderV2, error) {
	if len(head) < HeaderV2Len {
		return nil, errors.New("codec: v2 header too short")
	}
	magicVer := binary.LittleEndian.Uint16(head[0:2])
	magic, version := DecodeMagicVer(magicVer)
	if magic != MagicV2 {
		return nil, fmt.Errorf("%w: got %#x", ErrNotV2Magic, magic)
	}
	if version != VersionV2 {
		return nil, fmt.Errorf("%w: got %#x", ErrUnsupportedV2Version, version)
	}
	h := &HeaderV2{
		Flags:   head[2],
		Opcode:  head[3],
		Type:    binary.LittleEndian.Uint16(head[4:6]),
		CorrID:  binary.LittleEndian.Uint32(head[6:10]),
		DataLen: binary.LittleEndian.Uint32(head[10:14]),
	}
	return h, nil
}

// Decode 从 r 读出一条完整的 v2 帧：14 字节定长头 → （校验 DataLen 不超过
// maxSize）→ 负载字节。maxSize<=0 时复用 v1 DataPack 的 EffectiveMaxSize
// 兜底到 hardMaxPackageSize——帧长上限是与协议版本无关的防护策略（内存
// 放大 DoS 的防线），不应该为 v2 另开一套独立的上限常量，那样两套上限
// 漂移只是时间问题。ErrFrameTooLarge 同样复用 v1 那个哨兵错误，调用方用
// errors.Is 判别的方式不因协议版本而分裂成两套。
//
// beforeRead 语义与 v1 DataPack.Decode 完全一致（每次阻塞读之前调用一次，
// 供调用方重置读超时），可以为 nil。
func (dp *DataPackV2) Decode(r io.Reader, maxSize uint32, beforeRead func()) (*MessageV2, error) {
	effMax := EffectiveMaxSize(maxSize)

	if beforeRead != nil {
		beforeRead()
	}
	headBuf := make([]byte, HeaderV2Len)
	if _, err := io.ReadFull(r, headBuf); err != nil {
		return nil, fmt.Errorf("decode v2: read header: %w", err)
	}

	h, err := dp.UnPack(headBuf)
	if err != nil {
		return nil, fmt.Errorf("decode v2: unpack header: %w", err)
	}

	if h.DataLen > effMax {
		return nil, fmt.Errorf("decode v2: %w: dataLen=%d max=%d", ErrFrameTooLarge, h.DataLen, effMax)
	}

	var payload []byte
	if h.DataLen > 0 {
		if beforeRead != nil {
			beforeRead()
		}
		payload = make([]byte, h.DataLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("decode v2: read body: %w", err)
		}
	}

	return &MessageV2{Header: *h, Payload: payload}, nil
}
