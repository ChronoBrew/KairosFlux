package proto

import "encoding/binary"

// BANLV v2 响应体编解码（docs/rfc/BANLV-2.md §10/§11.2.2/§11.2.3）。放在本包
// 而不是 bannet/codec：与 put_frame.go/scan.go 同一个先例——codec 包只认
// v2 帧envelope（14 字节头+裸负载字节），不认负载内部该怎么解释；
// EncodeScanResponse 已经是"响应体的具体编码格式归 proto 包"的既有先例，
// 本文件延续同一分层。

// V2ErrPayload 编码 v2 ERR（opcode=0x81）响应负载：[code u16 LE]
// [reasonLen u16 LE][reason]。code 是 §10 机读错误码（本阶段最小子集，见
// bannet/codec.ErrCodeXxx），reason 是人读原因，两者都发（§10.2）。
func V2ErrPayload(code uint16, reason string) []byte {
	buf := make([]byte, 4+len(reason))
	binary.LittleEndian.PutUint16(buf[0:2], code)
	binary.LittleEndian.PutUint16(buf[2:4], uint16(len(reason)))
	copy(buf[4:], reason)
	return buf
}

// DecodeV2ErrPayload 解析 V2ErrPayload 编码的字节。
func DecodeV2ErrPayload(payload []byte) (code uint16, reason string, ok bool) {
	if len(payload) < 4 {
		return 0, "", false
	}
	code = binary.LittleEndian.Uint16(payload[0:2])
	n := int(binary.LittleEndian.Uint16(payload[2:4]))
	if len(payload) < 4+n {
		return 0, "", false
	}
	return code, string(payload[4 : 4+n]), true
}

// V2AckBody 编码 WINDOW_ACK(0x82)/STAT_ACK(0x83) 共享的响应体格式（RFC
// §11.2.2）：[corr_id u32 LE][received u32 LE][accepted u32 LE]
// [rejected u32 LE][firstErrCode u16 LE][firstErrReasonLen u16 LE]
// [firstErrReason]。
//
// corr_id 在这里（响应体内）与 v2 帧头部的 corr_id 字段是刻意的重复
// （RFC 原文两处都写了 corr_id）：头部 corr_id 供通用的"这是哪个 opcode/
// 哪次交互"的帧层面识别，体内 corr_id 是这份 §11.2.2/§11.2.3 定义的响应体
// 结构自身携带的字段，调用方（RouterV2）保证两处写入同一个值，但解析时
// 不应假设"body 里的 corr_id 一定等于头部 corr_id 就可以只读一处"——
// 协议定义就是两处都有，本函数忠实编解码 body 那一份。
func V2AckBody(corrID, received, accepted, rejected uint32, firstErrCode uint16, firstErrReason string) []byte {
	buf := make([]byte, 4+4+4+4+2+2+len(firstErrReason))
	off := 0
	binary.LittleEndian.PutUint32(buf[off:off+4], corrID)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], received)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], accepted)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], rejected)
	off += 4
	binary.LittleEndian.PutUint16(buf[off:off+2], firstErrCode)
	off += 2
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(firstErrReason)))
	off += 2
	copy(buf[off:], firstErrReason)
	return buf
}

// V2AckFields 是 DecodeV2AckBody 解出的字段集合，命名结构体而非多返回值——
// 六个同类型（多为 uint32）字段用位置传参在调用点极易读错顺序。
type V2AckFields struct {
	CorrID         uint32
	Received       uint32
	Accepted       uint32
	Rejected       uint32
	FirstErrCode   uint16
	FirstErrReason string
}

// DecodeV2AckBody 解析 V2AckBody 编码的字节。
func DecodeV2AckBody(body []byte) (V2AckFields, bool) {
	const fixedLen = 4 + 4 + 4 + 4 + 2 + 2
	if len(body) < fixedLen {
		return V2AckFields{}, false
	}
	off := 0
	corrID := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	received := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	accepted := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	rejected := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	errCode := binary.LittleEndian.Uint16(body[off : off+2])
	off += 2
	n := int(binary.LittleEndian.Uint16(body[off : off+2]))
	off += 2
	if len(body) < off+n {
		return V2AckFields{}, false
	}
	return V2AckFields{
		CorrID:         corrID,
		Received:       received,
		Accepted:       accepted,
		Rejected:       rejected,
		FirstErrCode:   errCode,
		FirstErrReason: string(body[off : off+n]),
	}, true
}
