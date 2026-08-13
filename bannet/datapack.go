package bannet

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// 报文格式:
//   [dataLen u32 LE][msgIDLen u16 LE][msgID bytes][data bytes]
// 头部固定 6 字节; msgID 与 data 是变长 trailing 部分。

type DataPack struct{}

var _ Codec = &DataPack{}

func NewDataPack() *DataPack { return &DataPack{} }

func (dp *DataPack) HeadLen() uint32 {
	return 6 // dataLen u32 + msgIDLen u16
}

// Pack 编码一帧。按最终长度一次性分配并直接写入定长头部，不经 bytes.Buffer 的增量扩容，
// 也不经 binary.Write 的反射路径——Pack 位于每个响应的必经路径上。
func (dp *DataPack) Pack(msg *Message) ([]byte, error) {
	id := msg.MsgID()
	if len(id) > 0xFFFF {
		return nil, fmt.Errorf("msgID too long: %d", len(id))
	}
	data := msg.Payload()

	head := int(dp.HeadLen())
	buf := make([]byte, head+len(id)+len(data))
	binary.LittleEndian.PutUint32(buf[0:4], msg.MsgLen())
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(id)))
	copy(buf[head:], id)
	copy(buf[head+len(id):], data)
	return buf, nil
}

// UnPack 只解析定长头部。
//
// 它不校验帧长上限：那是策略而非编解码，由连接侧在读取负载前执行（见 Connection.
// StartReader）。此前该校验在此处每帧读两次全局配置——把策略留在边界，解码器才能保持
// 无状态、不依赖全局。
//
// 原注释：只解析定长头部 (6 字节), 返回带 DataLen 与 IDLen 的占位 Message;
// 调用方拿到 IDLen 后, 还需要从连接读取 IDLen+DataLen 字节填充 Id 与 Data。
func (dp *DataPack) UnPack(data []byte) (*Message, error) {
	if len(data) < int(dp.HeadLen()) {
		return nil, errors.New("head too short")
	}

	// 直接按小端解析定长头部，不经 bytes.Reader + binary.Read 的反射路径。
	msg := &Message{}
	msg.DataLen = binary.LittleEndian.Uint32(data[0:4])
	idLen := binary.LittleEndian.Uint16(data[4:6])

	// 借用 Id 暂存 IDLen 信息: 调用方先从 MsgID() 拿不到东西, 通过头部之后另读 IDLen 字节填回。
	// 这里用 SetMsgLen 仅保留 DataLen 不冲突, IDLen 通过返回的 Message.IDLen 提供。
	msg.IDLen = idLen
	return msg, nil
}
