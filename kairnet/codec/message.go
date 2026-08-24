// Package codec 是 Kair 帧格式的编解码层：字节 ↔ Message 的转换，只认字节，
// 不认 msgID 该分派给谁、不认连接生命周期——这是重构 RFC
// （docs/rfc/bannet-重构.md）第一步迁移的目标包，从根包 kairnet 的
// message.go/datapack.go 原样搬入，本步不改变任何字节布局或行为，只搬家。
package codec

// Message 是一帧的内存表示：ID（msgID 字符串）+ Data（负载字节）。
// IDLen/DataLen 是解码定长头部时使用的中间字段。
type Message struct {
	ID string

	IDLen   uint16 // 仅在 UnPack 解析头部时使用, 调用方据此再读取 ID 字节
	DataLen uint32
	Data    []byte
}

func NewMessage(id string, data []byte) *Message {
	return &Message{
		ID:      id,
		DataLen: uint32(len(data)),
		Data:    data,
	}
}

func (m *Message) MsgID() string {
	return m.ID
}
func (m *Message) MsgLen() uint32 {
	return m.DataLen
}
func (m *Message) Payload() []byte {
	return m.Data
}

func (m *Message) SetMsgID(id string) {
	m.ID = id
}

func (m *Message) SetData(data []byte) {
	m.Data = data
}

func (m *Message) SetMsgLen(length uint32) {
	m.DataLen = length
}
