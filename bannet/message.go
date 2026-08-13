package bannet

import ()

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
