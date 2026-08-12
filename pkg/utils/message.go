package utils

import "encoding/binary"

type Message struct {
	Id string

	DataLen uint32
	Data    []byte
}

func NewKVData(key []byte, value []byte) []byte {
	Keylen := make([]byte, 4)
	valuelen := make([]byte, 4)

	binary.LittleEndian.PutUint32(Keylen, uint32(len(key)))
	binary.LittleEndian.PutUint32(valuelen, uint32(len(value)))

	return ByteBuilder(Keylen, valuelen, key, value)
}

func NewMessage(id string, key []byte, value []byte) *Message {
	data := NewKVData(key, value)
	return &Message{
		Id:      id,
		DataLen: uint32(len(data)),
		Data:    data,
	}
}

func NewMessage2(id string, data []byte) *Message {
	return &Message{
		Id:      id,
		DataLen: uint32(len(data)),
		Data:    data,
	}
}

func (m *Message) MsgID() string {
	return m.Id
}
func (m *Message) MsgLen() uint32 {
	return m.DataLen
}
func (m *Message) Payload() []byte {
	return m.Data
}

func (m *Message) SetMsgID(id string) {
	m.Id = id
}

func (m *Message) SetData(data []byte) {
	m.Data = data
}

func (m *Message) SetMsgLen(id uint32) {
	m.DataLen = id
}
