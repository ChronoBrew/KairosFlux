package dispatch

import (
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/handler"
)

// request 是"已解码帧 + 连接引用"的分派单元，归属分发层——见
// docs/rfc/bannet-重构.md 迁移映射表：request.go 整体迁入 dispatch。
// 它依赖 codec.Message（帧的内存表示）与 handler.Conn/handler.Request
// （业务契约），这正是 dispatch 在依赖图里的位置：dispatch --> codec,
// dispatch --> handler，不依赖 transport。
type request struct {
	msg  *codec.Message
	conn handler.Conn
}

var _ handler.Request = &request{}

// NewRequest 构造一个 Request。原本（root 包内部）是未导出的 newRequest——
// 迁入独立子包后，调用方（transport 层的连接读循环）与 dispatch 已经是
// 不同的包，构造函数必须导出。
func NewRequest(msg *codec.Message, conn handler.Conn) handler.Request {
	return &request{
		msg:  msg,
		conn: conn,
	}
}

func (req *request) MsgData() []byte {
	return req.msg.Payload()
}

// SetMsgData 改写负载并同步长度，避免 DataLen 与实际数据漂移。
func (req *request) SetMsgData(data []byte) {
	req.msg.SetData(data)
	req.msg.SetMsgLen(uint32(len(data)))
}

func (req *request) MsgID() string {
	return req.msg.MsgID()
}

func (req *request) Conn() handler.Conn {
	return req.conn
}
