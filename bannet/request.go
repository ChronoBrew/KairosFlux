package bannet

type request struct {
	msg  *Message
	conn Conn
}

var _ Request = &request{}

func newRequest(msg *Message, conn Conn) *request {
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

func (req *request) Conn() Conn {
	return req.conn
}
