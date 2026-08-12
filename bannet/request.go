package bannet

type Request struct {
	msg  IMessage
	conn IConnect
}

var _ IRequest = &Request{}

func NewRequest(msg IMessage, conn IConnect) *Request {
	return &Request{
		msg:  msg,
		conn: conn,
	}
}
func (req *Request) GetMsgData() []byte {
	return req.msg.GetData()
}

// SetMsgData 改写负载并同步长度，避免 DataLen 与实际数据漂移。
func (req *Request) SetMsgData(data []byte) {
	req.msg.SetData(data)
	req.msg.SetMsgLen(uint32(len(data)))
}

func (req *Request) GetMsgID() string {
	return req.msg.GetMsgID()
}

func (req *Request) GetConnection() IConnect {
	return req.conn
}
