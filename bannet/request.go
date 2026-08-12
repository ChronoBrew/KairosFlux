package bannet

type request struct {
	msg  Frame
	conn Conn
}

var _ Request = &request{}

func newRequest(msg Frame, conn Conn) *request {
	return &request{
		msg:  msg,
		conn: conn,
	}
}
func (req *request) GetMsgData() []byte {
	return req.msg.GetData()
}

// SetMsgData 改写负载并同步长度，避免 DataLen 与实际数据漂移。
func (req *request) SetMsgData(data []byte) {
	req.msg.SetData(data)
	req.msg.SetMsgLen(uint32(len(data)))
}

func (req *request) GetMsgID() string {
	return req.msg.GetMsgID()
}

func (req *request) GetConnection() Conn {
	return req.conn
}
