// Package proto 定义客户端/服务端的命名协议常量。
//
// 报文格式:
//
//	[dataLen u32 LE][msgIDLen u16 LE][msgID bytes][data bytes]
//
// GET 响应 data 负载: [statusLen u8][status bytes][valueLen u32 LE][value]
// PUT/DEL 响应 data 负载: [statusLen u8][status bytes]
package proto

// 请求/响应消息类型。
const (
	MsgPut     = "PUT"
	MsgGet     = "GET"
	MsgDelete  = "DEL"
	MsgScan    = "SCAN"
	MsgRespOK  = "OK"
	MsgRespErr = "ERR"
	MsgHello   = "HELLO"
	MsgBye     = "BYE"
)

// 响应负载内的状态字段。
const (
	StatusOK         = "ok"
	StatusError      = "error"
	StatusDropped    = "dropped"    // 被 PreHandle 钩子按策略丢弃，非传输错误
	StatusOverloaded = "overloaded" // 被网关自适应准入 shed（过载拒绝），可重试
)
