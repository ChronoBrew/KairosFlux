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
	// StatusNotFound 表示 key 不存在（或其最新版本是删除墓碑）。
	//
	// 与 StatusError 分开是必要的：二者对客户端的含义完全不同——「键不存在」是正常的查询
	// 结果，不应重试也不应记为故障；而 StatusError 指示服务端异常。若二者共用一个状态，
	// 客户端无从区分，也就无法表达「键不存在」这一常规语义。
	//
	// 对旧客户端向后兼容：客户端只把 StatusOK 视为成功、未知状态一律按失败处理，
	// 故新增状态不会改变其行为。
	StatusNotFound = "notfound"
)
