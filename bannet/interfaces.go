// 本文件集中定义 bannet 目前还没搬到子包的对外接口
// （ConnRegistry/Dispatcher，分别是重构第 5/3 步的迁移目标）。
// 接口与其实现同包是 Go 的惯例（如 net/http 的 Handler 接口与 Server
// 实现同包），故不再单设 iface 包。

package bannet

// Codec 的定义见 codec.go（类型别名，指向 bannet/codec.Codec）——重构第一步
// 把编解码相关类型迁到 bannet/codec 子包，根包只保留别名，故此处不再重复定义。

// Conn/Request/Handler/HookAction/BaseRouter 的定义见 handler.go（类型别名，
// 指向 bannet/handler 的对应类型）——重构第二步把业务契约类型迁到
// bannet/handler 子包，根包只保留别名，故此处不再重复定义。

type ConnRegistry interface {
	Add(conn Conn)
	Remove(conn Conn)
	Get(connID uint32) Conn
	Len() int
	ClearConn()
}

type Dispatcher interface {
	AddRouter(msgID string, router Handler)
	DoMsgHandle(request Request)
	StartWorkerPool()
	SendMsgToTaskQueue(request Request)
	Stop()
}
