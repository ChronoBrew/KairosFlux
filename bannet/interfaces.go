// 本文件集中定义 bannet 的对外接口。接口与其实现同包是 Go 的惯例
// （如 net/http 的 Handler 接口与 Server 实现同包），故不再单设 iface 包。

package bannet

import "net"

type IConnManager interface {
	Add(conn IConnect)
	Remove(conn IConnect)
	Get(connID uint32) IConnect
	Len() int
	ClearConn()
}

type IDataPack interface {
	GetHeadLen() uint32
	Pack(msg IMessage) ([]byte, error)
	UnPack([]byte) (IMessage, error)
}

type IMsgHandle interface {
	AddRouter(msgID string, router IRouter)
	DoMsgHandle(request IRequest)
	StartWorkerPool()
	SendMsgToTaskQueue(request IRequest)
	Stop()
}

type IRequest interface {
	GetConnection() IConnect
	GetMsgData() []byte
	GetMsgID() string
	// SetMsgData 改写本帧负载，供 PreHandle 钩子做脱敏/裁剪。
	SetMsgData([]byte)
}

// HookAction 是 PreHandle 钩子对一帧请求的处置决定。
type HookAction int

const (
	// HookPass 放行，继续走 Handle。
	HookPass HookAction = iota
	// HookDrop 丢弃该帧，跳过 Handle。丢弃方负责回写唯一响应，避免响应错位。
	HookDrop
)

type IRouter interface {
	PreHandle(request IRequest) HookAction
	Handle(request IRequest)
	PostHandle(request IRequest)
}

type IConnect interface {
	Start()
	Stop()
	GetTCPConn() *net.TCPConn
	GetConnID() uint32
	RemoteAddr() net.Addr
	SendMsg(msgID string, data []byte) error
	SendBuffMsg(msgID string, data []byte) error
	SetProperty(key string, value any)
	GetProperty(key string) any
	RemoveProperty(key string)
}

type IMessage interface {
	GetMsgID() string
	GetData() []byte
	GetMsgLen() uint32
	SetMsgLen(uint32)
	SetData([]byte)
	SetMsgID(string)
}

type IServer interface {
	Start()
	Stop()
	Serve()
	AddRouter(msgID string, router IRouter)
	GetConnMgr() IConnManager
	SetConnStartFunc(func(connect IConnect))
	SetConnStopFunc(func(connect IConnect))
	CallConnStartFunc(connect IConnect)
	CallConnStopFunc(connect IConnect)
}
