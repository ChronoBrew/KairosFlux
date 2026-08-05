package banIface

import "net"

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
