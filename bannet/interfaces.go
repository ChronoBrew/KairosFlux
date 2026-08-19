// 本文件集中定义 bannet 目前还没搬到子包的对外接口
// （ConnRegistry，重构第 5 步的迁移目标）。接口与其实现同包是 Go 的惯例
// （如 net/http 的 Handler 接口与 Server 实现同包），故不再单设 iface 包。

package bannet

import "time"

// Codec 的定义见 codec.go（类型别名，指向 bannet/codec.Codec）——重构第一步
// 把编解码相关类型迁到 bannet/codec 子包，根包只保留别名，故此处不再重复定义。

// Conn/Request/Handler/HookAction/BaseRouter 的定义见 handler.go（类型别名，
// 指向 bannet/handler 的对应类型）——重构第二步把业务契约类型迁到
// bannet/handler 子包，根包只保留别名，故此处不再重复定义。

// Dispatcher/MsgHandle 的定义见 dispatch.go（类型别名，指向 bannet/dispatch
// 的对应类型）——重构第三步把分发层迁到 bannet/dispatch 子包，根包只保留
// 别名，故此处不再重复定义。

type ConnRegistry interface {
	Add(conn Conn)
	Remove(conn Conn)
	Get(connID uint32) Conn
	Len() int
	ClearConn()

	// BeginClosingAll/Wait 是重构第四步（修复 bug①）新增的两个方法，
	// 支撑 Server 优雅关闭的"广播 -> 等待 -> 强制"三段式：
	// BeginClosingAll 给所有连接广播"决定关闭"（不物理清理），Wait
	// 阻塞到所有连接都已完成物理关闭或超时。ClearConn 保留作为超时后
	// 的强制兜底，语义不变。
	BeginClosingAll()
	Wait(timeout time.Duration) bool
}
