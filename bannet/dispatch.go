package bannet

import "github.com/NeverENG/BanDB/bannet/dispatch"

// 本文件是重构第三步（拆 dispatch 包，见 docs/rfc/bannet-重构.md C.7 步骤 3）
// 的门面：Dispatcher/MsgHandle 的实现已经搬进 bannet/dispatch，这里用类型
// 别名（非包装类型）把根包的公开标识符原样保留，server.go 等根包内部调用方
// 不需要改一行代码或 import 路径；本包也没有外部（bannet 之外）调用方直接
// 引用这两个类型（server.go 的 MsgHandle 字段类型是 Dispatcher 接口），
// 保留别名单纯是为了不破坏根包自身现有的引用点。

// Dispatcher 是分发层对外契约，参见 bannet/dispatch.Dispatcher。
type Dispatcher = dispatch.Dispatcher

// MsgHandle 是 Dispatcher 的默认实现（路由表 + worker 池调度），
// 参见 bannet/dispatch.MsgHandle。
type MsgHandle = dispatch.MsgHandle

// NewMsgHandle 构造一个 MsgHandle，转发到 bannet/dispatch.NewMsgHandle。
func NewMsgHandle() *MsgHandle {
	return dispatch.NewMsgHandle()
}
