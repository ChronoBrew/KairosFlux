package kairnet

import "github.com/ChronoBrew/KairosFlux/kairnet/transport"

// 本文件是重构第五步、也是最后一步（拆 transport 包，见
// docs/rfc/bannet-重构.md C.7 步骤 5）的门面：Connection/ConnManager/
// ConnRegistry 的实现已经搬进 kairnet/transport，这里用类型别名（非包装
// 类型）把根包的公开标识符原样保留——已有调用方与测试（本包内
// lifecycle_convergence_test.go 里对 *kairnet.Connection 的类型断言等）
// 不需要改一行代码或 import 路径。
//
// 相对 RFC 迁移映射表的一处偏差（第二处，第一处见 handler.go 关于 Conn
// 的说明）：表里把 acceptLoop/Server 也归到 transport（"server.go 的
// acceptLoop/Start/退避逻辑 → transport，直接移动"）。实现时发现 Server
// 是把 transport（监听/连接）、dispatch（MsgHandle）、进程级信号处理
// 三者组合在一起的编排对象，把它整体搬进 transport 会让 transport 依赖
// dispatch（Server.AddRouter 转发给 MsgHandle、Server.Stop 需要先后驱动
// MsgHandle.Stop 与 ConnMgr 的三个方法），直接违反 C.4.2 强调的"transport
// 与 dispatch 是兄弟关系，互相不依赖"。Server 保留在根包，只是把它内部
// 依赖的原始连接收发能力（Connection/ConnManager）委托给 transport——这与
// C.4.1 里"根包：组合门面...内部委托给上面五个子包"的定位一致，只是
// "委托的具体形态"是 Server 结构体本身留在根包，而不是被物理搬走。
// acceptLoop 是 Server 的方法，因此也留在根包（server.go），但它现在调用
// transport.NewConnection 而不是本包内的构造函数。

// Connection 是传输层的连接实现，参见 kairnet/transport.Connection。
type Connection = transport.Connection

// ConnRegistry 是连接注册表的契约，参见 kairnet/transport.ConnRegistry。
type ConnRegistry = transport.ConnRegistry

// ConnManager 是 ConnRegistry 的默认实现，参见 kairnet/transport.ConnManager。
type ConnManager = transport.ConnManager

// NewConnManager 构造一个 ConnManager，转发到 kairnet/transport.NewConnManager。
func NewConnManager() *ConnManager {
	return transport.NewConnManager()
}
