// Package handler 是 bannet 面向业务代码的契约层：用户实际要实现/使用的
// 唯一一组类型（Handler/Request/HookAction/Conn），边界不因本次重构而
// 变复杂——这是重构 RFC（docs/rfc/bannet-重构.md）C.2 所说的"业务层"。
//
// 本包不依赖 bannet 的任何其它子包（codec/lifecycle/transport/dispatch），
// 只依赖标准库 net——这是刻意的：它是分层依赖图里的叶子包之一，
// transport（持有 Conn 的具体实现 Connection）与 dispatch（持有引用
// Conn/Request 的 request 分派单元）都需要依赖它，但它不能反过来依赖
// 两者中的任何一个，否则会形成环。
//
// # 与迁移映射表的一处偏差（有意为之，非遗漏）
//
// RFC 附录 C.5 的迁移映射表把 Conn 标注为迁入 transport（与 ConnRegistry
// 放在一起）。实现时发现这会造成一个真实的循环依赖：Request 接口的
// Conn() 方法必须返回 Conn 类型，而 Request 按同一张表要迁入 handler；
// 若 Conn 留在 transport，handler 就需要 import transport 才能声明
// Request.Conn() 的返回类型——但 transport 的 Connection 又需要把解出来
// 的帧分派给 dispatch（层级上第三步会把这个分派决策移交 dispatch），
// RFC 本身又明确要求"transport 与 dispatch 是兄弟关系，互相不依赖，
// 只在根包协调"（C.4.2）。把 Conn 和 Request/Handler/HookAction 放在
// 同一个更底层的 handler 包里，能同时满足这两条约束：
//
//   - transport 依赖 handler（引用 Conn 类型），不依赖 dispatch；
//   - dispatch 依赖 handler（引用 Request/Handler/Conn 类型），不依赖 transport；
//   - transport 与 dispatch 之间仍然没有任何一条边——RFC 真正要保护的
//     不变量（"兄弟层不能互相 import"）原样成立，只是 Conn 的物理位置
//     从"迁移表字面写的 transport"挪到了"能让这条不变量成立的 handler"。
//
// 该偏差已记录在迭代文档里；对外行为无影响：根包 bannet.Conn 仍是
// 同一个类型（现在是 handler.Conn 的别名），外部调用方无需改动。
package handler

import "net"

// Conn 是业务 Handler 能拿到的连接契约：发送响应、读写连接级 key-value
// 属性、查询连接标识。具体实现（transport.Connection）在 transport 包，
// 但接口本身放在这里，理由见本文件顶部注释。
type Conn interface {
	Start()
	Stop()
	// BeginClosing 标记连接进入生命周期的 Closing 阶段（幂等）：只广播
	// "决定关闭"的信号，不做任何物理清理，socket 与写路径此时仍然可用。
	// 是重构第四步（拆 lifecycle 包）新增的方法，供 Server 优雅关闭时批量
	// 广播给所有连接——Reader 据此在完成当前在途请求后主动退出读循环，
	// 而不是被 Stop() 强行打断阻塞的读。与 Start/Stop 一样属于连接生命周期
	// 管理，放进同一个接口不算引入新的关注点，只是这个接口本来就同时承担
	// "业务发送 API" 与"生命周期控制"两类方法。
	BeginClosing()
	TCPConn() *net.TCPConn
	ID() uint32
	RemoteAddr() net.Addr
	SendMsg(msgID string, data []byte) error
	SendBuffMsg(msgID string, data []byte) error
	// SendRawMsg 直接把一个已经编码好的完整帧写出去，不经过 v1 的
	// codec.DataPack.Pack（那会把 data 当成 v1 msgID+payload 重新打包，产出
	// 错误的字节）。BANLV v2 响应（RFC docs/rfc/BANLV-2.md §2/§3/§11）与 §5
	// 的 HELLO 协商响应都用 codec.DataPackV2（或协商阶段的裸 v1 探测响应）
	// 自行编码好整帧后调用这个方法——v2 连接上的每一次响应都必须走这里，
	// 不能走 SendMsg/SendBuffMsg（那两个方法硬编码 v1 帧格式，v2 连接上调用
	// 会在 wire 上产出与协商结果不符的 v1 帧，是静默的协议破坏，不是"两者都
	// 能用、选一个方便的"）。
	SendRawMsg(frame []byte) error
	SetProperty(key string, value any)
	Property(key string) any
	RemoveProperty(key string)
}

// Request 是一次分派给 Handler 的请求单元：已解码帧 + 发出该帧的连接引用。
// 具体实现（dispatch 包的 request 结构体，原 bannet/request.go）在 dispatch，
// 接口放在 handler 是因为它是业务代码实际看到的契约类型，理由同 Conn。
type Request interface {
	Conn() Conn
	MsgData() []byte
	MsgID() string
	// SetMsgData 改写本帧负载，供 PreHandle 钩子做脱敏/裁剪。
	SetMsgData([]byte)
}

// RequestV2 是一次分派给 HandlerV2 的 BANLV v2 请求单元（RFC
// docs/rfc/BANLV-2.md §2/§3）：已解码的 v2 帧字段 + 发出该帧的连接引用。
// 与 v1 的 Request 分开是刻意的——v2 帧没有字符串 msgID，分派键是数字
// opcode，且额外携带 type/corr_id 两个 v1 没有的字段，勉强复用同一个接口
// 会让 v1 的 MsgID() 在 v2 场景下变成"opcode 的字符串化"这种没有意义的
// 兼容层，故用一个独立的、字段对齐 v2 帧结构的接口。
type RequestV2 interface {
	Conn() Conn
	Opcode() uint8
	Type() uint16
	CorrID() uint32
	Data() []byte
}

// HandlerV2 是处理 BANLV v2 帧的业务契约：单一方法内部按 Opcode 自行分派
// （与 v1 Handler.Handle 内部按 MsgID switch 的写法对称，见 service.Router.
// Handle）。不设 PreHandle/PostHandle 两个钩子——v2 的 ack=window/none 语义
// （RFC §11.2）需要在一次 HandleV2 调用内完成"这一帧算不算被接受"的判定
// 并累计计数，拆成三段钩子不会让这件事更清晰，只会让计数逻辑被迫散落在
// 三个方法之间。
type HandlerV2 interface {
	HandleV2(req RequestV2)
}

// HookAction 是 PreHandle 钩子对一帧请求的处置决定。
type HookAction int

const (
	// HookPass 放行，继续走 Handle。
	HookPass HookAction = iota
	// HookDrop 丢弃该帧，跳过 Handle。丢弃方负责回写唯一响应，避免响应错位。
	HookDrop
)

// Handler 是业务代码要实现的唯一契约：PreHandle 做前置校验/脱敏，
// Handle 做实际业务逻辑，PostHandle 做收尾（如指标记录）。
type Handler interface {
	PreHandle(request Request) HookAction
	Handle(request Request)
	PostHandle(request Request)
}

// BaseRouter 是 Handler 的默认空实现，业务 Router 可选择性嵌入后只覆盖
// 需要的方法（PreHandle/Handle/PostHandle 均为三个空方法）。
type BaseRouter struct{}

var _ Handler = &BaseRouter{}

func (b *BaseRouter) PreHandle(req Request) HookAction { return HookPass }

func (b *BaseRouter) Handle(req Request) {}

func (b *BaseRouter) PostHandle(req Request) {}
