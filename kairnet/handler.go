package kairnet

import "github.com/ChronoBrew/KairosFlux/kairnet/handler"

// 本文件是重构第二步（拆 handler 包，见 docs/rfc/bannet-重构.md C.7 步骤 2）的
// 门面：Conn/Request/Handler/HookAction/BaseRouter 的实现已经搬进
// kairnet/handler，这里用类型别名（非包装类型）把根包的公开标识符原样保留——
// 已有调用方（service/router.go、service/ingesthook/filter.go 等）不需要
// 改一行代码或 import 路径。
//
// Conn 相对 RFC 迁移映射表有一处记录在案的偏差（见 kairnet/handler/handler.go
// 顶部注释）：表里写的是"迁入 transport"，实际迁入了 handler，以避免
// handler→transport→（第三步之后）与 dispatch 的隐式耦合与 RFC 自身要求的
// "transport 与 dispatch 互不依赖"冲突。根包 kairnet.Conn 这个名字与其行为
// 不受影响。

// Conn 是业务 Handler 能拿到的连接契约，参见 kairnet/handler.Conn。
type Conn = handler.Conn

// Request 是一次分派给 Handler 的请求单元，参见 kairnet/handler.Request。
type Request = handler.Request

// RequestV2 是一次分派给 HandlerV2 的 Kair v2 请求单元，参见
// kairnet/handler.RequestV2。
type RequestV2 = handler.RequestV2

// HandlerV2 是处理 Kair v2 帧的业务契约，参见 kairnet/handler.HandlerV2。
type HandlerV2 = handler.HandlerV2

// Handler 是业务代码要实现的契约，参见 kairnet/handler.Handler。
type Handler = handler.Handler

// HookAction 是 PreHandle 钩子的处置决定，参见 kairnet/handler.HookAction。
type HookAction = handler.HookAction

const (
	// HookPass 放行，继续走 Handle。
	HookPass = handler.HookPass
	// HookDrop 丢弃该帧，跳过 Handle。
	HookDrop = handler.HookDrop
)

// BaseRouter 是 Handler 的默认空实现，参见 kairnet/handler.BaseRouter。
type BaseRouter = handler.BaseRouter
