package kairnet

import "github.com/ChronoBrew/KairosFlux/kairnet/handler"

// 本文件是重构第二步（拆 handler 包，见 docs/rfc/bannet-重构.md C.7 步骤 2）的
// 门面：Conn/Request/Handler/HookAction 的实现已经搬进 kairnet/handler，这里
// 用类型别名（非包装类型）把根包的公开标识符原样保留——已有调用方
// （service/router.go、service/ingesthook/filter.go 等）不需要改一行代码或
// import 路径。
//
// 分层收缩（五层重构，docs/调研-架构调整-分层与Node门面.md）：传输层公共 API
// 只暴露 Handler/HandlerV2 接口，不带任何 Router 概念——BaseRouter 别名已
// 随本次重构从根包撤出（kairnet/handler.BaseRouter 仍在 handler 包内，
// kairnet 自身的测试需要空实现时直接 import 该包），Server 的注册方法
// 相应换成 AddHandler/SetV2Handler（AddRouter/AddRouterV2 只作 deprecated
// 过渡别名）。
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

