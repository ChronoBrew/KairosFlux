package bannet

import ()

type BaseRouter struct{}

var _ IRouter = &BaseRouter{}

func (b *BaseRouter) PreHandle(req IRequest) HookAction { return HookPass }

func (b *BaseRouter) Handle(req IRequest) {}

func (b *BaseRouter) PostHandle(req IRequest) {}
