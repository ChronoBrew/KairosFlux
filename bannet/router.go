package bannet

import ()

type BaseRouter struct{}

var _ Handler = &BaseRouter{}

func (b *BaseRouter) PreHandle(req Request) HookAction { return HookPass }

func (b *BaseRouter) Handle(req Request) {}

func (b *BaseRouter) PostHandle(req Request) {}
