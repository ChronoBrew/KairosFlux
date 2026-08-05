package banNet

import (
	"github.com/NeverENG/BanDB/network/banIface"
)

type BaseRouter struct{}

var _ banIface.IRouter = &BaseRouter{}

func (b *BaseRouter) PreHandle(req banIface.IRequest) banIface.HookAction { return banIface.HookPass }

func (b *BaseRouter) Handle(req banIface.IRequest) {}

func (b *BaseRouter) PostHandle(req banIface.IRequest) {}
