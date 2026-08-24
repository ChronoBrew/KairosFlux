package dispatch

import (
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/handler"
)

// requestV2 是 Kair v2（docs/rfc/Kair-2.md）"已解码帧 + 连接引用"的分派
// 单元，与 v1 的 request（request.go）对称，字段来源是 codec.MessageV2
// 而不是 codec.Message——v2 帧的 opcode/type/corr_id 都是 v1 没有的字段，
// 直接照抄 v1 的 request 会丢失它们，故是独立类型而非泛化改造 request。
//
// 本类型不经由分发层的 worker 池/路由表调度（root 包的 onFrameV2 回调同步
// 调用 HandlerV2.HandleV2，见 kairnet/server.go 的注释）——放在 dispatch 包
// 只是为了与 v1 request 的构造位置保持一致（同一个包依赖 codec+handler，
// 不依赖 transport），不代表复用了 MsgHandle 的调度能力。
type requestV2 struct {
	msg  *codec.MessageV2
	conn handler.Conn
}

var _ handler.RequestV2 = &requestV2{}

// NewRequestV2 构造一个 RequestV2。
func NewRequestV2(msg *codec.MessageV2, conn handler.Conn) handler.RequestV2 {
	return &requestV2{msg: msg, conn: conn}
}

func (r *requestV2) Conn() handler.Conn { return r.conn }
func (r *requestV2) Opcode() uint8      { return r.msg.Header.Opcode }
func (r *requestV2) Type() uint16       { return r.msg.Header.Type }
func (r *requestV2) CorrID() uint32     { return r.msg.Header.CorrID }
func (r *requestV2) Data() []byte       { return r.msg.Payload }
