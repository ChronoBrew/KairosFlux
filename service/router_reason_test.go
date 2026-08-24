package service

import (
	"net"
	"testing"

	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// fakeConn 只实现测试需要的一个方法（SendBuffMsg），其余方法要么不会被调用、
// 要么返回零值即可——PreHandle 的丢弃路径只触碰 Conn().SendBuffMsg。
type fakeConn struct {
	lastMsgID string
	lastData  []byte
}

func (c *fakeConn) Start()                                  {}
func (c *fakeConn) Stop()                                   {}
func (c *fakeConn) BeginClosing()                           {}
func (c *fakeConn) TCPConn() *net.TCPConn                   { return nil }
func (c *fakeConn) ID() uint32                              { return 1 }
func (c *fakeConn) RemoteAddr() net.Addr                    { return nil }
func (c *fakeConn) SendMsg(msgID string, data []byte) error { return nil }
func (c *fakeConn) SendRawMsg(frame []byte) error           { return nil }
func (c *fakeConn) SendBuffMsg(msgID string, data []byte) error {
	c.lastMsgID = msgID
	c.lastData = data
	return nil
}
func (c *fakeConn) SetProperty(key string, value any) {}
func (c *fakeConn) Property(key string) any           { return nil }
func (c *fakeConn) RemoveProperty(key string)         {}

// fakePreHandleReq 是 kairnet.Request 的测试替身，只用于驱动 Router.PreHandle。
type fakePreHandleReq struct {
	conn *fakeConn
	data []byte
}

func (r *fakePreHandleReq) Conn() kairnet.Conn  { return r.conn }
func (r *fakePreHandleReq) MsgData() []byte     { return r.data }
func (r *fakePreHandleReq) MsgID() string       { return proto.MsgPut }
func (r *fakePreHandleReq) SetMsgData(d []byte) { r.data = d }

// TestDroppedPayload_RoundTrip 验证 droppedPayload 编码的字节可用 client 侧的
// parseStatus 语义正确解析：status="dropped"，随后紧跟 reasonLen(u16 LE)+reason，
// 与 docs/Kair-协议规范.md 记录的格式一致。
func TestDroppedPayload_RoundTrip(t *testing.T) {
	payload := droppedPayload("quote: non-positive price: open=-1")

	// 手工按 [statusLen u8][status][reasonLen u16 LE][reason] 解析，不引入
	// client 包依赖（避免 service -> client 的反向依赖）。
	n := int(payload[0])
	status := string(payload[1 : 1+n])
	if status != proto.StatusDropped {
		t.Fatalf("status=%q，期望 %q", status, proto.StatusDropped)
	}
	off := 1 + n
	reasonLen := int(payload[off]) | int(payload[off+1])<<8
	reason := string(payload[off+2 : off+2+reasonLen])
	if reason != "quote: non-positive price: open=-1" {
		t.Fatalf("reason=%q，与写入值不符", reason)
	}
}

// TestDroppedPayload_EmptyReasonDegradesToStatusPayload 验证 reason 为空时，
// droppedPayload 与旧的 statusPayload(dropped) 字节完全相同——这是老客户端
// （只读 statusLen+status、不管 rest）向后兼容的关键：即便本次改造让 reason
// 成为常态，为空时的行为也不该意外变化。
func TestDroppedPayload_EmptyReasonDegradesToStatusPayload(t *testing.T) {
	got := droppedPayload("")
	want := statusPayload(proto.StatusDropped)
	// 空 reason 时 droppedPayload 多出 2 字节的 reasonLen=0，这是老客户端从未
	// 读取过的「rest」部分，故这里只比对 statusPayload 覆盖的前缀部分。
	if string(got[:len(want)]) != string(want) {
		t.Fatalf("droppedPayload 的状态前缀与 statusPayload 不一致\n  got:  %x\n  want: %x", got[:len(want)], want)
	}
}

// TestRouterPreHandle_DropSendsReasonToConn 端到端验证：Router.PreHandle 在
// preHandleFunc 判定丢弃时，把 reason 编码进发给连接的响应负载——这是 QuantScout
// 反馈的真实问题的直接回归锁定：此前 reason 在这一路径上被悄悄丢弃。
func TestRouterPreHandle_DropSendsReasonToConn(t *testing.T) {
	r := NewRouterWithStore(nil)
	r.SetPreHandle(func(request kairnet.Request) (kairnet.HookAction, string) {
		return kairnet.HookDrop, "quote: missing required field \"code\""
	})

	conn := &fakeConn{}
	req := &fakePreHandleReq{conn: conn}

	if action := r.PreHandle(req); action != kairnet.HookDrop {
		t.Fatalf("PreHandle 应返回 HookDrop，得到 %v", action)
	}
	if conn.lastMsgID != proto.MsgRespErr {
		t.Fatalf("响应 msgID 应为 %q，得到 %q", proto.MsgRespErr, conn.lastMsgID)
	}

	n := int(conn.lastData[0])
	status := string(conn.lastData[1 : 1+n])
	if status != proto.StatusDropped {
		t.Fatalf("status=%q，期望 dropped", status)
	}
	off := 1 + n
	reasonLen := int(conn.lastData[off]) | int(conn.lastData[off+1])<<8
	reason := string(conn.lastData[off+2 : off+2+reasonLen])
	if reason != `quote: missing required field "code"` {
		t.Fatalf("reason 未正确回传，得到: %q", reason)
	}
}
