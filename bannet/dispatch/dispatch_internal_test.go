package dispatch

// 同包内测试：直接构造未导出字段，精确复现 SendMsgToTaskQueue 的
// divide-by-zero panic（workerPoolSize==0）。外部黑盒测试无法构造出这个
// 内部状态组合，故放在这个内部测试文件里。原位于 bannet 包的
// msghandle_internal_test.go，随 msghandle.go 整体迁入 dispatch（重构第
// 三步），fakeConnForDivZero 从"内嵌 bannet.Connection 再覆盖 ID()"改成
// 直接实现 handler.Conn 接口——dispatch 不依赖 transport，不能再内嵌
// transport 包的具体连接类型。

import (
	"net"
	"testing"

	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/bannet/handler"
)

// fakeConnForDivZero 是 handler.Conn 的最小测试替身：本测试只关心 ID()
// 返回什么（用于取模），其余方法都是不会被调用到的空实现。
type fakeConnForDivZero struct{}

var _ handler.Conn = &fakeConnForDivZero{}

func (f *fakeConnForDivZero) Start()                           {}
func (f *fakeConnForDivZero) Stop()                            {}
func (f *fakeConnForDivZero) TCPConn() *net.TCPConn            { return nil }
func (f *fakeConnForDivZero) ID() uint32                       { return 5 }
func (f *fakeConnForDivZero) RemoteAddr() net.Addr             { return nil }
func (f *fakeConnForDivZero) SendMsg(string, []byte) error     { return nil }
func (f *fakeConnForDivZero) SendBuffMsg(string, []byte) error { return nil }
func (f *fakeConnForDivZero) SetProperty(string, any)          {}
func (f *fakeConnForDivZero) Property(string) any              { return nil }
func (f *fakeConnForDivZero) RemoveProperty(string)            {}

// TestSendMsgToTaskQueue_ZeroWorkerPoolSizeNoPanic 锁定回归：workerPoolSize==0 时
// SendMsgToTaskQueue 此前会在 `request.Conn().ID() % m.workerPoolSize` 上直接
// panic: integer divide by zero。触发条件是 Connection.useWorkerPool（连接构造
// 时快照的 config.G.WorkerPoolSize>0）与本结构体的 workerPoolSize（MsgHandle
// 构造时快照的同一全局配置）两次独立快照不一致——本仓库测试普遍会临时改写
// config.G 的字段，不是纯理论场景。
//
// 修复后应直接同步执行 DoMsgHandle，不做取模，不 panic。
func TestSendMsgToTaskQueue_ZeroWorkerPoolSizeNoPanic(t *testing.T) {
	registered := false
	m := &MsgHandle{
		routers:        map[string]handler.Handler{"PUT": &recordingHandler{onHandle: func() { registered = true }}},
		workerPoolSize: 0, // 关键：制造与某个 Connection.useWorkerPool=true 不一致的状态
		taskQueues:     make([]chan handler.Request, 0),
	}
	req := &request{msg: &codec.Message{ID: "PUT"}, conn: &fakeConnForDivZero{}}

	m.SendMsgToTaskQueue(req) // 修复前：这一行 panic。修复后：直接同步执行。

	if !registered {
		t.Fatal("workerPoolSize=0 时应直接同步分派到已注册的 Handler，而不是丢弃请求")
	}
}

type recordingHandler struct {
	handler.BaseRouter
	onHandle func()
}

func (h *recordingHandler) Handle(handler.Request) { h.onHandle() }
