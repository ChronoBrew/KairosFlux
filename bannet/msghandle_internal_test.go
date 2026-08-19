package bannet

// 同包内测试：直接构造未导出字段，精确复现 SendMsgToTaskQueue 的
// divide-by-zero panic（workerPoolSize==0）。外部黑盒测试（bannet_test 包）
// 无法构造出这个内部状态组合，故放在这个内部测试文件里。

import "testing"

type fakeConnForDivZero struct{ Connection }

func (f *fakeConnForDivZero) ID() uint32 { return 5 }

// TestSendMsgToTaskQueue_ZeroWorkerPoolSizeNoPanic 锁定回归：workerPoolSize==0 时
// SendMsgToTaskQueue 此前会在 `request.Conn().ID() % m.workerPoolSize` 上直接
// panic: integer divide by zero。触发条件是 Connection.useWorkerPool（连接构造
// 时快照的 config.G.WorkerPoolSize>0）与本结构体的 workerPoolSize（MsgHandle
// 构造时快照的同一全局配置）两次独立快照不一致——本仓库测试普遍会临时改写
// config.G 的字段（本文件内、以及其它包的测试都是这个惯例），不是纯理论场景。
//
// 修复后应直接同步执行 DoMsgHandle，不做取模，不 panic。
func TestSendMsgToTaskQueue_ZeroWorkerPoolSizeNoPanic(t *testing.T) {
	registered := false
	m := &MsgHandle{
		routers:        map[string]Handler{"PUT": &recordingHandler{onHandle: func() { registered = true }}},
		workerPoolSize: 0, // 关键：制造与某个 Connection.useWorkerPool=true 不一致的状态
		taskQueues:     make([]chan Request, 0),
	}
	req := &request{msg: &Message{ID: "PUT"}, conn: &fakeConnForDivZero{}}

	m.SendMsgToTaskQueue(req) // 修复前：这一行 panic。修复后：直接同步执行。

	if !registered {
		t.Fatal("workerPoolSize=0 时应直接同步分派到已注册的 Handler，而不是丢弃请求")
	}
}

type recordingHandler struct {
	BaseRouter
	onHandle func()
}

func (h *recordingHandler) Handle(Request) { h.onHandle() }
