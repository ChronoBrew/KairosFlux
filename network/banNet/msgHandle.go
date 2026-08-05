package banNet

import (
	"log/slog"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

type MsgHandle struct {
	Arip           map[string]banIface.IRouter
	WorkerPoolSize uint32
	TaskQueue      []chan banIface.IRequest
}

func NewMsgHandle() *MsgHandle {
	return &MsgHandle{
		Arip:           make(map[string]banIface.IRouter),
		WorkerPoolSize: config.G.WorkerPoolSize,
		TaskQueue:      make([]chan banIface.IRequest, config.G.WorkerPoolSize),
	}
}

var _ banIface.IMsgHandle = &MsgHandle{}

func (m *MsgHandle) AddRouter(msgID string, r banIface.IRouter) {
	if _, ok := m.Arip[msgID]; ok {
		slog.Warn("banNet duplicate route registration ignored", "msgID", msgID)
		return
	}
	m.Arip[msgID] = r
}

func (m *MsgHandle) DoMsgHandle(request banIface.IRequest) {
	handler, ok := m.Arip[request.GetMsgID()]
	if !ok {
		slog.Error("banNet unregistered msgID", "msgID", request.GetMsgID())
		return
	}
	if handler.PreHandle(request) == banIface.HookDrop {
		return
	}
	handler.Handle(request)
	handler.PostHandle(request)
}

func (m *MsgHandle) StartWorkerPool() {
	for i := 0; i < int(m.WorkerPoolSize); i++ {
		m.TaskQueue[i] = make(chan banIface.IRequest, config.G.MaxWorkerTaskLen)
		go m.StartOneWorker(i, m.TaskQueue[i])
	}
}

func (m *MsgHandle) SendMsgToTaskQueue(request banIface.IRequest) {
	workerID := request.GetConnection().GetConnID() % m.WorkerPoolSize

	// 优先投递到专属 Worker
	select {
	case m.TaskQueue[workerID] <- request:
		return
	default:
	}

	// Work stealing: 专属队列满时，轮询其他 Worker
	for i := uint32(1); i < m.WorkerPoolSize; i++ {
		tryID := (workerID + i) % m.WorkerPoolSize
		select {
		case m.TaskQueue[tryID] <- request:
			return
		default:
		}
	}

	// 全部满，退化为阻塞等待
	m.TaskQueue[workerID] <- request
}

func (m *MsgHandle) StartOneWorker(workerID int, taskQueue chan banIface.IRequest) {
	slog.Debug("banNet worker started", "workerID", workerID)
	// taskQueue 关闭时 range 自然结束，无需显式判空。
	for request := range taskQueue {
		m.DoMsgHandle(request)
	}
	slog.Debug("banNet worker exited", "workerID", workerID)
}

func (m *MsgHandle) Stop() {
	slog.Debug("banNet worker pool stopping")
	for i := 0; i < int(m.WorkerPoolSize); i++ {
		if m.TaskQueue[i] != nil {
			close(m.TaskQueue[i])
		}
	}
}
