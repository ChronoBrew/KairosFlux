package bannet

import (
	"log/slog"

	"github.com/NeverENG/BanDB/config"
)

type MsgHandle struct {
	Arip           map[string]Handler
	WorkerPoolSize uint32
	TaskQueue      []chan Request
}

func NewMsgHandle() *MsgHandle {
	return &MsgHandle{
		Arip:           make(map[string]Handler),
		WorkerPoolSize: config.G.WorkerPoolSize,
		TaskQueue:      make([]chan Request, config.G.WorkerPoolSize),
	}
}

var _ Dispatcher = &MsgHandle{}

func (m *MsgHandle) AddRouter(msgID string, r Handler) {
	if _, ok := m.Arip[msgID]; ok {
		slog.Warn("banNet duplicate route registration ignored", "msgID", msgID)
		return
	}
	m.Arip[msgID] = r
}

func (m *MsgHandle) DoMsgHandle(request Request) {
	handler, ok := m.Arip[request.GetMsgID()]
	if !ok {
		slog.Error("banNet unregistered msgID", "msgID", request.GetMsgID())
		return
	}
	if handler.PreHandle(request) == HookDrop {
		return
	}
	handler.Handle(request)
	handler.PostHandle(request)
}

func (m *MsgHandle) StartWorkerPool() {
	for i := 0; i < int(m.WorkerPoolSize); i++ {
		m.TaskQueue[i] = make(chan Request, config.G.MaxWorkerTaskLen)
		go m.StartOneWorker(i, m.TaskQueue[i])
	}
}

func (m *MsgHandle) SendMsgToTaskQueue(request Request) {
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

func (m *MsgHandle) StartOneWorker(workerID int, taskQueue chan Request) {
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
