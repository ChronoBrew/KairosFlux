package bannet

import (
	"log/slog"

	"github.com/NeverENG/BanDB/config"
)

type MsgHandle struct {
	routers        map[string]Handler
	WorkerPoolSize uint32
	TaskQueue      []chan Request
}

func NewMsgHandle() *MsgHandle {
	return &MsgHandle{
		routers:        make(map[string]Handler),
		WorkerPoolSize: config.G.WorkerPoolSize,
		TaskQueue:      make([]chan Request, config.G.WorkerPoolSize),
	}
}

var _ Dispatcher = &MsgHandle{}

func (m *MsgHandle) AddRouter(msgID string, r Handler) {
	if _, ok := m.routers[msgID]; ok {
		slog.Warn("banNet duplicate route registration ignored", "msgID", msgID)
		return
	}
	m.routers[msgID] = r
}

func (m *MsgHandle) DoMsgHandle(request Request) {
	handler, ok := m.routers[request.MsgID()]
	if !ok {
		slog.Error("banNet unregistered msgID", "msgID", request.MsgID())
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
	workerID := request.Conn().ID() % m.WorkerPoolSize

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
