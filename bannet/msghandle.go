package bannet

import (
	"log/slog"
	"runtime/debug"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/internal/metrics"
)

type MsgHandle struct {
	routers        map[string]Handler
	workerPoolSize uint32
	taskQueues     []chan Request
}

func NewMsgHandle() *MsgHandle {
	return &MsgHandle{
		routers:        make(map[string]Handler),
		workerPoolSize: config.G.WorkerPoolSize,
		taskQueues:     make([]chan Request, config.G.WorkerPoolSize),
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

// DoMsgHandle 分派一条请求给注册的 Handler。
//
// recover 兜底：这里执行的是外部注册的业务代码（PreHandle/Handle/PostHandle），
// 一个坏帧触发业务代码里的 nil 解引用/越界/类型断言失败等任意 panic，此前会
// 直接顺着调用栈往上冒——由 go DoMsgHandle(req) 或 worker 池的
// for range taskQueue { DoMsgHandle(request) } 发起的这个 goroutine 一旦
// panic 不被捕获，Go 运行时会终止整个进程，不只是这一个连接。审计时用一个
// 故意 panic 的 Handler 验证过这一点是真实可触发的（服务端连同其它所有正常
// 连接一起被打死），不是假设风险。
//
// worker 池场景下后果更严重：panic 不仅崩进程，还会让那个 worker 的
// for range 循环终止——如果这次侥幸没崩掉整个进程（比如未来某处加了顶层
// recover），这个 worker 也已经永久停止消费它的任务队列。故 recover 必须
// 挡在这里，而不是只挡在更外层。
func (m *MsgHandle) DoMsgHandle(request Request) {
	defer func() {
		if r := recover(); r != nil {
			metrics.PanicsRecovered.Add(1)
			slog.Error("banNet handler panicked, recovered", "msgID", request.MsgID(),
				"panic", r, "stack", string(debug.Stack()))
		}
	}()

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
	for i := 0; i < int(m.workerPoolSize); i++ {
		m.taskQueues[i] = make(chan Request, config.G.MaxWorkerTaskLen)
		go m.StartOneWorker(i, m.taskQueues[i])
	}
}

// SendMsgToTaskQueue 按连接 ID 取模分派到专属 worker，专属队列满时 work-stealing。
//
// workerPoolSize==0 时直接同步执行，不做取模——见下方注释。这不只是防御性判空：
// Connection.useWorkerPool 与本结构体的 workerPoolSize 是两次独立的
// config.G.WorkerPoolSize 快照（分别在连接构造、MsgHandle 构造时读取），一旦
// 两次快照之间全局配置被改过（测试里很常见，本仓库测试普遍会临时改写
// config.G 的字段），前者为 true 而后者为 0 就会在下面的取模上直接
// panic: integer divide by zero——审计时用最小复现验证过是真实可触发的
// panic，不是假设。
func (m *MsgHandle) SendMsgToTaskQueue(request Request) {
	if m.workerPoolSize == 0 {
		m.DoMsgHandle(request)
		return
	}
	workerID := request.Conn().ID() % m.workerPoolSize

	// 优先投递到专属 Worker
	select {
	case m.taskQueues[workerID] <- request:
		return
	default:
	}

	// Work stealing: 专属队列满时，轮询其他 Worker
	for i := uint32(1); i < m.workerPoolSize; i++ {
		tryID := (workerID + i) % m.workerPoolSize
		select {
		case m.taskQueues[tryID] <- request:
			return
		default:
		}
	}

	// 全部满，退化为阻塞等待
	m.taskQueues[workerID] <- request
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
	for i := 0; i < int(m.workerPoolSize); i++ {
		if m.taskQueues[i] != nil {
			close(m.taskQueues[i])
		}
	}
}
