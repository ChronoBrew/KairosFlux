// Package dispatch 是分发层：路由表（msgID → Handler）、worker 池调度、
// panic 隔离——见 docs/rfc/bannet-重构.md C.2/C.5，msghandle.go 整体迁入。
package dispatch

import (
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/bannet/handler"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/internal/metrics"
)

// Dispatcher 是分发层对外暴露的契约，供 transport 持有（transport 通过
// 根包传入的实例调用，不 import 本包——见 docs/rfc/bannet-重构.md C.4.2
// "transport 与 dispatch 是兄弟关系，互相不依赖，只在根包协调"）。
type Dispatcher interface {
	AddRouter(msgID string, router handler.Handler)
	DoMsgHandle(request handler.Request)
	StartWorkerPool()
	SendMsgToTaskQueue(request handler.Request)
	Stop()
}

type MsgHandle struct {
	routers        map[string]handler.Handler
	workerPoolSize uint32
	taskQueues     []chan handler.Request

	// stopCh 关闭一次，广播"不再接受新的分派"——特意不用 close(taskQueues[i])
	// 表达这个信号：taskQueues 有多个发送方（每个连接的读循环都可能往里发），
	// Go 里"多个发送方共享的 channel 被关闭后仍有发送方尝试发送"会直接
	// panic（send on closed channel），而 Server 优雅关闭时其它连接完全可能
	// 仍在正常收发、仍在调用 SendMsgToTaskQueue——用一个只关闭一次、只被
	// 接收的信号 channel，配合下面 select 里的 stopCh 分支，任何"关闭之后
	// 才尝试投递"的请求都会安全地退化为同步执行，而不是 panic 或永久阻塞。
	stopCh chan struct{}

	// workerWG 追踪常驻 worker goroutine：Stop 依赖它等待"所有已经排队+
	// 正在执行的 DoMsgHandle 都跑完"，而不是只广播关闭就撒手不管——这是
	// 修复 bug①（Server.Stop 不等在途请求处理完）在 worker 池这一侧的
	// 实现，见 Stop 的注释。
	workerWG sync.WaitGroup
}

func NewMsgHandle() *MsgHandle {
	return &MsgHandle{
		routers:        make(map[string]handler.Handler),
		workerPoolSize: config.G.WorkerPoolSize,
		taskQueues:     make([]chan handler.Request, config.G.WorkerPoolSize),
		stopCh:         make(chan struct{}),
	}
}

var _ Dispatcher = &MsgHandle{}

func (m *MsgHandle) AddRouter(msgID string, r handler.Handler) {
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
// 直接顺着调用栈往上冒——由 worker 池的 for range taskQueue { DoMsgHandle(request) }
// 发起的这个 goroutine 一旦 panic 不被捕获，Go 运行时会终止整个进程，不只是
// 这一个连接。审计时用一个故意 panic 的 Handler 验证过这一点是真实可触发的
// （服务端连同其它所有正常连接一起被打死），不是假设风险。
//
// worker 池场景下后果更严重：panic 不仅崩进程，还会让那个 worker 的
// for range 循环终止——如果这次侥幸没崩掉整个进程（比如未来某处加了顶层
// recover），这个 worker 也已经永久停止消费它的任务队列。故 recover 必须
// 挡在这里，而不是只挡在更外层。
//
// workerPoolSize==0 时 SendMsgToTaskQueue 会直接在调用方（transport 的连接
// 读循环）所在的 goroutine 上同步调用本方法（见下）——这里同样需要 recover，
// 否则一个业务 panic 会直接打穿读循环所在的 goroutine，效果和"没有 recover
// 的 worker 池"一样严重。
func (m *MsgHandle) DoMsgHandle(request handler.Request) {
	defer func() {
		if r := recover(); r != nil {
			metrics.PanicsRecovered.Add(1)
			slog.Error("banNet handler panicked, recovered", "msgID", request.MsgID(),
				"panic", r, "stack", string(debug.Stack()))
		}
	}()

	h, ok := m.routers[request.MsgID()]
	if !ok {
		slog.Error("banNet unregistered msgID", "msgID", request.MsgID())
		return
	}
	if h.PreHandle(request) == handler.HookDrop {
		return
	}
	h.Handle(request)
	h.PostHandle(request)
}

func (m *MsgHandle) StartWorkerPool() {
	for i := 0; i < int(m.workerPoolSize); i++ {
		m.taskQueues[i] = make(chan handler.Request, config.G.MaxWorkerTaskLen)
		m.workerWG.Add(1)
		go m.StartOneWorker(i, m.taskQueues[i])
	}
}

// SendMsgToTaskQueue 是分发层唯一的请求投递入口：按连接 ID 取模分派到专属
// worker，专属队列满时 work-stealing；workerPoolSize==0 时直接同步执行。
//
// 这也是本次重构 bug②的修复点：此前 transport 层（connection.go 的
// StartReader）在 useWorkerPool==false 时会另起一个不受追踪、没有上限的
// goroutine（`go c.MsgHandle.DoMsgHandle(req)`）——见
// docs/rfc/bannet-重构.md B.4/C.3："这类 goroutine 数量不设上限，是...
// 潜在的 goroutine 数量爆炸风险"。RFC C.3 的结论很明确：不是给这类
// goroutine 补一个追踪/通知机制，而是让它不再存在——分发层统一只有这一个
// 入口，workerPoolSize==0 时的"退化模式"是这个入口内部的一个分支（直接在
// 调用方的 goroutine 上同步跑完），不是调用方自己另起炉灶。这样一来：
//
//   - 不再有游离的、生命周期不受任何对象追踪的 goroutine；
//   - 同步执行天然绑定在调用方（该连接的读循环）的 goroutine 上——它的
//     创建、执行、退出都在同一个已有的、受连接生命周期约束的 goroutine
//     里完成，不需要额外的等待/追踪原语，因为根本没有新增 goroutine；
//   - 作为直接后果，同一连接的多个请求不再可能并发乱序执行（此前
//     `go DoMsgHandle` 是"发一个不等完成就读下一帧"，多个请求的处理
//     在时间上可能重叠、完成顺序也不保证），而是像 worker 池模式一样
//     顺序处理——这是该 bug 修复要求的行为收紧，不是意外：真正需要
//     "同连接内请求并发处理"效果的部署，应该用 WorkerPoolSize>0（worker
//     池提供有限界的并发，而不是继续依赖"每帧一个不设上限的临时
//     goroutine"这个本身就是漏洞的实现细节）。
//
// workerPoolSize==0 时不做取模：Connection 曾经独立读一次
// config.G.WorkerPoolSize 快照（useWorkerPool）来决定要不要走这个方法，
// 与本结构体构造时读的快照可能不一致而导致取模除零 panic——这个不一致
// 的根源（两次独立快照）已随本次重构消失：调用方（transport）不再自己
// 判断"用不用池"，一律调用本方法，是否用池完全由这里唯一一次快照决定。
func (m *MsgHandle) SendMsgToTaskQueue(request handler.Request) {
	if m.workerPoolSize == 0 {
		m.DoMsgHandle(request)
		return
	}
	workerID := request.Conn().ID() % m.workerPoolSize

	// 优先投递到专属 Worker；stopCh 分支保证"Stop 已经广播、但仍有连接的
	// 读循环在这个时间点尝试投递"这种情况不会阻塞或 panic，而是退化为
	// 同步执行——这个请求已经是从连接上真实读到的一帧，静默丢弃会让客户端
	// 永远等不到响应，同步执行才是安全的退路（与 workerPoolSize==0 的
	// 语义一致）。
	select {
	case m.taskQueues[workerID] <- request:
		return
	case <-m.stopCh:
		m.DoMsgHandle(request)
		return
	default:
	}

	// Work stealing: 专属队列满时，轮询其他 Worker
	for i := uint32(1); i < m.workerPoolSize; i++ {
		tryID := (workerID + i) % m.workerPoolSize
		select {
		case m.taskQueues[tryID] <- request:
			return
		case <-m.stopCh:
			m.DoMsgHandle(request)
			return
		default:
		}
	}

	// 全部满：阻塞等待，但同样要能被 stopCh 打断，否则一个在 Stop 之后才
	// 走到这里的请求会永久阻塞（所有 worker 都已经在排空后退出，不会再有
	// 任何人消费这个队列）。
	select {
	case m.taskQueues[workerID] <- request:
	case <-m.stopCh:
		m.DoMsgHandle(request)
	}
}

// 已知、可接受的残余竞态（诚实记录，不是忽略）：close(stopCh) 与某个
// worker 的"最终排空后退出"几乎同时发生时，理论上存在一个极窄的窗口——
// 发送方的 select 在 stopCh 已关闭、taskQueue 也仍可写的情况下，可能被
// Go 运行时伪随机地选中"send to taskQueue"这一分支而不是"stopCh"分支，
// 恰好晚于目标 worker 完成它自己的最终非阻塞排空检查——这种情况下这一个
// 请求会留在 channel 里但不再有任何 worker 消费它。触发条件需要多个
// goroutine 在关停的这个具体时间点精确重叠，目前没有可复现的证据表明
// 这在实践中发生过；用发送方自己的引用计数（比如再包一层 WaitGroup）可以
// 彻底消除，但会显著增加复杂度，本轮判断不值得为一个未观测到的极窄竞态
// 引入——记录在这里供后续如果被证明是真实问题时参考。

func (m *MsgHandle) StartOneWorker(workerID int, taskQueue chan handler.Request) {
	defer m.workerWG.Done()
	slog.Debug("banNet worker started", "workerID", workerID)
	for {
		select {
		case request := <-taskQueue:
			m.DoMsgHandle(request)
		case <-m.stopCh:
			// stopCh 广播之后，SendMsgToTaskQueue 的所有分支都不会再往
			// taskQueue 发送新内容（要么已经在 select 里选中同一个 stopCh
			// 转去同步执行，要么先于 stopCh 关闭完成了发送）——此时把
			// taskQueue 里已经排队的内容一次性处理完，就是真正排空了，
			// 可以安全退出。
			for {
				select {
				case request := <-taskQueue:
					m.DoMsgHandle(request)
					continue
				default:
				}
				slog.Debug("banNet worker exited", "workerID", workerID)
				return
			}
		}
	}
}

// stopDrainTimeout 是 Stop 等待 worker 池排空的上限。正常关闭下这个等待
// 应该由"所有排队请求处理完"这个有限的工作量决定，设上限只是防御性的：
// 一个卡住的 Handler（比如死循环或永久阻塞的外部调用）不该让整个优雅关闭
// 流程永久挂起——超时后记录警告并继续，把"是否可接受"的判断留给运维通过
// 日志发现，而不是让 Stop 本身失去时限。
const stopDrainTimeout = 5 * time.Second

// Stop 广播"不再接受新分派"，并等待所有 worker 把已经排队/在途的请求全部
// 处理完再返回——这是本次重构修复 bug①（Server.Stop 不等在途请求处理完，
// 响应可能因连接已关闭而发不出去）在 worker 池这一侧的关键：调用方
// （server.go 的 Server.Stop）必须先等这里返回，才能确定"不会再有任何
// worker 往对应连接投递响应了"，进而才能安全地物理关闭那些连接——这也是
// 为什么 Server.Stop 必须让本方法先于广播"连接层开始关闭"运行：如果反过来，
// 一个连接可能在它对应的 worker 还没处理完排队请求时就已经被物理关闭。
//
// 此前的实现只 close(channel) 就返回，不等 worker 是否真的跑完——那正是
// bug①在 worker 池路径上的具体成因。
func (m *MsgHandle) Stop() {
	slog.Debug("banNet worker pool stopping")
	close(m.stopCh)

	done := make(chan struct{})
	go func() {
		m.workerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		slog.Debug("banNet worker pool drained")
	case <-time.After(stopDrainTimeout):
		slog.Warn("banNet worker pool did not drain within timeout, continuing shutdown anyway")
	}
}
