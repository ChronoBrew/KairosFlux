package kairnet

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/dispatch"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/kairnet/transport"
)

type Server struct {
	IP        string
	Port      int
	Name      string
	ipVersion string
	exitCh    chan os.Signal
	MsgHandle Dispatcher
	ConnMgr   ConnRegistry

	// V2Handler 处理 Kair v2 帧（RFC docs/rfc/Kair-2.md），可为 nil（不
	// 开启 v2 能力：连接协商 HELLO 探测帧仍会成功——v2 帧格式/协商本身与
	// 是否注册业务处理器无关——但协商切换之后的每一帧都会被静默丢弃，见
	// transport.Connection.OnFrameV2 字段注释）。与 v1 的 MsgHandle
	// （msgID → Handler 路由表）不同，v2 只有一个处理器：v2 帧靠数字
	// opcode 分派，内部由 HandlerV2.HandleV2 自行 switch（与
	// service.Router.Handle 对 msgID 的 switch 同一写法），不需要一张
	// opcode → Handler 的路由表。
	V2Handler HandlerV2

	ConnStartFunc func(conn Conn)
	ConnStopFunc  func(conn Conn)
	listener      *net.TCPListener

	// 生命周期：Stop 关闭 done 广播「正在关停」，accept 循环据此区分「主动关停」与「瞬时错误」；
	// stopOnce 保证 Stop 幂等（避免重复 close(done)/关闭 listener 触发 panic）。
	done     chan struct{}
	stopOnce sync.Once
}

func (s *Server) AddRouter(msgID string, router Handler) {
	s.MsgHandle.AddRouter(msgID, router)
}

func NewServer() *Server {
	return &Server{
		ipVersion: "tcp4",
		IP:        config.G.Host,
		Name:      config.G.Name,
		Port:      config.G.Port,
		exitCh:    make(chan os.Signal, 1), // 缓冲 1：signal.Notify 不阻塞、不丢信号
		MsgHandle: NewMsgHandle(),
		ConnMgr:   NewConnManager(),
		done:      make(chan struct{}),
	}
}

func (s *Server) Conns() ConnRegistry {
	return s.ConnMgr
}

// onFrame 是 transport.Connection 解出一帧后调用的分派回调——把
// transport 解出来的 Frame 转交给 dispatch，而不是让 transport 直接
// import dispatch（见 docs/rfc/bannet-重构.md C.4.2）。这个回调是本次
// 重构里 transport 与 dispatch 两个兄弟包唯一的接线点，只有根包（同时
// 依赖两者）能扮演这个角色。
func (s *Server) onFrame(msg *codec.Message, conn Conn) {
	req := dispatch.NewRequest(msg, conn)
	s.MsgHandle.SendMsgToTaskQueue(req)
}

// onNegotiate 是 transport.Connection.NegotiateFunc 的具体实现：把"这是不是
// HELLO 探测帧、该怎么响应"这个判断委托给 kairnet/negotiate（唯一认识
// proto.MsgHello 语义与 §5.1 负载格式的包），协商成功后把确认的 ack 档位
// 挂到连接属性上供 V2Handler（RouterV2）读取。transport 本身不 import
// negotiate/proto——这个闭包是根包（同时依赖 transport 与 negotiate）
// "组合而不是让子包互相依赖"的又一个例子，与 onFrame 桥接 transport/
// dispatch 是同一种模式。
func (s *Server) onNegotiate(msg *codec.Message, conn Conn) ([]byte, bool) {
	respond, ack, isProbe, err := negotiate.ServerHandleProbe(msg)
	if err != nil {
		slog.Error("kairnet v2 negotiate failed", "connID", conn.ID(), "error", err)
		return nil, false
	}
	if !isProbe {
		return nil, false
	}
	conn.SetProperty(negotiate.ConnPropertyAckTier, ack)
	return respond, true
}

// onFrameV2 是 transport.Connection.OnFrameV2 的具体实现：与 onFrame 对称，
// 但 v2 帧同步分派给 V2Handler（不经过 MsgHandle 的 worker 池）——这是
// RFC §11.2.2"窗口内写帧严格按到达顺序处理"这条不变量在实现层面的落点：
// 一条连接的 v2 帧永远在它自己的 Reader goroutine 里顺序处理，不存在
// MsgHandle.SendMsgToTaskQueue 的 work-stealing 可能打乱同连接内帧顺序的
// 风险（该风险在 v1 路径上是已知且被接受的，见
// service/ingesthook/filter.go 的注释）；作为直接推论，RouterV2 为每条
// 连接维护的窗口/累计计数状态不需要加锁——同一时刻只有这一个 goroutine
// 会碰它。V2Handler 为 nil 时静默丢弃（未开启 v2 业务能力的部署，协商仍
// 可以成功，但没有任何处理器）。
func (s *Server) onFrameV2(msg *codec.MessageV2, conn Conn) {
	if s.V2Handler == nil {
		return
	}
	req := dispatch.NewRequestV2(msg, conn)
	s.V2Handler.HandleV2(req)
}

// AddRouterV2 注册 v2 业务处理器（见 V2Handler 字段注释）。
func (s *Server) AddRouterV2(h HandlerV2) {
	s.V2Handler = h
}

func (s *Server) Start() {
	slog.Info("kairnet server starting", "name", s.Name, "addr", fmt.Sprintf("%s:%d", s.IP, s.Port))

	s.MsgHandle.StartWorkerPool()

	// 同步绑定监听器：在返回前确定成败并设好 s.listener，避免与 Stop 竞争、避免 Stop 早于
	// 绑定导致 accept 循环空转泄漏。绑定失败则服务不启动。
	tcpAddr, err := net.ResolveTCPAddr(s.ipVersion, fmt.Sprintf("%s:%d", s.IP, s.Port))
	if err != nil {
		slog.Error("kairnet resolve addr failed", "error", err)
		return
	}
	listener, err := net.ListenTCP(s.ipVersion, tcpAddr)
	if err != nil {
		slog.Error("kairnet listen failed", "error", err)
		return
	}
	s.listener = listener

	go s.acceptLoop(listener)
}

// acceptRetryMinDelay/acceptRetryMaxDelay 是瞬时 Accept 错误（如 EMFILE：句柄数耗尽）
// 的退避区间。没有退避的话，一旦进入这类"每次 Accept 都立即报错"的持续错误状态，
// acceptLoop 会用尽一个 CPU 核心空转打日志——这本身就是一种资源耗尽，而且是服务端
// 自己造成的，不需要任何攻击者配合。退避借鉴 net/http.Server 对临时 Accept 错误的
// 标准处理方式。
const (
	acceptRetryMinDelay = 5 * time.Millisecond
	acceptRetryMaxDelay = time.Second
)

// acceptLoop 接受连接直到 Stop 关闭 listener/done。
//
// 留在根包而不随迁移映射表字面表述搬进 transport：acceptLoop 是 Server
// 的方法，Server 本身出于"不能让 transport 依赖 dispatch"的约束留在根包
// （见 transport.go 顶部注释的第二处偏差说明），acceptLoop 自然跟着留下；
// 它现在调用 transport.NewConnection 而不是包内构造函数，实际的连接收发
// 逻辑已经完全在 transport 包里。
func (s *Server) acceptLoop(listener *net.TCPListener) {
	var cid uint32
	var retryDelay time.Duration
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			// Stop 已关闭 listener → Accept 立即报错。用 done 区分「主动关停」（退出，不再
			// 空转刷错误日志）与「瞬时错误」（继续，但退避）。
			select {
			case <-s.done:
				return
			default:
			}

			if retryDelay == 0 {
				retryDelay = acceptRetryMinDelay
			} else {
				retryDelay *= 2
				if retryDelay > acceptRetryMaxDelay {
					retryDelay = acceptRetryMaxDelay
				}
			}
			slog.Error("kairnet accept failed, retrying after backoff", "error", err, "backoff", retryDelay)
			time.Sleep(retryDelay)
			continue
		}
		retryDelay = 0 // 一次成功 Accept 即重置退避——只在连续失败时才升级等待

		if s.ConnMgr.Len() >= config.G.MaxConn {
			conn.Close()
			continue
		}

		go transport.NewConnection(conn, cid, s.ConnMgr, s.onFrame, s.ConnStartFunc, s.ConnStopFunc, s.onNegotiate, s.onFrameV2).Start()
		cid++
	}
}

// connGracePeriod 是优雅关闭里"等所有连接自己收尾"的上限——超过这个时间
// 还没收尾完的连接（比如卡住的 Handler、极慢的客户端）会被 ClearConn 强制
// 关闭，不无限等待。
const connGracePeriod = 5 * time.Second

// Stop 优雅关闭：detect（不再接受新连接/新工作）-> broadcast+wait（先排空
// 分发层，再通知连接层关闭并等它们自己收尾）-> close（物理关闭剩下的
// 一切），借鉴调研 Tokio 一节的三段式模型（不照抄其具体类型，用 context
// 风格的广播 + WaitGroup 风格的等待表达，见 docs/rfc/bannet-重构.md C.3）。
//
// 这是本次重构修复 bug①的落地点：此前的实现是 ClearConn（立即强制关闭
// 所有连接）在前、MsgHandle.Stop（此前也不等 worker 真正跑完）在后——
// 两者都不等在途的 DoMsgHandle 处理完，一个正在业务逻辑里执行的请求，
// 其响应会因为连接已经被 cancel/关闭而发不出去，见
// docs/rfc/bannet-重构.md B.2 记录的这个真实竞态窗口。
//
//  1. 停止接受新连接/新的顶层信号（不变）。
//  2. MsgHandle.Stop：先等 worker 池排空——此后不会再有任何 worker 往
//     任何连接投递响应。必须放在 ConnMgr.BeginClosingAll 之前：如果反过来，
//     一个连接可能在它对应的 worker 还没处理完排队/在途请求时就已经被
//     判定"可以物理关闭"（连接自身的 Writer 只知道"这个连接的读循环
//     退出了"，并不知道全局 worker 池是否还有它的活——这两者是不同粒度
//     的信息，只能靠这里的顺序来保证正确的先后关系）。
//  3. BeginClosingAll：广播"决定关闭"给所有连接——只标记 Closing，不做
//     物理清理，写路径仍然打开（见 lifecycle 包与 Connection.BeginClosing
//     的注释）；此时调用是安全的，因为上一步已经保证不会再有 worker
//     池的响应因为这一步而丢失。
//  4. ConnMgr.Wait：等所有连接自己完成收尾（读循环感知到 Closing 后退出
//     -> Writer 排空 -> 物理关闭），有界等待（connGracePeriod）。
//  5. ClearConn：强制关闭超过等待时间仍未收尾的连接（正常情况下这里应该
//     已经没有连接剩下，是慢连接/卡住 Handler 的兜底，不是常规路径）。
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		slog.Info("kairnet server stopping", "addr", fmt.Sprintf("%s:%d", s.IP, s.Port))
		close(s.done) // 广播关停：accept 循环据此干净退出
		if s.listener != nil {
			s.listener.Close() // 解除 AcceptTCP 阻塞
		}

		s.MsgHandle.Stop()          // 先等 worker 池排空在途+排队的请求
		s.ConnMgr.BeginClosingAll() // 广播：决定关闭，写路径仍打开
		if !s.ConnMgr.Wait(connGracePeriod) {
			slog.Warn("kairnet graceful shutdown grace period exceeded, forcing remaining connections closed")
		}
		s.ConnMgr.ClearConn() // 强制关闭仍未自行收尾的连接（正常情况下应为空）

		slog.Debug("kairnet server stopped")
	})
}

// Serve 启动服务并阻塞至收到 SIGINT/SIGTERM，随后优雅关停。
func (s *Server) Serve() {
	s.Start()
	signal.Notify(s.exitCh, syscall.SIGINT, syscall.SIGTERM)
	<-s.exitCh
	slog.Info("kairnet received shutdown signal, stopping")
	s.Stop()
}

func (s *Server) SetConnStartFunc(f func(conn Conn)) {
	s.ConnStartFunc = f
}
func (s *Server) SetConnStopFunc(f func(conn Conn)) {
	s.ConnStopFunc = f
}
