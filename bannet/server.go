package bannet

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/NeverENG/BanDB/config"
)

type Server struct {
	IP        string
	Port      int
	Name      string
	IPVersion string
	ExitCh    chan os.Signal
	MsgHandle IMsgHandle
	ConnMgr   IConnManager

	ConnStartFunc func(conn IConnect)
	ConnStopFunc  func(conn IConnect)
	listener      *net.TCPListener

	// 生命周期：Stop 关闭 done 广播「正在关停」，accept 循环据此区分「主动关停」与「瞬时错误」；
	// stopOnce 保证 Stop 幂等（避免重复 close(done)/关闭 listener 触发 panic）。
	done     chan struct{}
	stopOnce sync.Once
}

func (s *Server) AddRouter(msgID string, router IRouter) {
	s.MsgHandle.AddRouter(msgID, router)
}

func NewServer() IServer {
	return &Server{
		IPVersion: "tcp4",
		IP:        config.G.Host,
		Name:      config.G.Name,
		Port:      config.G.Port,
		ExitCh:    make(chan os.Signal, 1), // 缓冲 1：signal.Notify 不阻塞、不丢信号
		MsgHandle: NewMsgHandle(),
		ConnMgr:   NewConnManager(),
		done:      make(chan struct{}),
	}
}

func (s *Server) GetConnMgr() IConnManager {
	return s.ConnMgr
}

func (s *Server) Start() {
	slog.Info("banNet server starting", "name", s.Name, "addr", fmt.Sprintf("%s:%d", s.IP, s.Port))

	s.MsgHandle.StartWorkerPool()

	// 同步绑定监听器：在返回前确定成败并设好 s.listener，避免与 Stop 竞争、避免 Stop 早于
	// 绑定导致 accept 循环空转泄漏。绑定失败则服务不启动。
	tcpAddr, err := net.ResolveTCPAddr(s.IPVersion, fmt.Sprintf("%s:%d", s.IP, s.Port))
	if err != nil {
		slog.Error("banNet resolve addr failed", "error", err)
		return
	}
	listener, err := net.ListenTCP(s.IPVersion, tcpAddr)
	if err != nil {
		slog.Error("banNet listen failed", "error", err)
		return
	}
	s.listener = listener

	go s.acceptLoop(listener)
}

// acceptLoop 接受连接直到 Stop 关闭 listener/done。
func (s *Server) acceptLoop(listener *net.TCPListener) {
	var cid uint32
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			// Stop 已关闭 listener → Accept 立即报错。用 done 区分「主动关停」（退出，不再
			// 空转刷错误日志）与「瞬时错误」（继续）。
			select {
			case <-s.done:
				return
			default:
				slog.Error("banNet accept failed", "error", err)
				continue
			}
		}

		if s.ConnMgr.Len() >= config.G.MaxConn {
			conn.Close()
			continue
		}

		go NewConnection(conn, cid, s.MsgHandle, s).Start()
		cid++
	}
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		slog.Info("banNet server stopping", "addr", fmt.Sprintf("%s:%d", s.IP, s.Port))
		close(s.done) // 广播关停：accept 循环据此干净退出
		if s.listener != nil {
			s.listener.Close() // 解除 AcceptTCP 阻塞
		}
		s.ConnMgr.ClearConn() // 关闭所有在途连接（各自 cancel + 关 conn）
		s.MsgHandle.Stop()    // 关闭 worker 池
		slog.Debug("banNet server stopped")
	})
}

// Serve 启动服务并阻塞至收到 SIGINT/SIGTERM，随后优雅关停。
func (s *Server) Serve() {
	s.Start()
	signal.Notify(s.ExitCh, syscall.SIGINT, syscall.SIGTERM)
	<-s.ExitCh
	slog.Info("banNet received shutdown signal, stopping")
	s.Stop()
}

func (s *Server) SetConnStartFunc(f func(conn IConnect)) {
	s.ConnStartFunc = f
}
func (s *Server) SetConnStopFunc(f func(conn IConnect)) {
	s.ConnStopFunc = f
}
func (s *Server) CallConnStartFunc(conn IConnect) {
	if s.ConnStartFunc == nil {
		return // 未注册连接建立回调，静默跳过
	}
	s.ConnStartFunc(conn)
}

func (s *Server) CallConnStopFunc(conn IConnect) {
	if s.ConnStopFunc == nil {
		return // 未注册连接关闭回调，静默跳过
	}
	s.ConnStopFunc(conn)
}
