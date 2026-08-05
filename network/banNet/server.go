package banNet

import (
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

type Server struct {
	IP        string
	Port      int
	Name      string
	IPVersion string
	ExitCh    chan os.Signal
	MsgHandle banIface.IMsgHandle
	ConnMgr   banIface.IConnManager

	ConnStartFunc func(conn banIface.IConnect)
	ConnStopFunc  func(conn banIface.IConnect)
	listener      *net.TCPListener
}

func (s *Server) AddRouter(msgID string, router banIface.IRouter) {
	s.MsgHandle.AddRouter(msgID, router)
}

func NewServer() banIface.IServer {
	return &Server{
		IPVersion: "tcp4",
		IP:        config.G.Host,
		Name:      config.G.Name,
		Port:      config.G.Port,
		ExitCh:    make(chan os.Signal),
		MsgHandle: NewMsgHandle(),
		ConnMgr:   NewConnManager(),
	}
}

func (s *Server) GetConnMgr() banIface.IConnManager {
	return s.ConnMgr
}

func (s *Server) Start() {
	slog.Info("banNet server starting", "name", s.Name, "addr", fmt.Sprintf("%s:%d", s.IP, s.Port))

	go func() {
		s.MsgHandle.StartWorkerPool()

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

		var cid uint32
		for {
			select {
			case <-s.ExitCh:
				s.Stop()
				slog.Info("banNet server shutting down")
				return
			default:
				conn, err := listener.AcceptTCP()
				if err != nil {
					slog.Error("banNet accept failed", "error", err)
					continue
				}

				if s.ConnMgr.Len() >= config.G.MaxConn {
					conn.Close()
					continue
				}

				dealConn := NewConnection(conn, cid, s.MsgHandle, s)
				go dealConn.Start()
				cid++
			}
		}
	}()
}

func (s *Server) Stop() {
	slog.Info("banNet server stopping", "addr", fmt.Sprintf("%s:%d", s.IP, s.Port))
	s.ConnMgr.ClearConn()
	s.MsgHandle.Stop()
	if s.listener != nil {
		s.listener.Close()
	}
	slog.Debug("banNet server stopped")
}

func (s *Server) Serve() {
	s.Start()
	select {}
}

func (s *Server) SetConnStartFunc(f func(conn banIface.IConnect)) {
	s.ConnStartFunc = f
}
func (s *Server) SetConnStopFunc(f func(conn banIface.IConnect)) {
	s.ConnStopFunc = f
}
func (s *Server) CallConnStartFunc(conn banIface.IConnect) {
	if s.ConnStartFunc == nil {
		return // 未注册连接建立回调，静默跳过
	}
	s.ConnStartFunc(conn)
}

func (s *Server) CallConnStopFunc(conn banIface.IConnect) {
	if s.ConnStopFunc == nil {
		return // 未注册连接关闭回调，静默跳过
	}
	s.ConnStopFunc(conn)
}
