package banNet

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

var _ banIface.IConnect = &Connection{}

type Connection struct {
	TCPServer banIface.IServer // 注入 ConnMgr
	// 主要维护链接
	Conn *net.TCPConn
	// 链接的唯一 ID
	ConnID uint32
	// Stop 幂等化
	stopOnce  sync.Once
	MsgHandle banIface.IMsgHandle
	// 该链接状态
	ExitBuffChan chan bool

	ctx    context.Context
	cancel context.CancelFunc

	msgChan chan []byte

	msgBuffChan chan []byte

	property     map[string]interface{}
	propertyLock sync.RWMutex
}

func NewConnection(conn *net.TCPConn, ConnID uint32, handle banIface.IMsgHandle, server banIface.IServer) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		TCPServer:    server,
		Conn:         conn,
		ConnID:       ConnID,
		MsgHandle:    handle,
		ExitBuffChan: make(chan bool, 1),
		ctx:          ctx,
		cancel:       cancel,
		msgChan:      make(chan []byte, 10), // 高优通道加小缓冲，避免硬阻塞
		msgBuffChan:  make(chan []byte, config.G.MaxMsgChanLen),
	}
	c.TCPServer.GetConnMgr().Add(c)
	return c
}
func (c *Connection) StartReader() {
	slog.Debug("conn reader started", "connID", c.ConnID)
	defer slog.Debug("conn reader exited", "connID", c.ConnID)
	defer c.Stop()

	for {
		if c.Conn == nil {
			return
		}

		dp := NewDataPack()

		headData := make([]byte, dp.GetHeadLen())
		if _, err := io.ReadFull(c.Conn, headData); err != nil {
			slog.Debug("conn read header failed", "connID", c.ConnID, "error", err)
			c.ExitBuffChan <- true
			return
		}
		msg, err := dp.UnPack(headData)
		if err != nil {
			slog.Error("conn unpack header failed", "connID", c.ConnID, "error", err)
			c.ExitBuffChan <- true
			return
		}

		// 头部之后, 先按 IDLen 读取 msgID 字符串
		mImpl := msg.(*Message)
		if mImpl.IDLen > 0 {
			idBuf := make([]byte, mImpl.IDLen)
			if _, err := io.ReadFull(c.Conn, idBuf); err != nil {
				slog.Error("conn read msgID failed", "connID", c.ConnID, "error", err)
				c.ExitBuffChan <- true
				return
			}
			msg.SetMsgID(string(idBuf))
		}

		var data []byte
		if msg.GetMsgLen() > 0 {
			data = make([]byte, msg.GetMsgLen())

			if _, err := io.ReadFull(c.Conn, data); err != nil {
				slog.Error("conn read body failed", "connID", c.ConnID, "error", err)
				c.ExitBuffChan <- true
				return
			}
		}
		msg.SetData(data)
		req := NewRequest(msg, c)
		// 根据有没有启动 WorkPool 选择不同的结果
		if config.G.WorkerPoolSize > 0 {
			c.MsgHandle.SendMsgToTaskQueue(req)
		} else {
			go c.MsgHandle.DoMsgHandle(req)
		}
	}
}

func (c *Connection) StartWriter() {
	slog.Debug("conn writer started", "connID", c.ConnID)
	defer slog.Debug("conn writer exited", "connID", c.ConnID)
	defer c.Stop()

	for {
		// 检查最高优先级
		select {
		case <-c.ExitBuffChan:
			return
		case data, ok := <-c.msgChan:
			if !ok {
				return
			}
			if _, err := c.Conn.Write(data); err != nil {
				slog.Error("conn write failed", "connID", c.ConnID, "error", err)
				return
			}
		default:
			// 高优通道全空时，在此静默等待
			select {
			case <-c.ExitBuffChan:
				return
			case data, ok := <-c.msgBuffChan:
				if !ok {
					return
				}
				if _, err := c.Conn.Write(data); err != nil {
					slog.Error("conn write failed", "connID", c.ConnID, "error", err)
					return
				}
			}
		}
	}
}

func (c *Connection) Start() {
	slog.Debug("conn established", "connID", c.ConnID)
	go c.StartReader()
	go c.StartWriter()
	c.TCPServer.CallConnStartFunc(c)
	<-c.ExitBuffChan // 阻塞至连接退出（Reader/Writer 出错或 Stop 触发）
}

func (c *Connection) Stop() {
	c.stopOnce.Do(func() {
		slog.Debug("conn terminated", "connID", c.ConnID)
		c.TCPServer.CallConnStopFunc(c)
		c.cancel()
		c.Conn.Close()
		// 非阻塞通知 Start/Writer 退出；多次入口被 stopOnce 折叠
		select {
		case c.ExitBuffChan <- true:
		default:
		}
		c.TCPServer.GetConnMgr().Remove(c)
		// 不 close msgChan / msgBuffChan：worker 可能仍在 SendBuffMsg，close 会触发 send on closed channel
	})
}
func (c *Connection) GetConnID() uint32 {
	return c.ConnID
}
func (c *Connection) GetTcpConn() *net.TCPConn {
	return c.Conn
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *Connection) SendMsg(msgID string, data []byte) error {
	dp := NewDataPack()

	msg := NewMessage(msgID, data)
	Gdata, err := dp.Pack(msg)
	if err != nil {
		return err
	}
	if c.msgChan != nil {
		c.msgChan <- Gdata
	}
	return nil
}

func (c *Connection) SendBuffMsg(msgID string, data []byte) error {
	dp := NewDataPack()
	msg := NewMessage(msgID, data)
	Gdata, err := dp.Pack(msg)
	if err != nil {
		return err
	}
	if c.msgBuffChan != nil {
		c.msgBuffChan <- Gdata
	}

	return nil
}

func (c *Connection) SetProperty(key string, value interface{}) {
	c.propertyLock.Lock()
	defer c.propertyLock.Unlock()
	c.property[key] = value
}

func (c *Connection) GetProperty(key string) interface{} {
	c.propertyLock.RLock()
	defer c.propertyLock.RUnlock()
	return c.property[key]
}
func (c *Connection) RemoveProperty(key string) {
	c.propertyLock.Lock()
	defer c.propertyLock.Unlock()
	delete(c.property, key)
}
