package bannet

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/NeverENG/BanDB/config"
)

var _ IConnect = &Connection{}

type Connection struct {
	TCPServer IServer      // 注入 ConnMgr
	Conn      *net.TCPConn // 底层 TCP 连接
	ConnID    uint32       // 连接唯一 ID
	MsgHandle IMsgHandle

	// 生命周期：ctx 是唯一的取消信号。Stop 调 cancel() 广播退出、并关闭 Conn 以解除
	// Reader 的阻塞读；Writer 与 Start 都 select ctx.Done()。stopOnce 保证 Stop 幂等。
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once

	msgChan     chan []byte // 高优写通道
	msgBuffChan chan []byte // 普通写通道

	property     map[string]any
	propertyLock sync.RWMutex
}

func NewConnection(conn *net.TCPConn, connID uint32, handle IMsgHandle, server IServer) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		TCPServer:   server,
		Conn:        conn,
		ConnID:      connID,
		MsgHandle:   handle,
		ctx:         ctx,
		cancel:      cancel,
		msgChan:     make(chan []byte, 10), // 高优通道加小缓冲，避免硬阻塞
		msgBuffChan: make(chan []byte, config.G.MaxMsgChanLen),
		property:    make(map[string]any), // 必须初始化，否则 SetProperty 写 nil map 会 panic
	}
	c.TCPServer.GetConnMgr().Add(c)
	return c
}

// connReadBufSize 是每连接读缓冲大小。帧头与 msgID 都只有几字节，无缓冲时读一帧需要
// 三次 read syscall（头 / msgID / 负载）；一层缓冲即可把它们摊薄到每若干帧一次内核往返。
// 超过缓冲的大帧由 io.ReadFull 自行循环读取，故此值无需覆盖 MaxPackageSize。
const connReadBufSize = 16 << 10

func (c *Connection) StartReader() {
	slog.Debug("conn reader started", "connID", c.ConnID)
	defer slog.Debug("conn reader exited", "connID", c.ConnID)
	defer c.Stop()

	// reader 与 headData 在循环外创建：仅本 goroutine 读取该连接，故可安全复用。
	reader := bufio.NewReaderSize(c.Conn, connReadBufSize)
	dp := NewDataPack()
	headData := make([]byte, dp.GetHeadLen())

	for {
		if _, err := io.ReadFull(reader, headData); err != nil {
			slog.Debug("conn read header failed", "connID", c.ConnID, "error", err)
			return // defer Stop 取消 ctx、关闭连接
		}
		msg, err := dp.UnPack(headData)
		if err != nil {
			slog.Error("conn unpack header failed", "connID", c.ConnID, "error", err)
			return
		}

		// 头部之后, 先按 IDLen 读取 msgID 字符串
		mImpl := msg.(*Message)
		if mImpl.IDLen > 0 {
			idBuf := make([]byte, mImpl.IDLen)
			if _, err := io.ReadFull(reader, idBuf); err != nil {
				slog.Error("conn read msgID failed", "connID", c.ConnID, "error", err)
				return
			}
			msg.SetMsgID(string(idBuf))
		}

		var data []byte
		if msg.GetMsgLen() > 0 {
			data = make([]byte, msg.GetMsgLen())

			if _, err := io.ReadFull(reader, data); err != nil {
				slog.Error("conn read body failed", "connID", c.ConnID, "error", err)
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
		// 优先冲刷高优通道，其空时再取普通通道；两处都随 ctx 取消而退出。
		select {
		case <-c.ctx.Done():
			return
		case data := <-c.msgChan:
			if err := c.write(data); err != nil {
				return
			}
		default:
			select {
			case <-c.ctx.Done():
				return
			case data := <-c.msgChan:
				if err := c.write(data); err != nil {
					return
				}
			case data := <-c.msgBuffChan:
				if err := c.write(data); err != nil {
					return
				}
			}
		}
	}
}

// write 向连接写一帧，出错记日志并返回错误（调用方据此退出 Writer）。
func (c *Connection) write(data []byte) error {
	if _, err := c.Conn.Write(data); err != nil {
		slog.Error("conn write failed", "connID", c.ConnID, "error", err)
		return err
	}
	return nil
}

func (c *Connection) Start() {
	slog.Debug("conn established", "connID", c.ConnID)
	go c.StartReader()
	go c.StartWriter()
	c.TCPServer.CallConnStartFunc(c)
	<-c.ctx.Done() // 阻塞至连接被取消（Reader/Writer 出错或 Stop 触发）
}

func (c *Connection) Stop() {
	c.stopOnce.Do(func() {
		slog.Debug("conn terminated", "connID", c.ConnID)
		c.cancel()     // 唯一取消信号：唤醒 Writer 与 Start 的 <-ctx.Done()
		c.Conn.Close() // 解除 Reader 的阻塞读，使其返回
		c.TCPServer.CallConnStopFunc(c)
		c.TCPServer.GetConnMgr().Remove(c)
		// 不 close msgChan / msgBuffChan：worker 可能仍在 SendBuffMsg，close 会触发 send on closed channel
	})
}
func (c *Connection) GetConnID() uint32 {
	return c.ConnID
}
func (c *Connection) GetTCPConn() *net.TCPConn {
	return c.Conn
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

// errConnClosed 表示向已关闭的连接发送。
var errConnClosed = errors.New("banNet: connection closed")

func (c *Connection) SendMsg(msgID string, data []byte) error {
	packet, err := NewDataPack().Pack(NewMessage(msgID, data))
	if err != nil {
		return err
	}
	// 随 ctx 取消而返回错误，避免连接关闭后 Writer 不再排空时永久阻塞。
	select {
	case c.msgChan <- packet:
		return nil
	case <-c.ctx.Done():
		return errConnClosed
	}
}

func (c *Connection) SendBuffMsg(msgID string, data []byte) error {
	packet, err := NewDataPack().Pack(NewMessage(msgID, data))
	if err != nil {
		return err
	}
	select {
	case c.msgBuffChan <- packet:
		return nil
	case <-c.ctx.Done():
		return errConnClosed
	}
}

func (c *Connection) SetProperty(key string, value any) {
	c.propertyLock.Lock()
	defer c.propertyLock.Unlock()
	c.property[key] = value
}

func (c *Connection) GetProperty(key string) any {
	c.propertyLock.RLock()
	defer c.propertyLock.RUnlock()
	return c.property[key]
}
func (c *Connection) RemoveProperty(key string) {
	c.propertyLock.Lock()
	defer c.propertyLock.Unlock()
	delete(c.property, key)
}
