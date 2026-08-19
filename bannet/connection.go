package bannet

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/internal/metrics"
)

// recoverConnGoroutine 是每个连接生命周期 goroutine（Start/StartReader/StartWriter）
// 顶部统一挂的兜底：这些 goroutine 里除了帧解析代码，还会调用外部回调
// （ConnStartFunc/ConnStopFunc）与业务分派（MsgHandle.DoMsgHandle 已有自己的
// recover，这里是第二层，防的是分派之外的代码，如帧解析本身、回调本身的 bug）。
// 未被捕获的 panic 会终止整个进程而不只是这一个连接——见 msghandle.go 的
// DoMsgHandle 同款注释，该结论已用故意 panic 的 Handler 验证过。
func recoverConnGoroutine(connID uint32, where string) {
	if r := recover(); r != nil {
		metrics.PanicsRecovered.Add(1)
		slog.Error("banNet connection goroutine panicked, recovered",
			"connID", connID, "where", where, "panic", r, "stack", string(debug.Stack()))
	}
}

var _ Conn = &Connection{}

// hardMaxPackageSize 是帧长上限的绝对安全兜底：当 config.G.MaxPackageSize 被设为 0
// （运维本意常是"不限制"）时，若真的按"不限制"执行，攻击者只需发 6 字节头部声称
// dataLen=0xFFFFFFFF（近 4GiB）就能让服务端在读负载前先做一次 4GiB 的 make([]byte,...)
// ——这是 TLV 解析器最经典的内存放大 DoS，配置项的"0=不限"语义本身就是这个漏洞的
// 入口。此兜底把"0"重新解释为"用这个硬上限"而不是"完全不设上限"，消除这个入口；
// 真的需要更大帧的部署应显式调高 MaxPackageSize，而不是设 0。
const hardMaxPackageSize = 256 << 20 // 256MiB

type Connection struct {
	TCPServer *Server      // 注入 ConnMgr
	Conn      *net.TCPConn // 底层 TCP 连接
	ConnID    uint32       // 连接唯一 ID
	MsgHandle Dispatcher

	// 生命周期：ctx 是唯一的取消信号。Stop 调 cancel() 广播退出、并关闭 Conn 以解除
	// Reader 的阻塞读；Writer 与 Start 都 select ctx.Done()。stopOnce 保证 Stop 幂等。
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once

	msgChan     chan []byte // 高优写通道
	msgBuffChan chan []byte // 普通写通道

	property     map[string]any
	propertyLock sync.RWMutex

	// 构造时从全局配置快照的几项策略，避免在每帧读取路径上访问可变全局状态。
	maxPackageSize uint32
	useWorkerPool  bool
	// readTimeout<=0 表示不设读超时（不建议，见 NewConnection 的默认值来源）。
	readTimeout time.Duration
}

func NewConnection(conn *net.TCPConn, connID uint32, handle Dispatcher, server *Server) *Connection {
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

		maxPackageSize: config.G.MaxPackageSize,
		useWorkerPool:  config.G.WorkerPoolSize > 0,
		readTimeout:    time.Duration(config.G.ConnReadTimeoutMs) * time.Millisecond,
	}
	c.TCPServer.Conns().Add(c)
	return c
}

// effectiveMaxPackageSize 返回本连接实际生效的帧长上限：0 时退回 hardMaxPackageSize
// 而不是"不限制"，理由见 hardMaxPackageSize 的注释。
func (c *Connection) effectiveMaxPackageSize() uint32 {
	if c.maxPackageSize == 0 {
		return hardMaxPackageSize
	}
	return c.maxPackageSize
}

// connReadBufSize 是每连接读缓冲大小。帧头与 msgID 都只有几字节，无缓冲时读一帧需要
// 三次 read syscall（头 / msgID / 负载）；一层缓冲即可把它们摊薄到每若干帧一次内核往返。
// 超过缓冲的大帧由 io.ReadFull 自行循环读取，故此值无需覆盖 MaxPackageSize。
const connReadBufSize = 16 << 10

// resetReadDeadline 在每个逻辑读取单元（头部/msgID/负载）开始前重设读超时。
// readTimeout<=0 时不设置——保持零值行为（永久阻塞读），仅供显式选择该行为的
// 场景使用，默认配置下 readTimeout 恒为正值（见 config.G.ConnReadTimeoutMs）。
//
// 逐单元重设而非只在帧开头设一次：一个正常但慢的大帧不该被腰斩，只有"某一步
// 完全不发了"（哪怕只差 1 字节）才应该触发超时——这正是审计要防的"半个帧后不发"
// 的慢客户端/恶意连接，此前完全没有任何超时，这类连接会永久占住一个 goroutine
// 与一个 MaxConn 名额。
func (c *Connection) resetReadDeadline() {
	if c.readTimeout <= 0 {
		return
	}
	// SetReadDeadline 失败通常意味着连接已不可用，留给随后的 Read 调用返回错误
	// 处理，这里不需要额外处理错误——设置失败不代表读取一定失败，静默忽略即可。
	_ = c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout))
}

func (c *Connection) StartReader() {
	defer recoverConnGoroutine(c.ConnID, "StartReader")
	slog.Debug("conn reader started", "connID", c.ConnID)
	defer slog.Debug("conn reader exited", "connID", c.ConnID)
	defer c.Stop()

	// reader 与 headData 在循环外创建：仅本 goroutine 读取该连接，故可安全复用。
	reader := bufio.NewReaderSize(c.Conn, connReadBufSize)
	dp := NewDataPack()
	headData := make([]byte, dp.HeadLen())

	for {
		c.resetReadDeadline()
		if _, err := io.ReadFull(reader, headData); err != nil {
			slog.Debug("conn read header failed", "connID", c.ConnID, "error", err)
			return // defer Stop 取消 ctx、关闭连接（含读超时：net.Error.Timeout()）
		}
		msg, err := dp.UnPack(headData)
		if err != nil {
			slog.Error("conn unpack header failed", "connID", c.ConnID, "error", err)
			return
		}
		// 帧长上限在此执行：读取负载之前拒绝超限帧，避免按对端声称的长度分配内存。
		// 用 effectiveMaxPackageSize 而非 c.maxPackageSize 直接比较——配置为 0 时
		// 退回 hardMaxPackageSize，消除"0=不限"被当字面意思执行的内存放大入口。
		if maxSize := c.effectiveMaxPackageSize(); msg.MsgLen() > maxSize {
			slog.Warn("conn frame exceeds max package size",
				"connID", c.ConnID, "dataLen", msg.MsgLen(), "max", maxSize)
			return
		}

		// 头部之后, 先按 IDLen 读取 msgID 字符串
		if msg.IDLen > 0 {
			idBuf := make([]byte, msg.IDLen)
			c.resetReadDeadline()
			if _, err := io.ReadFull(reader, idBuf); err != nil {
				slog.Error("conn read msgID failed", "connID", c.ConnID, "error", err)
				return
			}
			msg.SetMsgID(string(idBuf))
		}

		var data []byte
		if msg.MsgLen() > 0 {
			data = make([]byte, msg.MsgLen())

			c.resetReadDeadline()
			if _, err := io.ReadFull(reader, data); err != nil {
				slog.Error("conn read body failed", "connID", c.ConnID, "error", err)
				return
			}
		}
		msg.SetData(data)
		req := newRequest(msg, c)
		// 根据有没有启动 worker 池选择投递方式
		if c.useWorkerPool {
			c.MsgHandle.SendMsgToTaskQueue(req)
		} else {
			go c.MsgHandle.DoMsgHandle(req)
		}
	}
}

func (c *Connection) StartWriter() {
	defer recoverConnGoroutine(c.ConnID, "StartWriter")
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
//
// 复用 readTimeout 做写超时预算：慢消费者（对端不读、TCP 发送缓冲区堆满）会让
// Write 阻塞，没有超时的话会跟"半个帧不发"的慢读客户端一样永久占住 Writer
// goroutine——用同一个超时数值是因为两者是同一类"对端不配合、连接该被回收"的
// 场景，没有必要为写方向单独引入一个新配置项。
func (c *Connection) write(data []byte) error {
	if c.readTimeout > 0 {
		_ = c.Conn.SetWriteDeadline(time.Now().Add(c.readTimeout))
	}
	if _, err := c.Conn.Write(data); err != nil {
		slog.Error("conn write failed", "connID", c.ConnID, "error", err)
		return err
	}
	return nil
}

func (c *Connection) Start() {
	defer recoverConnGoroutine(c.ConnID, "Start")
	slog.Debug("conn established", "connID", c.ConnID)
	go c.StartReader()
	go c.StartWriter()
	c.TCPServer.CallConnStartFunc(c) // 用户注册的回调；一样可能 panic，同一顶兜底
	<-c.ctx.Done()                   // 阻塞至连接被取消（Reader/Writer 出错或 Stop 触发）
}

func (c *Connection) Stop() {
	c.stopOnce.Do(func() {
		slog.Debug("conn terminated", "connID", c.ConnID)
		c.cancel()     // 唯一取消信号：唤醒 Writer 与 Start 的 <-ctx.Done()
		c.Conn.Close() // 解除 Reader 的阻塞读，使其返回
		c.TCPServer.CallConnStopFunc(c)
		c.TCPServer.Conns().Remove(c)
		// 不 close msgChan / msgBuffChan：worker 可能仍在 SendBuffMsg，close 会触发 send on closed channel
	})
}
func (c *Connection) ID() uint32 {
	return c.ConnID
}
func (c *Connection) TCPConn() *net.TCPConn {
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

func (c *Connection) Property(key string) any {
	c.propertyLock.RLock()
	defer c.propertyLock.RUnlock()
	return c.property[key]
}
func (c *Connection) RemoveProperty(key string) {
	c.propertyLock.Lock()
	defer c.propertyLock.Unlock()
	delete(c.property, key)
}
