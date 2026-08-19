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

	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/bannet/dispatch"
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

// 帧长上限的绝对安全兜底（原 hardMaxPackageSize）现由 bannet/codec 持有——
// 那是判断"一帧声明的长度是否合法"的编解码层职责，见 codec.EffectiveMaxSize
// 与 codec.DataPack.Decode 的注释；本包不再重复定义。

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
		readTimeout:    time.Duration(config.G.ConnReadTimeoutMs) * time.Millisecond,
	}
	c.TCPServer.Conns().Add(c)
	return c
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

	// reader 在循环外创建：仅本 goroutine 读取该连接，故可安全复用。
	reader := bufio.NewReaderSize(c.Conn, connReadBufSize)
	dp := codec.NewDataPack()

	for {
		// 帧的完整读取（头部 → 校验帧长上限 → msgID → 负载）已合并进
		// codec.DataPack.Decode——见 docs/rfc/bannet-重构.md B.3：此前这段逻辑
		// 散落在本循环里，是分层不完整的直接证据。resetReadDeadline 作为
		// beforeRead 回调传入，在每个逻辑读取单元（头部/msgID/负载）真正阻塞
		// 之前重设超时，语义与重写前完全一致：一个正常但慢的大帧不会被腰斩，
		// 只有"某一步完全不发了"才会超时。
		msg, err := dp.Decode(reader, c.maxPackageSize, c.resetReadDeadline)
		if err != nil {
			switch {
			case errors.Is(err, codec.ErrFrameTooLarge):
				// 对端声称的帧长超过上限：读取负载前已拒绝，不会按其声称的
				// 长度分配内存——沿用重写前的 Warn 级别（值得单独关注/计数）。
				slog.Warn("conn frame exceeds max package size", "connID", c.ConnID, "error", err)
			case errors.Is(err, io.EOF):
				// io.ReadFull 只在帧边界（尚未读到任何字节）返回 io.EOF：
				// 对端在两帧之间正常断开，是最常见的退出路径，沿用 Debug 级别。
				slog.Debug("conn read failed", "connID", c.ConnID, "error", err)
			default:
				// 其余情况（头部之外的位置提前断开 → io.ErrUnexpectedEOF、
				// 解析头部失败、读超时等）沿用 Error 级别；具体是在哪一步失败
				// 已由 Decode 通过 %w 链保留在 err 的文本里（"decode: read
				// msgID: ..." 等），无需在此再区分。
				slog.Error("conn decode frame failed", "connID", c.ConnID, "error", err)
			}
			return // defer Stop 取消 ctx、关闭连接（含读超时：net.Error.Timeout()）
		}

		req := dispatch.NewRequest(msg, c)
		// 投递方式（走 worker 池还是同步执行）完全由 dispatch 内部决定
		// （SendMsgToTaskQueue 是分发层唯一的请求入口），transport 不再自己
		// 判断"要不要另起一个 goroutine"——这正是本次重构修复的 bug②：此前
		// useWorkerPool==false 时会在此处 `go c.MsgHandle.DoMsgHandle(req)`，
		// 每帧一个不受追踪、无上限的临时 goroutine，是一个 goroutine 泄漏/
		// 爆炸的入口（见 docs/rfc/bannet-重构.md B.4）。现在统一调用
		// SendMsgToTaskQueue，workerPoolSize==0 时它在调用方（也就是本
		// goroutine）上同步执行，不新增任何 goroutine，天然绑定在连接的
		// 读循环生命周期上——不是给旧路径补追踪，是让这类 goroutine 不再
		// 存在，详见 bannet/dispatch/dispatch.go 的 SendMsgToTaskQueue 注释。
		c.MsgHandle.SendMsgToTaskQueue(req)
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
