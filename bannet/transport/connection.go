// Package transport 是传输层：只认原始字节的收发（Reader/Writer 循环、
// 连接注册表），不认 BANLV 帧内容该分派给谁——见 docs/rfc/bannet-重构.md
// C.2/C.5，是重构第五步（最后一步）的迁移目标。
//
// 依赖 codec（解出/编码 Frame）、lifecycle（连接生命周期状态机）、handler
// （Conn 契约类型），不依赖 dispatch——与 dispatch 是兄弟关系，互相不
// import，只在根包 bannet 里被组合到一起（根包用回调把 transport 解出来
// 的 Frame 转交给 dispatch，而不是让 transport 直接 import dispatch，见
// docs/rfc/bannet-重构.md C.4.2）。这也是 Connection 持有 OnFrame 回调
// 字段、而不是持有 dispatch.Dispatcher 字段的原因。
package transport

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/bannet/handler"
	"github.com/NeverENG/BanDB/bannet/lifecycle"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/internal/metrics"
)

// recoverConnGoroutine 是每个连接生命周期 goroutine（Start/StartReader/StartWriter）
// 顶部统一挂的兜底：这些 goroutine 里除了帧解析代码，还会调用外部回调
// （ConnStartFunc/ConnStopFunc）与业务分派（dispatch.MsgHandle.DoMsgHandle
// 已有自己的 recover，这里是第二层，防的是分派之外的代码，如帧解析本身、
// 回调本身的 bug）。未被捕获的 panic 会终止整个进程而不只是这一个连接——
// 见 dispatch 包 DoMsgHandle 的同款注释，该结论已用故意 panic 的 Handler
// 验证过。
//
// lc 非 nil 时，recover 到的 panic 会作为 EventPanicRecovered 事件推进
// 状态机——这是任务要求的"四种终止诱因都收敛到 Closing"里的第四种。调用方
// （StartReader/StartWriter）刻意把这个 defer 安排在 defer c.Stop() 之后
// 注册（见各自函数体），这样 LIFO 展开时它先于 c.Stop() 执行，Transition
// 记录的 EventPanicRecovered 才会是"生效的"那次收敛，而不是被 c.Stop()
// 内部默认的 EventExplicitStop 抢先。
func recoverConnGoroutine(connID uint32, where string, lc *lifecycle.Lifecycle) {
	if r := recover(); r != nil {
		metrics.PanicsRecovered.Add(1)
		slog.Error("banNet connection goroutine panicked, recovered",
			"connID", connID, "where", where, "panic", r, "stack", string(debug.Stack()))
		if lc != nil {
			lc.Transition(lifecycle.EventPanicRecovered)
		}
	}
}

var _ handler.Conn = &Connection{}

type Connection struct {
	Conn   *net.TCPConn // 底层 TCP 连接
	ConnID uint32       // 连接唯一 ID

	// Registry 是本连接建立/关闭时用于自我注册/注销的连接表，供 Server
	// 优雅关闭时统一枚举、广播 Closing、等待所有连接收尾——不依赖具体类型，
	// 只依赖同包定义的 ConnRegistry 接口。
	Registry ConnRegistry

	// OnFrame 在每次用 v1 codec 成功解出一帧后被调用，交给上层（根包
	// bannet，持有 dispatch.MsgHandle）决定怎么分派——这正是 transport 不
	// import dispatch 的关键：transport 只管"喂 Frame"，不知道、也不需要
	// 知道 Frame 之后被谁处理、用什么并发策略处理。
	OnFrame func(msg *codec.Message, conn handler.Conn)

	// NegotiateFunc 是 BANLV v2 协商的接入点（docs/rfc/BANLV-2.md §5/§5.1），
	// 可为 nil（v1-only 场景，如直接构造 Connection 的测试，行为与不存在
	// 这个字段时完全一致）。每次用 v1 codec 解出一帧、且本连接尚未切换到
	// v2 之前，都会先把这一帧交给 NegotiateFunc 判断——这不是逐帧都要
	// "嗅探字节"，判断逻辑（是不是 HELLO 探测帧）完全由外部注入的函数体
	// 决定，transport 本身不认识 HELLO 这个 msgID 语义，只认识"有一个可能
	// 拦截当前帧、要求切换解码格式的钩子"，与 ConnStartFunc/OnFrame 是
	// 同一种"transport 提供机制、根包注入策略"的模式。
	//
	// 返回 switchToV2=true 时：respond 是需要原样写回连接的完整响应帧
	// 字节（已经是 v2 格式），本帧不再走 OnFrame（协商帧不应该被当成一次
	// 正常业务请求分派出去），且此后本连接所有帧改用 v2 codec 解析、
	// 通过 OnFrameV2 分派。返回 switchToV2=false 表示这不是协商帧，
	// StartReader 照常调用 OnFrame，v1 客户端的行为不受这个字段存在与否
	// 影响（这是 RFC 任务要求的"未探测/v1 客户端照常走 v1 路径零影响"）。
	NegotiateFunc func(msg *codec.Message, conn handler.Conn) (respond []byte, switchToV2 bool)

	// OnFrameV2 在协商切换到 v2 之后，每次用 v2 codec 成功解出一帧被调用，
	// 与 OnFrame 对称但接收 codec.MessageV2。可为 nil（NegotiateFunc 为 nil
	// 时不会有任何帧切换到 v2，OnFrameV2 自然也不会被调用；即使
	// NegotiateFunc 非 nil 但 OnFrameV2 为 nil，切换后的 v2 帧会被静默
	// 丢弃分派——上层若开启了协商就应该同时提供 OnFrameV2，这是调用方
	// 的接线责任，不是 transport 需要校验的前置条件）。
	OnFrameV2 func(msg *codec.MessageV2, conn handler.Conn)

	// ConnStartFunc/ConnStopFunc 是连接建立/关闭时的用户回调（可为 nil），
	// 由 Server.SetConnStartFunc/SetConnStopFunc 注册后经由 NewConnection
	// 传入；调用时套了与收发 goroutine 相同的 recover 兜底（见
	// callConnStartFunc/callConnStopFunc）。
	ConnStartFunc func(conn handler.Conn)
	ConnStopFunc  func(conn handler.Conn)

	// 生命周期：显式状态机（Idle/Active/Closing/Closed），取代此前裸的
	// ctx/cancel/stopOnce 组合——见 docs/rfc/bannet-重构.md C.1，以及
	// bannet/lifecycle 包顶部注释里 Draining（决定关闭但写路径仍打开）与
	// Done（物理关闭）两个信号的区分，这是修复 bug①（优雅关闭丢响应）的
	// 关键机制。
	lc *lifecycle.Lifecycle

	// readerDone 在 StartReader 的 goroutine 返回前关闭，是"不会再有新的
	// SendMsg/SendBuffMsg 调用"的信号——Writer 靠它判断"可以做最后一次排空
	// 然后退出了"，而不是靠物理关闭信号（那样会有响应来不及写出就被关闭
	// socket 的竞态，见 Stop 与 StartWriter 的注释）。
	readerDone chan struct{}
	// writerDone 在 StartWriter 的 goroutine 返回前关闭。Stop 在物理关闭
	// socket 之前会等这个信号，确保 Writer 已经把队列里排空、不会有已经
	// 写完一半或即将要写的响应被"物理关闭"打断。
	writerDone chan struct{}

	msgChan     chan []byte // 高优写通道
	msgBuffChan chan []byte // 普通写通道

	property     map[string]any
	propertyLock sync.RWMutex

	// 构造时从全局配置快照的几项策略，避免在每帧读取路径上访问可变全局状态。
	maxPackageSize uint32
	// readTimeout<=0 表示不设读超时（不建议，见 NewConnection 的默认值来源）。
	readTimeout time.Duration
}

// NewConnection 构造一个连接并注册进 registry。onFrame 是分派回调（见
// Connection.OnFrame 的注释），connStartFunc/connStopFunc 可为 nil。
// negotiateFunc/onFrameV2 是 BANLV v2 协商/分派的接入点（见各自字段的
// 注释），同样可为 nil——nil 时本连接永远不会切换到 v2，行为与 v2 协商
// 能力不存在时完全一致。
func NewConnection(
	conn *net.TCPConn,
	connID uint32,
	registry ConnRegistry,
	onFrame func(msg *codec.Message, conn handler.Conn),
	connStartFunc func(conn handler.Conn),
	connStopFunc func(conn handler.Conn),
	negotiateFunc func(msg *codec.Message, conn handler.Conn) (respond []byte, switchToV2 bool),
	onFrameV2 func(msg *codec.MessageV2, conn handler.Conn),
) *Connection {
	c := &Connection{
		Conn:          conn,
		ConnID:        connID,
		Registry:      registry,
		OnFrame:       onFrame,
		NegotiateFunc: negotiateFunc,
		OnFrameV2:     onFrameV2,
		ConnStartFunc: connStartFunc,
		ConnStopFunc:  connStopFunc,
		lc:            lifecycle.New(),
		readerDone:    make(chan struct{}),
		writerDone:    make(chan struct{}),
		msgChan:       make(chan []byte, 10), // 高优通道加小缓冲，避免硬阻塞
		msgBuffChan:   make(chan []byte, config.G.MaxMsgChanLen),
		property:      make(map[string]any), // 必须初始化，否则 SetProperty 写 nil map 会 panic

		maxPackageSize: config.G.MaxPackageSize,
		readTimeout:    time.Duration(config.G.ConnReadTimeoutMs) * time.Millisecond,
	}
	c.Registry.Add(c)
	return c
}

// State 返回连接当前的生命周期状态，供测试/可观测性查询——见
// docs/rfc/bannet-重构.md C.1 状态表："谁能查询 | Lifecycle.State()"。
// 未放进 handler.Conn 接口（业务代码不需要关心这个），只在持有具体类型时
// 可用。
func (c *Connection) State() lifecycle.State {
	return c.lc.State()
}

// BeginClosing 实现 handler.Conn：标记本连接进入 Closing（幂等），只广播
// "决定关闭"信号，不做任何物理清理——socket 与写路径此时仍然可用。供
// Server 优雅关闭时批量广播给所有连接使用（见 registry.go 的
// ConnManager.BeginClosingAll），Reader 的读循环据此在完成当前在途请求后
// 主动退出，而不是被动等外部强行打断阻塞的读。
//
// 一个关键的补充：对于此刻正阻塞在 io.ReadFull 里等下一帧的连接（也就是
// "空闲"连接——没有在途请求，只是在等对端下一次发送，这在长连接/连接池
// 场景下是最常见的状态），仅仅关闭 Draining 信号本身是不够的：读循环只在
// 每次成功解出一帧、准备读下一帧之前才会检查 Draining，一个阻塞中的读不会
// 主动醒来检查它。如果不额外处理，Server 优雅关闭时 ConnMgr.Wait 会为
// 每一个空闲连接白白等满整个宽限期（实测：一个仅有 3 个长连接节点的集成
// 测试，Stop 耗时从原来的近乎瞬间涨到近 30 秒）——而空闲连接本来就没有
// 任何"在途请求的响应"需要保护，强制中断它的阻塞读不会丢失任何东西。
// 用 SetReadDeadline(now) 强制打断（无论读是否正阻塞中，未来的读也会立即
// 超时），读循环会像遇到真实读超时一样从 Decode 返回、被 EventReadTimeout
// 收敛，走正常的收尾路径——不影响写路径（Draining 语义不变，仍然只打断
// 读，不关 socket）。
func (c *Connection) BeginClosing() {
	if c.lc.Transition(lifecycle.EventExplicitStop) {
		_ = c.Conn.SetReadDeadline(time.Now())
	}
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
	// 注意注册顺序：defer c.Stop() 在前，defer close(c.readerDone) 在后，
	// defer recoverConnGoroutine 最后——LIFO 展开时执行顺序是
	// recoverConnGoroutine（若有 panic，先把 EventPanicRecovered 记进状态机）
	// -> close(readerDone)（通知 Writer 可以做最后排空了）-> c.Stop()（等
	// Writer 排空完再物理关闭）。这个顺序是 bug①修复能成立的前提，调整前
	// 请重新读一遍 Stop/StartWriter 的注释。
	defer c.Stop()
	slog.Debug("conn reader started", "connID", c.ConnID)
	defer slog.Debug("conn reader exited", "connID", c.ConnID)
	defer close(c.readerDone)
	defer recoverConnGoroutine(c.ConnID, "StartReader", c.lc)

	// reader 在循环外创建：仅本 goroutine 读取该连接，故可安全复用。
	reader := bufio.NewReaderSize(c.Conn, connReadBufSize)
	dp := codec.NewDataPack()
	dpV2 := codec.NewDataPackV2()
	// usingV2 一旦置 true 永不回退——BANLV v2 协商（RFC §5）是一次性的、
	// per-connection 的决定，一条连接不会中途"变回" v1。
	usingV2 := false

	for {
		if !usingV2 {
			// 帧的完整读取（头部 → 校验帧长上限 → msgID → 负载）已合并进
			// codec.DataPack.Decode——见 docs/rfc/bannet-重构.md B.3：此前这段
			// 逻辑散落在本循环里，是分层不完整的直接证据。resetReadDeadline
			// 作为 beforeRead 回调传入，在每个逻辑读取单元（头部/msgID/负载）
			// 真正阻塞之前重设超时，语义与重写前完全一致：一个正常但慢的
			// 大帧不会被腰斩，只有"某一步完全不发了"才会超时。
			msg, err := dp.Decode(reader, c.maxPackageSize, c.resetReadDeadline)
			if err != nil {
				c.handleDecodeError(err)
				return // defer 链负责收敛：Writer 排空后再物理关闭，见上方注释
			}

			// BANLV v2 协商接入点（docs/rfc/BANLV-2.md §5/§5.1）：在把这一帧
			// 交给正常的 v1 分派之前，先问一次 NegotiateFunc 这是不是协商
			// 探测帧——为 nil 或返回 switchToV2=false 时完全零影响，直接
			// 落到下面的 c.OnFrame(msg, c)，v1 客户端的路径不受影响。
			handledByNegotiate := false
			if c.NegotiateFunc != nil {
				if respond, switchToV2 := c.NegotiateFunc(msg, c); switchToV2 {
					if err := c.SendRawMsg(respond); err != nil {
						return
					}
					usingV2 = true
					handledByNegotiate = true
				}
			}

			// 分派完全交给上层回调决定（见 OnFrame 字段注释）：transport 只管
			// "喂字节、拿 Message"，不知道 Frame 之后被谁处理、用什么并发策略——
			// 这正是本次重构修复的 bug②：此前 useWorkerPool==false 时会在此处
			// `go c.MsgHandle.DoMsgHandle(req)`，每帧一个不受追踪、无上限的
			// 临时 goroutine（见 docs/rfc/bannet-重构.md B.4），现在这个决策
			// 完全下沉到 dispatch 包内部（根包的 OnFrame 实现里），transport
			// 不再持有、也不再需要知道 Dispatcher 类型。协商帧（
			// handledByNegotiate==true）不调用 OnFrame——它已经被完整处理
			// （响应已写回、连接已切换到 v2），不应该再被当成一次业务请求
			// 分派给路由表（本来也没有任何路由注册 msgID=="HELLO"）。
			if !handledByNegotiate {
				c.OnFrame(msg, c)
			}
		} else {
			msg2, err := dpV2.Decode(reader, c.maxPackageSize, c.resetReadDeadline)
			if err != nil {
				c.handleDecodeError(err)
				return
			}
			if c.OnFrameV2 != nil {
				c.OnFrameV2(msg2, c)
			}
		}

		// 每处理完一帧，检查是否已经进入 Closing（比如 Server 正在优雅
		// 关闭）——如果是，主动退出读循环，不再尝试读下一帧（那可能是一次
		// 永久阻塞的读，会让优雅关闭的等待失去意义）。这一步是 bug①修复的
		// 另一半：只有 Reader 主动让出，Server 端的等待才有个尽头。
		select {
		case <-c.lc.Draining():
			slog.Debug("conn reader stopping: closing in progress", "connID", c.ConnID)
			return
		default:
		}
	}
}

// handleDecodeError 把一次帧解码失败（无论 v1 还是 v2 codec 产出）归类到
// 生命周期状态机——两套 codec 的 Decode 都包裹同一批哨兵错误
// （codec.ErrFrameTooLarge、io.EOF、net.Error 超时），分类逻辑因此可以
// 完全复用，不需要为 v2 另写一份几乎相同的 switch。
func (c *Connection) handleDecodeError(err error) {
	switch {
	case errors.Is(err, codec.ErrFrameTooLarge):
		// 对端声称的帧长超过上限：读取负载前已拒绝，不会按其声称的
		// 长度分配内存——沿用重写前的 Warn 级别（值得单独关注/计数）。
		slog.Warn("conn frame exceeds max package size", "connID", c.ConnID, "error", err)
		c.lc.Transition(lifecycle.EventReadError)
	case errors.Is(err, io.EOF):
		// io.ReadFull 只在帧边界（尚未读到任何字节）返回 io.EOF：
		// 对端在两帧之间正常断开，是最常见的退出路径，沿用 Debug 级别。
		slog.Debug("conn read failed", "connID", c.ConnID, "error", err)
		c.lc.Transition(lifecycle.EventEOF)
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			// 读超时（SetReadDeadline 到期）：对端不配合但连接本身
			// 没有传输层错误，是我们主动放弃这个连接，Debug 级别，
			// 与 EOF 同级但打上不同的 event 标签，供状态机/日志区分
			// "对端主动挂断" vs "我们主动放弃"。
			slog.Debug("conn read timeout", "connID", c.ConnID, "error", err)
			c.lc.Transition(lifecycle.EventReadTimeout)
		} else {
			// 其余情况（头部之外的位置提前断开 → io.ErrUnexpectedEOF、
			// 解析头部失败等）沿用 Error 级别；具体是在哪一步失败
			// 已由 Decode 通过 %w 链保留在 err 的文本里（"decode: read
			// msgID: ..." 等），无需在此再区分。
			slog.Error("conn decode frame failed", "connID", c.ConnID, "error", err)
			c.lc.Transition(lifecycle.EventReadError)
		}
	}
}

func (c *Connection) StartWriter() {
	// 与 StartReader 对称的注册顺序，理由同上。
	defer c.Stop()
	slog.Debug("conn writer started", "connID", c.ConnID)
	defer slog.Debug("conn writer exited", "connID", c.ConnID)
	defer close(c.writerDone)
	defer recoverConnGoroutine(c.ConnID, "StartWriter", c.lc)

	for {
		select {
		case <-c.readerDone:
			// Reader 已经退出：不会再有新的 SendMsg/SendBuffMsg 调用（它们
			// 只可能由本连接的 Handle() 触发，而 Handle() 只在 Reader 的
			// 读循环里同步执行，或在 Reader 还活着时投递给的 worker 池里
			// 执行——Server 优雅关闭时保证了 dispatch 层会先排空 worker 池
			// 再让 Reader 感知 Draining 退出，见根包 server.go 的 Stop）。
			// 现在可以安全地把已经排队但还没写出的内容一次性冲刷完，再退出。
			c.drainPendingWrites()
			return
		case data := <-c.msgChan:
			if err := c.write(data); err != nil {
				return
			}
		default:
			select {
			case <-c.readerDone:
				c.drainPendingWrites()
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

// drainPendingWrites 非阻塞地把 msgChan/msgBuffChan 里已经排队但还没写出的
// 内容写完。只应该在确认 readerDone 已关闭（不会再有新内容被投递）之后调用，
// 否则这里的"排空"只是一个时间点快照，之后还可能有新内容进来但已经没有
// 循环再去处理它——这是 bug①修复的核心步骤，必须在 Stop 物理关闭 socket
// 之前完成，见 Stop 的注释。
func (c *Connection) drainPendingWrites() {
	for {
		select {
		case data := <-c.msgChan:
			_ = c.write(data) // 尽力而为：关闭流程里，写失败只记录日志，不重试
			continue
		default:
		}
		select {
		case data := <-c.msgBuffChan:
			_ = c.write(data)
			continue
		default:
		}
		return
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

// callConnStartFunc 调用用户注册的连接建立回调（可能为 nil），套上与收发
// goroutine 相同的 recover 兜底——取代此前"Server.CallConnStartFunc"的角色：
// Connection 不再持有 *Server 引用（那会让 transport 依赖根包，根包又依赖
// transport，形成环），回调改由构造时以函数值的形式直接注入。
func (c *Connection) callConnStartFunc() {
	if c.ConnStartFunc == nil {
		return
	}
	defer recoverConnGoroutine(c.ConnID, "ConnStartFunc", c.lc)
	c.ConnStartFunc(c)
}

func (c *Connection) callConnStopFunc() {
	if c.ConnStopFunc == nil {
		return
	}
	defer recoverConnGoroutine(c.ConnID, "ConnStopFunc", c.lc)
	c.ConnStopFunc(c)
}

func (c *Connection) Start() {
	defer recoverConnGoroutine(c.ConnID, "Start", c.lc)
	c.lc.MarkActive()
	slog.Debug("conn established", "connID", c.ConnID)
	go c.StartReader()
	go c.StartWriter()
	c.callConnStartFunc()
	<-c.lc.Done() // 阻塞至连接被物理关闭（Reader/Writer 出错或 Stop 触发）
}

// connStopWriterDrainTimeout 是 Stop 等 Writer 排空的上限：正常情况下这个
// 等待应该近乎瞬间完成（Writer 只是把内存里已经排队的字节写给一个此时还
// 打开着的 socket），设这个上限只是为了避免"Writer 从未真正启动过"这种
// 边界场景（比如构造后从未 Start 就直接 Stop）或极端异常情况下把 Stop
// 阻塞住——超时后记一条警告并继续物理关闭，不无限等待。
const connStopWriterDrainTimeout = 5 * time.Second

func (c *Connection) Stop() {
	// 复用 BeginClosing 而不是直接调 c.lc.Transition：BeginClosing 除了
	// 转状态，还会强制打断当前可能正阻塞的读（SetReadDeadline(now)）——
	// 直接调用 Stop（不经过 Server 优雅关闭的 BeginClosingAll）在连接仍是
	// Active、Reader 正阻塞等下一帧的场景下很常见（比如测试直接调用
	// conn.Stop()，或者本方法自己被 EOF/超时/panic 之外的路径调用），如果
	// 不强制打断，下面等 writerDone 的逻辑会因为 Reader 永远不会自己退出
	// 而白白等满整个超时——这不是理论场景，是被测试直接复现过的真实时序。
	c.BeginClosing()
	if !c.lc.Close() {
		return // 另一个并发的 Stop 调用已经在做/做完了物理清理
	}
	slog.Debug("conn terminated", "connID", c.ConnID)

	// 物理关闭 socket 之前，先等 Writer 确认已经把队列排空——这是 bug①
	// （Server.Stop 不等在途请求处理完，响应可能因连接已关闭而发不出去）的
	// 修复核心：Writer 只有在观察到 readerDone 关闭（不会再有新响应被投递）
	// 之后才会排空并关闭 writerDone，这里等它，就保证了不会有"已经产出但
	// 还没写出的响应"在 socket 关闭时被丢弃。只有真正 Start 过的连接才需要
	// 等（Started() 为 false 时说明 StartWriter 从未运行过，等它没有意义）。
	if c.lc.Started() {
		select {
		case <-c.writerDone:
		case <-time.After(connStopWriterDrainTimeout):
			slog.Warn("conn stop: writer did not drain within timeout, closing anyway", "connID", c.ConnID)
		}
	}

	c.Conn.Close() // 解除 Reader 的阻塞读（若它还没自己退出的话），使其返回
	c.callConnStopFunc()
	c.Registry.Remove(c)
	// 不 close msgChan / msgBuffChan：worker 可能仍在 SendBuffMsg，close 会触发 send on closed channel
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
	packet, err := codec.NewDataPack().Pack(codec.NewMessage(msgID, data))
	if err != nil {
		return err
	}
	// 只随"物理关闭"（Done）而返回错误，不随"决定关闭"（Draining）——
	// Closing 阶段写路径必须保持可用，否则在途请求处理完之后产出的响应会
	// 因为连接被判定为"已关闭"而发不出去，这正是 bug①的病因。见
	// bannet/lifecycle 包顶部关于 Draining 与 Done 的注释。
	select {
	case c.msgChan <- packet:
		return nil
	case <-c.lc.Done():
		return errConnClosed
	}
}

func (c *Connection) SendBuffMsg(msgID string, data []byte) error {
	packet, err := codec.NewDataPack().Pack(codec.NewMessage(msgID, data))
	if err != nil {
		return err
	}
	select {
	case c.msgBuffChan <- packet:
		return nil
	case <-c.lc.Done():
		return errConnClosed
	}
}

// SendRawMsg 实现 handler.Conn：写出一个已经编码好的完整帧，不经过
// codec.DataPack.Pack（那是 v1 专属的编码，会把 data 误当成 v1 msgID+
// payload 重新打包）。BANLV v2 的所有响应（含协商响应本身，见 StartReader
// 里 NegotiateFunc 分支）都必须走这里——与 SendMsg/SendBuffMsg 共享同一个
// msgChan，写入仍然完全串行化在 Writer goroutine 里，不会与 v1 帧或彼此
// 产生交织写入。
func (c *Connection) SendRawMsg(frame []byte) error {
	select {
	case c.msgChan <- frame:
		return nil
	case <-c.lc.Done():
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
