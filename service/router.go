package service

import (
	"encoding/binary"
	"errors"
	"log/slog"

	"github.com/ChronoBrew/KairosFlux/cluster"
	"github.com/ChronoBrew/KairosFlux/internal/admission"
	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/predicate"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/storage"
)

// KVStore 抽象出 Router 本地处理所需的 KV 能力（*KVServer 满足之）。抽成接口是为了
// 让分片集成测试注入互相隔离的内存 store，从而在一个进程内起多节点验证转发。
type KVStore interface {
	Write(cmd Command) error
	Get(key []byte) ([]byte, error)
	Scan(start, end []byte, pred predicate.Predicate, limit int) []proto.ScanEntry
}

// Router 基础路由处理器
type Router struct {
	kv    *KVServer // 具体实例，供 FSM 等；测试注入 store 时可为 nil
	store KVStore   // 本地 KV 操作（生产 = kv）

	// 分片路由（可选）：placement!=nil 时按 key 属主决定本地处理还是转发到 owner 节点。
	placement *cluster.Placement
	self      string // 本节点地址（= config.Peers[Me]）
	peers     *cluster.PeerPool

	// 网关自适应准入（可选）：limiter!=nil 时按并发上限准入，过载 shed。
	limiter *admission.Limiter

	// 前置处理函数；返回 HookDrop 表示丢弃本帧，reason 是丢弃原因（可为空），
	// PreHandle 会把它带上 sendDropped 回传给客户端。
	preHandleFunc func(request kairnet.Request) (kairnet.HookAction, string)
	// 后置处理函数
	postHandleFunc func(request kairnet.Request)
}

// NewRouter 创建新的路由处理器
func NewRouter(kv *KVServer) *Router {
	return &Router{
		kv:    kv,
		store: kv,
	}
}

// NewRouterWithStore 用注入的 KVStore 创建路由（供多节点集成测试隔离存储）。
func NewRouterWithStore(store KVStore) *Router {
	return &Router{store: store}
}

// SetRouting 开启分片路由：不属本节点的 key 转发到 owner。placement/peers 为 nil 时不路由。
func (r *Router) SetRouting(placement *cluster.Placement, self string, peers *cluster.PeerPool) {
	r.placement = placement
	r.self = self
	r.peers = peers
}

// forwardTarget 返回 (owner, true) 表示 key 不属本节点、应转发到 owner；否则本地处理。
func (r *Router) forwardTarget(key []byte) (string, bool) {
	if r.placement == nil || r.peers == nil {
		return "", false
	}
	owner := r.placement.OwnerOf(key)
	if owner == "" || owner == r.self {
		return "", false
	}
	return owner, true
}

// SetLimiter 开启网关自适应准入：过载时快速 shed（拒绝）而非无限排队。nil 关闭。
func (r *Router) SetLimiter(l *admission.Limiter) {
	r.limiter = l
}

// SetPreHandle 设置前置处理函数
func (r *Router) SetPreHandle(f func(request kairnet.Request) (kairnet.HookAction, string)) {
	r.preHandleFunc = f
}

// SetPostHandle 设置后置处理函数
func (r *Router) SetPostHandle(f func(request kairnet.Request)) {
	r.postHandleFunc = f
}

// PreHandle 前置处理。返回 HookDrop 时由本函数回写唯一的「丢弃」响应（携带
// preHandleFunc 给出的 reason，见 sendDropped），使纯请求-响应协议不发生响应
// 错位（见 OnConnStart 注释）。
func (r *Router) PreHandle(request kairnet.Request) kairnet.HookAction {
	if r.preHandleFunc == nil {
		return kairnet.HookPass
	}
	action, reason := r.preHandleFunc(request)
	if action == kairnet.HookDrop {
		sendDropped(request, reason)
	}
	return action
}

// Handle 处理请求。开启准入时先按并发上限准入：过载则 shed（回 overloaded 响应）不进处理。
func (r *Router) Handle(request kairnet.Request) {
	if r.limiter != nil {
		start, ok := r.limiter.Acquire()
		if !ok {
			metrics.AdmissionShed.Add(1)
			sendOverloaded(request)
			return
		}
		defer r.limiter.Release(start)
	}

	msgID := request.MsgID()
	data := request.MsgData()

	switch msgID {
	case proto.MsgPut:
		r.handlePut(data, request)
	case proto.MsgGet:
		r.handleGet(data, request)
	case proto.MsgDelete:
		r.handleDelete(data, request)
	case proto.MsgScan:
		r.handleScan(data, request)
	}
}

// statusPayload 编码 [statusLen u8][status bytes]
func statusPayload(status string) []byte {
	buf := make([]byte, 1+len(status))
	buf[0] = byte(len(status))
	copy(buf[1:], status)
	return buf
}

// sendErr 写回错误响应
func sendErr(req kairnet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespErr, statusPayload(proto.StatusError))
}

// sendNotFound 写回「键不存在」响应。与 sendErr 分开，使客户端能把常规的查不到
// 与服务端故障区分开——前者不应重试，后者可重试。
func sendNotFound(req kairnet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespErr, statusPayload(proto.StatusNotFound))
}

// sendOverloaded 写回「网关过载 shed」响应；保证每请求恰好一个响应、且可被客户端识别重试。
func sendOverloaded(req kairnet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespErr, statusPayload(proto.StatusOverloaded))
}

// sendOK 写回 PUT/DEL 成功响应
func sendOK(req kairnet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespOK, statusPayload(proto.StatusOK))
}

// droppedPayload 编码 [statusLen u8][status="dropped"][reasonLen u16 LE][reason bytes]。
//
// 向后兼容：这是在 statusPayload 的 [statusLen][status] 之后追加的新字段，老客户端
// 的 parseStatus 只读到 statusLen 声明的字节数为止，reasonLen/reason 落在它认为的
// "该操作特有的其余字节"（rest）里——旧版 Go SDK 的 Put/Delete 从不读取 rest，
// 新增这段不会让老客户端解析失败或崩溃，只是拿不到 reason（见
// docs/Kair-协议规范.md 的响应负载一节）。reason 为空时 reasonLen=0，行为退化为
// 与 statusPayload 完全相同的字节。
func droppedPayload(reason string) []byte {
	const maxReasonLen = 4096 // 远大于任何校验错误信息的实际长度，纯粹是防御性上限
	if len(reason) > maxReasonLen {
		reason = reason[:maxReasonLen]
	}
	status := proto.StatusDropped
	buf := make([]byte, 1+len(status)+2+len(reason))
	buf[0] = byte(len(status))
	copy(buf[1:], status)
	off := 1 + len(status)
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(reason)))
	copy(buf[off+2:], reason)
	return buf
}

// sendDropped 写回「被钩子按策略丢弃」响应，附带丢弃原因；保证每请求恰好一个响应。
func sendDropped(req kairnet.Request, reason string) {
	req.Conn().SendBuffMsg(proto.MsgRespErr, droppedPayload(reason))
}

// handlePut 处理 PUT 操作
func (r *Router) handlePut(data []byte, request kairnet.Request) {
	// 帧解析委托给 proto.DecodePutFrame（与 ingesthook.Filter 的 parsePut 共用
	// 同一实现，见该函数注释）；此前这里与那边各自实现过一遍同一段二进制解析。
	key, value, ok := proto.DecodePutFrame(data)
	if !ok {
		// 此前「长度头不足 8 字节」与「声明长度超出实际数据」是两条不同措辞、
		// 不同字段的 slog.Warn；解析逻辑合一后无法再区分这两种子情形，合并为
		// 一条通用日志。无测试断言过原日志文本，且不影响客户端可见行为
		// （两种情形下都是丢帧不回应，行为不变），仅日志可观测性上的收敛。
		slog.Warn("put frame malformed", "len", len(data))
		return
	}

	// 分片路由：不属本节点则转发到 owner。
	if owner, fwd := r.forwardTarget(key); fwd {
		if err := r.peers.Put(owner, key, value); err != nil {
			slog.Error("forward put to owner failed", "owner", owner, "error", err)
			metrics.WriteErrors.Add(1)
			sendErr(request)
			return
		}
		metrics.Writes.Add(1)
		sendOK(request)
		return
	}

	cmd := Command{
		Type:  CommandPut,
		Key:   key,
		Value: value,
	}

	if err := r.store.Write(cmd); err != nil {
		slog.Error("put failed", "error", err)
		metrics.WriteErrors.Add(1)
		sendErr(request)
		return
	}

	metrics.Writes.Add(1)
	sendOK(request)
}

// handleGet 处理 GET 操作
func (r *Router) handleGet(data []byte, request kairnet.Request) {
	key, ok := proto.DecodeKeyFrame(data)
	if !ok {
		return
	}

	metrics.Reads.Add(1)

	// 分片路由：不属本节点则转发到 owner 读取。
	var value []byte
	if owner, fwd := r.forwardTarget(key); fwd {
		v, found, err := r.peers.Get(owner, key)
		if err != nil {
			sendErr(request)
			return
		}
		if !found { // 转发路径同样要区分「键不存在」与转发失败
			sendNotFound(request)
			return
		}
		value = v
	} else {
		v, err := r.store.Get(key)
		if errors.Is(err, storage.ErrKeyNotFound) {
			sendNotFound(request)
			return
		}
		if err != nil {
			sendErr(request)
			return
		}
		value = v
	}

	// 响应负载: [statusLen u8][status bytes][valueLen u32 LE][value]
	status := proto.StatusOK
	response := make([]byte, 1+len(status)+4+len(value))
	response[0] = byte(len(status))
	copy(response[1:], status)
	binary.LittleEndian.PutUint32(response[1+len(status):1+len(status)+4], uint32(len(value)))
	copy(response[1+len(status)+4:], value)

	request.Conn().SendBuffMsg(proto.MsgRespOK, response)
}

// handleDelete 处理 DELETE 操作
func (r *Router) handleDelete(data []byte, request kairnet.Request) {
	key, ok := proto.DecodeKeyFrame(data)
	if !ok {
		return
	}

	// 分片路由：不属本节点则转发到 owner。
	if owner, fwd := r.forwardTarget(key); fwd {
		if err := r.peers.Delete(owner, key); err != nil {
			metrics.WriteErrors.Add(1)
			sendErr(request)
			return
		}
		metrics.Deletes.Add(1)
		sendOK(request)
		return
	}

	cmd := Command{
		Type: CommandDelete,
		Key:  key,
	}

	if err := r.store.Write(cmd); err != nil {
		metrics.WriteErrors.Add(1)
		sendErr(request)
		return
	}

	metrics.Deletes.Add(1)
	sendOK(request)
}

// handleScan 处理 SCAN 边缘范围查询：解码范围+谓词，服务端筛选后只回传命中切片。
func (r *Router) handleScan(data []byte, request kairnet.Request) {
	req, err := proto.DecodeScanRequest(data)
	if err != nil {
		slog.Warn("scan request decode failed", "error", err)
		sendErr(request)
		return
	}

	// SCAN 暂不做分片路由：范围查询跨分片需 scatter-gather，属后续工作，当前只扫本地。
	metrics.Scans.Add(1)
	entries := r.store.Scan(req.Start, req.End, req.Pred, 0)
	request.Conn().SendBuffMsg(proto.MsgRespOK, proto.EncodeScanResponse(proto.StatusOK, entries))
}

// PostHandle 后置处理
func (r *Router) PostHandle(request kairnet.Request) {
	if r.postHandleFunc != nil {
		r.postHandleFunc(request)
	}
}

// OnConnStart 连接建立回调。
// 不向客户端主动下发任何消息：这是纯请求-响应协议，连接建立时推送一条
// 未经请求的问候会让客户端把它误读为下一个请求的响应，造成整条连接的
// 响应错位（每条连接首个操作失败）。
func (r *Router) OnConnStart(conn kairnet.Conn) {}

// OnConnStop 连接关闭回调。同理不主动下发消息。
func (r *Router) OnConnStop(conn kairnet.Conn) {}

// FSM 获取 FSM 实例
func (r *Router) FSM() *KVServer {
	return r.kv
}
