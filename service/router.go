package service

import (
	"encoding/binary"
	"log/slog"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/pkg/admission"
	"github.com/NeverENG/BanDB/pkg/metrics"
	"github.com/NeverENG/BanDB/pkg/predicate"
	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/service/cluster"
)

// KVStore 抽象出 Router 本地处理所需的 KV 能力（*KVServer 满足之）。抽成接口是为了
// 让分片集成测试注入互相隔离的内存 store，从而在一个进程内起多节点验证转发。
type KVStore interface {
	Write(cmd Command) error
	Get(key []byte) ([]byte, error)
	Scan(start, end []byte, pred predicate.Predicate) []proto.ScanEntry
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

	// 前置处理函数；返回 HookDrop 表示丢弃本帧
	preHandleFunc func(request bannet.Request) bannet.HookAction
	// 后置处理函数
	postHandleFunc func(request bannet.Request)
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
func (r *Router) SetPreHandle(f func(request bannet.Request) bannet.HookAction) {
	r.preHandleFunc = f
}

// SetPostHandle 设置后置处理函数
func (r *Router) SetPostHandle(f func(request bannet.Request)) {
	r.postHandleFunc = f
}

// PreHandle 前置处理。返回 HookDrop 时由本函数回写唯一的「丢弃」响应，
// 使纯请求-响应协议不发生响应错位（见 OnConnStart 注释）。
func (r *Router) PreHandle(request bannet.Request) bannet.HookAction {
	if r.preHandleFunc == nil {
		return bannet.HookPass
	}
	action := r.preHandleFunc(request)
	if action == bannet.HookDrop {
		sendDropped(request)
	}
	return action
}

// Handle 处理请求。开启准入时先按并发上限准入：过载则 shed（回 overloaded 响应）不进处理。
func (r *Router) Handle(request bannet.Request) {
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
func sendErr(req bannet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespErr, statusPayload(proto.StatusError))
}

// sendOverloaded 写回「网关过载 shed」响应；保证每请求恰好一个响应、且可被客户端识别重试。
func sendOverloaded(req bannet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespErr, statusPayload(proto.StatusOverloaded))
}

// sendOK 写回 PUT/DEL 成功响应
func sendOK(req bannet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespOK, statusPayload(proto.StatusOK))
}

// sendDropped 写回「被钩子按策略丢弃」响应；保证每请求恰好一个响应。
func sendDropped(req bannet.Request) {
	req.Conn().SendBuffMsg(proto.MsgRespErr, statusPayload(proto.StatusDropped))
}

// handlePut 处理 PUT 操作
func (r *Router) handlePut(data []byte, request bannet.Request) {
	// 解析数据格式：key_len + key + value_len + value
	if len(data) < 8 {
		slog.Warn("[WARN] handlePut: data too short", "len", len(data))
		return
	}

	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))
	valueLen := int(binary.LittleEndian.Uint32(data[4:8]))

	if len(data) < 8+keyLen+valueLen {
		slog.Warn("[WARN] handlePut: incomplete data", "expected", 8+keyLen+valueLen, "got", len(data))
		return
	}

	key := data[8 : 8+keyLen]
	value := data[8+keyLen : 8+keyLen+valueLen]

	// 分片路由：不属本节点则转发到 owner。
	if owner, fwd := r.forwardTarget(key); fwd {
		if err := r.peers.Put(owner, key, value); err != nil {
			slog.Error("[ERROR] handlePut: forward failed", "owner", owner, "error", err)
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
		slog.Error("[ERROR] handlePut: write failed", "error", err)
		metrics.WriteErrors.Add(1)
		sendErr(request)
		return
	}

	metrics.Writes.Add(1)
	sendOK(request)
}

// handleGet 处理 GET 操作
func (r *Router) handleGet(data []byte, request bannet.Request) {
	if len(data) < 4 {
		return
	}

	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))

	if len(data) < 4+keyLen {
		return
	}

	key := data[4 : 4+keyLen]

	metrics.Reads.Add(1)

	// 分片路由：不属本节点则转发到 owner 读取。
	var value []byte
	if owner, fwd := r.forwardTarget(key); fwd {
		v, found, err := r.peers.Get(owner, key)
		if err != nil || !found {
			sendErr(request)
			return
		}
		value = v
	} else {
		v, err := r.store.Get(key)
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
func (r *Router) handleDelete(data []byte, request bannet.Request) {
	if len(data) < 4 {
		return
	}

	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))

	if len(data) < 4+keyLen {
		return
	}

	key := data[4 : 4+keyLen]

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
func (r *Router) handleScan(data []byte, request bannet.Request) {
	req, err := proto.DecodeScanRequest(data)
	if err != nil {
		slog.Warn("[WARN] handleScan: decode failed", "error", err)
		sendErr(request)
		return
	}

	// SCAN 暂不做分片路由：范围查询跨分片需 scatter-gather，属后续工作，当前只扫本地。
	metrics.Scans.Add(1)
	entries := r.store.Scan(req.Start, req.End, req.Pred)
	request.Conn().SendBuffMsg(proto.MsgRespOK, proto.EncodeScanResponse(proto.StatusOK, entries))
}

// PostHandle 后置处理
func (r *Router) PostHandle(request bannet.Request) {
	if r.postHandleFunc != nil {
		r.postHandleFunc(request)
	}
}

// OnConnStart 连接建立回调。
// 不向客户端主动下发任何消息：这是纯请求-响应协议，连接建立时推送一条
// 未经请求的问候会让客户端把它误读为下一个请求的响应，造成整条连接的
// 响应错位（每条连接首个操作失败）。
func (r *Router) OnConnStart(conn bannet.Conn) {}

// OnConnStop 连接关闭回调。同理不主动下发消息。
func (r *Router) OnConnStop(conn bannet.Conn) {}

// FSM 获取 FSM 实例
func (r *Router) FSM() *KVServer {
	return r.kv
}
