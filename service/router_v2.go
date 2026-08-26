package service

import (
	"encoding/binary"
	"errors"
	"log/slog"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/internal/identity"
	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook/schema"
	"github.com/ChronoBrew/KairosFlux/storage"
)

// RouterV2 是 Kair v2（docs/rfc/Kair-2.md）的业务处理器：单一 HandleV2
// 方法内部按 opcode switch（与 v1 Router.Handle 对 msgID switch 是同一写法），
// 由 kairnet.Server.AddRouterV2 注册。
//
// 与 v1 Router 的两处刻意的范围收缩（本阶段明确不做，不是遗漏）：
//   - 不支持 SetRouting 的分片转发能力——v2 本阶段的任务是 ack 三档协商/
//     窗口/对账/BYE 语义（RFC §11），分片路由是另一个维度的能力，与 v1
//     对齐是后续阶段的工作，见 DEVELOPMENT.md。
//   - 不接入 admission.Limiter 网关准入——理由同上。
//
// 并发模型：HandleV2 由 kairnet.Server.onFrameV2 在该连接自己的 Reader
// goroutine 里同步调用（RFC §11.2.2"窗口内写帧严格按到达顺序处理"这条
// 不变量的实现落点，见 kairnet/server.go 的 onFrameV2 注释）——因此
// v2ConnState 不需要加锁：同一时刻只有这一个 goroutine 会读写它，这是
// RFC §11.2.3 实现提醒里"是否需要锁"这个开放问题在本实现下的答案。
type RouterV2 struct {
	store TemporalRawStore
	// temporal 是时态内核 M0 接线（PUT_VERSIONED/GET_AS_OF/LIST_VERSIONS/
	// REPLAY_FINGERPRINT）的业务实现，构造时基于同一个 store 建立——版本化
	// 数据与 v1/v2 的普通 PUT/GET 数据共享同一份存储，不是另开一份。
	temporal *TemporalStore
	// filter 可为 nil：不做 schema 校验，PUT 只要帧本身能解出 key/value
	// 就一律 accepted（与 v1 在没有配置 ingesthook.Filter 时的行为对称）。
	filter *ingesthook.Filter
	// windowSafetyValveN 是 §11.2.2 的安全阀 N：ack=window 连接上，当前
	// 窗口收满这么多写帧后无条件关闭并发 WINDOW_ACK，独立于客户端自己按
	// corr_id/FLUSH 划分的批次边界，防止客户端一直不 FLUSH 导致服务端
	// 无限攒积压。
	windowSafetyValveN uint32

	// 内存护栏（可选）：memoryGuardrail!=nil 时，进程 RSS 超限（max_rss_mb）
	// 后写帧（PUT/PUT_VERSIONED/DEL）结构化拒绝（ErrCodeMemoryLimit，
	// "memory_limit_reached"），读帧照常服务。与 v1 Router 共享同一个实例
	// （服务装配时构造，见 guardrail.go）。
	memoryGuardrail *MemoryGuardrail
}

// DefaultV2WindowSafetyValveN 是生产环境的默认安全阀取值——测试可以在
// 构造 RouterV2 时传入远小的值以在短时间内触发安全阀关窗路径，不依赖
// config.G 全局状态（外科手术式修改：不新增全局配置字段，构造时注入即可
// 满足"测试用 N=3 而不改全局"的需要）。
const DefaultV2WindowSafetyValveN = 1000

// NewRouterV2 构造一个 RouterV2。filter 为 nil 时跳过 schema 校验；
// windowSafetyValveN<=0 时退化为 DefaultV2WindowSafetyValveN。store 的类型从
// KVStore 收紧为 TemporalRawStore（KVStore 的超集，多要求 ScanRaw）：生产唯一
// 调用点（cmd/kairosflux-server）与现有测试调用点都传入 *KVServer，*KVServer 已实现
// ScanRaw（见 service/fsm.go），故这一收紧不需要改动任何既有调用点。
func NewRouterV2(store TemporalRawStore, filter *ingesthook.Filter, windowSafetyValveN uint32) *RouterV2 {
	if windowSafetyValveN == 0 {
		windowSafetyValveN = DefaultV2WindowSafetyValveN
	}
	return &RouterV2{
		store:              store,
		temporal:           NewTemporalStore(store),
		filter:             filter,
		windowSafetyValveN: windowSafetyValveN,
	}
}

// v2ConnStatePropertyKey 是 v2ConnState 挂在 handler.Conn 属性袋上的 key。
const v2ConnStatePropertyKey = "kair.v2.connState"

// v2ConnState 是一条 v2 连接的窗口/累计计数状态（RFC §11.2.2/§11.2.3）。
// 窗口态（windowXxx）只在 ack=window 时有意义；累计态（cumXxx）覆盖整条
// 连接自建立以来的计数，供 §11.2.3 的 STAT 与 §11.4 BYE 的隐式 STAT 使用，
// 无论 ack=window 还是 none 都会维护。
type v2ConnState struct {
	windowOpen           bool
	windowCorrID         uint32
	windowReceived       uint32
	windowAccepted       uint32
	windowRejected       uint32
	windowFirstErrCode   uint16
	windowFirstErrReason string

	cumReceived       uint32
	cumAccepted       uint32
	cumRejected       uint32
	cumFirstErrCode   uint16
	cumFirstErrReason string
}

// SetMemoryGuardrail 挂载内存护栏（nil 表示不检查）。服务装配时调用
// （NewNode/Engine.attachNetworkShell），与 v1 Router 共享同一个实例。
func (r *RouterV2) SetMemoryGuardrail(g *MemoryGuardrail) { r.memoryGuardrail = g }

func (r *RouterV2) connState(conn kairnet.Conn) *v2ConnState {
	if s, ok := conn.Property(v2ConnStatePropertyKey).(*v2ConnState); ok {
		return s
	}
	s := &v2ConnState{}
	conn.SetProperty(v2ConnStatePropertyKey, s)
	return s
}

func (r *RouterV2) ackTier(conn kairnet.Conn) negotiate.AckTier {
	if ack, ok := conn.Property(negotiate.ConnPropertyAckTier).(negotiate.AckTier); ok {
		return ack
	}
	return negotiate.AckEvery
}

// HandleV2 实现 kairnet.HandlerV2：按 opcode 分派。
func (r *RouterV2) HandleV2(req kairnet.RequestV2) {
	switch req.Opcode() {
	case codec.OpcodePut, codec.OpcodeDel:
		r.handleWrite(req)
	case codec.OpcodeGet:
		r.maybeImplicitFlush(req)
		r.handleGet(req)
	case codec.OpcodeScan:
		r.maybeImplicitFlush(req)
		r.handleScan(req)
	case codec.OpcodeFlush:
		r.handleFlush(req)
	case codec.OpcodeStat:
		r.handleStat(req)
	case codec.OpcodeBye:
		r.handleBye(req)
	case codec.OpcodePutVersioned:
		r.handlePutVersioned(req)
	case codec.OpcodeGetAsOf:
		r.handleGetAsOf(req)
	case codec.OpcodeListVersions:
		r.handleListVersions(req)
	case codec.OpcodeReplayFingerprint:
		r.handleReplayFingerprint(req)
	case codec.OpcodeListWrites:
		r.handleListWrites(req)
	default:
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "unknown opcode")
	}
}

// maybeImplicitFlush 实现 RFC §11.5：ack=window 模式下，若当前窗口尚未
// 关闭时收到 GET/SCAN，先按 §11.2.2 规则隐式关闭它（发 WINDOW_ACK），
// 再正常处理这条读请求——保证同一连接上"写后立即读"不需要客户端先手动
// FLUSH。ack=every/none 模式下没有"窗口"这个概念，不做任何事。
func (r *RouterV2) maybeImplicitFlush(req kairnet.RequestV2) {
	if r.ackTier(req.Conn()) != negotiate.AckWindow {
		return
	}
	state := r.connState(req.Conn())
	if state.windowOpen {
		r.closeWindow(req.Conn(), state)
	}
}

// handleWrite 处理 PUT/DEL 两种写 opcode。三档 ack 的分支：
//   - every：与 v1 语义等价，逐帧回一个 OK/ERR。
//   - window：不逐帧回应，累计进当前窗口，按 corr_id 变化/安全阀 N 关窗。
//   - none：不逐帧回应，只累计连接级计数，无窗口概念。
func (r *RouterV2) handleWrite(req kairnet.RequestV2) {
	accepted, errCode, reason := r.applyWrite(req)

	switch r.ackTier(req.Conn()) {
	case negotiate.AckWindow:
		r.recordIntoWindow(req, accepted, errCode, reason)
	case negotiate.AckNone:
		r.recordCumulative(req.Conn(), accepted, errCode, reason)
	default: // AckEvery
		if accepted {
			r.sendOK(req.Conn(), req.Type(), req.CorrID(), nil)
		} else {
			r.sendErr(req.Conn(), req.Type(), req.CorrID(), errCode, reason)
		}
	}
}

// applyWrite 是 PUT/DEL 的实际业务处理：解帧 → （PUT 才做）schema 校验 →
// 落盘。返回 accepted=false 时 errCode/reason 说明原因，供 ack=every 的
// 即时响应与 ack=window/none 的计数复用同一套判定，不重复实现两遍。
//
// 一条本阶段的刻意行为差异（相对 v1 Router.handlePut，已记入交付报告的
// 偏差清单）：v1 对畸形 PUT 帧只是 slog.Warn 后静默丢帧，不回任何响应、
// 不计入任何计数；v2 这里把"帧解不出 key/value"算作 received 且 rejected
// 的一次——RFC §11.2.2 对 received 的定义是"成功解出的写帧数（含被拒绝
// 的）"，若把畸形帧排除在 received 之外，ack=none 的 STAT 对账会把"网络层
// 确实收到了字节、但帧本身不合法"这类问题永久排除在诊断范围之外，与
// §11.2.3 强调的诊断粒度设计意图相悖。
func (r *RouterV2) applyWrite(req kairnet.RequestV2) (accepted bool, errCode uint16, reason string) {
	// 内存护栏：进程 RSS 超限拒收写帧（含 DEL 与 PUT/PUT_VERSIONED 的公共
	// 落盘入口），结构化拒绝而不是静默丢帧——ack=window/none 的记账把这次
	// 拒绝计为 received 且 rejected（firstErrCode=0x1003），对账时可见。
	// 读帧不经过本函数，超限期间照常服务。
	if r.memoryGuardrail != nil && r.memoryGuardrail.Blocked() {
		return false, codec.ErrCodeMemoryLimit, "memory_limit_reached"
	}
	data := req.Data()

	if req.Opcode() == codec.OpcodeDel {
		key, ok := proto.DecodeKeyFrame(data)
		if !ok {
			// 与 v1 Router.handleDelete 对齐：畸形 DEL 帧本身也不计任何全局
			// metrics（v1 那里同样只是 `if !ok { return }`，见 service/router.go）。
			return false, codec.ErrCodeMalformedFrame, "malformed_frame"
		}
		if err := r.store.Write(Command{Type: CommandDelete, Key: key}); err != nil {
			metrics.WriteErrors.Add(1)
			return false, codec.ErrCodeNone, "store_error"
		}
		metrics.Deletes.Add(1)
		return true, codec.ErrCodeNone, ""
	}

	key, value, ok := proto.DecodePutFrame(data)
	if !ok {
		// 与 v1 ingesthook.Filter.Handle 对齐：畸形 PUT 帧计入
		// FramesDroppedMalformed——这是本仓库唯一的进程级可观测性出口
		// （headless 部署直接 tail 日志），v2 写路径必须与 v1 共享同一套
		// 全局计数器，不能只有本文件自己的连接级窗口/累计计数（RFC
		// §11.2.3 实现提醒明确要求两者并存，见本文件顶部注释）。
		metrics.FramesDroppedMalformed.Add(1)
		return false, codec.ErrCodeMalformedFrame, "malformed_frame"
	}

	if r.filter != nil {
		// ValidateForType（M1）：req.Type()==0（未声明类型）与此前的 Validate 完全
		// 等价，覆盖今天全部生产流量；非零时按声明的 TypeID 精确分派（Kair-2 RFC
		// §9.3），见 Filter.ValidateForType 的文档。
		newValue, _, result, code, reason := r.filter.ValidateForType(req.Type(), key, value)
		if result == ingesthook.ResultDrop {
			// Filter.ValidateForType 内部已经按具体丢弃原因（oversized/非单调/
			// schema）各自 .Add(1) 过对应的 FramesDroppedXxx 计数器，这里
			// 不需要、也不应该重复计数。
			//
			// code==0 的丢弃原因（oversized_value/non_monotonic_timestamp/
			// unknown_declared_type）沿用 M1 之前的行为：统一归入
			// ErrCodeSchemaValidation 这个通用桶——细分这些原因各自的错误码是
			// §10 错误码分类学整体的范围（0x1xxx/0x2xxx 段），不是本次 M1 契约层
			// 任务的目标（"缺列/单位错误结构化拒绝"，见方案 M1 验收标准）。
			// code!=0 时是 schema 校验器返回的具体子码（0x3001-0x3004，见
			// service/ingesthook/schema/error.go），比这个通用桶精确。
			if code == 0 {
				code = codec.ErrCodeSchemaValidation
			}
			return false, code, reason
		}
		value = newValue
	}

	if err := r.store.Write(Command{Type: CommandPut, Key: key, Value: value}); err != nil {
		metrics.WriteErrors.Add(1)
		return false, codec.ErrCodeNone, "store_error"
	}
	metrics.Writes.Add(1)
	return true, codec.ErrCodeNone, ""
}

// recordIntoWindow 把一次写结果计入当前窗口（ack=window 专用），实现
// RFC §11.2.2 的窗口边界规则：corr_id 变化则先隐式关闭旧窗口再开新窗口，
// 计数达到安全阀 N 则关闭当前窗口。同时镜像写入连接级累计计数——BYE 的
// 隐式 STAT 摘要（§11.4）需要它，无论窗口在那一刻是开是关。
func (r *RouterV2) recordIntoWindow(req kairnet.RequestV2, accepted bool, errCode uint16, reason string) {
	conn := req.Conn()
	state := r.connState(conn)

	if state.windowOpen && state.windowCorrID != req.CorrID() {
		r.closeWindow(conn, state)
	}
	if !state.windowOpen {
		state.windowOpen = true
		state.windowCorrID = req.CorrID()
		state.windowReceived, state.windowAccepted, state.windowRejected = 0, 0, 0
		state.windowFirstErrCode, state.windowFirstErrReason = codec.ErrCodeNone, ""
	}

	state.windowReceived++
	if accepted {
		state.windowAccepted++
	} else {
		state.windowRejected++
		if state.windowFirstErrCode == codec.ErrCodeNone && state.windowFirstErrReason == "" {
			state.windowFirstErrCode, state.windowFirstErrReason = errCode, reason
		}
	}
	r.recordCumulativeOnly(state, accepted, errCode, reason)

	if state.windowReceived >= r.windowSafetyValveN {
		r.closeWindow(conn, state)
	}
}

// closeWindow 关闭当前打开的窗口并发出 WINDOW_ACK（RFC §11.2.2），随后把
// 窗口态清空为"未打开"。调用前必须已确认 state.windowOpen==true。
func (r *RouterV2) closeWindow(conn kairnet.Conn, state *v2ConnState) {
	body := proto.V2AckBody(state.windowCorrID, state.windowReceived, state.windowAccepted,
		state.windowRejected, state.windowFirstErrCode, state.windowFirstErrReason)
	r.sendV2(conn, codec.OpcodeWindowAck, codec.TypeUnspecified, state.windowCorrID, body)
	state.windowOpen = false
}

// recordCumulative 把一次写结果计入连接级累计计数（ack=none 专用：没有
// 窗口概念，每帧独立累计，不发任何响应）。
func (r *RouterV2) recordCumulative(conn kairnet.Conn, accepted bool, errCode uint16, reason string) {
	state := r.connState(conn)
	r.recordCumulativeOnly(state, accepted, errCode, reason)
}

func (r *RouterV2) recordCumulativeOnly(state *v2ConnState, accepted bool, errCode uint16, reason string) {
	state.cumReceived++
	if accepted {
		state.cumAccepted++
	} else {
		state.cumRejected++
		if state.cumFirstErrCode == codec.ErrCodeNone && state.cumFirstErrReason == "" {
			state.cumFirstErrCode, state.cumFirstErrReason = errCode, reason
		}
	}
	// 协议不变量（RFC §11.2.2）：received == accepted + rejected。由本函数
	// 是三个计数器唯一的写入点这一事实保证成立，这里加一道防御性断言，
	// 若被打破说明本文件自身的记账逻辑有 bug（不是外部输入能触发的）。
	if state.cumReceived != state.cumAccepted+state.cumRejected {
		slog.Error("kairnet v2 accounting invariant violated",
			"received", state.cumReceived, "accepted", state.cumAccepted, "rejected", state.cumRejected)
	}
}

// handleFlush 处理 FLUSH（opcode 0x07，RFC §11.2.2）：ack=window 模式下
// 关闭当前窗口（若有）并发 WINDOW_ACK；没有打开的窗口时发一个全零的
// WINDOW_ACK（corr_id=0）作为"当前无待关闭窗口"的诚实回应，而不是静默
// 忽略这次 FLUSH。非 window 模式（every/none）下 FLUSH 没有意义，同样回一个
// 全零 WINDOW_ACK，不报错——协议未对"档位不匹配的控制帧"定义拒绝语义，
// 静默接受空响应比断连接更符合"读多写少场景，宽容优先"的取向。
func (r *RouterV2) handleFlush(req kairnet.RequestV2) {
	conn := req.Conn()
	if r.ackTier(conn) == negotiate.AckWindow {
		state := r.connState(conn)
		if state.windowOpen {
			r.closeWindow(conn, state)
			return
		}
	}
	body := proto.V2AckBody(0, 0, 0, 0, codec.ErrCodeNone, "")
	r.sendV2(conn, codec.OpcodeWindowAck, codec.TypeUnspecified, 0, body)
}

// handleStat 处理 STAT（opcode 0x08，RFC §11.2.3）：回传连接自建立以来的
// 累计「接收/接受/拒绝」计数，corr_id 取 STAT 请求帧本身的 corr_id
// （普通请求-响应关联，不是窗口分组）。非破坏性：不清零、不影响后续计数。
func (r *RouterV2) handleStat(req kairnet.RequestV2) {
	state := r.connState(req.Conn())
	body := proto.V2AckBody(req.CorrID(), state.cumReceived, state.cumAccepted,
		state.cumRejected, state.cumFirstErrCode, state.cumFirstErrReason)
	r.sendV2(req.Conn(), codec.OpcodeStatAck, codec.TypeUnspecified, req.CorrID(), body)
}

// handleBye 处理 BYE（opcode 0x06，RFC §11.4）。every 档位语义不变（保留
// 位，服务端不主动做任何事——礼貌关闭与否由客户端自行决定，符合
// service.Router.OnConnStart/OnConnStop"纯请求-响应协议不主动下发消息"的
// 既有原则）。window/none 档位第一次被赋予行为：
//  1. window 且窗口打开：先按 §11.2.2 关闭它（发 WINDOW_ACK）。
//  2. 再回传一次连接级累计摘要，复用 STAT_ACK 格式（RFC 原文："而不是另外
//     定义第三种响应格式"）。corr_id 取 BYE 帧本身的 corr_id——RFC 未明确
//     这一步的 corr_id 应该是什么（STAT_ACK 一节描述的是"STAT 请求触发"
//     的普通场景），这里选择"触发它的那一帧"这个最自然的读法，记入交付
//     报告的偏差清单。
func (r *RouterV2) handleBye(req kairnet.RequestV2) {
	conn := req.Conn()
	ack := r.ackTier(conn)
	if ack != negotiate.AckWindow && ack != negotiate.AckNone {
		return
	}

	state := r.connState(conn)
	if ack == negotiate.AckWindow && state.windowOpen {
		r.closeWindow(conn, state)
	}

	body := proto.V2AckBody(req.CorrID(), state.cumReceived, state.cumAccepted,
		state.cumRejected, state.cumFirstErrCode, state.cumFirstErrReason)
	r.sendV2(conn, codec.OpcodeStatAck, codec.TypeUnspecified, req.CorrID(), body)
}

// handleGet 处理 GET：与 v1 Router.handleGet 语义等价（含分片转发能力
// 本阶段不支持这一点差异），只是响应用 v2 帧格式。OK 响应负载直接是
// value 本身（§4 结构化响应体细节本轮不定稿，见 RFC，本阶段选择最小可用
// 编码：value 就是负载，不额外包一层状态字段——opcode 本身已经区分了
// 成功/失败）。
func (r *RouterV2) handleGet(req kairnet.RequestV2) {
	key, ok := proto.DecodeKeyFrame(req.Data())
	if !ok {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
		return
	}
	metrics.Reads.Add(1)
	value, err := r.store.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "notfound")
		return
	}
	if err != nil {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "error")
		return
	}
	r.sendOK(req.Conn(), req.Type(), req.CorrID(), value)
}

// handleScan 处理 SCAN：复用 proto.EncodeScanResponse 的条目编码（去掉 v1
// 特有的 status 字符串前缀，opcode 本身已经承担这个角色）。
func (r *RouterV2) handleScan(req kairnet.RequestV2) {
	sreq, err := proto.DecodeScanRequest(req.Data())
	if err != nil {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
		return
	}
	metrics.Scans.Add(1)
	entries := r.store.Scan(sreq.Start, sreq.End, sreq.Pred, 0)
	// EncodeScanResponse 编码的是 v1 格式（含 status 前缀）；v2 只需要
	// status 之后的 count+entries 部分，opcode=OK 已经表达了"成功"。
	full := proto.EncodeScanResponse(proto.StatusOK, entries)
	body := full[1+len(proto.StatusOK):]
	r.sendOK(req.Conn(), req.Type(), req.CorrID(), body)
}

// handlePutVersioned 处理 PUT_VERSIONED（时态内核 M0 新增 opcode）：与 v1/v2
// OpcodePut 刻意不同的语义——OpcodePut 覆盖写字面量 key（老客户端零影响，
// 见 codec.OpcodePutVersioned 的文档），这里的写入永不覆盖，每次调用产生
// 一条新的不可变版本。请求帧格式与 PUT 相同（proto.DecodePutFrame：key=
// 逻辑键，value=本次版本的负载），不新开一套帧格式。
//
// 不参与 §11.2.2 的 ack 三档窗口/累计记账（见 codec.OpcodePutVersioned 文档），
// 总是立即响应：OK 负载 = 分配到的 seq（[u64 LE] 8 字节），供调用方确认/
// 记录这次写对应的版本号；ERR 负载沿用既有 V2ErrPayload 结构。
func (r *RouterV2) handlePutVersioned(req kairnet.RequestV2) {
	// 内存护栏：PUT_VERSIONED 是独立落盘入口（不经 applyWrite），这里单独
	// 拦一道——与 applyWrite 同一判定、同一错误码，保证 v2 全部写帧语义一致。
	if r.memoryGuardrail != nil && r.memoryGuardrail.Blocked() {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMemoryLimit, "memory_limit_reached")
		return
	}
	key, value, source, ok := proto.DecodePutVersionedFrame(req.Data())
	if !ok {
		metrics.FramesDroppedMalformed.Add(1)
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
		return
	}

	// 协议层角色强制（M4 上报缺口：此前 internal/aiplane.WriteAsAgent 只是
	// 应用层 API 的一道闸门，任何调用方仍可绕过它直接对这个 opcode 发起
	// PUT_VERSIONED 写任意键，见 internal/aiplane/doc.go 已更新的"已知边界"
	// 一节）。source 由请求帧显式携带（M2 操作元数据信封字段），不读系统
	// 时钟/连接身份等隐式状态——与本仓库审计路径"确定性来源"的一贯要求
	// 一致。agent 身份（identity.SourceRole 判定，见其文档的 agent: 前缀
	// 约定）只允许写 Proposal 键空间（identity.IsProposalKey，与
	// aiplane.ProposalKey 拼键用的是同一个前缀常量），其余一律结构化拒绝——
	// 这条规则只覆盖 PUT_VERSIONED，不覆盖不携带 source 的字面量 PUT/DEL。
	if identity.SourceRole(source) == identity.RoleAgent && !identity.IsProposalKey(string(key)) {
		metrics.FramesDroppedUnauthorized.Add(1)
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeUnauthorizedRole, "agent_write_forbidden_kind")
		return
	}

	if r.filter != nil {
		// ValidateVersionedForType，不是 ValidateVersioned：跳过时间戳单调性
		// 启发式，见其文档——那条启发式假设"同一 key 被反复写入=时钟异常"，与
		// PUT_VERSIONED 的正常工作方式（反复对同一逻辑键写新版本）直接冲突，
		// 冲突在写这段代码时就用真实服务端 smoke test 复现过。req.Type()==0 时
		// 与此前的 ValidateVersioned 完全等价（M1，见 Filter.ValidateForType）。
		newValue, _, result, code, reason := r.filter.ValidateVersionedForType(req.Type(), key, value)
		if result == ingesthook.ResultDrop {
			if code == 0 {
				code = codec.ErrCodeSchemaValidation
			}
			r.sendErr(req.Conn(), req.Type(), req.CorrID(), code, reason)
			return
		}
		value = newValue
	}

	seq, err := r.temporal.PutVersioned(string(key), value, time.Now().UnixNano(), source, schemaVersionFor(req.Type(), key))
	if err != nil {
		metrics.WriteErrors.Add(1)
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "store_error")
		return
	}
	metrics.Writes.Add(1)

	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, seq)
	r.sendOK(req.Conn(), req.Type(), req.CorrID(), payload)
}

// schemaVersionFor 返回 typeID/key 对应的 schema 契约版本号（M2 操作元数据
// 信封新增字段，见 TemporalStore.PutVersioned 的文档），来源与 Filter 的
// schema 分派规则完全一致（Kair-2 RFC §9.3：typeID!=0 时按 TypeID 精确查表，
// 否则按 key 前缀最长匹配）——但不经过 Filter，直接查 schema 包的全局
// 注册表：schema_ver 是纯元数据采集，与"是否要做 schema 校验/是否配置了
// Filter"是两件独立的事，未注册类型返回 0（"未纳管"，不是错误）。
func schemaVersionFor(typeID uint16, key []byte) uint32 {
	if typeID != 0 {
		if d, ok := schema.LookupByType(typeID); ok {
			return uint32(d.SchemaVersion)
		}
		return 0
	}
	if d, ok := schema.LookupDescriptor(key); ok {
		return uint32(d.SchemaVersion)
	}
	return 0
}

// handleGetAsOf 处理 GET_AS_OF：请求帧 proto.DecodeAsOfFrame（key + as_of
// 纳秒时间戳），返回该时刻可见的最新版本（temporal.AsOf 语义：绝不返回未来
// 写入）。找不到（该逻辑键从未被版本化写过，或 as_of 早于其首次写入）与 v1/
// v2 GET 对齐，回 ERR reason="notfound"（不是协议错误，是"确实没有"）。
func (r *RouterV2) handleGetAsOf(req kairnet.RequestV2) {
	key, asOfNanos, ok := proto.DecodeAsOfFrame(req.Data())
	if !ok {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
		return
	}
	metrics.Reads.Add(1)
	v, found, err := r.temporal.GetAsOf(string(key), asOfNanos)
	if err != nil {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "error")
		return
	}
	if !found {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "notfound")
		return
	}
	r.sendOK(req.Conn(), req.Type(), req.CorrID(), proto.EncodeVersionEntry(v.Seq, v.WriteNanos, v.Payload))
}

// handleListVersions 处理 LIST_VERSIONS：请求帧 proto.DecodeKeyFrame（key=
// 逻辑键，与 GET/DELETE 共用同一帧格式）。返回该逻辑键全部版本，按 seq 升序；
// 没有任何版本是合法结果（OK，count=0），不是 ERR——"从未写过"与"请求本身
// 出错"是两件事，不应共用同一个错误响应通道。
func (r *RouterV2) handleListVersions(req kairnet.RequestV2) {
	key, ok := proto.DecodeKeyFrame(req.Data())
	if !ok {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
		return
	}
	metrics.Reads.Add(1)
	versions, err := r.temporal.ListVersions(string(key))
	if err != nil {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "error")
		return
	}
	entries := make([][]byte, len(versions))
	for i, v := range versions {
		entries[i] = proto.EncodeVersionEntry(v.Seq, v.WriteNanos, v.Payload)
	}
	r.sendOK(req.Conn(), req.Type(), req.CorrID(), proto.EncodeListVersionsResponse(entries))
}

// handleReplayFingerprint 处理 REPLAY_FINGERPRINT：请求帧 proto.DecodeKeyFrame
// （key 在这里的含义是逻辑键前缀，不是某一个具体逻辑键——与 LIST_VERSIONS
// 复用同一帧格式，但字段含义不同，故不共用同一个"key"命名的文档措辞，见
// TemporalStore.ReplayFingerprint 的文档）。对前缀下每个逻辑键做重放对账，
// 返回一致性摘要——这是验收三问第 2 问"账本可重放指纹一致"的可执行入口。
func (r *RouterV2) handleReplayFingerprint(req kairnet.RequestV2) {
	prefix, asOfNanos, ok := proto.DecodeReplayFingerprintRequest(req.Data())
	if !ok {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
		return
	}
	result, err := r.temporal.ReplayFingerprint(string(prefix), asOfNanos)
	if err != nil {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "error")
		return
	}
	body := proto.EncodeReplayFingerprintResponseV2(
		result.KeyCount, uint32(len(result.Mismatches)), result.Fingerprint, result.Mismatches, result.Bounded)
	r.sendOK(req.Conn(), req.Type(), req.CorrID(), body)
}

// handleListWrites 处理 LIST_WRITES（时态内核 M2 审计查询，方案 §M2 第 2
// 项）：请求帧 proto.DecodeListWritesRequestV2（prefix + 时间范围 + 来源
// 过滤 + 可选游标/limit，M5 方案 §C.1），返回命中的操作元数据信封列表 +
// 按来源聚合计数——这就是"定点分析查询"的最小完备形态（as_of(t) 定点取值
// + LIST_WRITES(key,t1..t2) 定点审计）。
//
// 结果体量防护（M2 遗留缺口，M5 修复）：一个前缀下的写入历史长度没有上界
// （不像 REPLAY_FINGERPRINT 只返回"每个逻辑键的最新版本"，LIST_WRITES 把
// 每条历史记录连同 payload 一起编进响应体），soak 测试
// （service/temporal_soak_bench_test.go，10 万键×10 版本=100 万条）证实这
// 不是假设性风险——单帧体量可以逼近甚至超过 codec.EffectiveMaxSize 的帧长
// 上限。M2 的处理是结构化拒绝（ErrCodeResultTooLarge，"查询范围太大"），
// 分页不在 M2 范围内故当时未做；M5 补上：请求可带游标/limit，服务端把响应
// 钉在 limit 条内、附 next_cursor 供续查，无界查询不再必然撞帧长上限。
//
// 两条路径的分界：请求无尾段（M2 老格式）时走与 M2 完全相同的旧路径——
// ListWrites + EncodeListWritesResponse + 超帧拒绝，响应逐字节不变（"只
// 追加向量"红线的响应侧落地，docs/kair/vectors-v2.json 旧向量不受影响）；
// 请求带游标或 limit（尾段）时走分页路径。分页路径的帧长检查保留为防御：
// limit 由调用方自定，若 limit 大到单帧仍超限，结果照旧被结构化拒绝，语义
// 不变。
func (r *RouterV2) handleListWrites(req kairnet.RequestV2) {
	prefix, tFrom, tTo, source, afterBytes, limit, ok := proto.DecodeListWritesRequestV2(req.Data())
	if !ok {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
		return
	}

	// M2 老格式请求（无尾段）：旧路径，响应逐字节不变。
	if len(afterBytes) == 0 && limit == 0 {
		result, err := r.temporal.ListWrites(string(prefix), tFrom, tTo, string(source))
		if err != nil {
			r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "error")
			return
		}
		body := encodeListWritesBody(result)
		if effMax := codec.EffectiveMaxSize(config.G.MaxPackageSize); uint32(len(body)) > effMax {
			slog.Warn("kairnet LIST_WRITES result exceeds frame size limit, rejecting",
				"bodyBytes", len(body), "effectiveMax", effMax, "matchCount", len(result.Entries))
			r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeResultTooLarge, "result_too_large")
			return
		}
		r.sendOK(req.Conn(), req.Type(), req.CorrID(), body)
		return
	}

	// 分页路径（M5）：游标非空时按 (LogicalKey, Seq) 解码，非法即畸形帧。
	var after *ListWritesCursor
	if len(afterBytes) > 0 {
		lk, seq, curOK := proto.DecodeListWritesCursor(afterBytes)
		if !curOK {
			r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeMalformedFrame, "malformed_frame")
			return
		}
		after = &ListWritesCursor{LogicalKey: lk, Seq: seq}
	}
	result, hasMore, err := r.temporal.ListWritesPage(string(prefix), tFrom, tTo, string(source), after, limit)
	if err != nil {
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeNone, "error")
		return
	}
	var nextCursor []byte
	if hasMore && len(result.Entries) > 0 {
		last := result.Entries[len(result.Entries)-1]
		nextCursor = proto.EncodeListWritesCursor(last.LogicalKey, last.Seq)
	}
	body := encodeListWritesBodyV2(result, nextCursor)
	if effMax := codec.EffectiveMaxSize(config.G.MaxPackageSize); uint32(len(body)) > effMax {
		slog.Warn("kairnet LIST_WRITES page exceeds frame size limit, rejecting",
			"bodyBytes", len(body), "effectiveMax", effMax, "matchCount", len(result.Entries))
		r.sendErr(req.Conn(), req.Type(), req.CorrID(), codec.ErrCodeResultTooLarge, "result_too_large")
		return
	}
	r.sendOK(req.Conn(), req.Type(), req.CorrID(), body)
}

// encodeListWritesBody 把 ListWritesResult 编码成 M2 格式 LIST_WRITES OK
// 响应体（EncodeListWritesResponse：entries + 按来源聚合计数）。
func encodeListWritesBody(result ListWritesResult) []byte {
	entries := make([][]byte, len(result.Entries))
	for i, e := range result.Entries {
		entries[i] = proto.EncodeWriteEnvelopeEntry(
			e.LogicalKey, e.Seq, e.WriteNanos, e.Source, e.SchemaVer, e.PersistedHash, e.Payload, e.HashOK)
	}
	sourceNames := make([]string, len(result.BySource))
	sourceCounts := make([]uint32, len(result.BySource))
	for i, sc := range result.BySource {
		sourceNames[i] = sc.Source
		sourceCounts[i] = sc.Count
	}
	return proto.EncodeListWritesResponse(entries, sourceNames, sourceCounts)
}

// encodeListWritesBodyV2 是分页路径的响应体编码：M2 基段 + next_cursor 尾段
// （nextCursor 为空 = 页面已尽）。
func encodeListWritesBodyV2(result ListWritesResult, nextCursor []byte) []byte {
	entries := make([][]byte, len(result.Entries))
	for i, e := range result.Entries {
		entries[i] = proto.EncodeWriteEnvelopeEntry(
			e.LogicalKey, e.Seq, e.WriteNanos, e.Source, e.SchemaVer, e.PersistedHash, e.Payload, e.HashOK)
	}
	sourceNames := make([]string, len(result.BySource))
	sourceCounts := make([]uint32, len(result.BySource))
	for i, sc := range result.BySource {
		sourceNames[i] = sc.Source
		sourceCounts[i] = sc.Count
	}
	return proto.EncodeListWritesResponseV2(entries, sourceNames, sourceCounts, nextCursor)
}

func (r *RouterV2) sendOK(conn kairnet.Conn, typ uint16, corrID uint32, payload []byte) {
	r.sendV2(conn, codec.OpcodeOK, typ, corrID, payload)
}

func (r *RouterV2) sendErr(conn kairnet.Conn, typ uint16, corrID uint32, errCode uint16, reason string) {
	r.sendV2(conn, codec.OpcodeErr, typ, corrID, proto.V2ErrPayload(errCode, reason))
}

func (r *RouterV2) sendV2(conn kairnet.Conn, opcode uint8, typ uint16, corrID uint32, payload []byte) {
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: typ, CorrID: corrID}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		slog.Error("kairnet v2 pack response failed", "error", err)
		return
	}
	if err := conn.SendRawMsg(frame); err != nil {
		slog.Debug("kairnet v2 send response failed", "connID", conn.ID(), "error", err)
	}
}
