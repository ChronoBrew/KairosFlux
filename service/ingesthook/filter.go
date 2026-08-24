// Package ingesthook 提供一个挂在采集入口的真实 PreHandle 过滤钩子示例：
// 在数据落盘前完成「丢弃畸形帧 + 时间戳单调性校验 + schema 校验 + 字段脱敏」四件事，
// 把「可编程边缘采集缓冲网关」从挂载点变成有内容的演示。
//
// 钩子只读取并改写请求负载，绝不向连接写响应——「丢弃即回写唯一响应」的
// 不变式由 service.Router.PreHandle 统一持有（见 service/router.go）。
//
// 传输层边界：Handle 只做 bannet 帧解析（畸形帧检测），核心清洗逻辑在 Validate 里，
// 只认 key/value 字节、不认 bannet.Request——因此 gRPC 等其它入口可以直接调用
// Filter.Validate 复用同一套清洗规则，见 internal/kvgrpc.GRPCServer.Put。
package ingesthook

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/internal/metrics"
	"github.com/NeverENG/BanDB/proto"
	"github.com/NeverENG/BanDB/service/ingesthook/schema"
)

// redactedValue 是脱敏字段被替换成的 JSON 值。
var redactedValue = json.RawMessage(`"[REDACTED]"`)

// Filter 是采集入口过滤器。零值不可用，请用 NewFilter 构造。
type Filter struct {
	// redactFields 命中的 JSON 字段会被脱敏改写。
	redactFields []string
	// maxValueLen 限制 value 字节数，超过视为畸形丢弃；<=0 表示不限。
	maxValueLen int
	// dropBackward 为 true 时，时间戳回退/重放的帧按设备丢弃。
	dropBackward bool

	mu sync.Mutex
	// lastTS 记录每个设备最近一次接受的时间戳，用于 best-effort 单调校验。
	lastTS map[string]int64
}

// NewFilter 构造过滤器。redactFields 为需脱敏的 JSON 字段名；maxValueLen<=0
// 表示不限 value 长度；dropBackward 控制是否丢弃时间戳回退帧。
func NewFilter(redactFields []string, maxValueLen int, dropBackward bool) *Filter {
	return &Filter{
		redactFields: redactFields,
		maxValueLen:  maxValueLen,
		dropBackward: dropBackward,
		lastTS:       make(map[string]int64),
	}
}

// Handle 实现 PreHandle 钩子签名。返回 HookDrop 表示丢弃本帧；reason 在丢弃时
// 非空，供 service.Router.PreHandle 附到 dropped 响应里回传给客户端（见
// service/router.go 的 sendDropped）——此前 reason 在这里被直接丢弃，客户端
// 只知道 dropped、不知道具体原因，逼得调用方自己在本地复刻一份校验规则去猜
// （QuantScout 全量实测暴露的真实问题，见
// docs/iteration-2026-08-20-quantscout-realdata-fixes.md 的 D2 记录）。
//
// 只做 bannet 特有的一步：从裸帧里解出 key/value（畸形帧在这一步就地丢弃，
// 它是「帧是否完整」的传输层问题，不是 Validate 管的「内容是否合法」问题）。
// 解出 key/value 之后的清洗全部委托给 Validate，保证与 gRPC 入口走同一套规则。
func (f *Filter) Handle(req bannet.Request) (bannet.HookAction, string) {
	// 钩子只针对写入帧：GET/DELETE 的负载格式不同，放行不动。
	if req.MsgID() != proto.MsgPut {
		return bannet.HookPass, ""
	}

	key, value, ok := parsePut(req.MsgData())
	if !ok {
		metrics.FramesDroppedMalformed.Add(1)
		return bannet.HookDrop, "malformed_frame" // 畸形帧：长度字段与实际数据不符
	}

	// v1 帧没有 type 字段，code 在这条路径上没有出口（v1 dropped 响应只有 reason
	// 字符串，见 docs/BANLV-协议规范.md §3.4），故丢弃。
	newValue, changed, result, _, reason := f.Validate(key, value)
	if result == ResultDrop {
		return bannet.HookDrop, reason
	}
	if changed {
		// 字段脱敏改写了 value：重建整帧（含新的 valueLen 前缀）。
		req.SetMsgData(encodePut(key, newValue))
	}
	return bannet.HookPass, ""
}

// Result 是 Validate 对一条 key/value 的处置结果，与传输层无关。
type Result int

const (
	// ResultPass 放行。
	ResultPass Result = iota
	// ResultDrop 丢弃：调用方不得把该记录继续往下写。
	ResultDrop
)

// Validate 是清洗核心：value 长度限制 + 时间戳单调性校验 + 按 key 前缀分派的
// schema 校验（见 service/ingesthook/schema）+ 字段脱敏——只认 key/value 字节，
// 不解析帧、不触碰连接，bannet.Request 与 gRPC 的 PutRequest 均可直接复用。
//
// 返回 newValue 是（可能经脱敏改写的）value；changed 标出是否真的被改写，未改写
// 时等于输入、调用方可跳过重建负载；code/reason 仅在 result 为 ResultDrop 时有
// 意义。code 是 M1 新增的机读错误码（非 schema 校验失败——oversized/非单调——时
// 恒为 0，调用方应退回自己的默认族码，如 bannet/codec.ErrCodeSchemaValidation；
// 见 schema.CodeOf 的文档）；reason 是给人看的描述，除了 metrics 计数外，BANLV
// 入口（Handle）还会把它原样回传给客户端（见 service/router.go 的
// sendDropped），故这里的字符串是面向调用方展示的，应保持简洁且不泄露不该暴露的
// 内部细节。
func (f *Filter) Validate(key, value []byte) (newValue []byte, changed bool, result Result, code uint16, reason string) {
	return f.validate(key, value, true)
}

// ValidateVersioned 是 Validate 的版本化写入变体（时态内核 M0，
// docs/rfc/时态内核-M0-版本化与as-of.md）：与 Validate 做完全相同的检查
// （value 长度上限/schema 校验/字段脱敏），唯独跳过下面 validate 里的时间戳
// 单调性启发式。
//
// 原因：那条启发式的前提是"同一个 key 短时间内被反复写入即代表时钟异常/
// 重放"（为无类型 IMU 设备流设计），而 PUT_VERSIONED 的正常工作方式恰恰是
// 反复对同一个逻辑键写入新版本——本仓库已经因为对不匹配的 key 形状套用这个
// 启发式吃过一次真实的亏（quote: 误杀事件，见下方 validate 的文档）。
// PUT_VERSIONED 自己有权威的排序机制（temporal.Version.Seq/WriteNanos，由
// 服务端分配），不依赖、也不应该被这条为完全不同场景写的启发式二次把关。
func (f *Filter) ValidateVersioned(key, value []byte) (newValue []byte, changed bool, result Result, code uint16, reason string) {
	return f.validate(key, value, false)
}

// ValidateForType 是 Validate 的 M1 变体：typeID 来自 BANLV v2 帧头的 type 字段
// （bannet/codec.HeaderV2.Type）。typeID==0（未声明类型）与 Validate 完全等价——
// 退回按 key 前缀最长匹配的旧路径，这覆盖了本仓库今天全部的生产写入流量（v1 没有
// type 概念；QuantScout 目前仍在 v1，见方案 §2.3）。typeID!=0 时，声明的类型是
// 权威来源：直接按 schema.LookupByType 查找，不再猜 key 前缀（BANLV-2 RFC §9.3：
// "分派规则从按 key 前缀最长匹配改为按 type 字段直接查表"）；找不到对应契约时
// 结构化拒绝（reason="unknown_declared_type"），不是悄悄退回前缀匹配——客户端
// 声明了一个服务端不认识的类型号本身就是需要暴露的问题，不应该被"猜对了前缀就
// 蒙混过关"掩盖。
func (f *Filter) ValidateForType(typeID uint16, key, value []byte) (newValue []byte, changed bool, result Result, code uint16, reason string) {
	return f.validateForType(typeID, key, value, true)
}

// ValidateVersionedForType 是 ValidateForType 的 PUT_VERSIONED 变体，语义与
// ValidateVersioned 相对 Validate 的差异相同：跳过时间戳单调性启发式。
func (f *Filter) ValidateVersionedForType(typeID uint16, key, value []byte) (newValue []byte, changed bool, result Result, code uint16, reason string) {
	return f.validateForType(typeID, key, value, false)
}

// validateForType 是 ValidateForType/ValidateVersionedForType 共享的实现。
func (f *Filter) validateForType(typeID uint16, key, value []byte, checkMonotonic bool) (newValue []byte, changed bool, result Result, code uint16, reason string) {
	if typeID == 0 {
		return f.validate(key, value, checkMonotonic)
	}
	if f.oversized(value) {
		metrics.FramesDroppedOversized.Add(1)
		return value, false, ResultDrop, 0, "oversized_value"
	}

	descriptor, ok := schema.LookupByType(typeID)
	if !ok {
		// 客户端声明了一个服务端不认识的类型号：这本身就是"类型规则不满足"的
		// 一种（连"这是什么类型"都答不上来，比"这条记录不满足某个已知类型的
		// 规则"更基础），计入同一个 FramesDroppedSchema 桶，不新开一个计数器——
		// router_v2.go 的调用方注释依赖"ValidateForType 内部已经按丢弃原因
		// 各自计数"这条不变量，这里必须兑现，否则这一类拒绝在 headless 部署下
		// （tail 日志是唯一可观测出口）完全不可见。
		metrics.FramesDroppedSchema.Add(1)
		return value, false, ResultDrop, 0, "unknown_declared_type"
	}

	// 声明的类型是权威来源：直接用它的校验器，不再跑基于 key 前缀的单调性
	// 启发式（那是"没有声明类型"场景的兜底，见 validate 的文档）。checkMonotonic
	// 在本分支不生效：M1 未实现 TimeSemantics.Kind != None 的结构化单调性检查
	// （schema.LoadContracts 在加载期已拒绝这类契约，见其文档），故没有可执行的
	// 单调性分支可跑；typeID==0 时上面已提前返回，checkMonotonic 在那条路径上
	// 仍然生效。

	if err := descriptor.Validation.Validate(value); err != nil {
		metrics.FramesDroppedSchema.Add(1)
		return value, false, ResultDrop, schema.CodeOf(err, 0), err.Error()
	}

	if nv, ch := redact(value, f.redactFields); ch {
		return nv, true, ResultPass, 0, ""
	}
	return value, false, ResultPass, 0, ""
}

// oversized 报告 value 是否超过 maxValueLen（<=0 表示不限）。抽出为独立方法，
// 让 validate 与 validateForType 共享同一处判断，不各自维护一份。
func (f *Filter) oversized(value []byte) bool {
	return f.maxValueLen > 0 && len(value) > f.maxValueLen
}

// validate 是 Validate/ValidateVersioned 共享的实现；checkMonotonic 控制是否
// 跑时间戳单调性启发式，两个方法名已经清楚表达各自适用场景，调用方不会看到
// 这个内部参数。
func (f *Filter) validate(key, value []byte, checkMonotonic bool) (newValue []byte, changed bool, result Result, code uint16, reason string) {
	if f.oversized(value) {
		metrics.FramesDroppedOversized.Add(1)
		return value, false, ResultDrop, 0, "oversized_value"
	}

	// 先查 key 是否命中已注册类型的 Descriptor——这个结果同时决定下面两件事：
	// 单调性校验该怎么分派（TimeSemantics.Kind）、是否要跑 schema 校验。
	descriptor, hasDescriptor := schema.LookupDescriptor(key)

	// 时间戳单调性校验（best-effort）：DoMsgHandle 的 work-stealing 在背压下
	// 可能让同一连接的帧落到不同 worker 而乱序，此处只做尽力而为的回退/重放
	// 拦截，不是顺序保证；DropBackward 关闭时仅放行不校验。
	//
	// M1 起，分派依据是 Descriptor.TimeSemantics.Kind（BANLV-2 RFC §9.3），
	// 不再是"有没有注册 schema"这个粗粒度布尔旁路：
	//   - 未注册任何 Descriptor（hasDescriptor=false）：这是为无类型 IMU 设备流
	//     设计的场景，parseKey 假设"设备:时间戳"这种末段为数字的 key 约定，是
	//     它的本来场景，不是误用；
	//   - 已注册 Descriptor 且 Kind==None（如 quote）：明确声明"这个类型不需要
	//     单调性检查"，无条件跳过——行情快照 key（quote:<日期>:<代码>）的末段是
	//     股票代码，同样可能被解析成数字，对它套用 parseKey 启发式会把「代码
	//     大小变化」误判成「时间戳回退」而错误丢弃。这不是假设性风险：
	//     QuantScout 全量实测（热身后按全量顺序跑 5241 行真实行情）复现了 5 行
	//     被误杀，见 docs/iteration-2026-08-20-quantscout-realdata-fixes.md 的
	//     D3 记录；
	//   - 已注册 Descriptor 且 Kind!=None：M1 未实现按 KeyLayout.KeyField 的结构化
	//     单调性检查（schema.LoadContracts 已在加载期拒绝这类契约，这个分支在
	//     当前契约集合下不可达），留空是诚实的"尚未支持"，不是静默放行。
	//
	// checkMonotonic=false（ValidateVersioned）额外跳过：PUT_VERSIONED 反复
	// 写同一逻辑键是设计如此，不是时钟异常，见 ValidateVersioned 的文档。
	if checkMonotonic && f.dropBackward && !hasDescriptor {
		if device, ts, ok := parseKey(key); ok {
			f.mu.Lock()
			last, seen := f.lastTS[device]
			if seen && ts <= last {
				f.mu.Unlock()
				metrics.FramesDroppedNonMonotonic.Add(1)
				return value, false, ResultDrop, 0, "non_monotonic_timestamp"
			}
			f.lastTS[device] = ts
			f.mu.Unlock()
		}
	}

	// 按 key 前缀分派到已注册类型的 schema 校验器；未纳管类型（无前缀匹配）不做
	// schema 校验、直接放行——旧数据类型在新增 schema 前不会被误伤。
	if hasDescriptor {
		if err := descriptor.Validation.Validate(value); err != nil {
			metrics.FramesDroppedSchema.Add(1)
			return value, false, ResultDrop, schema.CodeOf(err, 0), err.Error()
		}
	}

	// 字段脱敏：命中则返回改写后的 value。
	if nv, ch := redact(value, f.redactFields); ch {
		return nv, true, ResultPass, 0, ""
	}
	return value, false, ResultPass, 0, ""
}

// parsePut 解析 PUT 负载 keyLen(u32 LE)+valueLen(u32 LE)+key+value。委托给
// proto.DecodePutFrame——service.Router 的 handlePut 走的是同一个实现，此前
// 两处各自维护过一份相同的二进制解析（见 proto/put_frame.go 的注释）。保留
// 这个包内私有名字只是为了不动本包既有测试/调用点。
func parsePut(data []byte) (key, value []byte, ok bool) {
	return proto.DecodePutFrame(data)
}

// encodePut 按 PUT 负载格式重建一帧，委托给 proto.EncodePutFrame。
func encodePut(key, value []byte) []byte {
	return proto.EncodePutFrame(key, value)
}

// parseKey 从形如 "imu:dev0:<ts>" 的 key 中切出设备标识（末段之前的全部）
// 与数值时间戳。不符合约定的 key 返回 ok=false（跳过单调校验，不丢弃）。
func parseKey(key []byte) (device string, ts int64, ok bool) {
	s := string(key)
	i := strings.LastIndexByte(s, ':')
	if i <= 0 || i == len(s)-1 {
		return "", 0, false
	}
	ts, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return s[:i], ts, true
}

// redact 把 value（JSON 对象）中命中 fields 的字段替换为脱敏占位符；
// 非 JSON 或未命中任何字段时原样返回 changed=false。其余字段保留原始字节。
//
// mayContainAnyField 前置过滤：hasSchema 的记录（如 quote:）此前对同一个
// value 已经在 Validate 里做过一次 json.Unmarshal（校验器自己的类型化解析），
// redact 又独立做第二次——生产配置的 redactFields=["gps","user_id"] 对
// 不含这两个字段的行情记录而言，这第二次解析每条都白付出。mayContainAnyField
// 是安全的必要条件前置检查（细节见其注释），不命中时可以确定性地跳过这次
// Unmarshal，命中时仍回落到精确解析，语义不变。
func redact(value []byte, fields []string) (newValue []byte, changed bool) {
	if len(fields) == 0 {
		return value, false
	}
	if !mayContainAnyField(value, fields) {
		return value, false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(value, &m); err != nil {
		return value, false
	}
	for _, field := range fields {
		if _, present := m[field]; present {
			m[field] = redactedValue
			changed = true
		}
	}
	if !changed {
		return value, false
	}
	out, err := json.Marshal(m)
	if err != nil {
		return value, false
	}
	return out, true
}

// mayContainAnyField 判断 value 是否「可能」包含 fields 中任一字段名作为
// JSON key。一个 key 若以未转义的形式书写，它的带引号字面量（如 `"gps"`）
// 必然原样出现在 value 的字节里；反之，若这个带引号字面量完全不出现，且
// value 里也不含任何反斜杠（即不存在任何转义写法的可能），value 就一定不
// 含该 key——这一半的判断是精确的必要条件，可以安全地跳过后面的
// json.Unmarshal。
//
// 含反斜杠时不下这个结论：JSON 允许用 unicode 转义写字段名（如 "gps"
// 解出来的 key 就是普通字符串 "gps"），此时字面量 `"gps"` 不会原样出现在
// 字节流里，纯 bytes.Contains 会误判为「一定不含」而漏掉本该脱敏的字段——
// 遇到反斜杠一律回落到精确解析，不冒这个险。
//
// 命中带引号字面量也不代表一定存在该 key：字面量也可能出现在某个字符串值
// 内部（如 `{"note":"see gps log"}` 命中 `"gps"` 但字段名其实是 note），
// 命中时仍会回落到 redact 里精确的 map 查找，只是不再无条件解析——它把
// 「一定没有任何目标字段」这个常见情况从一次完整 json.Unmarshal 降到几次
// bytes.Contains，「可能有」的少数情况才继续走精确解析，整体行为（changed
// 的取值）不变。
func mayContainAnyField(value []byte, fields []string) bool {
	if bytes.IndexByte(value, '\\') >= 0 {
		return true
	}
	for _, f := range fields {
		if bytes.Contains(value, []byte(`"`+f+`"`)) {
			return true
		}
	}
	return false
}
