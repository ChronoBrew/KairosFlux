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
	"encoding/binary"
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

	newValue, changed, result, reason := f.Validate(key, value)
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
// 时等于输入、调用方可跳过重建负载；reason 仅在 result 为 ResultDrop 时非空，
// 除了 metrics 计数外，BANLV 入口（Handle）还会把它原样回传给客户端（见
// service/router.go 的 sendDropped），故这里的字符串是面向调用方展示的，应
// 保持简洁且不泄露不该暴露的内部细节。
func (f *Filter) Validate(key, value []byte) (newValue []byte, changed bool, result Result, reason string) {
	if f.maxValueLen > 0 && len(value) > f.maxValueLen {
		metrics.FramesDroppedOversized.Add(1)
		return value, false, ResultDrop, "oversized_value"
	}

	// 先查 key 是否命中已注册的 schema 类型——这个结果同时决定下面两件事：
	// 是否跳过单调性校验、是否要跑 schema 校验。
	validator, hasSchema := schema.Lookup(key)

	// 时间戳单调性校验（best-effort）：DoMsgHandle 的 work-stealing 在背压下
	// 可能让同一连接的帧落到不同 worker 而乱序，此处只做尽力而为的回退/重放
	// 拦截，不是顺序保证；DropBackward 关闭时仅放行不校验。
	//
	// hasSchema 时无条件跳过（不看 f.dropBackward）：parseKey 假设 "设备:时间戳"
	// 这种末段为数字的 key 约定，是为无类型的 IMU 场景写的启发式；已注册 schema
	// 的类型有自己明确的 key 语义——行情快照 key（quote:<日期>:<代码>）的末段是
	// 股票代码，同样可能被解析成数字，对它套用这个启发式会把「代码大小变化」
	// 误判成「时间戳回退」而错误丢弃。这不是假设性风险：QuantScout 全量实测
	// （热身后按全量顺序跑 5241 行真实行情）复现了 5 行被误杀，见
	// docs/iteration-2026-08-20-quantscout-realdata-fixes.md 的 D3 记录。
	// schema 校验器自己如果需要单调性保证，应在校验规则里显式实现，不应依赖
	// 这个为另一种数据形状设计的通用启发式。
	if f.dropBackward && !hasSchema {
		if device, ts, ok := parseKey(key); ok {
			f.mu.Lock()
			last, seen := f.lastTS[device]
			if seen && ts <= last {
				f.mu.Unlock()
				metrics.FramesDroppedNonMonotonic.Add(1)
				return value, false, ResultDrop, "non_monotonic_timestamp"
			}
			f.lastTS[device] = ts
			f.mu.Unlock()
		}
	}

	// 按 key 前缀分派到已注册类型的 schema 校验器；未纳管类型（无前缀匹配）不做
	// schema 校验、直接放行——旧数据类型在新增 schema 前不会被误伤。
	if hasSchema {
		if err := validator.Validate(value); err != nil {
			metrics.FramesDroppedSchema.Add(1)
			return value, false, ResultDrop, err.Error()
		}
	}

	// 字段脱敏：命中则返回改写后的 value。
	if nv, ch := redact(value, f.redactFields); ch {
		return nv, true, ResultPass, ""
	}
	return value, false, ResultPass, ""
}

// parsePut 解析 PUT 负载 keyLen(u32 LE)+valueLen(u32 LE)+key+value。
func parsePut(data []byte) (key, value []byte, ok bool) {
	if len(data) < 8 {
		return nil, nil, false
	}
	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))
	valueLen := int(binary.LittleEndian.Uint32(data[4:8]))
	if keyLen < 0 || valueLen < 0 || 8+keyLen+valueLen > len(data) {
		return nil, nil, false
	}
	key = data[8 : 8+keyLen]
	value = data[8+keyLen : 8+keyLen+valueLen]
	return key, value, true
}

// encodePut 按 PUT 负载格式重建一帧。
func encodePut(key, value []byte) []byte {
	buf := make([]byte, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(value)))
	copy(buf[8:], key)
	copy(buf[8+len(key):], value)
	return buf
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
func redact(value []byte, fields []string) (newValue []byte, changed bool) {
	if len(fields) == 0 {
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
