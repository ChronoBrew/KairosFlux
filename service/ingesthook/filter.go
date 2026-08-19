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

// Handle 实现 PreHandle 钩子签名。返回 HookDrop 表示丢弃本帧。
//
// 只做 bannet 特有的一步：从裸帧里解出 key/value（畸形帧在这一步就地丢弃，
// 它是「帧是否完整」的传输层问题，不是 Validate 管的「内容是否合法」问题）。
// 解出 key/value 之后的清洗全部委托给 Validate，保证与 gRPC 入口走同一套规则。
func (f *Filter) Handle(req bannet.Request) bannet.HookAction {
	// 钩子只针对写入帧：GET/DELETE 的负载格式不同，放行不动。
	if req.MsgID() != proto.MsgPut {
		return bannet.HookPass
	}

	key, value, ok := parsePut(req.MsgData())
	if !ok {
		metrics.FramesDroppedMalformed.Add(1)
		return bannet.HookDrop // 畸形帧：长度字段与实际数据不符
	}

	newValue, changed, result, _ := f.Validate(key, value)
	if result == ResultDrop {
		return bannet.HookDrop
	}
	if changed {
		// 字段脱敏改写了 value：重建整帧（含新的 valueLen 前缀）。
		req.SetMsgData(encodePut(key, newValue))
	}
	return bannet.HookPass
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
// 供调用方日志/观测使用（当前实现已通过 metrics 计数，reason 是给上层按需扩展用的）。
func (f *Filter) Validate(key, value []byte) (newValue []byte, changed bool, result Result, reason string) {
	if f.maxValueLen > 0 && len(value) > f.maxValueLen {
		metrics.FramesDroppedOversized.Add(1)
		return value, false, ResultDrop, "oversized_value"
	}

	// 时间戳单调性校验（best-effort）：DoMsgHandle 的 work-stealing 在背压下
	// 可能让同一连接的帧落到不同 worker 而乱序，此处只做尽力而为的回退/重放
	// 拦截，不是顺序保证；DropBackward 关闭时仅放行不校验。
	//
	// 注意：parseKey 假设 "设备:时间戳" 这种末段为数字的 key 约定，是为 IMU 场景
	// 写的启发式。行情快照 key（quote:<日期>:<代码>）的末段是股票代码，恰好也可能
	// 解析成数字——若对行情数据开启 dropBackward，会把「代码」误当「时间戳」校验
	// 单调性，产生错误丢弃。因此挂载行情快照入口的 Filter 必须传 dropBackward=false
	// （见 internal/kvgrpc 的构造处），这是已知的启发式局限，未来按数据类型分派
	// 单调校验规则时应一并解决。
	if f.dropBackward {
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
	if v, ok := schema.Lookup(key); ok {
		if err := v.Validate(value); err != nil {
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
