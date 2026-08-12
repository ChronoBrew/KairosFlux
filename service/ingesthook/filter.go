// Package ingesthook 提供一个挂在采集入口的真实 PreHandle 过滤钩子示例：
// 在数据落盘前完成「丢弃畸形帧 + 时间戳单调性校验 + 字段脱敏」三件事，
// 把「可编程边缘采集缓冲网关」从挂载点变成有内容的演示。
//
// 钩子只读取并改写请求负载，绝不向连接写响应——「丢弃即回写唯一响应」的
// 不变式由 service.Router.PreHandle 统一持有（见 service/router.go）。
package ingesthook

import (
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/pkg/metrics"
	"github.com/NeverENG/BanDB/pkg/proto"
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
func (f *Filter) Handle(req bannet.IRequest) bannet.HookAction {
	// 钩子只针对写入帧：GET/DELETE 的负载格式不同，放行不动。
	if req.GetMsgID() != proto.MsgPut {
		return bannet.HookPass
	}

	key, value, ok := parsePut(req.GetMsgData())
	if !ok {
		metrics.FramesDroppedMalformed.Add(1)
		return bannet.HookDrop // 畸形帧：长度字段与实际数据不符
	}

	if f.maxValueLen > 0 && len(value) > f.maxValueLen {
		metrics.FramesDroppedOversized.Add(1)
		return bannet.HookDrop // 畸形帧：value 超过上限
	}

	// 时间戳单调性校验（best-effort）：DoMsgHandle 的 work-stealing 在背压下
	// 可能让同一连接的帧落到不同 worker 而乱序，此处只做尽力而为的回退/重放
	// 拦截，不是顺序保证；DropBackward 关闭时仅放行不校验。
	if f.dropBackward {
		if device, ts, ok := parseKey(key); ok {
			f.mu.Lock()
			last, seen := f.lastTS[device]
			if seen && ts <= last {
				f.mu.Unlock()
				metrics.FramesDroppedNonMonotonic.Add(1)
				return bannet.HookDrop // 回退/重放帧
			}
			f.lastTS[device] = ts
			f.mu.Unlock()
		}
	}

	// 字段脱敏：命中则改写 value 并重建整帧（含新的 valueLen 前缀）。
	if newValue, changed := redact(value, f.redactFields); changed {
		req.SetMsgData(encodePut(key, newValue))
	}

	return bannet.HookPass
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
