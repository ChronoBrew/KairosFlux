package ingesthook

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/proto"
)

// fakeReq 是 bannet.Request 的测试替身。钩子不触碰连接，Conn 返回 nil。
type fakeReq struct {
	msgID string
	data  []byte
}

func (f *fakeReq) Conn() bannet.Conn   { return nil }
func (f *fakeReq) MsgData() []byte     { return f.data }
func (f *fakeReq) MsgID() string       { return f.msgID }
func (f *fakeReq) SetMsgData(d []byte) { f.data = d }

func putReq(key, value string) *fakeReq {
	return &fakeReq{msgID: proto.MsgPut, data: encodePut([]byte(key), []byte(value))}
}

// GET/DELETE 必须原样放行——它们的负载没有 valueLen 字段，误判会丢掉合法读。
func TestHandle_NonPutPassesThrough(t *testing.T) {
	f := NewFilter([]string{"gps"}, 0, true)
	// GET 负载是 keyLen+key，前 8 字节是 key 的一部分，绝不能被当 PUT 解析。
	req := &fakeReq{msgID: proto.MsgGet, data: []byte("anything")}
	if got, _ := f.Handle(req); got != bannet.HookPass {
		t.Fatalf("GET 应放行，得到 %v", got)
	}
}

func TestHandle_MalformedDropped(t *testing.T) {
	f := NewFilter(nil, 0, false)
	req := &fakeReq{msgID: proto.MsgPut, data: []byte{1, 2, 3}} // 不足 8 字节
	got, reason := f.Handle(req)
	if got != bannet.HookDrop {
		t.Fatalf("畸形帧应丢弃，得到 %v", got)
	}
	if reason != "malformed_frame" {
		t.Fatalf("畸形帧的丢弃原因应为 malformed_frame，得到 %q", reason)
	}
}

func TestHandle_OversizedDropped(t *testing.T) {
	f := NewFilter(nil, 4, false)
	got, reason := f.Handle(putReq("imu:dev0:1", "toolong"))
	if got != bannet.HookDrop {
		t.Fatalf("超长 value 应丢弃，得到 %v", got)
	}
	if reason != "oversized_value" {
		t.Fatalf("超长 value 的丢弃原因应为 oversized_value，得到 %q", reason)
	}
	if got, _ := f.Handle(putReq("imu:dev0:2", "ok")); got != bannet.HookPass {
		t.Fatalf("正常 value 应放行，得到 %v", got)
	}
}

func TestHandle_MonotonicDrop(t *testing.T) {
	f := NewFilter(nil, 0, true)
	if got, _ := f.Handle(putReq("imu:dev0:100", "{}")); got != bannet.HookPass {
		t.Fatalf("首帧应放行，得到 %v", got)
	}
	got, reason := f.Handle(putReq("imu:dev0:99", "{}"))
	if got != bannet.HookDrop {
		t.Fatalf("回退帧应丢弃，得到 %v", got)
	}
	if reason != "non_monotonic_timestamp" {
		t.Fatalf("回退帧的丢弃原因应为 non_monotonic_timestamp，得到 %q", reason)
	}
	if got, _ := f.Handle(putReq("imu:dev0:100", "{}")); got != bannet.HookDrop {
		t.Fatalf("重放（相等）帧应丢弃，得到 %v", got)
	}
	if got, _ := f.Handle(putReq("imu:dev0:101", "{}")); got != bannet.HookPass {
		t.Fatalf("前进帧应放行，得到 %v", got)
	}
	// 不同设备各自独立计水位。
	if got, _ := f.Handle(putReq("imu:dev1:1", "{}")); got != bannet.HookPass {
		t.Fatalf("另一设备首帧应放行，得到 %v", got)
	}
}

func TestHandle_MonotonicDisabled(t *testing.T) {
	f := NewFilter(nil, 0, false)
	f.Handle(putReq("imu:dev0:100", "{}"))
	if got, _ := f.Handle(putReq("imu:dev0:99", "{}")); got != bannet.HookPass {
		t.Fatalf("关闭单调校验后回退帧应放行，得到 %v", got)
	}
}

// 不符合 imu:dev:ts 约定的 key 不参与单调校验，应放行。
func TestHandle_UnconventionalKeyPasses(t *testing.T) {
	f := NewFilter(nil, 0, true)
	if got, _ := f.Handle(putReq("plainkey", "{}")); got != bannet.HookPass {
		t.Fatalf("无约定 key 应放行，得到 %v", got)
	}
}

func TestHandle_RedactRewritesPayload(t *testing.T) {
	f := NewFilter([]string{"gps", "user_id"}, 0, false)
	req := putReq("imu:dev0:1", `{"ax":0.01,"gps":"39.9,116.4","user_id":"u123"}`)
	if got, _ := f.Handle(req); got != bannet.HookPass {
		t.Fatalf("脱敏帧应放行，得到 %v", got)
	}

	// 钩子改写后，整帧必须可按 PUT 格式重新解析（valueLen 前缀已重建）。
	key, value, ok := parsePut(req.MsgData())
	if !ok {
		t.Fatal("改写后的帧无法解析，valueLen 前缀可能未重建")
	}
	if !bytes.Equal(key, []byte("imu:dev0:1")) {
		t.Fatalf("key 不应被改动，得到 %q", key)
	}

	var m map[string]any
	if err := json.Unmarshal(value, &m); err != nil {
		t.Fatalf("改写后的 value 不是合法 JSON: %v", err)
	}
	if m["gps"] != "[REDACTED]" || m["user_id"] != "[REDACTED]" {
		t.Fatalf("敏感字段未脱敏: %v", m)
	}
	if m["ax"] != 0.01 {
		t.Fatalf("非敏感字段应保留: %v", m)
	}
}

// 非 JSON 的 value 配置了脱敏字段时应原样放行，不丢弃、不改写。
func TestHandle_NonJSONValueUnchanged(t *testing.T) {
	f := NewFilter([]string{"gps"}, 0, false)
	req := putReq("imu:dev0:1", "rawbinaryblob")
	if got, _ := f.Handle(req); got != bannet.HookPass {
		t.Fatalf("非 JSON value 应放行，得到 %v", got)
	}
	_, value, _ := parsePut(req.MsgData())
	if !bytes.Equal(value, []byte("rawbinaryblob")) {
		t.Fatalf("非 JSON value 不应被改动，得到 %q", value)
	}
}

// TestHandle_SchemaRegisteredKeysSkipMonotonicCheck 复现 QuantScout 全量实测
// 暴露的真实问题：quote: 前缀已注册 schema，key 末段是股票代码而非时间戳；
// 乱序写入不同代码时（600000 之后写 000001），parseKey 会把 "000001" 解析成
// 比 "600000" 小的数值时间戳，若对已注册 schema 的类型仍套用单调性启发式，
// 会被误判为「时间戳回退」丢弃——即便 dropBackward=true，命中 schema 的 key
// 也必须跳过这项检查（见 Filter.Validate 的注释）。
func TestHandle_SchemaRegisteredKeysSkipMonotonicCheck(t *testing.T) {
	f := NewFilter(nil, 0, true) // dropBackward=true

	quote := func(code string) string {
		return `{"code":"` + code + `","date":"2026-08-17","open":10.0,"high":10.5,"low":9.8,"close":10.2,"volume":1000000}`
	}

	if got, reason := f.Handle(putReq("quote:2026-08-17:600000", quote("600000"))); got != bannet.HookPass {
		t.Fatalf("首条合法行情应放行，得到 %v，reason=%q", got, reason)
	}
	// 000001 < 600000：若单调性检查未对 schema 类型跳过，这里会被误杀。
	if got, reason := f.Handle(putReq("quote:2026-08-17:000001", quote("000001"))); got != bannet.HookPass {
		t.Fatalf("已注册 schema 的 key 应跳过单调性检查，不应被误判回退丢弃；得到 %v，reason=%q", got, reason)
	}
	// 与代码顺序无关：再写回一个"更大"代码也应正常放行，证明水位表压根没有
	// 被这类 key 写入过（hasSchema 分支从未进入过 parseKey/lastTS）。
	if got, reason := f.Handle(putReq("quote:2026-08-17:300069", quote("300069"))); got != bannet.HookPass {
		t.Fatalf("第三条合法行情应放行，得到 %v，reason=%q", got, reason)
	}
}

// TestHandle_SchemaDropCarriesReason 验证 quote: 前缀命中 schema 校验失败时，
// Handle 返回的 reason 是 schema 校验器给出的具体错误信息（非空、非通用占位），
// 这正是本轮要修的缺口：此前 reason 在 Handle 里被直接丢弃，客户端只知道
// dropped、不知道具体原因。
func TestHandle_SchemaDropCarriesReason(t *testing.T) {
	f := NewFilter(nil, 0, false)
	req := putReq("quote:2026-08-17:600000", `{"open":-1}`) // 非正价格 + 缺必填字段
	got, reason := f.Handle(req)
	if got != bannet.HookDrop {
		t.Fatalf("非法行情记录应丢弃，得到 %v", got)
	}
	if reason == "" {
		t.Fatal("schema 校验失败的丢弃应带上具体 reason，不应为空")
	}
	if !bytes.Contains([]byte(reason), []byte("quote")) {
		t.Fatalf("reason 应来自 quote schema 校验器，得到: %q", reason)
	}
}
