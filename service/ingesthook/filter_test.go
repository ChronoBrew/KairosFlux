package ingesthook

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/proto"
	"github.com/NeverENG/BanDB/service/ingesthook/schema"
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

// TestValidateVersioned_SkipsMonotonicCheckForRepeatedKey 是时态内核 M0 引入
// ValidateVersioned 的直接动因：PUT_VERSIONED 反复对同一逻辑键写入是设计如此
// （不是时钟异常），Validate 的单调性启发式会误杀第二次写——这在联调服务端
// 真实跑起来时立刻复现过（不是纸面推演）。
func TestValidateVersioned_SkipsMonotonicCheckForRepeatedKey(t *testing.T) {
	f := NewFilter(nil, 0, true)
	key := []byte("reading:2026-08-17:600000")

	if _, _, result, _, _ := f.ValidateVersioned(key, []byte("v1")); result != ResultPass {
		t.Fatalf("第一次版本化写入应放行, got %v", result)
	}
	// 同一个 key 再写两次——若走 Validate 第二次起会被判"非单调"丢弃（见
	// TestHandle_MonotonicDrop），ValidateVersioned 必须每次都放行，这正是它
	// 存在的理由。
	for i, val := range []string{"v2", "v3", "v4"} {
		if _, _, result, _, reason := f.ValidateVersioned(key, []byte(val)); result != ResultPass {
			t.Fatalf("第 %d 次版本化写入到同一逻辑键应放行, got %v reason=%q", i+2, result, reason)
		}
	}

	// ValidateVersioned 完全不touch lastTS 水位（不只是跳过判定，也不记录）：
	// 换一条从未经过 ValidateVersioned 的普通 key，Validate 的单调性判定不受
	// 上面这些版本化写入的干扰，行为与 TestHandle_MonotonicDrop 完全一致。
	if _, _, result, _, _ := f.Validate([]byte("imu:dev0:100"), []byte("{}")); result != ResultPass {
		t.Fatal("独立 key 首次 Validate 应放行")
	}
	if _, _, result, _, reason := f.Validate([]byte("imu:dev0:99"), []byte("{}")); result != ResultDrop || reason != "non_monotonic_timestamp" {
		t.Fatalf("Validate 自身的单调性判定应不受 ValidateVersioned 调用影响: result=%v reason=%q", result, reason)
	}
}

// TestValidateVersioned_StillEnforcesSchemaAndLength 验证 ValidateVersioned
// 只跳过单调性检查这一项，value 长度上限与 schema 校验与 Validate 完全一致
// （不是"版本化写入绕过一切校验"）。
func TestValidateVersioned_StillEnforcesSchemaAndLength(t *testing.T) {
	f := NewFilter(nil, 3, true) // maxValueLen=3
	if _, _, result, _, reason := f.ValidateVersioned([]byte("k"), []byte("toolong")); result != ResultDrop || reason != "oversized_value" {
		t.Fatalf("超长 value 应仍被拒绝, got result=%v reason=%q", result, reason)
	}
}

// TestValidateForType_UnknownDeclaredTypeIsRejectedNotFallenBackToPrefix 是 M1
// "类型号与契约一一映射"这条验收标准的唯一直接测试：typeID=4242 从未被任何
// Descriptor 注册过，但 key 前缀 "quote:" 本来会命中 quote 的 Descriptor——
// 这个组合是刻意选的，用来证明声明了不存在的类型号会被结构化拒绝
// （reason="unknown_declared_type"），而不是悄悄退回按 key 前缀猜测（那样会让
// "类型号"这个字段变得可有可无，白白声明错了也蒙混过关）。没有这条测试，未来
// 有人把 validateForType 里的 `if !ok { ... }` 反过来写，不会有任何测试失败。
func TestValidateForType_UnknownDeclaredTypeIsRejectedNotFallenBackToPrefix(t *testing.T) {
	f := NewFilter(nil, 0, false)
	validQuote := []byte(`{"code":"600000","date":"2026-08-20","open":10.0,"high":10.5,"low":9.8,"close":10.2,"volume":1000000}`)

	const unknownTypeID = 4242
	_, _, result, code, reason := f.ValidateForType(unknownTypeID, []byte("quote:2026-08-20:600000"), validQuote)
	if result != ResultDrop {
		t.Fatalf("未注册的 TypeID 应被拒绝，得到 result=%v", result)
	}
	if reason != "unknown_declared_type" {
		t.Fatalf(`reason=%q，期望 "unknown_declared_type"（不应退回前缀匹配后报出 quote 校验器自己的错误）`, reason)
	}
	if code != 0 {
		t.Fatalf("code=%#x，期望 0（未分配子码，调用方应退回自己的默认族码）", code)
	}

	// 对照组：同一条合法记录，typeID=0（未声明）时必须走 key 前缀路径正常放行——
	// 证明上面的拒绝确实来自"类型号未知"，不是这条记录本身有问题。
	if _, _, result, _, reason := f.ValidateForType(0, []byte("quote:2026-08-20:600000"), validQuote); result != ResultPass {
		t.Fatalf("typeID=0 时同一条合法记录应按前缀匹配放行，得到 result=%v reason=%q", result, reason)
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

// alwaysPassValidator 是测试专用的 schema.Validator：无条件通过，只用于让某个
// key 前缀"命中 schema"而不引入真实校验规则的干扰。
type alwaysPassValidator struct{}

func (alwaysPassValidator) Validate(value []byte) error { return nil }

// TestHandle_SchemaAndRedactBothApplyToSameRecord 锁定 redact 的
// mayContainAnyField 前置过滤（合并/减少双重反序列化的优化点）不会误伤
// "hasSchema 且确实命中脱敏字段"这一组合场景：schema 校验先跑（且必须通过），
// 脱敏改写也必须照常发生——二者互不影响、顺序不变。此前没有测试同时覆盖
// 这两条路径都被触发的情形。
func TestHandle_SchemaAndRedactBothApplyToSameRecord(t *testing.T) {
	schema.Register("hasschema:", alwaysPassValidator{})
	t.Cleanup(func() { schema.Unregister("hasschema:") })

	f := NewFilter([]string{"gps"}, 0, false)
	req := putReq("hasschema:1", `{"ax":0.01,"gps":"39.9,116.4"}`)

	got, reason := f.Handle(req)
	if got != bannet.HookPass {
		t.Fatalf("已注册 schema 且校验通过的记录不应被丢弃，得到 %v，reason=%q", got, reason)
	}

	key, value, ok := parsePut(req.MsgData())
	if !ok {
		t.Fatal("改写后的帧无法解析")
	}
	if !bytes.Equal(key, []byte("hasschema:1")) {
		t.Fatalf("key 不应被改动，得到 %q", key)
	}
	var m map[string]any
	if err := json.Unmarshal(value, &m); err != nil {
		t.Fatalf("改写后的 value 不是合法 JSON: %v", err)
	}
	if m["gps"] != "[REDACTED]" {
		t.Fatalf("命中 schema 的记录仍应正常脱敏，得到: %v", m)
	}
	if m["ax"] != 0.01 {
		t.Fatalf("非敏感字段应保留: %v", m)
	}
}

// TestHandle_SchemaNonJSONValueReasonComesFromValidator 锁定：hasSchema 的
// key 若 value 根本不是合法 JSON，丢弃 reason 必须来自 schema 校验器自身的
// "invalid json" 错误（而不是某个脱敏侧/通用解析产生的另一种措辞）——
// redact 的 mayContainAnyField 优化只影响 redact 是否解析，不改变 Validate
// 里 schema 校验先行、且用校验器自己的错误信息这条既有顺序/契约。
func TestHandle_SchemaNonJSONValueReasonComesFromValidator(t *testing.T) {
	f := NewFilter([]string{"gps"}, 0, false)
	req := putReq("quote:2026-08-17:600000", "not json at all")
	got, reason := f.Handle(req)
	if got != bannet.HookDrop {
		t.Fatalf("非 JSON 的行情记录应被 schema 校验拒绝，得到 %v", got)
	}
	if !bytes.Contains([]byte(reason), []byte("quote: invalid json")) {
		t.Fatalf("reason 应来自 quote 校验器的 invalid json 错误，得到: %q", reason)
	}
}

// TestHandle_RedactCatchesEscapedFieldName 锁定 redact 的 mayContainAnyField
// 前置过滤（合并/减少双重反序列化的优化点）不会漏判 JSON 转义写法的字段名：
// `"gps"` 是 "gps" 的合法转义形式，json.Unmarshal 解出来的 key 就是
// "gps"，但它的带引号字面量不会以 `"gps"` 原样出现在字节流里。若前置过滤
// 只做字面量 bytes.Contains，会把这类记录误判为"一定不含目标字段"而跳过
// 脱敏，造成 PII 从网络输入方一路不脱敏地流入下游——这正是"保留 redact
// 现有语义"要守住的边界。
func TestHandle_RedactCatchesEscapedFieldName(t *testing.T) {
	f := NewFilter([]string{"gps"}, 0, false)
	// 用 unicode 转义写字段名首字符（g = 'g'）：json.Unmarshal 解出来的
	// key 是普通字符串 "gps"，但字节流里不含 `"gps"` 这个带引号字面量——这正是
	// mayContainAnyField 的字面量匹配会漏判的情形。
	req := putReq("imu:dev0:1", `{"\u0067ps":"39.9,116.4","ax":0.01}`)
	got, _ := f.Handle(req)
	if got != bannet.HookPass {
		t.Fatalf("合法 JSON 应放行，得到 %v", got)
	}
	_, value, ok := parsePut(req.MsgData())
	if !ok {
		t.Fatal("改写后的帧无法解析")
	}
	var m map[string]any
	if err := json.Unmarshal(value, &m); err != nil {
		t.Fatalf("改写后的 value 不是合法 JSON: %v", err)
	}
	if m["gps"] != "[REDACTED]" {
		t.Fatalf("转义写法的 gps 字段也应被脱敏，得到: %v", m)
	}
}
