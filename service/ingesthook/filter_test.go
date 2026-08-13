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
	if got := f.Handle(req); got != bannet.HookPass {
		t.Fatalf("GET 应放行，得到 %v", got)
	}
}

func TestHandle_MalformedDropped(t *testing.T) {
	f := NewFilter(nil, 0, false)
	req := &fakeReq{msgID: proto.MsgPut, data: []byte{1, 2, 3}} // 不足 8 字节
	if got := f.Handle(req); got != bannet.HookDrop {
		t.Fatalf("畸形帧应丢弃，得到 %v", got)
	}
}

func TestHandle_OversizedDropped(t *testing.T) {
	f := NewFilter(nil, 4, false)
	if got := f.Handle(putReq("imu:dev0:1", "toolong")); got != bannet.HookDrop {
		t.Fatalf("超长 value 应丢弃，得到 %v", got)
	}
	if got := f.Handle(putReq("imu:dev0:2", "ok")); got != bannet.HookPass {
		t.Fatalf("正常 value 应放行，得到 %v", got)
	}
}

func TestHandle_MonotonicDrop(t *testing.T) {
	f := NewFilter(nil, 0, true)
	if got := f.Handle(putReq("imu:dev0:100", "{}")); got != bannet.HookPass {
		t.Fatalf("首帧应放行，得到 %v", got)
	}
	if got := f.Handle(putReq("imu:dev0:99", "{}")); got != bannet.HookDrop {
		t.Fatalf("回退帧应丢弃，得到 %v", got)
	}
	if got := f.Handle(putReq("imu:dev0:100", "{}")); got != bannet.HookDrop {
		t.Fatalf("重放（相等）帧应丢弃，得到 %v", got)
	}
	if got := f.Handle(putReq("imu:dev0:101", "{}")); got != bannet.HookPass {
		t.Fatalf("前进帧应放行，得到 %v", got)
	}
	// 不同设备各自独立计水位。
	if got := f.Handle(putReq("imu:dev1:1", "{}")); got != bannet.HookPass {
		t.Fatalf("另一设备首帧应放行，得到 %v", got)
	}
}

func TestHandle_MonotonicDisabled(t *testing.T) {
	f := NewFilter(nil, 0, false)
	f.Handle(putReq("imu:dev0:100", "{}"))
	if got := f.Handle(putReq("imu:dev0:99", "{}")); got != bannet.HookPass {
		t.Fatalf("关闭单调校验后回退帧应放行，得到 %v", got)
	}
}

// 不符合 imu:dev:ts 约定的 key 不参与单调校验，应放行。
func TestHandle_UnconventionalKeyPasses(t *testing.T) {
	f := NewFilter(nil, 0, true)
	if got := f.Handle(putReq("plainkey", "{}")); got != bannet.HookPass {
		t.Fatalf("无约定 key 应放行，得到 %v", got)
	}
}

func TestHandle_RedactRewritesPayload(t *testing.T) {
	f := NewFilter([]string{"gps", "user_id"}, 0, false)
	req := putReq("imu:dev0:1", `{"ax":0.01,"gps":"39.9,116.4","user_id":"u123"}`)
	if got := f.Handle(req); got != bannet.HookPass {
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
	if got := f.Handle(req); got != bannet.HookPass {
		t.Fatalf("非 JSON value 应放行，得到 %v", got)
	}
	_, value, _ := parsePut(req.MsgData())
	if !bytes.Equal(value, []byte("rawbinaryblob")) {
		t.Fatalf("非 JSON value 不应被改动，得到 %q", value)
	}
}
