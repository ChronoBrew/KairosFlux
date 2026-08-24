package service

// 端到端验证时态内核 M0 新增的四个 v2 opcode（PUT_VERSIONED/GET_AS_OF/
// LIST_VERSIONS/REPLAY_FINGERPRINT，docs/rfc/时态内核-M0-版本化与as-of.md），
// 复用 router_v2_integration_test.go 的最小 v2 测试客户端（dialV2/send/recv）
// 与真实服务端（startRouterV2TestServer）——不新起一套测试基建。

import (
	"context"
	"testing"

	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/bannet/negotiate"
	"github.com/NeverENG/BanDB/client"
	"github.com/NeverENG/BanDB/predicate"
	"github.com/NeverENG/BanDB/proto"
)

func (c *v2Client) putVersioned(corrID uint32, key, value string) *codec.MessageV2 {
	c.send(codec.OpcodePutVersioned, codec.TypeUnspecified, corrID, proto.EncodePutFrame([]byte(key), []byte(value)))
	return c.recv()
}

func (c *v2Client) getAsOf(corrID uint32, key string, asOfNanos int64) *codec.MessageV2 {
	c.send(codec.OpcodeGetAsOf, codec.TypeUnspecified, corrID, proto.EncodeAsOfFrame([]byte(key), asOfNanos))
	return c.recv()
}

func (c *v2Client) listVersions(corrID uint32, key string) *codec.MessageV2 {
	c.send(codec.OpcodeListVersions, codec.TypeUnspecified, corrID, proto.EncodeKeyOnlyFrame([]byte(key)))
	return c.recv()
}

func (c *v2Client) replayFingerprint(corrID uint32, prefix string) *codec.MessageV2 {
	c.send(codec.OpcodeReplayFingerprint, codec.TypeUnspecified, corrID, proto.EncodeKeyOnlyFrame([]byte(prefix)))
	return c.recv()
}

func TestRouterV2_PutVersionedThenGetAsOfAndListVersions(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	logical := "reading:2026-08-17:600000"

	// 三次版本化写入，seq 应从 1 起严格递增。
	for i, val := range []string{"v1", "v2", "v3"} {
		msg := c.putVersioned(uint32(i+1), logical, val)
		if msg.Header.Opcode != codec.OpcodeOK {
			t.Fatalf("PUT_VERSIONED 第 %d 次应成功, opcode=%#x", i+1, msg.Header.Opcode)
		}
		if len(msg.Payload) != 8 {
			t.Fatalf("PUT_VERSIONED OK 负载应是 8 字节 seq, got %d 字节", len(msg.Payload))
		}
	}

	// LIST_VERSIONS 应看到全部 3 条，按 seq 升序。
	listMsg := c.listVersions(10, logical)
	if listMsg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("LIST_VERSIONS 应成功, opcode=%#x", listMsg.Header.Opcode)
	}
	versions, ok := proto.DecodeListVersionsResponse(listMsg.Payload)
	if !ok || len(versions) != 3 {
		t.Fatalf("应解出 3 条版本: versions=%+v ok=%v", versions, ok)
	}
	for i, want := range []string{"v1", "v2", "v3"} {
		if versions[i].Seq != uint64(i+1) || string(versions[i].Payload) != want {
			t.Fatalf("第 %d 条不符: %+v", i, versions[i])
		}
	}

	// GET_AS_OF：早于所有写入 -> not found；介于第 1/2 条写入时刻之间的 as_of
	// 应命中"当时可见"的那一条（写入时刻由服务端 time.Now() 赋，此处只能验证
	// "早于全部" not found 与"晚于全部"命中最新版本这两条与时钟无关的断言）。
	tooEarly := c.getAsOf(20, logical, 1) // unix nanos = 1，早于任何真实写入时刻
	if tooEarly.Header.Opcode != codec.OpcodeErr {
		t.Fatalf("as_of 早于全部写入应 ERR(notfound), opcode=%#x", tooEarly.Header.Opcode)
	}
	if code, reason, ok := proto.DecodeV2ErrPayload(tooEarly.Payload); !ok || reason != "notfound" {
		t.Fatalf("ERR 原因应为 notfound: code=%d reason=%q ok=%v", code, reason, ok)
	}

	future := c.getAsOf(21, logical, 1<<62) // 远未来时刻，必然 >= 所有真实写入
	if future.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("as_of 晚于全部写入应命中最新版本, opcode=%#x", future.Header.Opcode)
	}
	seq, _, payload, _, ok := proto.DecodeVersionEntry(future.Payload)
	if !ok || seq != 3 || string(payload) != "v3" {
		t.Fatalf("应得最新版本 seq=3/v3: seq=%d payload=%q ok=%v", seq, payload, ok)
	}
}

func TestRouterV2_GetTransparentlyResolvesVersionedKeyOverWire(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	logical := "reading:2026-08-17:600000"
	if msg := c.putVersioned(1, logical, "v1"); msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("PUT_VERSIONED 应成功, opcode=%#x", msg.Header.Opcode)
	}
	if msg := c.putVersioned(2, logical, "v2"); msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("PUT_VERSIONED 应成功, opcode=%#x", msg.Header.Opcode)
	}

	// 普通 v2 GET（与 v1 GET 共享同一个 KVServer.Get 实现）应透明看到最新版本，
	// 无需知道这个 key 是被版本化写入的。
	getMsg := c.get(3, logical)
	if getMsg.Header.Opcode != codec.OpcodeOK || string(getMsg.Payload) != "v2" {
		t.Fatalf("GET 应透明返回最新版本 v2: opcode=%#x payload=%q", getMsg.Header.Opcode, getMsg.Payload)
	}
}

func TestRouterV2_ReplayFingerprintReportsZeroMismatchAfterNormalWrites(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	for _, logical := range []string{"reading:2026-08-17:600000", "reading:2026-08-17:600001"} {
		for i, val := range []string{"a", "b"} {
			if msg := c.putVersioned(uint32(i+1), logical, val); msg.Header.Opcode != codec.OpcodeOK {
				t.Fatalf("PUT_VERSIONED 应成功: opcode=%#x", msg.Header.Opcode)
			}
		}
	}

	msg := c.replayFingerprint(100, "reading:2026-08-17:")
	if msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("REPLAY_FINGERPRINT 应成功, opcode=%#x", msg.Header.Opcode)
	}
	result, ok := proto.DecodeReplayFingerprintResponse(msg.Payload)
	if !ok {
		t.Fatal("响应体应能解析")
	}
	if result.KeyCount != 2 {
		t.Fatalf("应发现 2 个逻辑键, got %d", result.KeyCount)
	}
	if result.MismatchCount != 0 || len(result.MismatchKeys) != 0 {
		t.Fatalf("正常写入后应零不一致（验收三问第 2 问）: %+v", result)
	}
	if result.Fingerprint == "" {
		t.Fatal("指纹不应为空")
	}
}

func TestRouterV2_ListVersionsEmptyForNeverWrittenKey(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	msg := c.listVersions(1, "never:written")
	if msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("从未写过的逻辑键应 OK+空列表，不是 ERR: opcode=%#x", msg.Header.Opcode)
	}
	versions, ok := proto.DecodeListVersionsResponse(msg.Payload)
	if !ok || len(versions) != 0 {
		t.Fatalf("应解出空列表: versions=%+v ok=%v", versions, ok)
	}
}

// TestRouterV2_V1ScanUnaffectedByVersionedWrites 是 v1 零回归 + SCAN 内部键
// 隔离的端到端版本：一条 v1 客户端在 v2 客户端写入版本化数据之后 SCAN 同一
// 前缀，应只看到 v1 自己写的 key，看不到任何 ":vSEQ"/":current" 内部键。
func TestRouterV2_V1ScanUnaffectedByVersionedWrites(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)

	v2c := dialV2(t, addr, negotiate.AckEvery)
	defer v2c.close()
	if msg := v2c.putVersioned(1, "reading:2026-08-17:600000", "v1"); msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("PUT_VERSIONED 应成功, opcode=%#x", msg.Header.Opcode)
	}

	v1c, err := client.New(client.Options{Addrs: []string{addr}})
	if err != nil {
		t.Fatalf("构造 v1 客户端失败: %v", err)
	}
	defer v1c.Close()

	ctx := context.Background()
	if err := v1c.Put(ctx, []byte("reading:2026-08-17:600001"), []byte(`{"plain":true}`)); err != nil {
		t.Fatalf("v1 PUT 失败: %v", err)
	}

	entries, err := v1c.Scan(ctx, []byte("reading:2026-08-17:"), []byte("reading:2026-08-17:\xff"), predicate.Predicate{})
	if err != nil {
		t.Fatalf("v1 SCAN 失败: %v", err)
	}
	if len(entries) != 1 || string(entries[0].Key) != "reading:2026-08-17:600001" {
		t.Fatalf("v1 SCAN 应只看到自己写的 plain key: %+v", entries)
	}
}
