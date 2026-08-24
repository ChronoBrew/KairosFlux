package service

// 端到端验证时态内核 M0 新增的四个 v2 opcode（PUT_VERSIONED/GET_AS_OF/
// LIST_VERSIONS/REPLAY_FINGERPRINT，docs/rfc/时态内核-M0-版本化与as-of.md），
// 复用 router_v2_integration_test.go 的最小 v2 测试客户端（dialV2/send/recv）
// 与真实服务端（startRouterV2TestServer）——不新起一套测试基建。

import (
	"context"
	"testing"

	"github.com/ChronoBrew/KairosFlux/client"
	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/predicate"
	"github.com/ChronoBrew/KairosFlux/proto"
)

func (c *v2Client) putVersioned(corrID uint32, key, value string) *codec.MessageV2 {
	c.send(codec.OpcodePutVersioned, codec.TypeUnspecified, corrID, proto.EncodePutFrame([]byte(key), []byte(value)))
	return c.recv()
}

// putVersionedWithSource 是 putVersioned 的 M2 变体：显式声明写入方来源
// （proto.EncodePutVersionedFrame 的可选尾部字段）。
func (c *v2Client) putVersionedWithSource(corrID uint32, key, value, source string) *codec.MessageV2 {
	c.send(codec.OpcodePutVersioned, codec.TypeUnspecified, corrID, proto.EncodePutVersionedFrame([]byte(key), []byte(value), source))
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

// replayFingerprintBounded 是 replayFingerprint 的 M2 变体：带 asOfNanos 上界
// （proto.EncodeReplayFingerprintRequest 的可选尾部字段）。
func (c *v2Client) replayFingerprintBounded(corrID uint32, prefix string, asOfNanos int64) *codec.MessageV2 {
	c.send(codec.OpcodeReplayFingerprint, codec.TypeUnspecified, corrID, proto.EncodeReplayFingerprintRequest([]byte(prefix), asOfNanos))
	return c.recv()
}

// listWrites 对应 LIST_WRITES（时态内核 M2 新增 opcode 0x0D）。
func (c *v2Client) listWrites(corrID uint32, prefix string, tFromNanos, tToNanos int64, source string) *codec.MessageV2 {
	c.send(codec.OpcodeListWrites, codec.TypeUnspecified, corrID,
		proto.EncodeListWritesRequest([]byte(prefix), tFromNanos, tToNanos, []byte(source)))
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

// TestRouterV2_PutVersionedWithSourceIsQueryableByListWrites 端到端验证 M2
// 操作元数据信封在真实网络路径上的接线：PUT_VERSIONED 声明 source →
// LIST_WRITES 按 source 过滤能查到，且信封字段（seq/write_ts/schema_ver/
// hash_ok）经过网络编解码后保持完整。
func TestRouterV2_PutVersionedWithSourceIsQueryableByListWrites(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	logical := "reading:2026-08-17:600000"
	if msg := c.putVersionedWithSource(1, logical, "v1", "quantbrew-daily"); msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("PUT_VERSIONED(source) 应成功, opcode=%#x", msg.Header.Opcode)
	}
	if msg := c.putVersionedWithSource(2, logical, "v2", "other-job"); msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("PUT_VERSIONED(source) 应成功, opcode=%#x", msg.Header.Opcode)
	}

	msg := c.listWrites(3, "reading:2026-08-17:", 0, 0, "quantbrew-daily")
	if msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("LIST_WRITES 应成功, opcode=%#x", msg.Header.Opcode)
	}
	entries, counts, ok := proto.DecodeListWritesResponse(msg.Payload)
	if !ok {
		t.Fatal("响应体应能解析")
	}
	if len(entries) != 1 || entries[0].Seq != 1 || entries[0].Source != "quantbrew-daily" || string(entries[0].Payload) != "v1" {
		t.Fatalf("按 source 过滤应只命中 seq=1: %+v", entries)
	}
	if !entries[0].HashOK {
		t.Fatalf("未被篡改的记录 HashOK 应为 true: %+v", entries[0])
	}
	// BySource 反映的是"过滤后命中集合内部"的来源分布，不是"过滤前的全量
	// 分布"——按 source 过滤到只剩一种来源时，聚合计数应显示这一种来源的
	// 命中数，而不是无条件清空（那样反而丢失了"这次查询命中了多少条"这个
	// 信息，与 LIST_VERSIONS 的空列表≠错误 是同一种"如实反映过滤后集合"的
	// 设计取向）。
	if len(counts) != 1 || counts[0].Source != "quantbrew-daily" || counts[0].Count != 1 {
		t.Fatalf("按 source 过滤后的聚合计数应反映命中集合本身: %+v", counts)
	}
}

// TestRouterV2_ListWritesAggregatesBySourceAcrossMultipleWriters 验证无过滤
// LIST_WRITES 请求的 COUNT 聚合（M2 任务书第 2 项："某键某时段被写了几次、
// 来自谁"）：按 source 升序返回聚合计数。
func TestRouterV2_ListWritesAggregatesBySourceAcrossMultipleWriters(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	logical := "reading:2026-08-17:600000"
	sources := []string{"job-b", "job-a", "job-a"}
	for i, src := range sources {
		if msg := c.putVersionedWithSource(uint32(i+1), logical, "v", src); msg.Header.Opcode != codec.OpcodeOK {
			t.Fatalf("PUT_VERSIONED(source) 第 %d 次应成功, opcode=%#x", i+1, msg.Header.Opcode)
		}
	}

	msg := c.listWrites(10, "reading:2026-08-17:", 0, 0, "")
	if msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("LIST_WRITES 应成功, opcode=%#x", msg.Header.Opcode)
	}
	entries, counts, ok := proto.DecodeListWritesResponse(msg.Payload)
	if !ok || len(entries) != 3 {
		t.Fatalf("应命中全部 3 条: entries=%+v ok=%v", entries, ok)
	}
	if len(counts) != 2 || counts[0].Source != "job-a" || counts[0].Count != 2 ||
		counts[1].Source != "job-b" || counts[1].Count != 1 {
		t.Fatalf("按来源聚合计数不符（应按 source 升序: job-a=2, job-b=1): %+v", counts)
	}
}

// TestRouterV2_ReplayFingerprintBoundedRequestMarksResponseBounded 验证 M2
// REPLAY_FINGERPRINT 服务化升级在真实网络路径上的接线：带 asOfNanos 的请求
// 得到 Bounded=true 的响应，且 mismatchCount 恒为 0（未做 :current 对账，
// 不是核对通过，见 proto.ReplayFingerprintView.Bounded 的文档）；不带
// asOfNanos 的旧格式请求（本测试文件其它用例仍在用的 c.replayFingerprint）
// 保持 Bounded=false，零回归。
func TestRouterV2_ReplayFingerprintBoundedRequestMarksResponseBounded(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	logical := "reading:2026-08-17:600000"
	if msg := c.putVersioned(1, logical, "v1"); msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("PUT_VERSIONED 应成功, opcode=%#x", msg.Header.Opcode)
	}

	unbounded := c.replayFingerprint(2, "reading:2026-08-17:")
	if unbounded.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("REPLAY_FINGERPRINT 应成功, opcode=%#x", unbounded.Header.Opcode)
	}
	unboundedResult, ok := proto.DecodeReplayFingerprintResponse(unbounded.Payload)
	if !ok || unboundedResult.Bounded {
		t.Fatalf("不带 asOfNanos 的旧格式请求应得 Bounded=false: %+v ok=%v", unboundedResult, ok)
	}

	bounded := c.replayFingerprintBounded(3, "reading:2026-08-17:", 1<<62)
	if bounded.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("REPLAY_FINGERPRINT(bounded) 应成功, opcode=%#x", bounded.Header.Opcode)
	}
	boundedResult, ok := proto.DecodeReplayFingerprintResponse(bounded.Payload)
	if !ok || !boundedResult.Bounded {
		t.Fatalf("带 asOfNanos 的请求应得 Bounded=true: %+v ok=%v", boundedResult, ok)
	}
	if boundedResult.MismatchCount != 0 {
		t.Fatalf("区间查询的 mismatchCount 恒应为 0（未做对账): %+v", boundedResult)
	}
}

// TestRouterV2_ListWritesRejectsResultExceedingFrameSizeLimit 验证
// handleListWrites 的结果体量防护（见其文档）：一条会产生超过
// codec.EffectiveMaxSize(config.G.MaxPackageSize) 响应体的查询应得到结构化
// 拒绝（ErrCodeResultTooLarge/"result_too_large"），而不是服务端把一个客户端
// 终将拒绝的巨帧硬发出去、让调用方对着超时干等——这是 soak 测试
// （service/temporal_soak_bench_test.go：10 万键×10 版本，真实响应体量级
// 远超生产默认的 16MiB 上限）揭示的真实风险，不是假设性加固。
//
// 测试本身把 config.G.MaxPackageSize 临时调小到几十字节，用几条普通大小的
// 记录就能触发超限——不需要真的构造 MB 级 payload，保持测试快速；被调小的
// 是"生效上限"这个配置量，不是被检查的响应体本身，验证的仍是生产代码里
// 那个真实的比较逻辑。
func TestRouterV2_ListWritesRejectsResultExceedingFrameSizeLimit(t *testing.T) {
	oldMax := config.G.MaxPackageSize
	config.G.MaxPackageSize = 64 // 几条 "reading:2026-08-17:600000"+"v1" 级记录的响应体就会超过这个上限
	t.Cleanup(func() { config.G.MaxPackageSize = oldMax })

	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	logical := "reading:2026-08-17:600000"
	for i, val := range []string{"v1", "v2", "v3"} {
		if msg := c.putVersioned(uint32(i+1), logical, val); msg.Header.Opcode != codec.OpcodeOK {
			t.Fatalf("PUT_VERSIONED 应成功, opcode=%#x", msg.Header.Opcode)
		}
	}

	msg := c.listWrites(10, "reading:2026-08-17:", 0, 0, "")
	if msg.Header.Opcode != codec.OpcodeErr {
		t.Fatalf("超限查询应被结构化拒绝, opcode=%#x", msg.Header.Opcode)
	}
	code, reason, ok := proto.DecodeV2ErrPayload(msg.Payload)
	if !ok || code != codec.ErrCodeResultTooLarge || reason != "result_too_large" {
		t.Fatalf("拒绝原因不符: code=%#x reason=%q ok=%v", code, reason, ok)
	}
}
