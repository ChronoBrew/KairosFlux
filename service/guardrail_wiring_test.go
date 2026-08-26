package service

// 内存护栏与两个 Router 的接线测试：构造"已 blocked"的护栏（注入假采样器
// + 手动 tick，不开 goroutine），验证 v1 写拒收（overloaded）读照常、v2 写
// 结构化拒绝（0x1003）读照常、解除后写恢复——与 guardrail_test.go 的纯状态
// 机测试互补（那里不经过 Router）。
//
// v2 侧起一个真实服务端（复制 startRouterV2TestServer 的装配，只是多挂
// 一个护栏实例——共享测试基建是"零改动"红线，见仓库协作规范"外科手术式
// 修改"，不为了本测试给既有 helper 加参数）。

import (
	"bytes"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
)

// blockedTestGuardrail 构造一个已处于拒收状态的护栏（RSS 假采样恒超限）。
func blockedTestGuardrail(t *testing.T) *MemoryGuardrail {
	t.Helper()
	g := NewMemoryGuardrail(100, nil) // 100MiB 上限
	if g == nil {
		t.Fatal("NewMemoryGuardrail(100) 不应为 nil")
	}
	g.sample = func() uint64 { return 200 << 20 } // 200MiB > 100MiB
	g.tick()
	if !g.Blocked() {
		t.Fatal("前置条件：护栏应处于 blocked")
	}
	return g
}

// fakeMsgReq 是 kairnet.Request 的测试替身，msgID 可指定（fakePreHandleReq
// 硬编码 MsgPut，护栏测试需要驱动 GET/DEL 走不同分支）。
type fakeMsgReq struct {
	conn  *fakeConn
	data  []byte
	msgID string
}

func (r *fakeMsgReq) Conn() kairnet.Conn  { return r.conn }
func (r *fakeMsgReq) MsgData() []byte     { return r.data }
func (r *fakeMsgReq) MsgID() string       { return r.msgID }
func (r *fakeMsgReq) SetMsgData(d []byte) { r.data = d }

// TestGuardrailWiring_V1BlocksWritesKeepsReads 验证 v1 Router.Handle：护栏
// blocked 时 PUT 回 overloaded（复用既有可重试状态）、GET 照常进处理器
// （not found 走正常语义）；解除后 PUT 恢复。
func TestGuardrailWiring_V1BlocksWritesKeepsReads(t *testing.T) {
	kv := NewKVServer()
	router := NewRouter(kv)
	guardrail := blockedTestGuardrail(t)
	router.SetMemoryGuardrail(guardrail)

	conn := &fakeConn{}
	req := &fakeMsgReq{conn: conn}

	// 写：PUT 被拒，回 overloaded（MsgRespErr + StatusOverloaded）。
	req.msgID = proto.MsgPut
	req.data = proto.EncodePutFrame([]byte("k"), []byte("v"))
	router.Handle(req)
	if conn.lastMsgID != proto.MsgRespErr || !bytes.Equal(conn.lastData, statusPayload(proto.StatusOverloaded)) {
		t.Fatalf("blocked 时 PUT 应回 overloaded，got msgID=%q data=%q", conn.lastMsgID, conn.lastData)
	}

	// 读：GET 照常进处理器——不存在的键走 sendNotFound，而不是被护栏拦掉。
	req.msgID = proto.MsgGet
	req.data = proto.EncodeKeyOnlyFrame([]byte("k"))
	router.Handle(req)
	if conn.lastMsgID != proto.MsgRespErr || !bytes.Equal(conn.lastData, statusPayload(proto.StatusNotFound)) {
		t.Fatalf("blocked 时 GET 应照常处理（not found），got msgID=%q data=%q", conn.lastMsgID, conn.lastData)
	}

	// 解除后：PUT 恢复成功。
	guardrail.sample = func() uint64 { return 10 << 20 }
	guardrail.tick()
	if guardrail.Blocked() {
		t.Fatal("前置条件：护栏应已解除")
	}
	req.msgID = proto.MsgPut
	req.data = proto.EncodePutFrame([]byte("k"), []byte("v"))
	router.Handle(req)
	if conn.lastMsgID != proto.MsgRespOK {
		t.Fatalf("解除后 PUT 应成功，got msgID=%q", conn.lastMsgID)
	}
}

// startRouterV2TestServerWithGuardrail 是 startRouterV2TestServer 的护栏变体：
// 同样的装配，只是构造后把护栏挂到 v1/v2 两个 Router 上（真实生产接线同构，
// 见 service/node.go NewNode）。
func startRouterV2TestServerWithGuardrail(t *testing.T, windowN uint32, guardrail *MemoryGuardrail) string {
	t.Helper()

	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.Mode = config.ModeStandalone
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode = oldWAL, oldSST, oldMode
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("取空闲端口失败: %v", err)
	}
	addr := l.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	l.Close()

	kv := NewKVServer()
	router := NewRouter(kv)
	filter := ingesthook.NewFilter(nil, 0, false)
	router.SetPreHandle(filter.Handle)
	routerV2 := NewRouterV2(kv, filter, windowN)

	router.SetMemoryGuardrail(guardrail)
	routerV2.SetMemoryGuardrail(guardrail)

	srv := kairnet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddHandler(proto.MsgPut, router)
	srv.AddHandler(proto.MsgGet, router)
	srv.AddHandler(proto.MsgDelete, router)
	srv.AddHandler(proto.MsgScan, router)
	srv.SetV2Handler(routerV2)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("v2 测试服务端未就绪: %s", addr)
	return ""
}

// TestGuardrailWiring_V2RejectsWritesWithMemoryLimitKeepsReads 验证 v2 写路径：
// 护栏 blocked 时 PUT_VERSIONED 结构化拒绝（ErrCodeMemoryLimit=0x1003，
// reason=memory_limit_reached）、LIST_WRITES 读照常服务；解除后恢复。
func TestGuardrailWiring_V2RejectsWritesWithMemoryLimitKeepsReads(t *testing.T) {
	guardrail := blockedTestGuardrail(t)
	addr := startRouterV2TestServerWithGuardrail(t, DefaultV2WindowSafetyValveN, guardrail)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	// 写：PUT_VERSIONED 被拒，ERR 负载解出 0x1003 + memory_limit_reached。
	msg := c.putVersioned(1, "reading:2026-08-17:600000", "v1")
	if msg.Header.Opcode != codec.OpcodeErr {
		t.Fatalf("blocked 时 PUT_VERSIONED 应回 ERR，got opcode=%#x", msg.Header.Opcode)
	}
	code, reason, ok := proto.DecodeV2ErrPayload(msg.Payload)
	if !ok || code != codec.ErrCodeMemoryLimit || reason != "memory_limit_reached" {
		t.Fatalf("ERR 负载应解出 (0x1003, memory_limit_reached)，got code=%#x reason=%q ok=%v", code, reason, ok)
	}

	// 读：LIST_WRITES 照常服务（OK，空结果）——读不受护栏影响。
	msg = c.listWrites(2, "reading:2026-08-17", 0, 0, "")
	if msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("blocked 时 LIST_WRITES 应照常 OK，got opcode=%#x", msg.Header.Opcode)
	}

	// 解除后：PUT_VERSIONED 恢复。
	guardrail.sample = func() uint64 { return 10 << 20 }
	guardrail.tick()
	msg = c.putVersioned(3, "reading:2026-08-17:600000", "v1")
	if msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("解除后 PUT_VERSIONED 应成功，got opcode=%#x", msg.Header.Opcode)
	}
}

// TestGuardrailWiring_V2HandlePutVersioned 验证独立落盘入口 handlePutVersioned
// 与 applyWrite 走同一判定（不经 applyWrite 的 PUT_VERSIONED 单独拦一道）。
func TestGuardrailWiring_V2HandlePutVersioned(t *testing.T) {
	guardrail := blockedTestGuardrail(t)
	addr := startRouterV2TestServerWithGuardrail(t, DefaultV2WindowSafetyValveN, guardrail)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	msg := c.putVersionedWithSource(1, "reading:2026-08-17:600000", "v1", "probe")
	if msg.Header.Opcode != codec.OpcodeErr {
		t.Fatalf("blocked 时带 source 的 PUT_VERSIONED 也应被拒，got opcode=%#x", msg.Header.Opcode)
	}
	code, reason, ok := proto.DecodeV2ErrPayload(msg.Payload)
	if !ok || code != codec.ErrCodeMemoryLimit || reason != "memory_limit_reached" {
		t.Fatalf("ERR 负载应解出 (0x1003, memory_limit_reached)，got code=%#x reason=%q ok=%v", code, reason, ok)
	}
}
