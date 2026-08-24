package service

// RouterV2（docs/rfc/Kair-2.md §11）的端到端集成测试：起一个真实的
// kairnet.Server（与 cmd/kairosflux-server 同样的接线：v1 Router 处理 PUT/GET/DEL、
// RouterV2 处理 v2 帧），用一个手写的最小 v2 测试客户端（不经过任何生产
// SDK，直接用 kairnet/codec 与 kairnet/negotiate 拼帧）驱动，覆盖：
//
//  1. v1 客户端连新服务端零影响（RouterV2 的存在不改变 v1 路径）。
//  2. v2 ack=every：逐帧即时响应。
//  3. v2 ack=window 的三条名义关窗路径（corr_id 变化/安全阀 N/显式 FLUSH）
//     与 §11.5 新增的第四条隐式路径（GET/SCAN 触发的隐式 FLUSH）。
//  4. v2 ack=none + STAT 对账：计数吻合、以及注入 schema 拒绝后 accepted
//     与 received 出现差额（诊断粒度体现在 firstErrCode/reason 上）。
//  5. BYE 在 window/none 两档下的收尾行为（RFC §11.4）。
//  6. v2 写路径与 v1 共享同一套全局 metrics（不只是本文件自己的连接级
//     计数），以及多条并发 v2 连接的窗口/累计状态互不干扰（佐证"同步处理
//     无需加锁"这条结论确实在并发场景下成立，而不只是单连接测试侥幸
//     没有触发竞态）。
import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/client"
	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook/schema"
)

// startRouterV2TestServer 起一个真实服务端：v1 PUT/GET/DEL 路由 + RouterV2，
// 挂一个不做单调性校验的 ingesthook.Filter（复用 schema 包 init() 自注册的
// "quote:" 校验器，供本文件的 schema 拒绝场景使用，与 crosslang_test.go 的
// dropBackward=false 理由相同：quote key 末段是股票代码而非时间戳）。
func startRouterV2TestServer(t *testing.T, windowN uint32) string {
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

	srv := kairnet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddRouter(proto.MsgPut, router)
	srv.AddRouter(proto.MsgGet, router)
	srv.AddRouter(proto.MsgDelete, router)
	srv.AddRouter(proto.MsgScan, router) // cmd/kairosflux-server 生产接线的四个 v1 opcode 本测试服务端应悉数具备，之前缺 SCAN 纯属遗漏
	srv.AddRouterV2(routerV2)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	waitRouterV2ServerReady(t, addr)
	return addr
}

func waitRouterV2ServerReady(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("服务端在 2s 内未就绪: %s", addr)
}

// v2Client 是本文件专用的最小 v2 测试客户端：不经过任何生产 SDK，直接用
// kairnet/codec 拼帧，只为测试驱动而存在。
type v2Client struct {
	t    *testing.T
	conn net.Conn
}

func dialV2(t *testing.T, addr string, want negotiate.AckTier) *v2Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	version, ack, err := negotiate.ClientNegotiateWithAck(conn, 3*time.Second, want)
	if err != nil {
		conn.Close()
		t.Fatalf("协商失败: %v", err)
	}
	if version != negotiate.VersionV2 {
		conn.Close()
		t.Fatalf("协商结果=%v，期望 VersionV2", version)
	}
	if ack != want {
		conn.Close()
		t.Fatalf("服务端确认档位=%v，期望 %v", ack, want)
	}
	return &v2Client{t: t, conn: conn}
}

func (c *v2Client) close() { c.conn.Close() }

func (c *v2Client) send(opcode uint8, typ uint16, corrID uint32, payload []byte) {
	c.t.Helper()
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: typ, CorrID: corrID}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		c.t.Fatalf("v2 Pack 失败: %v", err)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		c.t.Fatalf("设置写超时失败: %v", err)
	}
	if _, err := c.conn.Write(frame); err != nil {
		c.t.Fatalf("v2 写帧失败: %v", err)
	}
}

func (c *v2Client) recv() *codec.MessageV2 {
	c.t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		c.t.Fatalf("设置读超时失败: %v", err)
	}
	msg, err := codec.NewDataPackV2().Decode(c.conn, 0, nil)
	if err != nil {
		c.t.Fatalf("v2 读帧失败: %v", err)
	}
	return msg
}

func (c *v2Client) putRaw(corrID uint32, typ uint16, key, value []byte) {
	c.send(codec.OpcodePut, typ, corrID, proto.EncodePutFrame(key, value))
}

func (c *v2Client) put(corrID uint32, key, value string) {
	c.putRaw(corrID, codec.TypeUnspecified, []byte(key), []byte(value))
}

func (c *v2Client) get(corrID uint32, key string) *codec.MessageV2 {
	c.send(codec.OpcodeGet, codec.TypeUnspecified, corrID, proto.EncodeKeyOnlyFrame([]byte(key)))
	return c.recv()
}

func (c *v2Client) flush(corrID uint32) *codec.MessageV2 {
	c.send(codec.OpcodeFlush, codec.TypeUnspecified, corrID, nil)
	return c.recv()
}

func (c *v2Client) stat(corrID uint32) *codec.MessageV2 {
	c.send(codec.OpcodeStat, codec.TypeUnspecified, corrID, nil)
	return c.recv()
}

func (c *v2Client) bye(corrID uint32) *codec.MessageV2 {
	c.send(codec.OpcodeBye, codec.TypeUnspecified, corrID, nil)
	return c.recv()
}

// decodeAck 断言 msg 是一个合法的 WINDOW_ACK/STAT_ACK 并解出字段。
func decodeAck(t *testing.T, msg *codec.MessageV2, wantOpcode uint8) proto.V2AckFields {
	t.Helper()
	if msg.Header.Opcode != wantOpcode {
		t.Fatalf("opcode=%#x，期望 %#x", msg.Header.Opcode, wantOpcode)
	}
	fields, ok := proto.DecodeV2AckBody(msg.Payload)
	if !ok {
		t.Fatalf("解析 ack body 失败: % x", msg.Payload)
	}
	return fields
}

// ---------------------------------------------------------------------
// 1. v1 客户端连新服务端零影响
// ---------------------------------------------------------------------

func TestRouterV2_V1ClientUnaffected(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)

	c, err := client.New(client.Options{Addrs: []string{addr}})
	if err != nil {
		t.Fatalf("构造 v1 客户端失败: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	if err := c.Put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	got, err := c.Get(ctx, []byte("k1"))
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("读回=%q，期望 v1", got)
	}
}

// ---------------------------------------------------------------------
// 2. ack=every
// ---------------------------------------------------------------------

func TestRouterV2_AckEvery_ImmediateResponsePerFrame(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	c.put(1, "ek1", "ev1")
	resp := c.recv()
	if resp.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("PUT 响应 opcode=%#x，期望 OK", resp.Header.Opcode)
	}

	resp = c.get(2, "ek1")
	if resp.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("GET 响应 opcode=%#x，期望 OK", resp.Header.Opcode)
	}
	if string(resp.Payload) != "ev1" {
		t.Fatalf("GET 负载=%q，期望 ev1", resp.Payload)
	}
}

// ---------------------------------------------------------------------
// 3. ack=window：三条名义路径 + 一条 §11.5 隐式路径
// ---------------------------------------------------------------------

func TestRouterV2_Window_CorrIDChangeClosesPreviousWindow(t *testing.T) {
	addr := startRouterV2TestServer(t, 1000) // 安全阀设很大，确保不会提前触发
	c := dialV2(t, addr, negotiate.AckWindow)
	defer c.close()

	c.put(10, "wk1", "v1")
	c.put(10, "wk2", "v2")
	// corr_id 从 10 变成 11：应隐式关闭 corr_id=10 的窗口。
	c.put(11, "wk3", "v3")

	ack := decodeAck(t, c.recv(), codec.OpcodeWindowAck)
	if ack.CorrID != 10 {
		t.Fatalf("WINDOW_ACK corr_id=%d，期望 10（被关闭的是旧窗口，不是触发关闭的新帧）", ack.CorrID)
	}
	if ack.Received != 2 || ack.Accepted != 2 || ack.Rejected != 0 {
		t.Fatalf("窗口计数=%+v，期望 received=2 accepted=2 rejected=0", ack)
	}
}

func TestRouterV2_Window_SafetyValveNClosesWindow(t *testing.T) {
	const n = 3
	addr := startRouterV2TestServer(t, n)
	c := dialV2(t, addr, negotiate.AckWindow)
	defer c.close()

	c.put(20, "sk1", "v1")
	c.put(20, "sk2", "v2")
	c.put(20, "sk3", "v3") // 第 3 条达到安全阀 N=3，应自动关窗

	ack := decodeAck(t, c.recv(), codec.OpcodeWindowAck)
	if ack.CorrID != 20 {
		t.Fatalf("WINDOW_ACK corr_id=%d，期望 20", ack.CorrID)
	}
	if ack.Received != n || ack.Accepted != n {
		t.Fatalf("窗口计数=%+v，期望 received=accepted=%d", ack, n)
	}
}

func TestRouterV2_Window_ExplicitFlushClosesWindow(t *testing.T) {
	addr := startRouterV2TestServer(t, 1000)
	c := dialV2(t, addr, negotiate.AckWindow)
	defer c.close()

	c.put(30, "fk1", "v1")
	c.put(30, "fk2", "v2")
	ack := decodeAck(t, c.flush(0), codec.OpcodeWindowAck)
	if ack.CorrID != 30 {
		t.Fatalf("WINDOW_ACK corr_id=%d，期望 30（FLUSH 帧本身没有 corr_id 语义，应取被关闭窗口的 corr_id）", ack.CorrID)
	}
	if ack.Received != 2 || ack.Accepted != 2 {
		t.Fatalf("窗口计数=%+v，期望 received=accepted=2", ack)
	}
}

func TestRouterV2_Window_ImplicitFlushOnGet(t *testing.T) {
	addr := startRouterV2TestServer(t, 1000)
	c := dialV2(t, addr, negotiate.AckWindow)
	defer c.close()

	c.put(40, "gk1", "gv1")
	// 窗口尚未关闭时发 GET：应先收到隐式 FLUSH 产生的 WINDOW_ACK，再收到 GET 的正常响应。
	c.send(codec.OpcodeGet, codec.TypeUnspecified, 0, proto.EncodeKeyOnlyFrame([]byte("gk1")))

	ack := decodeAck(t, c.recv(), codec.OpcodeWindowAck)
	if ack.CorrID != 40 || ack.Received != 1 || ack.Accepted != 1 {
		t.Fatalf("隐式 FLUSH 的 WINDOW_ACK=%+v，期望 corr_id=40 received=accepted=1", ack)
	}

	getResp := c.recv()
	if getResp.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("GET 响应 opcode=%#x，期望 OK", getResp.Header.Opcode)
	}
	if string(getResp.Payload) != "gv1" {
		t.Fatalf("GET 负载=%q，期望 gv1（写后立即读，不应要求客户端手动 FLUSH）", getResp.Payload)
	}
}

// ---------------------------------------------------------------------
// 4. ack=none + STAT 对账
// ---------------------------------------------------------------------

func TestRouterV2_None_StatMatchesLocalSentCount(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckNone)
	defer c.close()

	const n = 5
	for i := 0; i < n; i++ {
		c.put(0, "nk"+strconv.Itoa(i), "v")
	}

	ack := decodeAck(t, c.stat(99), codec.OpcodeStatAck)
	if ack.CorrID != 99 {
		t.Fatalf("STAT_ACK corr_id=%d，期望回带 STAT 请求自己的 corr_id=99", ack.CorrID)
	}
	if ack.Received != n || ack.Accepted != n || ack.Rejected != 0 {
		t.Fatalf("STAT_ACK=%+v，期望 received=accepted=%d rejected=0（本地发送计数与服务端吻合）", ack, n)
	}
}

func TestRouterV2_None_StatShowsInjectedRejections(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckNone)
	defer c.close()

	validQuote := []byte(`{"code":"600000","date":"2026-08-20","open":10.0,"high":10.5,"low":9.8,"close":10.2,"volume":1000000,"prev_close":10.0}`)
	invalidQuote := []byte(`{"code":"600000","date":"2026-08-20","open":-1,"high":10.5,"low":9.8,"close":10.2,"volume":1000000}`)

	localSent := 0
	c.putRaw(0, codec.TypeQuote, []byte("quote:2026-08-20:600000"), validQuote)
	localSent++
	c.putRaw(0, codec.TypeQuote, []byte("quote:2026-08-20:600001"), invalidQuote) // 人为注入的拒绝：非法价格
	localSent++
	c.putRaw(0, codec.TypeQuote, []byte("quote:2026-08-20:600002"), validQuote)
	localSent++

	ack := decodeAck(t, c.stat(1), codec.OpcodeStatAck)
	if int(ack.Received) != localSent {
		t.Fatalf("received=%d，期望与本地发送计数 %d 一致（帧层面没有丢失）", ack.Received, localSent)
	}
	if ack.Accepted != 2 || ack.Rejected != 1 {
		t.Fatalf("accepted/rejected=%d/%d，期望 2/1（诊断粒度：accepted 对不上 received，而不是 received 本身丢失）", ack.Accepted, ack.Rejected)
	}
	// M1 起，声明了 type（本测试的 PUT 都带 codec.TypeQuote）时按 TypeID 精确
	// 分派到 quote 校验器，schema 校验失败按 RFC §10.3 的子码细分（这里是
	// "非正价格"，不再是笼统的 ErrCodeSchemaValidation——那是留给"没有更精确
	// 子码可用"场景的默认桶，见 service/router_v2.go 的 applyWrite）。
	if ack.FirstErrCode != schema.ErrCodeNonPositivePrice {
		t.Fatalf("firstErrCode=%#x，期望 %#x（非正价格，quote 类型的 RFC §10.3 子码）", ack.FirstErrCode, schema.ErrCodeNonPositivePrice)
	}
	if ack.FirstErrReason == "" {
		t.Fatal("firstErrReason 为空，期望携带具体拒绝原因")
	}
}

// ---------------------------------------------------------------------
// 5. BYE 收尾（RFC §11.4）
// ---------------------------------------------------------------------

func TestRouterV2_Bye_WindowClosesOutstandingWindowThenSummarizes(t *testing.T) {
	addr := startRouterV2TestServer(t, 1000)
	c := dialV2(t, addr, negotiate.AckWindow)
	defer c.close()

	c.put(50, "bk1", "v1")
	c.put(50, "bk2", "v2")

	windowAck := decodeAck(t, c.bye(7), codec.OpcodeWindowAck)
	if windowAck.CorrID != 50 || windowAck.Received != 2 || windowAck.Accepted != 2 {
		t.Fatalf("BYE 触发的 WINDOW_ACK=%+v，期望 corr_id=50 received=accepted=2", windowAck)
	}

	statAck := decodeAck(t, c.recv(), codec.OpcodeStatAck)
	if statAck.CorrID != 7 {
		t.Fatalf("BYE 隐式 STAT_ACK 的 corr_id=%d，期望取 BYE 帧自己的 corr_id=7", statAck.CorrID)
	}
	if statAck.Received != 2 || statAck.Accepted != 2 {
		t.Fatalf("BYE 隐式 STAT_ACK=%+v，期望反映连接累计 received=accepted=2", statAck)
	}
}

func TestRouterV2_Bye_NoneSummarizesCumulative(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckNone)
	defer c.close()

	c.put(0, "nk-bye-1", "v1")
	c.put(0, "nk-bye-2", "v2")
	c.put(0, "nk-bye-3", "v3")

	statAck := decodeAck(t, c.bye(8), codec.OpcodeStatAck)
	if statAck.CorrID != 8 {
		t.Fatalf("BYE 隐式 STAT_ACK 的 corr_id=%d，期望取 BYE 帧自己的 corr_id=8", statAck.CorrID)
	}
	if statAck.Received != 3 || statAck.Accepted != 3 || statAck.Rejected != 0 {
		t.Fatalf("BYE 隐式 STAT_ACK=%+v，期望 received=accepted=3 rejected=0", statAck)
	}
}

// ---------------------------------------------------------------------
// 6a. v2 写路径必须与 v1 共享同一套全局 metrics（不只是本文件自己的连接级
//     计数）——RFC §11.2.3 实现提醒明确要求两者并存，见 router_v2.go 顶部
//     注释。用 before/after 差值断言，而不是绝对值：metrics 是进程级全局
//     atomic 计数器，同一测试二进制里其它测试（本文件与 crosslang 等）
//     也会累加它们，只有差值在并发/顺序执行下才是可靠的断言。
// ---------------------------------------------------------------------

func TestRouterV2_GlobalMetricsMoveOnV2Path(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckWindow)
	defer c.close()

	before := metrics.Take()

	c.put(60, "mk1", "v1")
	c.put(60, "mk2", "v2")
	// 畸形 PUT 负载（少于 8 字节，proto.DecodePutFrame 解不出 key/value）：
	// 应计入 FramesDroppedMalformed，与 v1 ingesthook.Filter.Handle 对齐。
	c.send(codec.OpcodePut, codec.TypeUnspecified, 60, []byte{0x01, 0x02})
	c.flush(0)

	after := metrics.Take()
	if got := after.Writes - before.Writes; got != 2 {
		t.Fatalf("metrics.Writes 增量=%d，期望 2（v2 写路径必须驱动全局 metrics，不能只有连接级窗口计数）", got)
	}
	if got := after.DroppedMalformed - before.DroppedMalformed; got != 1 {
		t.Fatalf("metrics.FramesDroppedMalformed 增量=%d，期望 1", got)
	}
}

// ---------------------------------------------------------------------
// 6b. 多条并发 v2 连接的窗口/累计状态互不干扰——这是"HandleV2 同步跑在
//     该连接自己的 Reader goroutine 里，v2ConnState 不需要加锁"这条结论
//     在真实并发场景下的验证：单连接测试通过 -race 只能说明"没有观测到
//     竞态"，不能说明"多连接下各自的状态确实被正确隔离"，两者是不同的
//     断言，本测试专门覆盖后者。
// ---------------------------------------------------------------------

func TestRouterV2_ConcurrentConnectionsIsolatedState(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)

	// 注意：两个 goroutine 内部一律不调用 t.Fatalf/t.Fatal——testing 包要求
	// FailNow 系列只能在跑该测试函数的 goroutine 里调用；这里改为把结果/
	// 错误通过 channel 传回主 goroutine，由主 goroutine 统一断言，避免
	// 触发"从其它 goroutine 调用 FailNow"这个未定义行为（这本身也是本次
	// 交付要规避的一类真实 bug，不只是风格问题）。
	type outcome struct {
		label string
		err   error
		ack   proto.V2AckFields
	}
	results := make(chan outcome, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	// 连接 A：ack=window，写 4 条后 FLUSH，期望这条连接自己的窗口只看到 4。
	go func() {
		defer wg.Done()
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			results <- outcome{"A", err, proto.V2AckFields{}}
			return
		}
		defer conn.Close()
		if _, _, err := negotiate.ClientNegotiateWithAck(conn, 3*time.Second, negotiate.AckWindow); err != nil {
			results <- outcome{"A", err, proto.V2AckFields{}}
			return
		}
		for i := 0; i < 4; i++ {
			payload := proto.EncodePutFrame([]byte("isoA"+strconv.Itoa(i)), []byte("v"))
			if err := writeV2Frame(conn, codec.OpcodePut, codec.TypeUnspecified, 1, payload); err != nil {
				results <- outcome{"A", err, proto.V2AckFields{}}
				return
			}
		}
		if err := writeV2Frame(conn, codec.OpcodeFlush, codec.TypeUnspecified, 0, nil); err != nil {
			results <- outcome{"A", err, proto.V2AckFields{}}
			return
		}
		msg, err := readV2Frame(conn)
		if err != nil {
			results <- outcome{"A", err, proto.V2AckFields{}}
			return
		}
		fields, _ := proto.DecodeV2AckBody(msg.Payload)
		results <- outcome{"A", nil, fields}
	}()

	// 连接 B：ack=none，写 7 条后 STAT，期望这条连接自己的累计只看到 7，
	// 不受连接 A 并发写入的影响（若状态串了，received 会明显偏离 7）。
	go func() {
		defer wg.Done()
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			results <- outcome{"B", err, proto.V2AckFields{}}
			return
		}
		defer conn.Close()
		if _, _, err := negotiate.ClientNegotiateWithAck(conn, 3*time.Second, negotiate.AckNone); err != nil {
			results <- outcome{"B", err, proto.V2AckFields{}}
			return
		}
		for i := 0; i < 7; i++ {
			payload := proto.EncodePutFrame([]byte("isoB"+strconv.Itoa(i)), []byte("v"))
			if err := writeV2Frame(conn, codec.OpcodePut, codec.TypeUnspecified, 0, payload); err != nil {
				results <- outcome{"B", err, proto.V2AckFields{}}
				return
			}
		}
		if err := writeV2Frame(conn, codec.OpcodeStat, codec.TypeUnspecified, 1, nil); err != nil {
			results <- outcome{"B", err, proto.V2AckFields{}}
			return
		}
		msg, err := readV2Frame(conn)
		if err != nil {
			results <- outcome{"B", err, proto.V2AckFields{}}
			return
		}
		fields, _ := proto.DecodeV2AckBody(msg.Payload)
		results <- outcome{"B", nil, fields}
	}()

	wg.Wait()
	close(results)

	for r := range results {
		if r.err != nil {
			t.Fatalf("连接 %s 失败: %v", r.label, r.err)
		}
		switch r.label {
		case "A":
			if r.ack.Received != 4 || r.ack.Accepted != 4 {
				t.Fatalf("连接A窗口计数=%+v，期望 received=accepted=4（不应被连接B的并发写入污染）", r.ack)
			}
		case "B":
			if r.ack.Received != 7 || r.ack.Accepted != 7 {
				t.Fatalf("连接B累计计数=%+v，期望 received=accepted=7（不应被连接A的并发写入污染）", r.ack)
			}
		}
	}
}

// writeV2Frame/readV2Frame 是 TestRouterV2_ConcurrentConnectionsIsolatedState
// 专用的裸 net.Conn 收发（不依赖 v2Client，因为 v2Client 内部调用
// t.Fatalf，不能在后台 goroutine 里安全使用，见上面的注释）。
func writeV2Frame(conn net.Conn, opcode uint8, typ uint16, corrID uint32, payload []byte) error {
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: typ, CorrID: corrID}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	_, err = conn.Write(frame)
	return err
}

func readV2Frame(conn net.Conn) (*codec.MessageV2, error) {
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	return codec.NewDataPackV2().Decode(conn, 0, nil)
}
