package negotiate_test

// negotiate 包的协商集成测试，覆盖 docs/rfc/BANLV-2.md §5 描述的两条路径：
//
//  1. v2 客户端连一个真正的 v1 生产服务端（起一个真实的 service.Router +
//     bannet.Server，与 client/python/crosslang_test.go 的做法一致）——
//     v1 的 Router.Handle switch 没有 MsgHello case，收到探测帧后静默不
//     响应，客户端应在配置的超时窗口内判定为 VersionV1，不报错。这是
//     "已用代码验证过、不是假设"（RFC §5 原话）在本阶段的测试证据。
//  2. v2 客户端连一个（测试本地构造的）最小 v2-aware 服务端——用现有的
//     v1 codec.DataPack.Decode 读出探测帧、识别 msgID=="HELLO"、用
//     negotiate.BuildHelloResponseV2 回复——客户端应判定为 VersionV2。
//     这个"最小 v2 服务端"不是生产代码，只是验证 negotiate 包对称的
//     两侧（客户端探测 + 服务端应答）确实能互相说通，真正接入
//     bannet.Server 的生产读循环是后续阶段的工作。
//
// 两个测试都用真实 TCP 回环连接，不用 net.Pipe——net.Pipe 的写操作会
// 阻塞到对端读取，若真的模拟"v1 服务端永不响应"，用 net.Pipe 会在服务端
// 侧自己的读循环内死锁，而不是重现"客户端超时"这个场景。

import (
	"errors"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/bannet/negotiate"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/proto"
	"github.com/NeverENG/BanDB/service"
)

// startRealV1Server 起一个与生产接线完全一致的 v1 服务端（KVServer + Router，
// standalone 模式，数据落临时目录），不注册任何 HELLO 处理——这正是当前
// 生产代码的真实状态，不是为了这个测试专门裁剪的行为。
func startRealV1Server(t *testing.T) string {
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

	kv := service.NewKVServer()
	router := service.NewRouter(kv)

	srv := bannet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddRouter(proto.MsgPut, router)
	srv.AddRouter(proto.MsgGet, router)
	srv.AddRouter(proto.MsgDelete, router)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	waitServerReady(t, addr)
	return addr
}

func waitServerReady(t *testing.T, addr string) {
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

// TestClientNegotiate_RealV1Server_DowngradesOnTimeout 是 §5 降级路径的核心
// 证据：对一个真实的、未做任何裁剪的 v1 生产服务端发起协商，断言在超时
// 窗口内被判定为 VersionV1、且不返回错误。
func TestClientNegotiate_RealV1Server_DowngradesOnTimeout(t *testing.T) {
	addr := startRealV1Server(t)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()

	const timeout = 300 * time.Millisecond
	start := time.Now()
	version, ack, err := negotiate.ClientNegotiate(conn, timeout)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ClientNegotiate 返回错误: %v（期望超时降级，不是错误）", err)
	}
	if version != negotiate.VersionV1 {
		t.Fatalf("协商结果=%v，期望 VersionV1", version)
	}
	if ack != negotiate.AckEvery {
		t.Fatalf("ack=%v，期望 AckEvery", ack)
	}
	// 必须真的等到了接近 timeout（不能提前返回——提前返回意味着把某种
	// 读错误误判成了"降级"，而不是真的超时）；也不能远超 timeout（意味着
	// 读超时没有生效）。留足够宽的容差应对 CI 环境的调度抖动。
	if elapsed < timeout/2 {
		t.Fatalf("耗时=%v，远小于配置的超时=%v，怀疑没有真的等待", elapsed, timeout)
	}
	if elapsed > timeout*3 {
		t.Fatalf("耗时=%v，远大于配置的超时=%v，怀疑读超时未生效", elapsed, timeout)
	}

	// 降级之后这条连接应仍可当 v1 连接正常使用——用一次真实的 PUT/GET 验证
	// "复用同一条连接"这一步没有被协商阶段的读超时或半读字节污染。
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("设置读写超时失败: %v", err)
	}
	putFrame, err := codec.NewDataPack().Pack(codec.NewMessage(proto.MsgPut, encodePutPayload(t, "k-negotiate", "v-negotiate")))
	if err != nil {
		t.Fatalf("编码 PUT 帧失败: %v", err)
	}
	if _, err := conn.Write(putFrame); err != nil {
		t.Fatalf("写 PUT 帧失败: %v", err)
	}
	status := readStatus(t, conn)
	if status != proto.StatusOK {
		t.Fatalf("降级后 PUT 状态=%q，期望 %q", status, proto.StatusOK)
	}
}

// TestClientNegotiate_MinimalV2Server_Succeeds 验证 negotiate 包提供的
// 客户端探测（ClientNegotiate）与服务端应答（BuildHelloResponseV2）两端
// 确实能互相说通：本地起一个只做"读一帧 v1 探测帧、回一帧 v2 响应"的
// 最小服务端（非生产代码），断言客户端判定为 VersionV2。
func TestClientNegotiate_MinimalV2Server_Succeeds(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer l.Close()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- serveOneHelloV2(l)
	}()

	conn, err := net.DialTimeout("tcp", l.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()

	version, ack, err := negotiate.ClientNegotiate(conn, 3*time.Second)
	if err != nil {
		t.Fatalf("ClientNegotiate 返回错误: %v", err)
	}
	if version != negotiate.VersionV2 {
		t.Fatalf("协商结果=%v，期望 VersionV2", version)
	}
	if ack != negotiate.AckEvery {
		t.Fatalf("ack=%v，期望 AckEvery（本阶段恒定）", ack)
	}

	if err := <-serverErr; err != nil {
		t.Fatalf("最小 v2 服务端处理失败: %v", err)
	}
}

// TestClientNegotiate_PartialResponseThenSilence_ReturnsError 覆盖三态判断
// 里最容易被错误合并的一种情形：对端写出了 magic+ver 的第 1 个字节就不再
// 发送（既不发完剩下的字节、也不关闭连接），客户端应该在超时后返回错误，
// 绝不能因为"确实超时了"就把它和"完全没收到字节"的正常降级路径混同——
// 半读意味着这条连接已经不可信（RFC §5 的降级只覆盖"完全静默"这一种，
// 见 ClientNegotiate 文档的情形 2）。
func TestClientNegotiate_PartialResponseThenSilence_ReturnsError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer l.Close()

	accepted := make(chan struct{})
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// 只写 magic+ver 字段的第 1 个字节（0x02，本该是 version），然后
		// 挂起直到测试结束——模拟"对端半途卡住/网络抖动丢了后续字节"。
		_, _ = conn.Write([]byte{0x02})
		close(accepted)
		<-time.After(2 * time.Second)
	}()

	conn, err := net.DialTimeout("tcp", l.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()
	<-accepted

	version, _, err := negotiate.ClientNegotiate(conn, 200*time.Millisecond)
	if err == nil {
		t.Fatalf("ClientNegotiate 未返回错误，期望半读场景报错；version=%v", version)
	}
	if version == negotiate.VersionV1 {
		t.Fatalf("半读场景被误判为 VersionV1（应报错，不应静默降级）")
	}
}

// TestClientNegotiate_MagicMatchesUnsupportedVersion_ReturnsError 覆盖
// SniffUnsupportedVersion 分支：magic 字节匹配但 version 不是本实现认识的
// VersionV2，必须报协议错误，不能塌缩成"当作 v1 处理"（那样会让未来的
// v3 服务端看起来像"降级成功"，而实际上双方根本不兼容）。
func TestClientNegotiate_MagicMatchesUnsupportedVersion_ReturnsError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer l.Close()

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// magic+ver 数值 = magic<<8|version = 0xBA03，LE 存储：[0]=0x03(version), [1]=0xBA(magic)。
		_, _ = conn.Write([]byte{0x03, codec.MagicV2})
	}()

	conn, err := net.DialTimeout("tcp", l.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()

	version, _, err := negotiate.ClientNegotiate(conn, time.Second)
	if err == nil {
		t.Fatalf("ClientNegotiate 未返回错误，期望版本不兼容报错；version=%v", version)
	}
	if !errors.Is(err, negotiate.ErrProtocol) {
		t.Fatalf("错误=%v，期望包裹 negotiate.ErrProtocol", err)
	}
	if version == negotiate.VersionV1 {
		t.Fatalf("magic 匹配但版本不兼容的场景被误判为 VersionV1（应报错，不应静默降级为 v1）")
	}
}

// serveOneHelloV2 接受一条连接，用 v1 codec 读出探测帧，确认是 HELLO 探测
// 后回复 negotiate.BuildHelloResponseV2 构造的 v2 响应帧。
func serveOneHelloV2(l net.Listener) error {
	conn, err := l.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}

	msg, err := codec.NewDataPack().Decode(conn, 0, nil)
	if err != nil {
		return err
	}
	if msg.MsgID() != proto.MsgHello {
		return errors.New("serveOneHelloV2: 收到的不是 HELLO 探测帧")
	}

	resp, err := negotiate.BuildHelloResponseV2()
	if err != nil {
		return err
	}
	_, err = conn.Write(resp)
	return err
}

func encodePutPayload(t *testing.T, key, value string) []byte {
	t.Helper()
	k, v := []byte(key), []byte(value)
	buf := make([]byte, 8+len(k)+len(v))
	putUint32LE(buf[0:4], uint32(len(k)))
	putUint32LE(buf[4:8], uint32(len(v)))
	copy(buf[8:], k)
	copy(buf[8+len(k):], v)
	return buf
}

func putUint32LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

// readStatus 读一帧 v1 响应并解出 [statusLen u8][status] 里的 status 字符串。
func readStatus(t *testing.T, conn net.Conn) string {
	t.Helper()
	var head [6]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		t.Fatalf("读响应头失败: %v", err)
	}
	dataLen := uint32(head[0]) | uint32(head[1])<<8 | uint32(head[2])<<16 | uint32(head[3])<<24
	idLen := uint16(head[4]) | uint16(head[5])<<8
	rest := make([]byte, int(idLen)+int(dataLen))
	if _, err := io.ReadFull(conn, rest); err != nil {
		t.Fatalf("读响应体失败: %v", err)
	}
	respData := rest[idLen:]
	if len(respData) < 1 {
		t.Fatal("响应负载为空")
	}
	n := int(respData[0])
	return string(respData[1 : 1+n])
}
