package kairnet_test

// 畸形帧测试集：对一个真实运行的 kairnet 服务端逐个打这些场景，断言服务端优雅
// 拒绝（关连接/丢弃/记日志）且进程不死、其它连接不受影响。这是 A 段审计的
// 验收核心——"进程不死"不是靠断言 recover 计数，而是靠这些测试本身如果真的
// panic 会让整个 go test 进程连带崩溃、后面的用例全部拿不到结果，用测试运行
// 本身的生死作为最终判据。

import (
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/handler"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// echoHandler 记录收到的每一帧（msgID + data 快照）供断言，并回一个固定 OK 响应
// 证明请求确实走到了业务层、且业务层处理之后连接仍然完好可用。
type echoHandler struct {
	handler.BaseRouter
	mu   sync.Mutex
	seen []seenFrame
}

type seenFrame struct {
	msgID string
	data  []byte
}

func (h *echoHandler) Handle(req kairnet.Request) {
	h.mu.Lock()
	h.seen = append(h.seen, seenFrame{msgID: req.MsgID(), data: append([]byte(nil), req.MsgData()...)})
	h.mu.Unlock()
	req.Conn().SendMsg(proto.MsgRespOK, []byte("ok"))
}

func (h *echoHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.seen)
}

// startMalformedTestServer 起一个真实服务端，PUT 路由挂 echoHandler。
func startMalformedTestServer(t *testing.T) (addr string, h *echoHandler) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	addr = ln.Addr().String()
	ln.Close()

	h = &echoHandler{}
	srv := kairnet.NewServer()
	srv.IP, srv.Port = host, port
	srv.AddRouter(proto.MsgPut, h)
	srv.Start()
	t.Cleanup(srv.Stop)

	waitMalformedServerReady(t, addr)
	return addr, h
}

func waitMalformedServerReady(t *testing.T, addr string) {
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

// assertServerStillHealthy 用一条全新连接跑一次完整合法 PUT 往返，证明前面的
// 畸形输入没有把服务端拖垮——这是"进程不死、服务仍可用"的直接证据，而不只是
// "这个 goroutine 没崩"。
func assertServerStillHealthy(t *testing.T, addr string, h *echoHandler) {
	t.Helper()
	before := h.count()

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("服务端已不可连接，很可能已被前面的畸形输入拖垮: %v", err)
	}
	defer conn.Close()

	key, value := []byte("healthcheck"), []byte("ok")
	putPayload := make([]byte, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(putPayload[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(putPayload[4:8], uint32(len(value)))
	copy(putPayload[8:], key)
	copy(putPayload[8+len(key):], value)

	head := make([]byte, 6)
	binary.LittleEndian.PutUint32(head[0:4], uint32(len(putPayload)))
	binary.LittleEndian.PutUint16(head[4:6], uint16(len(proto.MsgPut)))
	frame := append(head, append([]byte(proto.MsgPut), putPayload...)...)

	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("健康检查写入失败: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp := make([]byte, 64)
	n, err := conn.Read(resp)
	if err != nil {
		t.Fatalf("健康检查未收到响应，服务端可能已不可用: %v", err)
	}
	if n == 0 {
		t.Fatal("健康检查收到空响应")
	}
	if h.count() != before+1 {
		t.Fatalf("健康检查请求未被业务层处理，count 应从 %d 变为 %d，实际 %d", before, before+1, h.count())
	}
}

// 场景 1：截断帧——只发 3 字节头部（6 字节头部声称需要）后立即断连。
func TestMalformedFrame_TruncatedHeader(t *testing.T) {
	addr, h := startMalformedTestServer(t)

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Write([]byte{0x01, 0x02, 0x03}) // 只有 6 字节头部的一半
	conn.Close()                         // 立即断连，服务端应从 io.ReadFull 拿到 EOF/ErrUnexpectedEOF

	time.Sleep(100 * time.Millisecond) // 给服务端一点时间处理断连
	assertServerStillHealthy(t, addr, h)
}

// 场景 2：超长声明——声称的负载远超硬上限，即便 MaxPackageSize 被配成 0
// （运维本意"不限制"）也必须被 hardMaxPackageSize 兜底拒绝，不能真的去分配。
func TestMalformedFrame_OversizedWithZeroConfiguredLimitStillCapped(t *testing.T) {
	oldMax := config.G.MaxPackageSize
	config.G.MaxPackageSize = 0 // 显式"不限制"
	t.Cleanup(func() { config.G.MaxPackageSize = oldMax })

	addr, h := startMalformedTestServer(t)

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	head := make([]byte, 6)
	binary.LittleEndian.PutUint32(head[0:4], 300<<20) // 300MiB，超过 256MiB 硬上限
	binary.LittleEndian.PutUint16(head[4:6], uint16(len(proto.MsgPut)))
	conn.Write(append(head, []byte(proto.MsgPut)...)) // 只发头部，不发 300MiB 负载

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("超限帧后连接仍可读，应已被服务端拒绝并关闭")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("服务端既未拒绝也未关闭连接——MaxPackageSize=0 时的硬上限可能没生效")
	}

	assertServerStillHealthy(t, addr, h)
}

// 场景 3：零长帧——dataLen=0、idLen=0，是合法但空洞的一帧（无 msgID、无负载）。
// 不应被当成错误：连接应保持可用，只是这一帧因为 msgID 未注册（空字符串）
// 不会被分派到任何 Handler。
func TestMalformedFrame_ZeroLengthFrame(t *testing.T) {
	addr, h := startMalformedTestServer(t)

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	head := make([]byte, 6) // 全零：dataLen=0, idLen=0
	if _, err := conn.Write(head); err != nil {
		t.Fatalf("写入零长帧失败: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	assertServerStillHealthy(t, addr, h)
}

// 场景 4：非法 msgID——idLen 声明的字节是非 UTF-8 的乱码，且不匹配任何已注册路由。
func TestMalformedFrame_IllegalMsgID(t *testing.T) {
	addr, h := startMalformedTestServer(t)

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	garbage := []byte{0x00, 0xFF, 0xFE, 0x01} // 非法 UTF-8、不对应任何 msgID
	head := make([]byte, 6)
	binary.LittleEndian.PutUint16(head[4:6], uint16(len(garbage)))
	if _, err := conn.Write(append(head, garbage...)); err != nil {
		t.Fatalf("写入非法 msgID 帧失败: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	assertServerStillHealthy(t, addr, h)
}

// 场景 5：乱码 payload——msgID 合法（PUT，已注册路由），但负载是随机二进制垃圾，
// 不符合任何已知的内部格式（key_len+key+value_len+value）。kairnet 自身不解析
// PUT 负载内部结构（那是 service/ingesthook 的职责），只负责按 dataLen 原样
// 把字节交给 Handler——这里验证的是 kairnet 这一层对任意字节内容都不会出错。
func TestMalformedFrame_GarbledPayload(t *testing.T) {
	addr, h := startMalformedTestServer(t)

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	garbage := bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 16)
	head := make([]byte, 6)
	binary.LittleEndian.PutUint32(head[0:4], uint32(len(garbage)))
	binary.LittleEndian.PutUint16(head[4:6], uint16(len(proto.MsgPut)))
	frame := append(head, append([]byte(proto.MsgPut), garbage...)...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("写入乱码负载帧失败: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp := make([]byte, 64)
	if _, err := conn.Read(resp); err != nil {
		t.Fatalf("乱码负载应被正常分派给 Handler 并收到响应，得到错误: %v", err)
	}
	if h.count() != 1 {
		t.Fatalf("乱码负载应被分派到 Handler 一次，得到 count=%d", h.count())
	}

	assertServerStillHealthy(t, addr, h)
}

// 场景 6：半帧后断连——头部与 msgID 都发完，负载只发一半就断连。
func TestMalformedFrame_HalfPayloadThenDisconnect(t *testing.T) {
	addr, h := startMalformedTestServer(t)

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	fullPayload := bytes.Repeat([]byte{0x01}, 100)
	head := make([]byte, 6)
	binary.LittleEndian.PutUint32(head[0:4], uint32(len(fullPayload))) // 声称 100 字节
	binary.LittleEndian.PutUint16(head[4:6], uint16(len(proto.MsgPut)))
	conn.Write(head)
	conn.Write([]byte(proto.MsgPut))
	conn.Write(fullPayload[:50]) // 只发一半
	conn.Close()                 // 断连，服务端应从 io.ReadFull(data) 拿到 ErrUnexpectedEOF

	time.Sleep(100 * time.Millisecond)
	assertServerStillHealthy(t, addr, h)
}

// 场景 7（读超时）：半个头部后保持连接打开、什么也不发——没有第 2 步"断连"这个
// 显式信号，服务端必须靠读超时主动回收，否则这类连接会永久占住一个 goroutine 与
// 一个 MaxConn 名额。用短超时验证行为，不等默认的 30s。
func TestMalformedFrame_SlowClientSilentHalfHeader_TimesOut(t *testing.T) {
	oldTimeout := config.G.ConnReadTimeoutMs
	config.G.ConnReadTimeoutMs = 300 // 300ms，测试用短超时
	t.Cleanup(func() { config.G.ConnReadTimeoutMs = oldTimeout })

	addr, h := startMalformedTestServer(t)

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte{0x01, 0x02, 0x03}) // 半个头部，之后彻底沉默、不断连

	// 服务端应在 ConnReadTimeoutMs 左右主动关闭连接：对端（这条 conn）应观察到
	// EOF/RST，而不是永远悬着。给够裕量（超时值的 3 倍）避免测试本身抖动误判。
	conn.SetReadDeadline(time.Now().Add(1200 * time.Millisecond))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("慢客户端连接应已被服务端读超时关闭")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("服务端未在读超时窗口内关闭沉默连接——ConnReadTimeoutMs 可能没有生效")
	}

	assertServerStillHealthy(t, addr, h)
}
