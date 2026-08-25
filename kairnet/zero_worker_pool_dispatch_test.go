package kairnet_test

// bug②的回归测试：workerPoolSize==0（即不启用 worker 池，"退化模式"）时，
// 此前 connection.go 的 StartReader 会对每一帧成功解出的请求都
// `go c.MsgHandle.DoMsgHandle(req)`——每帧一个不受任何对象追踪、没有任何
// 上限的临时 goroutine，见 docs/rfc/bannet-重构.md B.4："这类 goroutine
// 数量不设上限...是潜在的 goroutine 数量爆炸风险"。重构第三步（拆 dispatch
// 包）把这条路径去掉了：workerPoolSize==0 时统一走
// dispatch.MsgHandle.SendMsgToTaskQueue，它在调用方（即该连接的读循环）
// 所在的 goroutine 上同步执行 DoMsgHandle，不新增任何 goroutine。
//
// 直接断言"没有新增 goroutine"容易受 GC/调度时机影响而不可靠（哪怕 bug
// 仍在，如果这些一次性 goroutine 执行得足够快，采样时可能已经退出）。
// 用一个会阻塞的 Handler 做确定性验证更可靠：修复前，reader 循环不等
// DoMsgHandle 完成就继续读下一帧、再起一个新 goroutine，于是发送 N 个
// 请求后应该能观察到 N 个并发阻塞在 Handle() 里的调用；修复后，reader
// 循环必须等前一个请求的 DoMsgHandle 同步跑完才能读下一帧，所以任意时刻
// 至多只有 1 个请求处于 Handle() 中——这个"至多 1 个并发"的性质，只有在
// bug 真的被修复的情况下才会成立，是比"goroutine 数没涨"更强、更不依赖
// 时机的断言。

import (
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/handler"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// blockingHandler 的 Handle 会把执行卡在 release 被 close 之前，同时记录
// "同时有多少个 Handle 调用正在阻塞里"的峰值，用于证明请求是否被串行处理。
type blockingHandler struct {
	handler.BaseRouter
	release   chan struct{}
	inFlight  atomic.Int32
	peak      atomic.Int32
	completed atomic.Int32
}

func (h *blockingHandler) Handle(kairnet.Request) {
	cur := h.inFlight.Add(1)
	for {
		p := h.peak.Load()
		if cur <= p || h.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	<-h.release
	h.inFlight.Add(-1)
	h.completed.Add(1)
}

// TestZeroWorkerPoolSizeDoesNotConcurrentlyDispatch 是 bug②的直接回归：
// workerPoolSize==0 时，同一连接上背靠背发送的多个请求不应该被并发分派到
// Handle——它们必须排队等前一个跑完，因为分发现在绑定在连接读循环这一个
// goroutine 上，不再各自起一个不受追踪的临时 goroutine。
func TestZeroWorkerPoolSizeDoesNotConcurrentlyDispatch(t *testing.T) {
	oldPool := config.G.WorkerPoolSize
	config.G.WorkerPoolSize = 0
	t.Cleanup(func() { config.G.WorkerPoolSize = oldPool })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	addr := ln.Addr().String()
	ln.Close()

	h := &blockingHandler{release: make(chan struct{})}
	srv := kairnet.NewServer()
	srv.IP, srv.Port = host, port
	srv.AddRouter(proto.MsgPut, h)
	srv.Start()
	t.Cleanup(srv.Stop)

	var conn net.Conn
	for i := 0; i < 100; i++ {
		if conn, err = net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("服务端未就绪: %v", err)
	}
	defer conn.Close()

	const n = 20
	frame := make([]byte, 6) // dataLen=0, idLen=3("PUT")
	frame[4] = 3
	frame = append(frame, []byte(proto.MsgPut)...)
	var allFrames []byte
	for i := 0; i < n; i++ {
		allFrames = append(allFrames, frame...)
	}
	if _, err := conn.Write(allFrames); err != nil {
		t.Fatalf("写入 %d 个背靠背请求失败: %v", n, err)
	}

	// 等第一个请求进入 Handle（此时它会卡在 release 上）。
	for i := 0; i < 100; i++ {
		if h.inFlight.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h.inFlight.Load() == 0 {
		t.Fatal("发出的请求似乎从未被分派到 Handle")
	}

	// 再等一小段时间，让"如果 bug 还在"的情况下有机会把其余请求也并发分派进来
	// （bug 存在时，reader 不等 Handle 完成就继续读下一帧并另起 goroutine）。
	time.Sleep(200 * time.Millisecond)

	if peak := h.peak.Load(); peak != 1 {
		t.Fatalf("同一时刻处于 Handle() 中的请求数峰值 = %d，want 1"+
			"（workerPoolSize==0 时请求应严格排队，不应并发分派——"+
			"峰值 > 1 说明 bug②又回来了：每帧一个未受追踪的临时 goroutine）", peak)
	}

	close(h.release) // 放行，让所有请求排队跑完

	for i := 0; i < 200; i++ {
		if h.completed.Load() == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := h.completed.Load(); got != n {
		t.Fatalf("完成的请求数 = %d，want %d（全部 %d 个请求都应该最终被处理，只是不并发）", got, n, n)
	}
}
