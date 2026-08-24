package kairnet_test

// 两个真实 panic 的回归测试。审计时用最小复现验证过：修复前，两者都会让整个
// 进程崩溃（不是仅这一个 goroutine）——TestHandlerPanicRecovered 尤其关键，
// 复现的是"业务 Handler 里任意一个 bug 就打崩整个服务"这条最高优先级发现。

import (
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/proto"
)

type panickingHandler struct {
	kairnet.BaseRouter
	calls atomic.Int32
}

func (h *panickingHandler) Handle(kairnet.Request) {
	h.calls.Add(1)
	panic("simulated bug in business handler: nil pointer or whatever")
}

// TestHandlerPanicRecovered 复现并锁定最高优先级的修复：业务 Handler.Handle
// 里任意一个 panic，此前会顺着 goroutine 调用栈一路冒到 Go 运行时、终止整个
// 进程（已用故意 panic 的 Handler 验证过，见迭代记录）。现在 dispatch.MsgHandle.
// DoMsgHandle 有 recover 兜底：这个 goroutine（worker 池的常驻 worker，或
// workerPoolSize==0 时同步执行 DoMsgHandle 的连接读循环 goroutine——重构后
// 不再有"无 worker 池时的一次性 go DoMsgHandle"这条路径，见
// kairnet/dispatch/dispatch.go 的 SendMsgToTaskQueue 注释）会记录 panic 并
// 继续存活，其它连接完全不受影响。用真实 TCP 服务端而非直接调用
// DoMsgHandle，是因为回归的正是"进程会不会被打死"这件事，必须让 panic
// 真的在独立 goroutine 里发生。
func TestHandlerPanicRecovered(t *testing.T) {
	before := metrics.PanicsRecovered.Load()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	addr := ln.Addr().String()
	ln.Close()

	h := &panickingHandler{}
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

	head := make([]byte, 6) // dataLen=0, idLen=3("PUT")
	head[4] = 3
	if _, err := conn.Write(append(head, []byte(proto.MsgPut)...)); err != nil {
		t.Fatalf("写入触发 panic 的请求失败: %v", err)
	}
	conn.Close()

	// 轮询等 Handler 被调用（不能靠固定 sleep 猜时机，worker 池是异步分派的）。
	for i := 0; i < 100; i++ {
		if h.calls.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if h.calls.Load() == 0 {
		t.Fatal("触发 panic 的请求似乎从未被分派到 Handler")
	}

	// 如果这一行能跑到，就证明进程在 Handler panic 之后仍然存活——这正是回归点：
	// 修复前，上面那次 panic 会直接终止整个 go test 进程，这个测试函数本身
	// 都不会有机会执行到这里报告失败，而是让整条测试运行连带崩溃。
	if got := metrics.PanicsRecovered.Load() - before; got < 1 {
		t.Fatalf("PanicsRecovered 应至少 +1（业务 Handler 的 panic 被 DoMsgHandle 捕获），得到增量 %d", got)
	}

	// 服务端应仍然接受新连接——不只是"没有整体崩溃"，是"还能正常服务"。
	conn2, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("panic 之后服务端应仍可连接，得到错误: %v", err)
	}
	conn2.Close()
}

// divide-by-zero 的精确复现（workerPoolSize==0 时 SendMsgToTaskQueue 的取模操作）
// 需要直接构造未导出字段，随 msghandle.go 迁入 dispatch 包后见
// kairnet/dispatch/dispatch_internal_test.go 的
// TestSendMsgToTaskQueue_ZeroWorkerPoolSizeNoPanic。
//
// 每帧一次性 goroutine（曾经的 bug②：workerPoolSize==0 时 `go DoMsgHandle`
// 不受追踪、无上限）的回归锁定见本文件下方的
// TestZeroWorkerPoolSizeDoesNotLeakGoroutines。
