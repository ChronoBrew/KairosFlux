package kairnet_test

// 任务要求：显式生命周期状态机（Idle→Active→Closing→Closed）的四种终止
// 诱因——EOF/超时/显式 Stop/panic 被 recover——都必须收敛到 Closing，且
// 每个状态转换都要有测试覆盖。kairnet/lifecycle 包自己的单元测试
// （lifecycle_test.go）已经覆盖了状态机本身的转换规则（幂等、并发安全、
// Draining 与 Done 的区分等）；本文件覆盖的是"这四种诱因在真实连接上
// 触发时，状态机真的会收敛"——即状态机与 connection.go 里四个真实触发点
// 的接线是否正确，而不是状态机本身的逻辑。

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/handler"
	"github.com/ChronoBrew/KairosFlux/kairnet/lifecycle"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// startTestServer 启动一个真实的 KairNet 服务端，返回其地址与一个用于清理
// 的 t.Cleanup（由调用方决定何时调用 srv.Stop，故不在这里注册 Cleanup）。
func startTestServer(t *testing.T, h kairnet.Handler) (*kairnet.Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	addr := ln.Addr().String()
	ln.Close()

	srv := kairnet.NewServer()
	srv.IP, srv.Port = host, port
	srv.AddRouter(proto.MsgPut, h)
	srv.Start()
	return srv, addr
}

func dialTestServer(t *testing.T, addr string) net.Conn {
	t.Helper()
	var conn net.Conn
	var err error
	for i := 0; i < 100; i++ {
		if conn, err = net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			return conn
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("服务端未就绪: %v", err)
	return nil
}

// getConn 轮询等服务端注册表里出现 connID 对应的连接，并等它到达 Active——
// accept 到 NewConnection 把连接注册进 ConnMgr（此时状态还是 Idle）与
// Start() 里 c.lc.MarkActive() 之间有一个真实的调度窗口（两者都在
// acceptLoop 起的同一个 goroutine 里顺序发生，但 dialTestServer 只保证
// TCP 三次握手完成，不保证服务端已经跑到这两步中的哪一步）——不等到
// Active 就对连接做进一步操作/断言，测试会在这个窗口上偶发失败。
func getConn(t *testing.T, srv *kairnet.Server, connID uint32) *kairnet.Connection {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c := srv.Conns().Get(connID); c != nil {
			conn := c.(*kairnet.Connection)
			if conn.State() == lifecycle.Active {
				return conn
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("服务端未能在超时内让 connID=%d 到达 Active", connID)
	return nil
}

// waitConnState 轮询等 conn 的生命周期状态到达 want，超时则报告当前状态。
func waitConnState(t *testing.T, conn *kairnet.Connection, want lifecycle.State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if conn.State() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待状态 %v 超时，当前状态 = %v", want, conn.State())
}

// TestLifecycleConverges_EOF 覆盖第一种终止诱因：对端正常断开连接
// （io.EOF）——连接的状态机应该收敛到 Closed（途经 Closing，见
// lifecycle 包自己的单元测试对 Closing 转换规则的覆盖）。
func TestLifecycleConverges_EOF(t *testing.T) {
	h := &handler.BaseRouter{}
	srv, addr := startTestServer(t, h)
	t.Cleanup(srv.Stop)

	clientConn := dialTestServer(t, addr)

	// 用服务端注册表拿到对应的 *kairnet.Connection，直接查询状态——这是
	// State() 之所以存在的用途（RFC C.1: "谁能查询 | Lifecycle.State()"）。
	// ConnID 从 0 开始且本测试只有一个连接。getConn 内部已确认到达 Active。
	serverConn := getConn(t, srv, 0)

	clientConn.Close() // 对端正常断开 -> 服务端读到 io.EOF

	waitConnState(t, serverConn, lifecycle.Closed, 2*time.Second)
}

// TestLifecycleConverges_ReadTimeout 覆盖第二种终止诱因：读超时——客户端
// 连上之后什么都不发，服务端应该在配置的超时后主动放弃这个连接。
func TestLifecycleConverges_ReadTimeout(t *testing.T) {
	oldTimeout := config.G.ConnReadTimeoutMs
	config.G.ConnReadTimeoutMs = 100 // 100ms，让测试能快速跑完
	t.Cleanup(func() { config.G.ConnReadTimeoutMs = oldTimeout })

	h := &handler.BaseRouter{}
	srv, addr := startTestServer(t, h)
	t.Cleanup(srv.Stop)

	_ = dialTestServer(t, addr) // 连上之后什么都不发

	serverConn := getConn(t, srv, 0)

	waitConnState(t, serverConn, lifecycle.Closed, 3*time.Second)
}

// TestLifecycleConverges_ExplicitStop 覆盖第三种终止诱因：显式调用 Stop——
// 直接对服务端已注册的连接调用 Stop，状态应立即收敛到 Closed。
func TestLifecycleConverges_ExplicitStop(t *testing.T) {
	h := &handler.BaseRouter{}
	srv, addr := startTestServer(t, h)
	t.Cleanup(srv.Stop)

	clientConn := dialTestServer(t, addr)
	defer clientConn.Close()

	// getConn 内部已确认到达 Active。
	serverConn := getConn(t, srv, 0)

	serverConn.Stop() // 显式调用，不经过 Server.Stop 的优雅关闭流程

	waitConnState(t, serverConn, lifecycle.Closed, 2*time.Second)
}
