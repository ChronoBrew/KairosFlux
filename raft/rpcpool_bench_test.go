package raft

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

// echo RPC 服务：用于量化 rpc.Dial-per-call 与连接池复用的开销差。
type EchoArgs struct{ X int }
type EchoReply struct{ X int }
type echoSvc struct{}

func (echoSvc) Echo(a *EchoArgs, r *EchoReply) error { r.X = a.X; return nil }

func startEchoServer(tb testing.TB) (addr string, stop func()) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Echo", echoSvc{}); err != nil {
		tb.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	go srv.Accept(ln)
	return ln.Addr().String(), func() { ln.Close() }
}

// BenchmarkRPC_DialPerCall：每次调用都 Dial+Call+Close（当前 Send*/callPropose 的做法）。
func BenchmarkRPC_DialPerCall(b *testing.B) {
	addr, stop := startEchoServer(b)
	defer stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			b.Fatal(err)
		}
		var r EchoReply
		_ = c.Call("Echo.Echo", &EchoArgs{X: i}, &r)
		c.Close()
	}
}

// BenchmarkRPC_Pooled：复用每对端一个 *rpc.Client。
func BenchmarkRPC_Pooled(b *testing.B) {
	addr, stop := startEchoServer(b)
	defer stop()
	pool := newRPCPool(5 * time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r EchoReply
		_ = pool.call(addr, "Echo.Echo", &EchoArgs{X: i}, &r)
	}
}

// 并发场景（更贴近多分片 Raft）：多 goroutine 打同一对端。
func BenchmarkRPC_DialPerCall_Parallel(b *testing.B) {
	addr, stop := startEchoServer(b)
	defer stop()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c, err := rpc.Dial("tcp", addr)
			if err != nil {
				b.Error(err)
				return
			}
			var r EchoReply
			_ = c.Call("Echo.Echo", &EchoArgs{X: 1}, &r)
			c.Close()
		}
	})
}

func BenchmarkRPC_Pooled_Parallel(b *testing.B) {
	addr, stop := startEchoServer(b)
	defer stop()
	pool := newRPCPool(5 * time.Second)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var r EchoReply
			_ = pool.call(addr, "Echo.Echo", &EchoArgs{X: 1}, &r)
		}
	})
}
