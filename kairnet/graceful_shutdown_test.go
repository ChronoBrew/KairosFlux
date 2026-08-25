package kairnet_test

// bug①的客户端可观测回归测试：Server.Stop() 此前不等在途请求处理完，
// 一个正在 Handle() 里执行业务逻辑的请求，其响应会因为连接已经被强制关闭
// 而发不出去——见 docs/rfc/bannet-重构.md B.2 记录的这个真实竞态窗口。
//
// 只断言"Stop 返回了""状态机到 Closed 了"不足以验证这个 bug 真的被修了：
// 那些断言在 bug 仍然存在的情况下也会通过（Stop 之前就是立刻返回的）。
// 唯一有区分力的断言是"客户端真的收到了完整的响应帧"——这正是本文件要做的：
// 用一个会阻塞的 Handler 制造"请求已经在处理、但还没写响应"这个时间窗口，
// 在这个窗口内触发 Server.Stop()，再放行 Handler，最后从真实的客户端
// socket 上读一个完整的响应帧。

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/handler"
	"github.com/ChronoBrew/KairosFlux/proto"
)

type blockingUntilReleaseHandler struct {
	handler.BaseRouter
	release  chan struct{}
	entered  chan struct{}
	respID   string
	respData []byte
}

func (h *blockingUntilReleaseHandler) Handle(req kairnet.Request) {
	close(h.entered)
	<-h.release
	// 故意在放行之后才真正产出响应——这正是要保护的时间窗口：Server.Stop()
	// 在此期间已经开始跑，响应现在才被投递到 msgChan。
	_ = req.Conn().SendMsg(h.respID, h.respData)
}

// TestGracefulStopDeliversInFlightResponse 是 bug①的核心回归：Server.Stop()
// 期间一个正在处理中的请求，其响应必须真的送达客户端，不能因为连接被提前
// 物理关闭而丢失。
func TestGracefulStopDeliversInFlightResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	addr := ln.Addr().String()
	ln.Close()

	h := &blockingUntilReleaseHandler{
		release:  make(chan struct{}),
		entered:  make(chan struct{}),
		respID:   proto.MsgRespOK,
		respData: []byte("in-flight response must not be lost"),
	}
	srv := kairnet.NewServer()
	srv.IP, srv.Port = host, port
	srv.AddRouter(proto.MsgPut, h)
	srv.Start()

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

	// 发一个合法的 PUT 请求，触发 blockingUntilReleaseHandler.Handle。
	head := make([]byte, 6)
	head[4] = byte(len(proto.MsgPut))
	if _, err := conn.Write(append(head, []byte(proto.MsgPut)...)); err != nil {
		t.Fatalf("写入请求失败: %v", err)
	}

	// 等 Handle 真的进入并卡在 release 上。
	select {
	case <-h.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Handle 从未被调用")
	}

	// 此时请求正"在途"：已经被分派、正在执行，但响应还没产出。在这个窗口内
	// 触发 Server.Stop()——这正是 bug① 描述的竞态场景。Stop 现在会阻塞
	// （等这个连接优雅收尾），所以放到另一个 goroutine 里跑。
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.Stop()
	}()

	// 给 Stop 一点时间真正跑起来（进入 ConnMgr.Wait 的等待阶段），再放行
	// Handler——模拟"请求处理在关闭流程已经开始之后才完成"。
	time.Sleep(100 * time.Millisecond)
	close(h.release)

	// 核心断言：客户端必须能读到一个完整、正确的响应帧，而不是连接被
	// 提前关闭导致的 EOF/reset。用 codec.Decode 直接从真实 socket 上解码，
	// 与生产环境客户端读响应的路径完全一致。
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	dp := codec.NewDataPack()
	msg, err := dp.Decode(conn, 0, nil)
	if err != nil {
		t.Fatalf("未能读到完整响应帧（bug①未修复的话，Stop 会提前关闭连接导致这里失败）: %v", err)
	}
	if msg.MsgID() != h.respID {
		t.Fatalf("响应 msgID = %q, want %q", msg.MsgID(), h.respID)
	}
	if string(msg.Payload()) != string(h.respData) {
		t.Fatalf("响应负载 = %q, want %q", msg.Payload(), h.respData)
	}

	wg.Wait() // Stop 应该能正常返回，不应该因为这次交互而卡住/永远等待
}
