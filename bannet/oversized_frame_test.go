package bannet_test

import (
	"encoding/binary"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/proto"
)

// countingHandler 记录被分派到的请求数，用于断言超限帧未进入业务处理。
type countingHandler struct {
	bannet.BaseRouter
	handled chan struct{}
}

func (h *countingHandler) Handle(bannet.Request) {
	select {
	case h.handled <- struct{}{}:
	default:
	}
}

// TestOversizedFrameRejectedBeforeReadingPayload 守护帧长上限的执行点。
//
// 该校验此前在 DataPack.UnPack 内，每帧读两次全局配置；现已移到连接侧（读取负载之前），
// 让编解码器保持无状态。移动执行点意味着必须有测试证明它仍在执行——否则一个恶意或损坏的
// 帧头就能让服务端按对端声称的长度分配内存。
//
// 构造方式：只发一个声称负载极大的帧头，不发负载。服务端必须在读取负载前就断开连接，
// 且该帧不得被分派给业务处理。
func TestOversizedFrameRejectedBeforeReadingPayload(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	addr := ln.Addr().String()
	ln.Close()

	oldMax := config.G.MaxPackageSize
	config.G.MaxPackageSize = 1024
	t.Cleanup(func() { config.G.MaxPackageSize = oldMax })

	h := &countingHandler{handled: make(chan struct{}, 1)}
	srv := bannet.NewServer()
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

	// 帧头: [dataLen u32 LE][idLen u16 LE]，声称 64MiB 负载但一个字节都不发。
	head := make([]byte, 6)
	binary.LittleEndian.PutUint32(head[0:4], 64<<20)
	binary.LittleEndian.PutUint16(head[4:6], uint16(len(proto.MsgPut)))
	if _, err := conn.Write(append(head, []byte(proto.MsgPut)...)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// 服务端应断开连接：读将返回 EOF/RST，而不是一直等着那 64MiB。
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("超限帧后连接仍可读，应已被服务端关闭")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("服务端既未拒绝也未关闭连接——很可能正按声称的长度等待/分配 64MiB")
	}

	select {
	case <-h.handled:
		t.Fatal("超限帧不应被分派到业务处理")
	default:
	}
}
