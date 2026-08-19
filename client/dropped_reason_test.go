package client_test

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/client"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/proto"
	"github.com/NeverENG/BanDB/service"
	"github.com/NeverENG/BanDB/service/ingesthook"
)

// startServerWithFilter 与 startServer 相同，但额外挂载一个 ingesthook.Filter——
// 用于测试落盘前清洗（含 schema 校验）拒绝时，reason 是否正确回传给客户端。
func startServerWithFilter(t *testing.T, filter *ingesthook.Filter) string {
	t.Helper()

	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = dir + "/wal.log"
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
	router.SetPreHandle(filter.Handle)

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

	waitListening(t, addr)
	return addr
}

// TestPut_DroppedByFilterCarriesReason 端到端验证 QuantScout 反馈的缺口已修复：
// 被落盘前清洗（这里用 quote: schema 校验）拒绝的写入，Go 客户端不仅收到
// ErrDropped，还能从错误信息里读到具体原因——不再只知道"dropped"、猜不出为什么。
func TestPut_DroppedByFilterCarriesReason(t *testing.T) {
	filter := ingesthook.NewFilter(nil, 0, false)
	addr := startServerWithFilter(t, filter)
	c := newClient(t, addr)

	// 非正价格：quote schema 校验器应拒绝，reason 里应能看到具体字段与原因。
	key := []byte("quote:2026-08-17:600000")
	value := []byte(`{"code":"600000","date":"2026-08-17","open":-1,"high":10.5,"low":9.8,"close":10.2,"volume":1000000}`)

	err := c.Put(context.Background(), key, value)
	if err == nil {
		t.Fatal("非法行情记录应被拒绝，Put 不应成功")
	}
	if !errors.Is(err, client.ErrDropped) {
		t.Fatalf("错误应为 ErrDropped，得到: %v", err)
	}
	if !strings.Contains(err.Error(), "non-positive price") {
		t.Fatalf("错误信息应携带 schema 校验的具体原因，得到: %v", err)
	}
}

// TestPut_MalformedFrameDroppedCarriesReason 验证畸形帧（非 schema 校验）的丢弃
// 同样带 reason——本轮修复覆盖的是 Handle 的所有丢弃分支，不只是 schema 分支。
func TestPut_MalformedFrameDroppedCarriesReason(t *testing.T) {
	filter := ingesthook.NewFilter(nil, 4 /* maxValueLen */, false)
	addr := startServerWithFilter(t, filter)
	c := newClient(t, addr)

	err := c.Put(context.Background(), []byte("k1"), []byte("toolong-value"))
	if err == nil {
		t.Fatal("超限 value 应被拒绝")
	}
	if !errors.Is(err, client.ErrDropped) {
		t.Fatalf("错误应为 ErrDropped，得到: %v", err)
	}
	if !strings.Contains(err.Error(), "oversized_value") {
		t.Fatalf("错误信息应携带 oversized_value 原因，得到: %v", err)
	}
}
