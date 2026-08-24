package jobctl

// V2Store 端到端集成测试：起一个真实的 KairosFlux v2 服务端（与
// cmd/kairosflux-server 同样的接线），证明 jobctl.Reconciler 真的能通过
// PUT_VERSIONED/GET_AS_OF opcode 操作 job: 键空间——单元测试（reconciler_
// test.go）用 fakeStore 验证的是"决策逻辑幂等"，这里额外证明"决策逻辑接到
// 真实协议上也是同一套行为"，两者互补、不重复。
//
// 服务端起法复制自 service/router_v2_integration_test.go 的
// startRouterV2TestServer（本包在 service 包之外，用它导出的
// NewKVServer/NewRouter/NewRouterV2 等构造函数自己拼一遍，不能直接调用那
// 个测试专用的非导出函数）。

import (
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
)

func startTestKairosFluxServer(t *testing.T) string {
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
	filter := ingesthook.NewFilter(nil, 0, false)
	router.SetPreHandle(filter.Handle)
	routerV2 := service.NewRouterV2(kv, filter, service.DefaultV2WindowSafetyValveN)

	srv := kairnet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddRouter(proto.MsgPut, router)
	srv.AddRouter(proto.MsgGet, router)
	srv.AddRouter(proto.MsgDelete, router)
	srv.AddRouter(proto.MsgScan, router)
	srv.AddRouterV2(routerV2)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("服务端在 2s 内未就绪: %s", addr)
	return ""
}

func TestV2Store_PutVersionedThenGetLatestRoundTrip(t *testing.T) {
	addr := startTestKairosFluxServer(t)
	store := NewV2Store(addr, 3*time.Second)
	defer store.Close()

	seq, err := store.PutVersioned("job:status:integration_job", []byte(`{"phase":"succeeded"}`))
	if err != nil {
		t.Fatalf("PutVersioned 失败: %v", err)
	}
	if seq != 1 {
		t.Fatalf("首次写入 seq 应为 1，实际 %d", seq)
	}

	payload, found, err := store.GetLatest("job:status:integration_job")
	if err != nil {
		t.Fatalf("GetLatest 失败: %v", err)
	}
	if !found {
		t.Fatal("刚写入的键应能读到")
	}
	if string(payload) != `{"phase":"succeeded"}` {
		t.Fatalf("读回内容不符: %s", payload)
	}

	_, found, err = store.GetLatest("job:status:never_written")
	if err != nil {
		t.Fatalf("GetLatest 对未写入键出错: %v", err)
	}
	if found {
		t.Fatal("未写入的键应返回 found=false")
	}
}

// TestReconciler_OverRealV2Store 端到端跑一次完整的 Reconciler.Reconcile：
// 真实 v2 协议 + 真实 TemporalStore 语义 + 假 Clock/Executor（不需要真的
// 起子进程）。证明 reconciler.go 的读写路径与真实 opcode 字节格式对得上，
// 不是只在 fakeStore 的内存模拟里"看起来幂等"。
func TestReconciler_OverRealV2Store(t *testing.T) {
	addr := startTestKairosFluxServer(t)
	store := NewV2Store(addr, 3*time.Second)
	defer store.Close()

	clock := newFakeClock(fixedTestTime())
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}
	spec := testSpec("real_proto_job")

	for i := 0; i < 50; i++ { // 真实网络往返较慢，50 次足够证明幂等，不需要一万次
		if _, err := r.Reconcile(spec); err != nil {
			t.Fatalf("第 %d 次 Reconcile 出错: %v", i, err)
		}
	}
	if got := exec.callCount(); got != 1 {
		t.Fatalf("Executor 应只被调用 1 次，实际 %d 次", got)
	}
}
