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

	seq, err := store.PutVersioned("job:status:integration_job", []byte(`{"phase":"succeeded"}`), EngineSource)
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

// TestReconciler_OverRealV2Store 是"同一 Job 重跑一万次，结果与账本一致"
// 这条验收标准对真实账本的版本：真实 v2 协议 + 真实 service.TemporalStore
// 语义（seqCache/seqLocks/两写崩溃安全顺序/:current 指针）+ 假 Clock/
// Executor（不需要真的起子进程）。TestReconcile_TenThousandRerunsAreIdempotent
// 用 fakeStore 证明的是"reconcile 决策逻辑本身幂等"；本测试额外证明"这套
// 决策接到真实 PUT_VERSIONED/GET_AS_OF/LIST_VERSIONS opcode 上、真实账本
// 的版本数不随重跑次数增长"——不是只对着自己写的内存模拟看起来幂等。
//
// 用 LIST_VERSIONS（既有 opcode，只在这里做审计断言，Reconciler 热路径
// 仍然只用 PutVersioned/GetLatest）直接查真实账本的版本数，而不是只看
// Executor 调用次数——调用次数相同也可能账本背后多写了空版本，两件事要
// 分别断言。
func TestReconciler_OverRealV2Store(t *testing.T) {
	addr := startTestKairosFluxServer(t)
	store := NewV2Store(addr, 3*time.Second)
	defer store.Close()

	clock := newFakeClock(fixedTestTime())
	exec := newCountingExecutor(ExecResult{ExitCode: 0})
	r := &Reconciler{Store: store, Executor: exec, Clock: clock, AlertSink: &nullAlertSink{}}
	spec := testSpec("real_proto_job")

	const reruns = 10000
	start := time.Now()
	for i := 0; i < reruns; i++ {
		if _, err := r.Reconcile(spec); err != nil {
			t.Fatalf("第 %d 次 Reconcile 出错: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("对真实 v2 协议服务端重跑 %d 次真实耗时: %s", reruns, elapsed)

	if got := exec.callCount(); got != 1 {
		t.Fatalf("Executor 应只被调用 1 次，实际 %d 次", got)
	}

	statusVersions, err := store.ListVersions(StatusKey(spec.Name))
	if err != nil {
		t.Fatalf("LIST_VERSIONS(job:status) 出错: %v", err)
	}
	if len(statusVersions) != 2 {
		t.Fatalf("真实账本里 job:status:%s 应恰好 2 条版本（①running + ④终态），实际 %d 条", spec.Name, len(statusVersions))
	}
	eventVersions, err := store.ListVersions(EventsKey(spec.Name))
	if err != nil {
		t.Fatalf("LIST_VERSIONS(job:events) 出错: %v", err)
	}
	if len(eventVersions) != 1 {
		t.Fatalf("真实账本里 job:events:%s 应恰好 1 条版本，实际 %d 条", spec.Name, len(eventVersions))
	}

	// 键空间实测样例：把真实账本里存的那一条版本原样打进测试日志（不是
	// 靠描述"应该长什么样"，是真的从 GET_AS_OF/LIST_VERSIONS 读回来的字节）。
	t.Logf("键空间实测样例 job:status:%s seq=%d payload=%s",
		spec.Name, statusVersions[0].Seq, statusVersions[0].Payload)
	t.Logf("键空间实测样例 job:events:%s:v%020d payload=%s",
		spec.Name, eventVersions[0].Seq, eventVersions[0].Payload)
}
