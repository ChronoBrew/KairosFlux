//go:build pprof

package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook/schema"
)

func main() {
	// 加载数据契约，见 server.go（非 pprof 构建）同一处调用的注释——两个 main
	// 变体（//go:build pprof 互斥）在这一步必须做同样的事，不能只有其中一个
	// 强制契约校验。
	if err := schema.LoadContractsDefault(); err != nil {
		fmt.Println("[ERROR] failed to load data contracts:", err)
		os.Exit(1)
	}

	go func() {
		fmt.Println("pprof is starting")

		if err := http.ListenAndServe(":6060", nil); err != nil {
			fmt.Println("[ERROR] pprof start err:", err)
		}
	}()
	KVServer := service.NewKVServer()

	// 启动 FSM
	go KVServer.Run()

	// 初始化 HA
	ha := service.NewHA(KVServer)

	// 初始化网络服务
	server := kairnet.NewServer()

	// 创建路由
	router := service.NewRouter(KVServer)

	// 按配置开启分片路由：不属本节点的 key 经 KairNet 转发到 owner（默认关闭）。
	service.EnableShardRoutingFromConfig(router)

	// 按配置开启网关自适应准入（默认关闭）。
	service.EnableAdmissionFromConfig(router)

	// 挂载采集入口过滤钩子：落盘前丢弃畸形帧、按设备做 best-effort 时间戳
	// 单调校验、对敏感字段脱敏。
	filter := ingesthook.NewFilter([]string{"gps", "user_id"}, 0, true)
	router.SetPreHandle(filter.Handle)

	// 注册路由
	server.AddRouter(proto.MsgPut, router)
	server.AddRouter(proto.MsgGet, router)
	server.AddRouter(proto.MsgDelete, router)
	server.AddRouter(proto.MsgScan, router)

	// 注册连接生命周期回调
	server.SetConnStartFunc(router.OnConnStart)
	server.SetConnStopFunc(router.OnConnStop)

	// 启动周期性指标快照：headless 边缘设备 tail 日志即可观测运行状态
	metrics.StartLogger(context.Background(), 10*time.Second)

	// 启动服务
	fmt.Println("Starting Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	// 单节点下等待选主完成再开放端口，避免启动瞬间写入被拒（#86）
	KVServer.WaitUntilReady()
	// 按配置启动下游投递循环（默认关闭）
	service.StartDeliveryFromConfig(context.Background(), KVServer)
	server.Serve()
}
