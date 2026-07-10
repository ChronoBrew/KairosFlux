package main

import (
	"context"
	"fmt"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banNet"
	"github.com/NeverENG/BanDB/pkg/metrics"
	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/service"
	"github.com/NeverENG/BanDB/service/ingesthook"
)

func main() {
	// 初始化 FSM
	KVServer := service.NewKVServer()

	// 启动 FSM
	go KVServer.Run()

	// 初始化 HA
	ha := service.NewHA(KVServer)

	// 初始化网络服务
	server := banNet.NewServer()

	// 创建路由
	router := service.NewRouter(KVServer)

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

	// 可选：启动 Prometheus /metrics 出口（MetricsAddr 为空则不启用）
	if err := metrics.StartPrometheusServer(config.G.MetricsAddr); err != nil {
		fmt.Printf("[WARN] metrics endpoint not started: %v\n", err)
	}

	// 启动服务
	fmt.Println("Starting Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	// 单节点下等待选主完成再开放端口，避免启动瞬间写入被拒（#86）
	KVServer.WaitUntilReady()
	server.Serve()
}
