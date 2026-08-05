package main

import (
	"context"
	"fmt"
	"time"

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

	// 按配置开启分片路由：不属本节点的 key 经 BanNet 转发到 owner（默认关闭）。
	service.EnableShardRoutingFromConfig(router)

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
