//go:build !pprof

// 与 server_pprof.go(//go:build pprof) 互斥：两者各自定义 main，缺少本约束会使
// `go build -tags pprof` 因 main 重复声明而失败，pprof 构建不可用。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/internal/metrics"
	"github.com/NeverENG/BanDB/proto"
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
	server := bannet.NewServer()

	// 创建路由
	router := service.NewRouter(KVServer)

	// 按配置开启分片路由：不属本节点的 key 经 BanNet 转发到 owner（默认关闭）。
	service.EnableShardRoutingFromConfig(router)

	// 按配置开启网关自适应准入（默认关闭）。
	service.EnableAdmissionFromConfig(router)

	// 挂载采集入口过滤钩子：落盘前丢弃畸形帧、按设备做 best-effort 时间戳
	// 单调校验、对敏感字段脱敏。
	filter := ingesthook.NewFilter([]string{"gps", "user_id"}, 0, true)
	router.SetPreHandle(filter.Handle)

	// v2 业务处理器（BANLV v2：ack 三档协商、窗口写入、STAT/BYE 对账）。
	// 与 v1 Router 共用同一个 store 与 filter——同一份数据、同一套 schema
	// 校验，只是传输语义不同（RFC docs/rfc/BANLV-2.md）。
	routerV2 := service.NewRouterV2(KVServer, filter, 0)

	// 注册路由
	server.AddRouter(proto.MsgPut, router)
	server.AddRouter(proto.MsgGet, router)
	server.AddRouter(proto.MsgDelete, router)
	server.AddRouter(proto.MsgScan, router)
	server.AddRouterV2(routerV2)

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
