package service

// Node 是服务器进程的生命周期唯一持有者（五层重构的 service 层门面，见
// docs/调研-架构调整-分层与Node门面.md）：KVServer/Router/RouterV2/Filter/
// 投递/HA/metrics/kairnet.Server 的装配与启停全部收敛在这一处，对外只暴露
// Serve/Stop/Addr。cmd/kairosflux-server 的 main 只是它的薄壳。
//
// 装配顺序是旧 main 代码的逐行迁移（契约加载→KVServer→HA→Router→分片/
// 准入→Filter→RouterV2→Handler 注入→metrics→Serve），配置门控的启动项
// （分片路由/准入/投递）保持各自 bootstrap 函数的语义不变；Handler 注入
// 走 kairnet 新路径 AddHandler/SetV2Handler（Router 概念已从传输层 API
// 撤出，见 kairnet/handler.go 顶部注释）。
//
// 进程外组件不在 Node 范围内：internal/jobctl 只被 cmd/kairosflux-jobctl
// （客户端 CLI）使用，internal/aiplane 是外部 agent 侧的客户端 SDK——两者
// 与服务器衔接靠 internal/identity 的 source 契约（协议层强制，见
// RouterV2.handlePutVersioned），不挂载进服务器进程。
import (
	"context"
	"fmt"
	"time"

	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook/schema"
)

// Node 持有服务器进程全部组件（见包顶注释）；Addr 在构造后即可读，
// Serve 阻塞运行，Stop 幂等优雅关停。
type Node struct {
	kv     *KVServer
	ha     *HA
	server *kairnet.Server
	ctx    context.Context
	cancel context.CancelFunc
}

// NewNode 装配全部进程内组件并返回 Node；任何启动前置条件（如数据契约
// 加载失败）在此以 error 拒绝，调用方（main）据此退出，不退回静默降级。
func NewNode() (*Node, error) {
	// 加载数据契约（方案 M1：contracts/*.schema.json，"服务端启动时加载契约
	// 并强制校验"）。失败即拒绝启动——契约目录缺失/损坏不应该退回旧行为
	// 悄悄跳过类型校验，那正是方案 §2.4 反对的"静默降级"。
	if err := schema.LoadContractsDefault(); err != nil {
		return nil, err
	}

	kv := NewKVServer()
	go kv.Run()

	ha := NewHA(kv)

	router := NewRouter(kv)
	// 按配置开启分片路由：不属本节点的 key 经 KairNet 转发到 owner（默认关闭）。
	EnableShardRoutingFromConfig(router)
	// 按配置开启网关自适应准入（默认关闭）。
	EnableAdmissionFromConfig(router)

	// 挂载采集入口过滤钩子：落盘前丢弃畸形帧、按设备做 best-effort 时间戳
	// 单调校验、对敏感字段脱敏。
	filter := ingesthook.NewFilter([]string{"gps", "user_id"}, 0, true)
	router.SetPreHandle(filter.Handle)

	// v2 业务处理器（Kair v2：ack 三档协商、窗口写入、STAT/BYE 对账）。
	// 与 v1 Router 共用同一个 store 与 filter——同一份数据、同一套 schema
	// 校验，只是传输语义不同（RFC docs/rfc/Kair-2.md）。
	routerV2 := NewRouterV2(kv, filter, 0)

	server := kairnet.NewServer()
	server.AddHandler(proto.MsgPut, router)
	server.AddHandler(proto.MsgGet, router)
	server.AddHandler(proto.MsgDelete, router)
	server.AddHandler(proto.MsgScan, router)
	server.SetV2Handler(routerV2)
	server.SetConnStartFunc(router.OnConnStart)
	server.SetConnStopFunc(router.OnConnStop)

	// 周期性指标快照：headless 边缘设备 tail 日志即可观测运行状态，随 Node
	// 生命周期启停（ctx 由 Stop 取消）。
	ctx, cancel := context.WithCancel(context.Background())
	metrics.StartLogger(ctx, 10*time.Second)

	return &Node{kv: kv, ha: ha, server: server, ctx: ctx, cancel: cancel}, nil
}

// Addr 返回本节点的监听地址（Host:Port，来自 config.G 的默认值，NewNode
// 时即确定，与 kairnet.Server 的 IP/Port 字段同源）。
func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d", n.server.IP, n.server.Port)
}

// Serve 阻塞至收到 SIGINT/SIGTERM：先等选主完成再开放端口（避免启动瞬间
// 写入被拒，#86），随后按配置启动下游投递循环（默认关闭），最后进入
// kairnet 的 Serve 循环（信号到达后内部自行优雅关停）。
func (n *Node) Serve() {
	fmt.Println("Starting Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", n.ha.IsHealthy())
	n.kv.WaitUntilReady()
	StartDeliveryFromConfig(n.ctx, n.kv)
	n.server.Serve()
}

// Stop 优雅关停（幂等）：投递循环随 ctx 取消退出，kairnet.Server 按
// detect→broadcast+wait→close 三段式排空在途请求后关闭监听。
func (n *Node) Stop() {
	n.cancel()
	n.server.Stop()
}
