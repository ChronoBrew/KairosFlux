// 命令 ban-grpc-server 是 BANLV(bannet TLV) 协议的基准测试/协议对照服务端，
// 不是生产摄入入口——生产摄入用 cmd/ban-server（BANLV）。据作者压测评估，
// BANLV 比 gRPC 约快 26%，这是自研协议存在的理由；本命令的作用是给 BANLV
// 提供一个可对照的性能与协议基线，见 internal/kvgrpc 包注释与 docs/BANLV-协议规范.md。
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/NeverENG/BanDB/internal/kvgrpc"
	"github.com/NeverENG/BanDB/service"
	"github.com/NeverENG/BanDB/service/ingesthook"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "gRPC server listen address")
	flag.Parse()

	kvServer := service.NewKVServer()

	go kvServer.Run()

	ha := service.NewHA(kvServer)

	// 挂载与 bannet 入口同一套清洗规则（见 service/ingesthook.Filter），修复 gRPC
	// 入口此前直接 kv.Write、完全跳过畸形值/单调性/schema 校验的缺口。
	//
	// dropBackward 固定为 false：parseKey 的 "设备:时间戳" 启发式无法安全处理
	// quote:<日期>:<代码> 这类末段为数字代码（而非时间戳）的 key，开启会把「代码」
	// 误当「时间戳」校验单调性、产生错误丢弃（见 service/ingesthook.Filter.Validate
	// 的注释）。按数据类型分派单调校验规则留待后续。
	filter := ingesthook.NewFilter(nil, 0, false)
	grpcSrv := kvgrpc.NewGRPCServer(kvServer, filter)

	fmt.Println("Starting gRPC Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	fmt.Printf("Listening on %s\n", *addr)

	if err := grpcSrv.Serve(*addr); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server error: %v\n", err)
		os.Exit(1)
	}
}
