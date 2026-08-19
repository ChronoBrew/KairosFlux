// 命令 ban-grpc-server 是 BANLV(bannet TLV) 协议的基准测试/协议对照服务端，
// 不是生产摄入入口——生产摄入用 cmd/ban-server（BANLV）。实测显示 BANLV 在
// 不受 fsync 约束的读路径上吞吐约为 gRPC 的 2.7 倍（写路径两者持平，均受同一
// fsync 瓶颈约束），这是自研协议存在的理由；本命令的作用是给 BANLV 提供一个
// 可对照的性能与协议基线，见 internal/kvgrpc 包注释、README.md 的性能一节与
// docs/BANLV-协议规范.md。
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
	// dropBackward 固定为 false：quote:<日期>:<代码> 这类已注册 schema 的 key
	// 现在会自动跳过单调性启发式（见 service/ingesthook.Filter.Validate），
	// 但本命令是通用基准测试入口，可能承载任意未来的、尚未注册 schema 的 key
	// 类型——对那些类型，parseKey 的 "设备:时间戳" 启发式仍可能对末段非时间戳
	// 的 key 产生误判。false 是对未知 key 类型的保守默认值，不是遗留的规避。
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
