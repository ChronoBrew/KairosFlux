package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/NeverENG/BanDB/internal/kvgrpc"
	"github.com/NeverENG/BanDB/service"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "gRPC server listen address")
	flag.Parse()

	kvServer := service.NewKVServer()

	go kvServer.Run()

	ha := service.NewHA(kvServer)

	grpcSrv := kvgrpc.NewGRPCServer(kvServer)

	fmt.Println("Starting gRPC Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	fmt.Printf("Listening on %s\n", *addr)

	if err := grpcSrv.Serve(*addr); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server error: %v\n", err)
		os.Exit(1)
	}
}
