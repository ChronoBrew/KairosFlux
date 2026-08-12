package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/NeverENG/BanDB/service"
	"github.com/NeverENG/BanDB/test_grpc"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "gRPC server listen address")
	flag.Parse()

	kvServer := service.NewKVServer()

	go kvServer.Run()

	ha := service.NewHA(kvServer)

	grpcSrv := test_grpc.NewGRPCServer(kvServer)

	fmt.Println("Starting gRPC Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	fmt.Printf("Listening on %s\n", *addr)

	if err := grpcSrv.Serve(*addr); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server error: %v\n", err)
		os.Exit(1)
	}
}
