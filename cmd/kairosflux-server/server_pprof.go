//go:build pprof

// 与 server.go(//go:build !pprof) 互斥：两者各自定义 main。pprof 变体在
// service.Node 之外只多一步：起 :6060 的 net/http/pprof 探针（其余装配与
// 生命周期全部走与生产构建相同的 Node 路径，见 server.go 顶部注释）。
package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/ChronoBrew/KairosFlux/service"
)

func main() {
	go func() {
		fmt.Println("pprof is starting")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			fmt.Println("[ERROR] pprof start err:", err)
		}
	}()

	node, err := service.NewNode()
	if err != nil {
		fmt.Fprintln(os.Stderr, "[ERROR] failed to bootstrap node:", err)
		os.Exit(1)
	}
	node.Serve()
}
