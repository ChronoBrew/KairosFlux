//go:build !pprof

// 与 server_pprof.go(//go:build pprof) 互斥：两者各自定义 main，缺少本约束会使
// `go build -tags pprof` 因 main 重复声明而失败，pprof 构建不可用。
//
// 五层重构（docs/调研-架构调整-分层与Node门面.md）后 main 只是
// service.Node 的薄壳：装配与生命周期全部收敛在 Node（唯一持有者），
// 这里只负责构造、失败退出、启动。
package main

import (
	"fmt"
	"os"

	"github.com/ChronoBrew/KairosFlux/service"
)

func main() {
	node, err := service.NewNode()
	if err != nil {
		fmt.Fprintln(os.Stderr, "[ERROR] failed to bootstrap node:", err)
		os.Exit(1)
	}
	node.Serve()
}
