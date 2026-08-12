// 命令 ban-cli 是 BanDB 的命令行客户端，基于 client SDK。
//
// 无位置参数时进入交互模式，否则按单条命令执行后退出：
//
//	ban-cli                              # 交互模式
//	ban-cli -addr 127.0.0.1:9000         # 交互模式，指定服务端
//	ban-cli put <key> <value>            # 单条命令
//	ban-cli get <key>
//	ban-cli delete <key>
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	bandb "github.com/NeverENG/BanDB/client"
)

// cmdTimeout 是单条命令的超时。
const cmdTimeout = 5 * time.Second

func main() {
	addr := flag.String("addr", "localhost:8080", "服务端地址")
	flag.Usage = usage
	flag.Parse()

	// 位置参数即命令；没有则进入交互模式。
	if flag.NArg() == 0 {
		runInteractive(*addr)
		return
	}
	os.Exit(runCommand(*addr, flag.Args()))
}

// runCommand 执行单条命令，返回进程退出码。
func runCommand(addr string, args []string) int {
	c, err := bandb.New(bandb.Options{Addrs: []string{addr}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建客户端失败: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()

	switch args[0] {
	case "put":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "用法: ban-cli put <key> <value>")
			return 2
		}
		if err := c.Put(ctx, []byte(args[1]), []byte(args[2])); err != nil {
			fmt.Fprintf(os.Stderr, "写入失败: %v\n", err)
			return 1
		}
		fmt.Printf("已写入: %s = %s\n", args[1], args[2])

	case "get":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: ban-cli get <key>")
			return 2
		}
		value, err := c.Get(ctx, []byte(args[1]))
		switch {
		case errors.Is(err, bandb.ErrKeyNotFound):
			// 「查不到」不是故障：以独立退出码表达，便于脚本判别。
			fmt.Fprintf(os.Stderr, "键不存在: %s\n", args[1])
			return 3
		case err != nil:
			fmt.Fprintf(os.Stderr, "读取失败: %v\n", err)
			return 1
		}
		fmt.Printf("%s\n", value)

	case "delete":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: ban-cli delete <key>")
			return 2
		}
		if err := c.Delete(ctx, []byte(args[1])); err != nil {
			fmt.Fprintf(os.Stderr, "删除失败: %v\n", err)
			return 1
		}
		fmt.Printf("已删除: %s\n", args[1])

	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", args[0])
		usage()
		return 2
	}
	return 0
}

func runInteractive(addr string) {
	ic, err := NewInteractiveClient(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
		os.Exit(1)
	}
	defer ic.Close()
	ic.Run()
}

func usage() {
	fmt.Fprintln(os.Stderr, `用法:
  ban-cli [-addr <host:port>]                 进入交互模式
  ban-cli [-addr <host:port>] put <key> <val> 写入
  ban-cli [-addr <host:port>] get <key>       读取（键不存在时退出码 3）
  ban-cli [-addr <host:port>] delete <key>    删除

示例:
  ban-cli
  ban-cli -addr 127.0.0.1:9000
  ban-cli put name Alice`)
}
