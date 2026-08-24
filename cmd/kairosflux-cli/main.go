// 命令 kairosflux-cli 是 KairosFlux 的命令行客户端，基于 client SDK。
//
// 无位置参数时进入交互模式，否则按单条命令执行后退出：
//
//	kairosflux-cli                              # 交互模式
//	kairosflux-cli -addr 127.0.0.1:9000         # 交互模式，指定服务端
//	kairosflux-cli put <key> <value>            # 单条命令
//	kairosflux-cli get <key>
//	kairosflux-cli delete <key>
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	bandb "github.com/ChronoBrew/KairosFlux/client"
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
	// 时态内核 M0 新增的四个命令走独立的 v2 瘦客户端（cmd/kairosflux-cli/temporal.go），
	// 不经过下面的 v1 client SDK（bandb.New 建立的是纯 v1 连接池，PUT_VERSIONED
	// 等 opcode 对它不存在）。提前分派，避免为这几个命令白建一条不会用到的 v1
	// 连接。
	switch args[0] {
	case "put-versioned", "get-as-of", "list-versions", "fingerprint", "list-writes", "export-writes":
		return runTemporalCommand(addr, args)
	}

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
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli put <key> <value>")
			return 2
		}
		if err := c.Put(ctx, []byte(args[1]), []byte(args[2])); err != nil {
			fmt.Fprintf(os.Stderr, "写入失败: %v\n", err)
			return 1
		}
		fmt.Printf("已写入: %s = %s\n", args[1], args[2])

	case "get":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli get <key>")
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
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli delete <key>")
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
  kairosflux-cli [-addr <host:port>]                 进入交互模式
  kairosflux-cli [-addr <host:port>] put <key> <val> 写入
  kairosflux-cli [-addr <host:port>] get <key>       读取（键不存在时退出码 3）
  kairosflux-cli [-addr <host:port>] delete <key>    删除

时态内核 M0（版本化写入/as-of 读取/指纹重放对账，v2 协议，另开连接）:
  kairosflux-cli [-addr <host:port>] put-versioned <key> <val> [source]     版本化写入（不覆盖），打印分配到的 seq；
                                                                            source 可选，写入方标识（M2）
  kairosflux-cli [-addr <host:port>] get-as-of <key> <as_of_nanos>          读取该时刻可见的版本（找不到时退出码 3）
  kairosflux-cli [-addr <host:port>] list-versions <key>                   列出该逻辑键全部版本
  kairosflux-cli [-addr <host:port>] fingerprint <prefix> [as_of_nanos]    对前缀下的逻辑键重放指纹；不带 as_of_nanos
                                                                            时与 :current 对账，带则为区间查询（不核对）

时态内核 M2（操作元数据信封审计查询/导出）:
  kairosflux-cli [-addr <host:port>] list-writes <prefix> [from] [to] [source]     列出命中的写入记录 + 按来源计数
                                                                                    （from/to 为 write_ts 纳秒范围，<=0 表示无界）
  kairosflux-cli [-addr <host:port>] export-writes <prefix> <outfile> [from] [to] [source]  同上，导出为 append-only JSONL

示例:
  kairosflux-cli
  kairosflux-cli -addr 127.0.0.1:9000
  kairosflux-cli put name Alice`)
}
