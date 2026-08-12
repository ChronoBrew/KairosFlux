package main

import (
	"fmt"
	"os"
)

func main() {
	// 如果有命令行参数，使用命令行模式
	if len(os.Args) >= 2 {
		runCommandLineMode()
		return
	}

	// 否则使用交互模式
	runInteractiveMode()
}

// runCommandLineMode 命令行模式（原有逻辑）
func runCommandLineMode() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	cmd := os.Args[1]
	addr := "localhost:8080" // 默认服务端地址

	// 创建客户端
	c := NewClient(addr)

	// 连接服务端
	err := c.Connect()
	if err != nil {
		fmt.Printf("Error connecting to Server: %v\n", err)
		return
	}
	defer c.Close()

	switch cmd {
	case "put":
		if len(os.Args) < 4 {
			fmt.Println("Usage: client put <key> <value>")
			return
		}
		key := os.Args[2]
		value := os.Args[3]
		err := c.SendPut([]byte(key), []byte(value))
		if err != nil {
			fmt.Printf("Error putting key-value: %v\n", err)
			return
		}
		fmt.Printf("Put successful: %s = %s\n", key, value)

	case "get":
		if len(os.Args) < 3 {
			fmt.Println("Usage: client get <key>")
			return
		}
		key := os.Args[2]
		value, err := c.SendGet([]byte(key))
		if err != nil {
			fmt.Printf("Error getting value: %v\n", err)
			return
		}
		fmt.Printf("Get successful: %s = %s\n", key, string(value))

	case "delete":
		if len(os.Args) < 3 {
			fmt.Println("Usage: client delete <key>")
			return
		}
		key := os.Args[2]
		err := c.SendDelete([]byte(key))
		if err != nil {
			fmt.Printf("Error deleting key: %v\n", err)
			return
		}
		fmt.Printf("Delete successful: %s\n", key)

	default:
		usage()
	}
}

// runInteractiveMode 交互模式（新模式）
func runInteractiveMode() {
	addr := "localhost:8080" // 默认服务端地址

	// 可以通过环境变量或命令行参数指定地址
	if len(os.Args) >= 2 {
		addr = os.Args[1]
	}

	// 创建交互式客户端
	client, err := NewInteractiveClient(addr)
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// 运行交互循环
	client.Run()
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  Interactive mode: client [server_address]")
	fmt.Println("  Command mode:")
	fmt.Println("    client put <key> <value> - Put a key-value pair")
	fmt.Println("    client get <key>         - Get value by key")
	fmt.Println("    client delete <key>      - Delete a key")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  client                    # Enter interactive mode")
	fmt.Println("  client localhost:8080     # Connect to specific address")
	fmt.Println("  client put name Alice     # Command mode")
}
