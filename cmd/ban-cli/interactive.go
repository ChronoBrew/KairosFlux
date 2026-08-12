package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/NeverENG/BanDB/pkg/predicate"
	"github.com/NeverENG/BanDB/pkg/proto"
)

// InteractiveClient 交互式客户端
type InteractiveClient struct {
	client *Client
	reader *bufio.Reader
}

// NewInteractiveClient 创建交互式客户端
func NewInteractiveClient(addr string) (*InteractiveClient, error) {
	client := NewClient(addr)

	// 连接服务器
	err := client.Connect()
	if err != nil {
		return nil, fmt.Errorf("连接失败: %v", err)
	}

	fmt.Printf("已连接到 %s\n", addr)
	fmt.Println("输入命令进行操作，输入 'quit' 或 'exit' 退出")
	fmt.Println("支持命令: put <key> <value>, get <key>, delete <key>, scan <start|-> <end|-> [field op operand]")
	fmt.Println()

	return &InteractiveClient{
		client: client,
		reader: bufio.NewReader(os.Stdin),
	}, nil
}

// Close 关闭客户端
func (ic *InteractiveClient) Close() {
	if ic.client != nil {
		ic.client.Close()
	}
}

// Run 运行交互式循环
func (ic *InteractiveClient) Run() {
	for {
		// 显示提示符
		fmt.Print("> ")

		// 读取用户输入
		line, err := ic.reader.ReadString('\n')
		if err != nil {
			fmt.Printf("读取输入失败: %v\n", err)
			break
		}

		// 去除首尾空白和换行符
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 检查退出命令
		if strings.ToLower(line) == "quit" || strings.ToLower(line) == "exit" {
			fmt.Println("再见！")
			break
		}

		// 解析并执行命令
		ic.executeCommand(line)
	}
}

// executeCommand 执行命令
func (ic *InteractiveClient) executeCommand(line string) {
	// 分割命令和参数
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}

	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "put":
		ic.handlePut(parts)
	case "get":
		ic.handleGet(parts)
	case "delete":
		ic.handleDelete(parts)
	case "scan":
		ic.handleScan(parts)
	case "help":
		ic.showHelp()
	default:
		fmt.Printf("未知命令: %s\n", cmd)
		fmt.Println("输入 'help' 查看帮助")
	}
}

// handlePut 处理 PUT 命令
func (ic *InteractiveClient) handlePut(parts []string) {
	if len(parts) < 3 {
		fmt.Println("用法: put <key> <value>")
		return
	}

	key := parts[1]
	value := strings.Join(parts[2:], " ") // 支持 value 中有空格

	err := ic.client.SendPut([]byte(key), []byte(value))
	if err != nil {
		fmt.Printf("❌ 错误: %v\n", err)
		return
	}

	fmt.Println("✅ OK")
}

// handleGet 处理 GET 命令
func (ic *InteractiveClient) handleGet(parts []string) {
	if len(parts) < 2 {
		fmt.Println("用法: get <key>")
		return
	}

	key := parts[1]

	value, err := ic.client.SendGet([]byte(key))
	if err != nil {
		fmt.Printf("❌ 错误: %v\n", err)
		return
	}

	fmt.Printf("\"%s\"\n", string(value))
}

// handleDelete 处理 DELETE 命令
func (ic *InteractiveClient) handleDelete(parts []string) {
	if len(parts) < 2 {
		fmt.Println("用法: delete <key>")
		return
	}

	key := parts[1]

	err := ic.client.SendDelete([]byte(key))
	if err != nil {
		fmt.Printf("❌ 错误: %v\n", err)
		return
	}

	fmt.Println("✅ OK")
}

// handleScan 处理 SCAN 命令: scan <start|-> <end|-> [field op operand]
func (ic *InteractiveClient) handleScan(parts []string) {
	if len(parts) < 3 {
		fmt.Println("用法: scan <start|-> <end|-> [field op operand]")
		fmt.Println("示例: scan imu:dev0:100 imu:dev0:200 az > 9.9")
		return
	}

	req := proto.ScanRequest{
		Start: dashToEmpty(parts[1]),
		End:   dashToEmpty(parts[2]),
		Pred:  predicate.Predicate{Op: predicate.OpNone},
	}

	if len(parts) >= 6 {
		op, ok := parseOp(parts[4])
		if !ok {
			fmt.Printf("不支持的算子: %s (支持 > >= < <= == !=)\n", parts[4])
			return
		}
		req.Pred = predicate.Predicate{Field: parts[3], Op: op, Operand: strings.Join(parts[5:], " ")}
	} else if len(parts) > 3 {
		fmt.Println("谓词需 3 段: field op operand，例: az > 9.9")
		return
	}

	entries, err := ic.client.SendScan(req)
	if err != nil {
		fmt.Printf("❌ 错误: %v\n", err)
		return
	}

	fmt.Printf("命中 %d 条:\n", len(entries))
	for _, e := range entries {
		fmt.Printf("  %s = %s\n", string(e.Key), string(e.Value))
	}
}

// dashToEmpty 把 "-" 视为不限边界（空），否则返回原值字节。
func dashToEmpty(s string) []byte {
	if s == "-" {
		return nil
	}
	return []byte(s)
}

// parseOp 把算子符号映射为 predicate.Op。
func parseOp(s string) (predicate.Op, bool) {
	switch s {
	case ">":
		return predicate.OpGT, true
	case ">=":
		return predicate.OpGTE, true
	case "<":
		return predicate.OpLT, true
	case "<=":
		return predicate.OpLTE, true
	case "==":
		return predicate.OpEQ, true
	case "!=":
		return predicate.OpNE, true
	}
	return predicate.OpNone, false
}

// showHelp 显示帮助信息
func (ic *InteractiveClient) showHelp() {
	fmt.Println("\n=== BanKV 客户端帮助 ===")
	fmt.Println("命令:")
	fmt.Println("  put <key> <value>  - 存储键值对")
	fmt.Println("  get <key>          - 获取值")
	fmt.Println("  delete <key>       - 删除键")
	fmt.Println("  scan <start|-> <end|-> [field op operand] - 范围查询(只回传命中切片)")
	fmt.Println("  help               - 显示此帮助信息")
	fmt.Println("  quit/exit          - 退出客户端")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  > put name Alice")
	fmt.Println("  > get name")
	fmt.Println("  > delete name")
	fmt.Println()
}
