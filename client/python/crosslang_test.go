// Package python_test 是 Kair 协议的跨语言联调测试：起一个真实的 kairosflux-server
// （与 cmd/kairosflux-server 同样的接线：KVServer + Router + ingesthook.Filter），
// 分别用 Go SDK（client 包）与本目录的 Python 客户端（bandb_client.py）对同一批
// 场景发起写入，断言服务端对两者的行为完全一致——包括被 ingesthook 清洗拒绝的路径。
//
// 这是"gRPC 只是基准测试/对照用途，生产摄入走 kairnet TLV(Kair)"这一定位下，
// Kair 协议本身的验收核心：协议实现分裂成 Go/Python 两份，若不做跨语言联调，
// 两侧可能各自演进而在客户端看不见的地方悄悄分叉（如响应状态码语义、字段序）。
//
// 环境无 python3 时跳过（不阻塞 go test ./...），见 requirePython3。
package python_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/client"
	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
)

// requirePython3 在环境无 python3 时跳过测试——联调测试依赖外部解释器，
// 不应因为某台 CI/开发机没装 Python 就让 go test ./... 整体失败。
func requirePython3(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("跳过：未找到 python3，无法运行跨语言联调测试")
	}
	return path
}

// startCrosslangServer 起一个真实的 Kair 服务端：standalone 模式、数据落临时
// 目录、挂载与 cmd/kairosflux-server 一致的 ingesthook.Filter（含 quote: schema 校验，
// 见 service/ingesthook/schema 包 init() 自注册）。maxValueLen=2048 用于制造
// "oversized" 场景，与生产默认值（不限）不同，仅为测试需要。
func startCrosslangServer(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.Mode = config.ModeStandalone
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode = oldWAL, oldSST, oldMode
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("取空闲端口失败: %v", err)
	}
	addr := l.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	l.Close()

	kv := service.NewKVServer()
	router := service.NewRouter(kv)

	// dropBackward=false：quote key 的末段是股票代码而非时间戳，与 cmd/kairosflux-grpc-server
	// 的构造理由相同（见 service/ingesthook.Filter.Validate 的注释）。
	filter := ingesthook.NewFilter(nil, 2048, false)
	router.SetPreHandle(filter.Handle)

	srv := kairnet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddRouter(proto.MsgPut, router)
	srv.AddRouter(proto.MsgGet, router)
	srv.AddRouter(proto.MsgDelete, router)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	waitCrosslangServerReady(t, addr)
	return addr
}

func waitCrosslangServerReady(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("服务端在 2s 内未就绪: %s", addr)
}

// rawPutStatus 是 Go 侧的低级 PUT：绕过 client.Client（它总是编码正确的负载），
// 直接用 kairnet.DataPack 手工构造一帧发送，用于发出刻意畸形的 PUT 负载。
// 与 Python 侧 BanDBClient.raw_put 是同一场景的两种语言实现。
func rawPutStatus(t *testing.T, addr string, payload []byte) string {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	frame, err := kairnet.NewDataPack().Pack(kairnet.NewMessage(proto.MsgPut, payload))
	if err != nil {
		t.Fatalf("Pack 失败: %v", err)
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	var head [6]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		t.Fatalf("读响应头失败: %v", err)
	}
	dataLen := binary.LittleEndian.Uint32(head[0:4])
	idLen := binary.LittleEndian.Uint16(head[4:6])
	rest := make([]byte, int(idLen)+int(dataLen))
	if _, err := io.ReadFull(conn, rest); err != nil {
		t.Fatalf("读响应体失败: %v", err)
	}
	respData := rest[idLen:]
	if len(respData) < 1 {
		t.Fatal("响应负载为空")
	}
	n := int(respData[0])
	return string(respData[1 : 1+n])
}

// goStatus 用 Go SDK 执行一次 Put，把结果映射成与 Python 侧一致的状态字符串
// （"ok" 或状态名，如 "dropped"）。
func goStatus(t *testing.T, addr string, key, value []byte) string {
	t.Helper()
	c, err := client.New(client.Options{Addrs: []string{addr}, MaxRetries: -1})
	if err != nil {
		t.Fatalf("构造 Go 客户端失败: %v", err)
	}
	defer c.Close()

	err = c.Put(context.Background(), key, value)
	if err == nil {
		return "ok"
	}
	switch {
	case errors.Is(err, client.ErrDropped):
		return "dropped"
	case errors.Is(err, client.ErrOverloaded):
		return "overloaded"
	case errors.Is(err, client.ErrServer):
		return "error"
	default:
		t.Fatalf("Put 返回未预期错误: %v", err)
		return ""
	}
}

// pythonStatus 以子进程调用 crosslang_probe.py 驱动 Python 客户端执行同一场景，
// 解析其打印的 "status=xxx" 行。
func pythonStatus(t *testing.T, python3, addr, scenario, key string) string {
	t.Helper()
	script := filepath.Join("examples", "crosslang_probe.py")
	cmd := exec.Command(python3, script, "--addr", addr, "--scenario", scenario, "--key", key)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python3 crosslang_probe.py 场景 %s 执行失败: %v\n输出:\n%s", scenario, err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "status=") {
			return strings.TrimPrefix(line, "status=")
		}
	}
	t.Fatalf("未能从 python3 输出解析 status，输出:\n%s", out)
	return ""
}

// TestCrosslang_GoAndPythonAgree 是跨语言联调测试的验收核心：对合法行情、
// 非法价格、超限 value、畸形负载四个场景，Go 客户端与 Python 客户端各写一份
// （不同 key，避免互相覆盖），断言服务端对两者的判定状态完全一致。
func TestCrosslang_GoAndPythonAgree(t *testing.T) {
	python3 := requirePython3(t)
	addr := startCrosslangServer(t)

	validQuote := []byte(`{"code":"600000","date":"2026-08-17","open":10.0,"high":10.5,"low":9.8,"close":10.2,"volume":1000000,"prev_close":10.0}`)
	invalidPriceQuote := []byte(`{"code":"600000","date":"2026-08-17","open":-1,"high":10.5,"low":9.8,"close":10.2,"volume":1000000}`)
	oversized := bytes.Repeat([]byte("x"), 4096)

	cases := []struct {
		scenario string
		goKey    string
		pyKey    string
		goValue  []byte
		wantGo   string // 空串表示走 rawPut
	}{
		{"valid_quote", "quote:2026-08-17:600000", "quote:2026-08-17:600100", validQuote, "ok"},
		{"invalid_price", "quote:2026-08-17:600001", "quote:2026-08-17:600101", invalidPriceQuote, "dropped"},
		{"oversized", "quote:2026-08-17:600002", "quote:2026-08-17:600102", oversized, "dropped"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.scenario, func(t *testing.T) {
			gotGo := goStatus(t, addr, []byte(tc.goKey), tc.goValue)
			gotPy := pythonStatus(t, python3, addr, tc.scenario, tc.pyKey)

			if gotGo != tc.wantGo {
				t.Fatalf("Go 客户端状态=%q，期望 %q", gotGo, tc.wantGo)
			}
			if gotPy != tc.wantGo {
				t.Fatalf("Python 客户端状态=%q，期望 %q", gotPy, tc.wantGo)
			}
			if gotGo != gotPy {
				t.Fatalf("两端行为不一致: go=%q python=%q", gotGo, gotPy)
			}
		})
	}

	// 畸形负载场景单独跑：Go 走 rawPut，Python 走 crosslang_probe.py 的
	// malformed_lengths 分支，两者发送的是同一份字节（见 docs/kair/vectors.json
	// 的 put_request_malformed_lengths 向量）。
	t.Run("malformed_lengths", func(t *testing.T) {
		malformed := append(binary.LittleEndian.AppendUint32(nil, 100), binary.LittleEndian.AppendUint32(nil, 0)...)
		malformed = append(malformed, []byte("ab")...)

		gotGo := rawPutStatus(t, addr, malformed)
		gotPy := pythonStatus(t, python3, addr, "malformed_lengths", "unused")

		if gotGo != "dropped" {
			t.Fatalf("Go rawPut 状态=%q，期望 dropped", gotGo)
		}
		if gotPy != "dropped" {
			t.Fatalf("Python 状态=%q，期望 dropped", gotPy)
		}
	})

	// 交叉核验：valid_quote 场景里 Python 客户端写入的字节，经真实服务端持久化后，
	// 用 Go 客户端读回应与 Python 发送的原始 value 完全一致——证明的不只是"状态码
	// 相同"，而是 Python 编码的整帧被服务端按与 Go 完全相同的语义正确解析、落盘。
	t.Run("python_written_value_readable_by_go_client", func(t *testing.T) {
		c, err := client.New(client.Options{Addrs: []string{addr}})
		if err != nil {
			t.Fatalf("构造 Go 客户端失败: %v", err)
		}
		defer c.Close()

		got, err := c.Get(context.Background(), []byte("quote:2026-08-17:600100"))
		if err != nil {
			t.Fatalf("Get 失败: %v", err)
		}
		if string(got) != string(validQuote) {
			t.Fatalf("Python 写入的值经 Go 客户端读回不一致\n  期望: %s\n  实际: %s", validQuote, got)
		}
	})
}

// TestCrosslang_V2FrameCrossLanguage 是 Kair v2 帧编解码（docs/rfc/Kair-2.md
// §2/§3）的跨语言联调测试：不需要起服务端，只验证两侧的 v2 帧编解码本身
// 互相兼容——这是 vectors-v2.json 静态向量比对之外的第二重证据（两侧各自
// 读同一份 JSON 断言，理论上仍可能"两侧对同一份向量的理解方式恰好一致地
// 错"；本测试让 Go 与 Python 直接互相喂对方产出的字节，不经过共享的向量
// 文件这个中间层）。
//
// 覆盖两个方向：
//  1. Python 编码一帧 → Go 用 kairnet/codec.DataPackV2.UnPack 解析，断言字段
//     与构造参数一致（"Python 编的帧 Go 能解"）。
//  2. Go 用 kairnet/codec.DataPackV2.Pack 编码一帧 → Python 侧
//     examples/v2_probe.py decode 解析，断言打印出的字段与 Go 的构造参数
//     一致（"Go 编的帧 Python 能解"）。
func TestCrosslang_V2FrameCrossLanguage(t *testing.T) {
	python3 := requirePython3(t)
	script := filepath.Join("examples", "v2_probe.py")

	t.Run("python_encodes_go_decodes", func(t *testing.T) {
		cmd := exec.Command(python3, script, "encode",
			"--opcode", "1", "--type", "1", "--corrid", "42", "--payload-hex", "6b317631")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("python3 v2_probe.py encode 执行失败: %v\n输出:\n%s", err, out)
		}
		frameHex := parseFrameHexLine(t, string(out))

		frame, err := hex.DecodeString(frameHex)
		if err != nil {
			t.Fatalf("Python 输出的 frame_hex 不是合法十六进制: %v", err)
		}
		h, err := codec.NewDataPackV2().UnPack(frame[:codec.HeaderV2Len])
		if err != nil {
			t.Fatalf("Go 侧 UnPack Python 编码的帧失败: %v", err)
		}
		if h.Opcode != 1 || h.Type != 1 || h.CorrID != 42 {
			t.Fatalf("字段不一致: opcode=%d type=%d corr_id=%d，期望 1/1/42", h.Opcode, h.Type, h.CorrID)
		}
		if got := hex.EncodeToString(frame[codec.HeaderV2Len:]); got != "6b317631" {
			t.Fatalf("负载=%q，期望 6b317631", got)
		}
	})

	t.Run("go_encodes_python_decodes", func(t *testing.T) {
		msg := codec.NewMessageV2(codec.HeaderV2{
			Opcode: 3,
			Type:   0,
			CorrID: 7,
		}, []byte("k1"))
		frame, err := codec.NewDataPackV2().Pack(msg)
		if err != nil {
			t.Fatalf("Pack 失败: %v", err)
		}

		cmd := exec.Command(python3, script, "decode", "--frame-hex", hex.EncodeToString(frame))
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("python3 v2_probe.py decode 执行失败: %v\n输出:\n%s", err, out)
		}
		line := strings.TrimSpace(string(out))
		want := "flags=0 opcode=3 type=0 corr_id=7 data_hex=6b31"
		if line != want {
			t.Fatalf("Python 解析结果=%q，期望 %q", line, want)
		}
	})
}

// parseFrameHexLine 从 v2_probe.py encode 的输出里解析 "frame_hex=..." 行。
func parseFrameHexLine(t *testing.T, out string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "frame_hex=") {
			return strings.TrimPrefix(line, "frame_hex=")
		}
	}
	t.Fatalf("未能从 python3 输出解析 frame_hex，输出:\n%s", out)
	return ""
}

// startCrosslangV2Server 起一个真实服务端：与 startCrosslangServer 一样的
// v1 PUT/GET/DEL 路由（零影响），额外挂 service.RouterV2 处理 Kair v2
// 帧（RFC docs/rfc/Kair-2.md §11）——本文件的 v2 跨语言联调测试用它验证
// Go 服务端确实能和一个独立的 Python 客户端实现（bandb_client.BanDBClientV2）
// 就 ack=window/none 协议说通，不只是双方各自的单元测试自洽。
func startCrosslangV2Server(t *testing.T, windowN uint32) string {
	t.Helper()

	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.Mode = config.ModeStandalone
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode = oldWAL, oldSST, oldMode
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("取空闲端口失败: %v", err)
	}
	addr := l.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	l.Close()

	kv := service.NewKVServer()
	router := service.NewRouter(kv)
	filter := ingesthook.NewFilter(nil, 0, false)
	router.SetPreHandle(filter.Handle)
	routerV2 := service.NewRouterV2(kv, filter, windowN)

	srv := kairnet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddRouter(proto.MsgPut, router)
	srv.AddRouter(proto.MsgGet, router)
	srv.AddRouter(proto.MsgDelete, router)
	srv.AddRouterV2(routerV2)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	waitCrosslangServerReady(t, addr)
	return addr
}

// runV2WindowProbe 以子进程运行 v2_window_probe.py，返回按 "key=value"
// 逐行解析出的结果集合（足以覆盖本文件两个联调测试需要读取的所有字段，
// 不需要为每种输出行单独定义结构体）。
func runV2WindowProbe(t *testing.T, python3 string, args ...string) map[string]string {
	t.Helper()
	script := filepath.Join("examples", "v2_window_probe.py")
	cmd := exec.Command(python3, append([]string{script}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python3 v2_window_probe.py %v 执行失败: %v\n输出:\n%s", args, err, out)
	}

	result := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		prefix := ""
		if len(fields) > 0 && strings.Contains(fields[0], ":") {
			prefix = strings.TrimSuffix(fields[0], ":") + "."
			fields = fields[1:]
		}
		for _, f := range fields {
			kv := strings.SplitN(f, "=", 2)
			if len(kv) == 2 {
				result[prefix+kv[0]] = kv[1]
			}
		}
	}
	return result
}

func mustAtoi(t *testing.T, m map[string]string, key string) int {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("探测脚本输出缺少字段 %q，完整输出: %+v", key, m)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("字段 %q=%q 不是整数: %v", key, v, err)
	}
	return n
}

// TestCrosslang_V2WindowBatchAndReconcile 是 Kair v2 ack=window（RFC §11.2.2）
// 与 BYE 收尾（§11.4）的跨语言联调核心：Go 服务端 × Python 客户端
// （bandb_client.BanDBClientV2），批量写入一批帧、显式 FLUSH、再 BYE，
// 断言 Python 客户端解析出的 WINDOW_ACK/STAT_ACK 计数与实际写入条数一致。
func TestCrosslang_V2WindowBatchAndReconcile(t *testing.T) {
	python3 := requirePython3(t)
	addr := startCrosslangV2Server(t, 1000)

	const n = 6
	out := runV2WindowProbe(t, python3, "window", "--addr", addr, "--count", strconv.Itoa(n), "--corrid", "77")

	if got := mustAtoi(t, out, "negotiated_ack"); got != 1 { // ACK_WINDOW=1
		t.Fatalf("negotiated_ack=%d，期望 1(ACK_WINDOW)", got)
	}
	if got := mustAtoi(t, out, "flush.corr_id"); got != 77 {
		t.Fatalf("flush corr_id=%d，期望 77", got)
	}
	if got := mustAtoi(t, out, "flush.received"); got != n {
		t.Fatalf("flush received=%d，期望 %d", got, n)
	}
	if got := mustAtoi(t, out, "flush.accepted"); got != n {
		t.Fatalf("flush accepted=%d，期望 %d", got, n)
	}
	// FLUSH 已经关闭了窗口，BYE 时不应该再有一次 WINDOW_ACK——只应看到
	// bye_stat 这一帧，探测脚本在 window_ack 为 None 时不会打印
	// "bye_window:" 前缀的任何字段。
	if _, ok := out["bye_window.corr_id"]; ok {
		t.Fatalf("FLUSH 之后 BYE 不应再触发 WINDOW_ACK，但探测脚本输出了 bye_window 字段: %+v", out)
	}
	if got := mustAtoi(t, out, "bye_stat.received"); got != n {
		t.Fatalf("BYE 隐式 STAT_ACK received=%d，期望 %d（连接累计）", got, n)
	}
	if got := mustAtoi(t, out, "bye_stat.accepted"); got != n {
		t.Fatalf("BYE 隐式 STAT_ACK accepted=%d，期望 %d", got, n)
	}
}

// TestCrosslang_V2NoneStatReconcile 是 Kair v2 ack=none + STAT 对账
// （RFC §11.2.3）的跨语言联调：Python 客户端写入一批帧（含人为注入的
// schema 拒绝），调用 reconcile()（库内置对账，不提供绕开它的路径），
// 断言 Go 服务端报告的 received 与 Python 本地发送计数一致、且
// accepted/rejected 正确反映注入的拒绝。
func TestCrosslang_V2NoneStatReconcile(t *testing.T) {
	python3 := requirePython3(t)
	addr := startCrosslangV2Server(t, 1000)

	const total, bad = 5, 2
	out := runV2WindowProbe(t, python3, "none", "--addr", addr, "--count", strconv.Itoa(total), "--bad-count", strconv.Itoa(bad))

	if got := mustAtoi(t, out, "negotiated_ack"); got != 2 { // ACK_NONE=2
		t.Fatalf("negotiated_ack=%d，期望 2(ACK_NONE)", got)
	}
	if out["reconcile_status"] != "matched" {
		t.Fatalf("reconcile_status=%q，期望 matched（帧层面不应丢失）：%+v", out["reconcile_status"], out)
	}
	if got := mustAtoi(t, out, "reconcile.received"); got != total {
		t.Fatalf("reconcile received=%d，期望 %d", got, total)
	}
	if got := mustAtoi(t, out, "reconcile.accepted"); got != total-bad {
		t.Fatalf("reconcile accepted=%d，期望 %d", got, total-bad)
	}
	if got := mustAtoi(t, out, "reconcile.rejected"); got != bad {
		t.Fatalf("reconcile rejected=%d，期望 %d", got, bad)
	}
	// v2_window_probe.py 的注入拒绝用 type_=1（TYPE_QUOTE）+ 非正价格（open=-1），
	// M1 起按声明的 TypeID 精确分派到 quote 校验器，schema 拒绝按 RFC §10.3 的
	// 子码细分为 0x3002（非正价格），不再是笼统的 0x3001（ErrCodeSchemaValidation，
	// 那是"没有更精确子码可用"场景的默认桶，见 service/router_v2.go 的
	// applyWrite；quote 类型的完整子码表见 service/ingesthook/schema/error.go）。
	wantErrCode := fmt.Sprintf("%d", 0x3002)
	if got := out["reconcile.first_err_code"]; got != wantErrCode {
		t.Fatalf("reconcile first_err_code=%s，期望 %s（非正价格）", got, wantErrCode)
	}
	if got := mustAtoi(t, out, "bye_stat.received"); got != total {
		t.Fatalf("BYE 隐式 STAT_ACK received=%d，期望 %d", got, total)
	}
}
