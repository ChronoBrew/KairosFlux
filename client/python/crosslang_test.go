// Package python_test 是 BANLV 协议的跨语言联调测试：起一个真实的 ban-server
// （与 cmd/ban-server 同样的接线：KVServer + Router + ingesthook.Filter），
// 分别用 Go SDK（client 包）与本目录的 Python 客户端（bandb_client.py）对同一批
// 场景发起写入，断言服务端对两者的行为完全一致——包括被 ingesthook 清洗拒绝的路径。
//
// 这是"gRPC 只是基准测试/对照用途，生产摄入走 bannet TLV(BANLV)"这一定位下，
// BANLV 协议本身的验收核心：协议实现分裂成 Go/Python 两份，若不做跨语言联调，
// 两侧可能各自演进而在客户端看不见的地方悄悄分叉（如响应状态码语义、字段序）。
//
// 环境无 python3 时跳过（不阻塞 go test ./...），见 requirePython3。
package python_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/client"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/proto"
	"github.com/NeverENG/BanDB/service"
	"github.com/NeverENG/BanDB/service/ingesthook"
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

// startCrosslangServer 起一个真实的 BANLV 服务端：standalone 模式、数据落临时
// 目录、挂载与 cmd/ban-server 一致的 ingesthook.Filter（含 quote: schema 校验，
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

	// dropBackward=false：quote key 的末段是股票代码而非时间戳，与 cmd/ban-grpc-server
	// 的构造理由相同（见 service/ingesthook.Filter.Validate 的注释）。
	filter := ingesthook.NewFilter(nil, 2048, false)
	router.SetPreHandle(filter.Handle)

	srv := bannet.NewServer()
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
// 直接用 bannet.DataPack 手工构造一帧发送，用于发出刻意畸形的 PUT 负载。
// 与 Python 侧 BanDBClient.raw_put 是同一场景的两种语言实现。
func rawPutStatus(t *testing.T, addr string, payload []byte) string {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	frame, err := bannet.NewDataPack().Pack(bannet.NewMessage(proto.MsgPut, payload))
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
	// malformed_lengths 分支，两者发送的是同一份字节（见 docs/banlv/vectors.json
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

// TestCrosslang_V2FrameCrossLanguage 是 BANLV v2 帧编解码（docs/rfc/BANLV-2.md
// §2/§3）的跨语言联调测试：不需要起服务端，只验证两侧的 v2 帧编解码本身
// 互相兼容——这是 vectors-v2.json 静态向量比对之外的第二重证据（两侧各自
// 读同一份 JSON 断言，理论上仍可能"两侧对同一份向量的理解方式恰好一致地
// 错"；本测试让 Go 与 Python 直接互相喂对方产出的字节，不经过共享的向量
// 文件这个中间层）。
//
// 覆盖两个方向：
//  1. Python 编码一帧 → Go 用 bannet/codec.DataPackV2.UnPack 解析，断言字段
//     与构造参数一致（"Python 编的帧 Go 能解"）。
//  2. Go 用 bannet/codec.DataPackV2.Pack 编码一帧 → Python 侧
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
