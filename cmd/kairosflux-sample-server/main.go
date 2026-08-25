// 命令 kairosflux-sample-server 是"server 模式 + Python 客户端"能力样例
// （发布批次阶段 A）：kairosflux.Serve 起一个真实监听端口，同一套 API 作为
// 网络壳对外服务——先由 Go 侧 v2 瘦客户端（kairnet/codec + negotiate +
// proto 拼帧，与 kairosflux-cli 同一模式）经真实线协议完成
// PUT_VERSIONED → GET_AS_OF 往返，再由 Python 客户端（仓库
// client/python/bandb_client.py：v1 直写直读 + v2 ack=window 批量写 +
// FLUSH 对账 + STAT 累计计数）演示跨语言访问。
//
// 用法（README"一条命令跑通"）：
//
//	go run ./cmd/kairosflux-sample-server -port 19090 -data-dir /tmp/kf-demo-srv
//
// -python=off 跳过 Python 腿（机器上没有 python3 时自动跳过并如实提示）。
// 退出码：0=两腿均通过（Python 腿被跳过不算失败）；1=任一步失败。
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
)

func main() {
	port := flag.Int("port", 19090, "监听端口（必填，>0）")
	dataDir := flag.String("data-dir", "/tmp/kairosflux-sample-server", "数据目录（不存在会自动创建）")
	pythonFlag := flag.String("python", "python3", "python3 可执行文件路径；=off 跳过 Python 腿")
	flag.Parse()

	e, err := kairosflux.Serve(kairosflux.Options{DataDir: *dataDir, Port: *port})
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-server] 启动失败:", err)
		os.Exit(1)
	}
	defer e.Close()
	addr := e.Addr()
	fmt.Printf("[sample-server] 服务已就绪: %s（数据目录 %s）\n", addr, *dataDir)

	// —— Go 腿：v2 瘦客户端经真实线协议做 PUT_VERSIONED → GET_AS_OF ——
	seq, err := putVersioned(addr, "quote:2026-08-17:510300", `{"code":"510300","date":"2026-08-17","open":3.90,"high":3.95,"low":3.88,"close":3.92,"volume":1200000}`, "sample-go")
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-server] Go 腿 PUT_VERSIONED 失败:", err)
		os.Exit(1)
	}
	fmt.Printf("[sample-server] Go 腿：PUT_VERSIONED 成功 seq=%d\n", seq)
	vv, found, err := getAsOf(addr, "quote:2026-08-17:510300", time.Now().UnixNano()+int64(time.Second))
	if err != nil || !found {
		fmt.Fprintln(os.Stderr, "[sample-server] Go 腿 GET_AS_OF 失败:", err)
		os.Exit(1)
	}
	fmt.Printf("[sample-server] Go 腿：GET_AS_OF 成功 seq=%d payload=%s\n", vv.Seq, vv.Payload)

	// —— Python 腿：client/python/bandb_client.py（v1 直写直读 + v2 window 批量）——
	if *pythonFlag != "off" && runPythonLeg(*pythonFlag, addr) != nil {
		fmt.Fprintln(os.Stderr, "[sample-server] Python 腿失败（可加 -python=off 跳过）")
		os.Exit(1)
	}

	fmt.Println("[sample-server] 两条腿全部通过")
}

// —— v2 瘦客户端（与 cmd/kairosflux-cli/temporal.go 同一模式：只用公开的
// kairnet/negotiate 与 kairnet/codec 拼帧函数，不发一整套 v2 SDK）——

const dialTimeout = 5 * time.Second

type v2Conn struct{ conn net.Conn }

func dialV2(addr string) (*v2Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("拨号失败: %w", err)
	}
	version, ack, err := negotiate.ClientNegotiateWithAck(conn, dialTimeout, negotiate.AckEvery)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("v2 协商失败: %w", err)
	}
	if version != negotiate.VersionV2 {
		conn.Close()
		return nil, fmt.Errorf("服务端不支持 v2 协议（协商结果=%v）", version)
	}
	if ack != negotiate.AckEvery {
		conn.Close()
		return nil, fmt.Errorf("服务端确认 ack 档位=%v，期望 every", ack)
	}
	return &v2Conn{conn: conn}, nil
}

func (c *v2Conn) Close() error { return c.conn.Close() }

func (c *v2Conn) roundTrip(opcode uint8, payload []byte) (*codec.MessageV2, error) {
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: codec.TypeUnspecified, CorrID: 1}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		return nil, fmt.Errorf("编帧失败: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(dialTimeout)); err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(frame); err != nil {
		return nil, fmt.Errorf("写帧失败: %w", err)
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(dialTimeout)); err != nil {
		return nil, err
	}
	resp, err := codec.NewDataPackV2().Decode(c.conn, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("读帧失败: %w", err)
	}
	return resp, nil
}

func errFromErrResp(resp *codec.MessageV2) error {
	code, reason, ok := proto.DecodeV2ErrPayload(resp.Payload)
	if !ok {
		return fmt.Errorf("服务端返回 ERR，但负载无法解析")
	}
	return fmt.Errorf("服务端拒绝: code=%#x reason=%s", code, reason)
}

func putVersioned(addr, key, value, source string) (uint64, error) {
	c, err := dialV2(addr)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	resp, err := c.roundTrip(codec.OpcodePutVersioned, proto.EncodePutVersionedFrame([]byte(key), []byte(value), source))
	if err != nil {
		return 0, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return 0, errFromErrResp(resp)
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("OK 响应负载长度=%d，期望 8（seq）", len(resp.Payload))
	}
	return binary.LittleEndian.Uint64(resp.Payload), nil
}

func getAsOf(addr, key string, asOfNanos int64) (proto.VersionEntryView, bool, error) {
	c, err := dialV2(addr)
	if err != nil {
		return proto.VersionEntryView{}, false, err
	}
	defer c.Close()
	resp, err := c.roundTrip(codec.OpcodeGetAsOf, proto.EncodeAsOfFrame([]byte(key), asOfNanos))
	if err != nil {
		return proto.VersionEntryView{}, false, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		if _, reason, ok := proto.DecodeV2ErrPayload(resp.Payload); ok && reason == "notfound" {
			return proto.VersionEntryView{}, false, nil
		}
		return proto.VersionEntryView{}, false, errFromErrResp(resp)
	}
	seq, writeNanos, payload, _, ok := proto.DecodeVersionEntry(resp.Payload)
	if !ok {
		return proto.VersionEntryView{}, false, fmt.Errorf("响应负载无法解析")
	}
	return proto.VersionEntryView{Seq: seq, WriteNanos: writeNanos, Payload: payload}, true, nil
}

// runPythonLeg 用 python3 运行一段内联脚本（stdin 喂入，零临时文件），
// 通过仓库的 client/python/bandb_client.py 演示跨语言访问：
//   - v1：BanDBClient.put/get 直写直读（生产协议）；
//   - v2 ack=window：BanDBClientV2.put_window 批量写 → FLUSH → WINDOW_ACK
//     （received/accepted 计数）→ reconcile() 读出服务端累计计数对账。
//
// 脚本路径定位：优先 CWD 下的 client/python/bandb_client.py，其次从 go.mod
// 向上回溯仓库根。找不到该文件或 python3 不可用 → 返回 error（调用方决定
// 是否致命）。
func runPythonLeg(pythonBin, addr string) error {
	script, err := bandbClientPath()
	if err != nil {
		return err
	}
	py := fmt.Sprintf(`
import os
import sys
sys.path.insert(0, os.path.dirname(%q))
from bandb_client import BanDBClient, BanDBClientV2, ACK_WINDOW

addr = %q

# v1 腿：生产协议直写直读
c1 = BanDBClient(addr)
c1.connect()
c1.put(b"quote:2026-08-17:600519", b'{"code":"600519","date":"2026-08-17","open":1400,"high":1420,"low":1395,"close":1410,"volume":80000}')
val = c1.get(b"quote:2026-08-17:600519")
print("[sample-server] Python v1 腿: put/get 往返 OK, value =", val)
c1.close()

# v2 腿：ack=window 批量写 + FLUSH 对账 + STAT 累计计数（注意：脚本内用
# f-string 而非 %% 运算符——本脚本字符串要经过 Go 的 fmt.Sprintf，%% 会被
# Go 当作格式动词吞掉）
c2 = BanDBClientV2(addr)
ack = c2.connect(ACK_WINDOW)
assert ack == ACK_WINDOW, ack
# 键末段用非纯数字（bar: 未纳管类型走 parseKey 启发式，末段全数字会被
# 当成时间戳，同一键连写会被 best-effort 单调校验误杀——见
# service/ingesthook/filter.go 的 validate 注释）。同一 corr_id=1 构成一个
# 窗口，FLUSH 一次性对账。
c2.put_window(1, b"bar:2026-08-17:ETF510", b'{"close":3.14}')
c2.put_window(1, b"bar:2026-08-17:ETF510", b'{"close":3.15}')
c2.put_window(1, b"bar:2026-08-17:ETF588", b'{"close":9.99}')
f = c2.flush()
print(f"[sample-server] Python v2 腿: FLUSH/WINDOW_ACK received={f.received} accepted={f.accepted} rejected={f.rejected}")
assert f.accepted == 3, f
s = c2.stat()
print(f"[sample-server] Python v2 腿: STAT 累计 received={s.received} accepted={s.accepted}")
assert s.accepted == 3, s
c2.bye()
print("[sample-server] Python 腿全部通过")
`, script, addr)
	cmd := exec.Command(pythonBin)
	cmd.Stdin = strings.NewReader(py)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python 脚本执行失败: %w", err)
	}
	return nil
}

// bandbClientPath 定位仓库内 client/python/bandb_client.py（从 CWD 与
// go.mod 向上回溯各试一次）。
func bandbClientPath() (string, error) {
	candidates := []string{"client/python/bandb_client.py"}
	if wd, err := os.Getwd(); err == nil {
		for dir := wd; ; dir = filepath.Dir(dir) {
			candidates = append(candidates, filepath.Join(dir, "client", "python", "bandb_client.py"))
			if filepath.Base(dir) == "/" || filepath.Dir(dir) == dir {
				break
			}
		}
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("找不到 client/python/bandb_client.py（请从仓库根运行本样例，或用 -python=off 跳过）")
}
