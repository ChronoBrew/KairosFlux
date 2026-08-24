package main

// kairosflux-cli 对时态内核 M0 新增四个 v2 opcode（PUT_VERSIONED/GET_AS_OF/
// LIST_VERSIONS/REPLAY_FINGERPRINT，docs/rfc/时态内核-M0-版本化与as-of.md）的
// 命令行入口。v1 client SDK（client 包）只覆盖 PUT/GET/DELETE/SCAN 这四个 v1
// opcode，没有 v2 能力；这里不去扩建一整套 v2 SDK，只用 kairnet/negotiate 与
// kairnet/codec 已经导出的协商/拼帧函数拼出"连一次、发一帧、收一帧"的最小
// 客户端——与 service/router_v2_integration_test.go 的测试用 v2Client 是同一
// 思路，区别只是这里是生产可执行文件而不是测试代码。
//
// REPLAY_FINGERPRINT 之所以做成服务端 opcode 而不是脱离网络直接开库的离线
// 工具：本仓库已有 cmd/kairosflux-ingest 那种直接 storage.NewEngine 的离线模式先例，
// 但那要求没有其它进程同时持有同一份 LSM 数据目录——REPLAY_FINGERPRINT 的
// 典型使用场景恰恰是"服务端正在跑，我想现在核对一下账本"，两个进程各自打开
// 同一组 SSTable/WAL 文件是不安全的（见 storage.Engine 的 fileMu 系列注释）。
// 让服务端自己算、CLI 只是瘦客户端，避免了这个问题。

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// runTemporalCommand 处理 put-versioned/get-as-of/list-versions/fingerprint
// 四条命令，返回进程退出码。与 runCommand（v1 命令）分开，因为这四条走独立的
// v2 瘦客户端，不共享 v1 client SDK 的连接/错误类型。
func runTemporalCommand(addr string, args []string) int {
	switch args[0] {
	case "put-versioned":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli put-versioned <key> <val>")
			return 2
		}
		seq, err := putVersioned(addr, args[1], args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "写入失败: %v\n", err)
			return 1
		}
		fmt.Printf("已写入版本 seq=%d: %s = %s\n", seq, args[1], args[2])

	case "get-as-of":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli get-as-of <key> <as_of_nanos>")
			return 2
		}
		asOfNanos, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "as_of_nanos 不是合法整数: %v\n", err)
			return 2
		}
		v, found, err := getAsOf(addr, args[1], asOfNanos)
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取失败: %v\n", err)
			return 1
		}
		if !found {
			fmt.Fprintf(os.Stderr, "该时刻无可见版本: %s @ %d\n", args[1], asOfNanos)
			return 3
		}
		fmt.Printf("seq=%d write_nanos=%d payload=%s\n", v.Seq, v.WriteNanos, v.Payload)

	case "list-versions":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli list-versions <key>")
			return 2
		}
		versions, err := listVersions(addr, args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取失败: %v\n", err)
			return 1
		}
		if len(versions) == 0 {
			fmt.Println("（无版本）")
			return 0
		}
		for _, v := range versions {
			fmt.Printf("seq=%d write_nanos=%d payload=%s\n", v.Seq, v.WriteNanos, v.Payload)
		}

	case "fingerprint":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli fingerprint <prefix>")
			return 2
		}
		result, err := fingerprintReplay(addr, args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "重放失败: %v\n", err)
			return 1
		}
		fmt.Printf("逻辑键数=%d 不一致数=%d 指纹=%s\n", result.KeyCount, result.MismatchCount, result.Fingerprint)
		for _, k := range result.MismatchKeys {
			fmt.Printf("  不一致: %s\n", k)
		}
		if result.MismatchCount > 0 {
			return 4 // 独立退出码：对账不一致，区别于 1(故障)/2(用法)/3(未找到)
		}
	}
	return 0
}

const v2DialTimeout = 5 * time.Second

// v2Conn 是一条已完成 v2 协商（ack=every）的连接，只支持"发一帧、收一帧"的
// 请求-响应，够用于这四个控制类 opcode——它们本身就不参与 ack 三档窗口/
// 累计记账（见 kairnet/codec.OpcodePutVersioned 的文档），用 every 语义天然
// 匹配。
type v2Conn struct {
	conn net.Conn
}

func dialV2(addr string) (*v2Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, v2DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("拨号失败: %w", err)
	}
	version, ack, err := negotiate.ClientNegotiateWithAck(conn, v2DialTimeout, negotiate.AckEvery)
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

// roundTrip 发一帧、等一帧响应；corr_id 固定为 1——这条连接只做单次请求-响应
// 就断开，不需要用 corr_id 区分并发的多个请求。
func (c *v2Conn) roundTrip(opcode uint8, payload []byte) (*codec.MessageV2, error) {
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: codec.TypeUnspecified, CorrID: 1}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		return nil, fmt.Errorf("编帧失败: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(v2DialTimeout)); err != nil {
		return nil, fmt.Errorf("设置写超时失败: %w", err)
	}
	if _, err := c.conn.Write(frame); err != nil {
		return nil, fmt.Errorf("写帧失败: %w", err)
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(v2DialTimeout)); err != nil {
		return nil, fmt.Errorf("设置读超时失败: %w", err)
	}
	resp, err := codec.NewDataPackV2().Decode(c.conn, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("读帧失败: %w", err)
	}
	return resp, nil
}

// errFromErrResp 把一个已确认是 ERR 的响应转成 Go error。
func errFromErrResp(resp *codec.MessageV2) error {
	code, reason, ok := proto.DecodeV2ErrPayload(resp.Payload)
	if !ok {
		return fmt.Errorf("服务端返回 ERR，但负载无法解析")
	}
	return fmt.Errorf("服务端拒绝: code=%#x reason=%s", code, reason)
}

// putVersioned 对应 PUT_VERSIONED：返回本次写入分配到的 seq。
func putVersioned(addr, key, value string) (uint64, error) {
	c, err := dialV2(addr)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	resp, err := c.roundTrip(codec.OpcodePutVersioned, proto.EncodePutFrame([]byte(key), []byte(value)))
	if err != nil {
		return 0, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return 0, errFromErrResp(resp)
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("OK 响应负载长度=%d，期望 8（seq）", len(resp.Payload))
	}
	// RouterV2.handlePutVersioned 的 OK 响应负载就是裸 [seq u64 LE] 8 字节
	// （不是 EncodeVersionEntry——那是 GET_AS_OF/LIST_VERSIONS 的格式，PUT_VERSIONED
	// 的调用方已经知道自己刚写的 payload 是什么，只需要服务端告诉它分配到的 seq）。
	return binary.LittleEndian.Uint64(resp.Payload), nil
}

// getAsOf 对应 GET_AS_OF。
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

// listVersions 对应 LIST_VERSIONS。
func listVersions(addr, key string) ([]proto.VersionEntryView, error) {
	c, err := dialV2(addr)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	resp, err := c.roundTrip(codec.OpcodeListVersions, proto.EncodeKeyOnlyFrame([]byte(key)))
	if err != nil {
		return nil, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return nil, errFromErrResp(resp)
	}
	versions, ok := proto.DecodeListVersionsResponse(resp.Payload)
	if !ok {
		return nil, fmt.Errorf("响应负载无法解析")
	}
	return versions, nil
}

// fingerprintReplay 对应 REPLAY_FINGERPRINT。
func fingerprintReplay(addr, prefix string) (proto.ReplayFingerprintView, error) {
	c, err := dialV2(addr)
	if err != nil {
		return proto.ReplayFingerprintView{}, err
	}
	defer c.Close()

	resp, err := c.roundTrip(codec.OpcodeReplayFingerprint, proto.EncodeKeyOnlyFrame([]byte(prefix)))
	if err != nil {
		return proto.ReplayFingerprintView{}, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return proto.ReplayFingerprintView{}, errFromErrResp(resp)
	}
	result, ok := proto.DecodeReplayFingerprintResponse(resp.Payload)
	if !ok {
		return proto.ReplayFingerprintView{}, fmt.Errorf("响应负载无法解析")
	}
	return result, nil
}
