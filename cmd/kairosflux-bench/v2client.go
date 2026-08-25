package main

// v2client.go：压测用的 v2 瘦客户端（与 cmd/kairosflux-sample-server 同一
// 模式——只用公开的 kairnet/negotiate + kairnet/codec 拼帧函数），支持
// ack 三档：every（逐帧回 OK）、window（服务端攒窗口计数，本客户端只在
// 统计口径上用 FLUSH/STAT 对账）、none（不读逐帧响应，纯计数）。

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
)

type benchV2Conn struct {
	conn net.Conn
	ack  negotiate.AckTier
}

const v2DialTimeout = 5 * time.Second

// dialV2 拨号并按指定 ack 档位协商 v2 连接。
func dialV2(addr string, ack negotiate.AckTier) (*benchV2Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, v2DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("拨号失败: %w", err)
	}
	version, acked, err := negotiate.ClientNegotiateWithAck(conn, v2DialTimeout, ack)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("v2 协商失败: %w", err)
	}
	if version != negotiate.VersionV2 {
		conn.Close()
		return nil, fmt.Errorf("服务端不支持 v2 协议（协商结果=%v）", version)
	}
	if acked != ack {
		conn.Close()
		return nil, fmt.Errorf("服务端确认 ack 档位=%v，期望 %v", acked, ack)
	}
	return &benchV2Conn{conn: conn, ack: acked}, nil
}

func (c *benchV2Conn) Close() error { return c.conn.Close() }

// roundTrip 发送一帧并读回响应（ack=every/window 下 PUT_VERSIONED 恒有逐帧
// 响应；ack=none 下服务端仍回 OK——handlePutVersioned 与 ack 档位无关地
// 逐帧应答，见 service/router_v2.go，客户端统一读响应保持口径一致）。
func (c *benchV2Conn) roundTrip(opcode uint8, payload []byte) (*codec.MessageV2, error) {
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: codec.TypeUnspecified, CorrID: 1}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		return nil, fmt.Errorf("编帧失败: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(v2DialTimeout)); err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(frame); err != nil {
		return nil, fmt.Errorf("写帧失败: %w", err)
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(v2DialTimeout)); err != nil {
		return nil, err
	}
	resp, err := codec.NewDataPackV2().Decode(c.conn, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("读帧失败: %w", err)
	}
	return resp, nil
}

// putVersioned 走 PUT_VERSIONED（seq 编码在 OK 响应负载前 8 字节）。
func (c *benchV2Conn) putVersioned(key, value []byte, source string) (uint64, error) {
	resp, err := c.roundTrip(codec.OpcodePutVersioned, proto.EncodePutVersionedFrame(key, value, source))
	if err != nil {
		return 0, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return 0, fmt.Errorf("服务端拒绝: opcode=%#x reason=%q", resp.Header.Opcode, string(resp.Payload))
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("OK 负载长度=%d, want 8", len(resp.Payload))
	}
	return binary.LittleEndian.Uint64(resp.Payload), nil
}

// getAsOf 走 GET_AS_OF。
func (c *benchV2Conn) getAsOf(key []byte, asOfNanos int64) ([]byte, bool, error) {
	resp, err := c.roundTrip(codec.OpcodeGetAsOf, proto.EncodeAsOfFrame(key, asOfNanos))
	if err != nil {
		return nil, false, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return nil, false, fmt.Errorf("服务端拒绝: opcode=%#x", resp.Header.Opcode)
	}
	_, _, payload, _, ok := proto.DecodeVersionEntry(resp.Payload)
	if !ok {
		return nil, false, fmt.Errorf("响应负载无法解析")
	}
	return payload, true, nil
}

// listWrites 走 LIST_WRITES（审计扫描），返回信封条数。
func (c *benchV2Conn) listWrites(prefix []byte, tFrom, tTo int64) (int, error) {
	frame := proto.EncodeListWritesRequest(prefix, tFrom, tTo, nil)
	resp, err := c.roundTrip(codec.OpcodeListWrites, frame)
	if err != nil {
		return 0, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return 0, fmt.Errorf("服务端拒绝: opcode=%#x", resp.Header.Opcode)
	}
	entries, _, ok := proto.DecodeListWritesResponse(resp.Payload)
	if !ok {
		return 0, fmt.Errorf("LIST_WRITES 响应无法解析")
	}
	return len(entries), nil
}

// flush 发 FLUSH（ack=window 时用于对账；every/none 下服务端同样回
// WINDOW_ACK，见 router_v2.handleFlush）。
func (c *benchV2Conn) flush() (received, accepted, rejected int, err error) {
	resp, err := c.roundTrip(codec.OpcodeFlush, nil)
	if err != nil {
		return 0, 0, 0, err
	}
	if resp.Header.Opcode != codec.OpcodeWindowAck {
		return 0, 0, 0, fmt.Errorf("FLUSH 响应 opcode=%#x", resp.Header.Opcode)
	}
	fields, ok := proto.DecodeV2AckBody(resp.Payload)
	if !ok {
		return 0, 0, 0, fmt.Errorf("WINDOW_ACK 响应无法解析")
	}
	return int(fields.Received), int(fields.Accepted), int(fields.Rejected), nil
}
