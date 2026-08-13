package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/NeverENG/BanDB/proto"
)

// 线格式（与 pkg/proto 的包注释一致，服务端对应实现为 bannet.DataPack）：
//
//	帧: [dataLen u32 LE][idLen u16 LE][id bytes][data bytes]
//
// SDK 自行实现编解码而不导入 bannet：后者是服务端实现包，含监听、连接管理与 worker 池，
// 客户端不应把它们拖进依赖图。这份实现与服务端的一致性由 wire_compat_test.go 交叉校验，
// 避免两侧各自演进而漂移。
const frameHeadLen = 6

// conn 是一条到服务端的连接。BanNet 是严格的请求-响应协议：一条连接上必须
// 「发一帧、收一帧」后才能发下一帧，故 conn 不可被并发使用——并发由连接池提供。
type conn struct {
	nc net.Conn
	// broken 标记该连接已不可信（协议错误或读写中途失败）。此类连接不放回池中，
	// 否则残留的半个响应会与后续请求串话。
	broken bool
}

func dial(addr string, timeout time.Duration) (*conn, error) {
	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &conn{nc: nc}, nil
}

func (c *conn) close() error { return c.nc.Close() }

// encodeFrame 按线格式编码一帧，一次分配到最终长度。
func encodeFrame(msgID string, data []byte) []byte {
	buf := make([]byte, frameHeadLen+len(msgID)+len(data))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(data)))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(msgID)))
	copy(buf[frameHeadLen:], msgID)
	copy(buf[frameHeadLen+len(msgID):], data)
	return buf
}

// roundTrip 发出一帧并读回一帧，返回响应的 msgID 与 data 负载。
// deadline 同时约束写与读，由调用方从 context 推导。
func (c *conn) roundTrip(msgID string, data []byte, deadline time.Time) (string, []byte, error) {
	if err := c.nc.SetDeadline(deadline); err != nil {
		c.broken = true
		return "", nil, err
	}
	if _, err := c.nc.Write(encodeFrame(msgID, data)); err != nil {
		c.broken = true // 可能只写出了半帧，连接不可再用
		return "", nil, err
	}

	var head [frameHeadLen]byte
	if _, err := io.ReadFull(c.nc, head[:]); err != nil {
		c.broken = true
		return "", nil, err
	}
	dataLen := binary.LittleEndian.Uint32(head[0:4])
	idLen := binary.LittleEndian.Uint16(head[4:6])

	rest := make([]byte, int(idLen)+int(dataLen))
	if _, err := io.ReadFull(c.nc, rest); err != nil {
		c.broken = true
		return "", nil, err
	}
	return string(rest[:idLen]), rest[idLen:], nil
}

// parseStatus 解析响应负载头部的 [statusLen u8][status bytes]，返回状态与其后的字节。
func parseStatus(payload []byte) (string, []byte, error) {
	if len(payload) < 1 {
		return "", nil, fmt.Errorf("%w: 响应负载为空", ErrProtocol)
	}
	n := int(payload[0])
	if len(payload) < 1+n {
		return "", nil, fmt.Errorf("%w: 状态字段长度 %d 超出负载 %d 字节", ErrProtocol, n, len(payload))
	}
	return string(payload[1 : 1+n]), payload[1+n:], nil
}

// statusError 把服务端状态映射为 SDK 哨兵错误。返回 nil 表示成功。
func statusError(status string) error {
	switch status {
	case proto.StatusOK:
		return nil
	case proto.StatusNotFound:
		return ErrKeyNotFound
	case proto.StatusOverloaded:
		return ErrOverloaded
	case proto.StatusDropped:
		return ErrDropped
	case proto.StatusError:
		return ErrServer
	default:
		// 未知状态按服务端错误处理，而非静默当作成功——新版服务端可能引入新状态。
		return fmt.Errorf("%w: 未知状态 %q", ErrServer, status)
	}
}

// retryable 判定该错误是否值得重试。
//
// 「键不存在」与「被策略丢弃」是确定性结果，重试只会浪费往返；过载是服务端的背压信号，
// 正是为重试而设计；网络类错误可能是瞬时的，也重试。
func retryable(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrKeyNotFound), errors.Is(err, ErrDropped), errors.Is(err, ErrClosed):
		return false
	case errors.Is(err, ErrOverloaded), errors.Is(err, ErrServer), errors.Is(err, ErrProtocol):
		return true
	default:
		return true // 网络/超时类错误
	}
}
