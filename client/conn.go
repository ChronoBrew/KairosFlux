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

// maxResponseFrameSize 是客户端愿意为一个响应负载预分配的硬上限。dataLen 是对端
// （服务端，或任何能在网络上冒充服务端的角色）声明的 u32，最大约 4.29GiB——不设
// 上限的话，`make([]byte, dataLen)` 会在实际读到任何负载字节之前就按对端声称的
// 长度分配内存，是 TLV 解析器的经典内存放大漏洞，服务端侧同一漏洞已在
// bannet.Connection（hardMaxPackageSize）修过，这里是同一条协议在客户端侧的镜像
// 攻击面：一个恶意或被攻陷的服务端只需回一个 6 字节头部就能让客户端尝试分配
// 数 GiB。此值与服务端默认的 MaxPackageSize 数量级一致，属于合理的响应体量上限。
const maxResponseFrameSize = 64 << 20 // 64MiB

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

	// 校验放在分配之前：绝不能先按对端声称的长度 make()，再发现读不到那么多字节——
	// 到那时候内存已经花出去了。见 maxResponseFrameSize 的注释。
	if dataLen > maxResponseFrameSize {
		c.broken = true // 协议要求之外的巨帧，判定连接不再可信，不放回连接池
		return "", nil, fmt.Errorf("%w: 响应声明的负载长度 %d 超过上限 %d", ErrProtocol, dataLen, maxResponseFrameSize)
	}

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

// statusError 把服务端状态映射为 SDK 哨兵错误。返回 nil 表示成功。rest 是状态
// 字段之后的剩余字节；目前只有 dropped 状态用它取丢弃原因（见 parseDropReason）。
func statusError(status string, rest []byte) error {
	switch status {
	case proto.StatusOK:
		return nil
	case proto.StatusNotFound:
		return ErrKeyNotFound
	case proto.StatusOverloaded:
		return ErrOverloaded
	case proto.StatusDropped:
		if reason := parseDropReason(rest); reason != "" {
			return fmt.Errorf("%w: %s", ErrDropped, reason)
		}
		return ErrDropped
	case proto.StatusError:
		return ErrServer
	default:
		// 未知状态按服务端错误处理，而非静默当作成功——新版服务端可能引入新状态。
		return fmt.Errorf("%w: 未知状态 %q", ErrServer, status)
	}
}

// parseDropReason 从 dropped 响应的剩余字节里解出丢弃原因：
// [reasonLen u16 LE][reason bytes]（见 service/router.go 的 droppedPayload、
// docs/BANLV-协议规范.md 的响应负载一节）。老服务端未实现该字段、或字节格式
// 不符时返回空字符串而不报错——这是可选的协议扩展，不应让老服务端的响应
// 被判为协议错误。
func parseDropReason(rest []byte) string {
	if len(rest) < 2 {
		return ""
	}
	n := int(binary.LittleEndian.Uint16(rest[0:2]))
	if len(rest) < 2+n {
		return ""
	}
	return string(rest[2 : 2+n])
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
