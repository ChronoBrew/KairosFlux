package jobctl

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// V2Store 是 Store 的生产实现：对 KairosFlux 服务端发起 v2 协议请求，只用
// PUT_VERSIONED / GET_AS_OF 两个既有 opcode（M2 并行分支在扩 storage 版本
// 记录编码与新增 opcode，本包不碰那部分，也不需要新 opcode）。
//
// 拼帧方式与 cmd/kairosflux-cli/temporal.go 的 putVersioned/getAsOf 同一
// 思路（"只用 kairnet/negotiate 与 kairnet/codec 已经导出的协商/拼帧函数
// 拼出最小客户端，不去扩建一整套 v2 SDK"），区别是这里维护一条长连接给
// reconcile loop 反复复用，而不是每次都新拨号——reconcile loop 的调用
// 频率远高于 CLI 的一次性调用，每次都拨号+协商的开销不适合长驻进程。
type V2Store struct {
	addr    string
	timeout time.Duration

	mu   sync.Mutex
	conn net.Conn
}

// NewV2Store 构造一个尚未连接的 V2Store；第一次调用 PutVersioned/GetLatest
// 时才真正拨号（懒连接），连接失败或读写出错后下一次调用会重新拨号。
func NewV2Store(addr string, timeout time.Duration) *V2Store {
	return &V2Store{addr: addr, timeout: timeout}
}

func (s *V2Store) ensureConn() (net.Conn, error) {
	if s.conn != nil {
		return s.conn, nil
	}
	conn, err := net.DialTimeout("tcp", s.addr, s.timeout)
	if err != nil {
		return nil, fmt.Errorf("拨号 %s 失败: %w", s.addr, err)
	}
	version, ack, err := negotiate.ClientNegotiateWithAck(conn, s.timeout, negotiate.AckEvery)
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
	s.conn = conn
	return conn, nil
}

// roundTrip 发一帧、等一帧响应；corr_id 固定为 1，与 CLI 瘦客户端同理——
// 这条连接对这两个 opcode 只做"发一个请求、等一个响应"，不需要 corr_id
// 区分并发请求（reconcile loop 是单进程串行调用 Store，见 reconciler.go
// 的调用方式，同一时刻只有一个请求在途）。出错时关闭并清空连接，下次调用
// 触发重连——不在这里自动重试，重试策略是 reconciler 的职责（区分"这次
// 网络故障"与"job 执行体本身失败"两种不同的重试语义，混在一起会让重试
// 计数的含义变得含糊）。
func (s *V2Store) roundTrip(opcode uint8, payload []byte) (*codec.MessageV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.ensureConn()
	if err != nil {
		return nil, err
	}

	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: codec.TypeUnspecified, CorrID: 1}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		return nil, fmt.Errorf("编帧失败: %w", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		s.dropConn()
		return nil, fmt.Errorf("设置写超时失败: %w", err)
	}
	if _, err := conn.Write(frame); err != nil {
		s.dropConn()
		return nil, fmt.Errorf("写帧失败: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(s.timeout)); err != nil {
		s.dropConn()
		return nil, fmt.Errorf("设置读超时失败: %w", err)
	}
	resp, err := codec.NewDataPackV2().Decode(conn, 0, nil)
	if err != nil {
		s.dropConn()
		return nil, fmt.Errorf("读帧失败: %w", err)
	}
	return resp, nil
}

// dropConn 必须在已持有 s.mu 时调用。
func (s *V2Store) dropConn() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func errFromErrResp(resp *codec.MessageV2) error {
	code, reason, ok := proto.DecodeV2ErrPayload(resp.Payload)
	if !ok {
		return fmt.Errorf("服务端返回 ERR，但负载无法解析")
	}
	return fmt.Errorf("服务端拒绝: code=%#x reason=%s", code, reason)
}

// PutVersioned 实现 Store：对应 PUT_VERSIONED opcode。
func (s *V2Store) PutVersioned(logicalKey string, payload []byte) (uint64, error) {
	resp, err := s.roundTrip(codec.OpcodePutVersioned, proto.EncodePutFrame([]byte(logicalKey), payload))
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

// GetLatest 实现 Store：对应 GET_AS_OF opcode，as_of 取调用时刻——PUT_VERSIONED
// 保证不会有"未来"写入，as_of=now 恒等于"当前最新版本"（GetAsOf 语义见
// internal/temporal.AsOf 文档），这就是本包不新增"GET_CURRENT"之类的 opcode
// 也能拿到当前值的原因。
func (s *V2Store) GetLatest(logicalKey string) ([]byte, bool, error) {
	resp, err := s.roundTrip(codec.OpcodeGetAsOf, proto.EncodeAsOfFrame([]byte(logicalKey), time.Now().UnixNano()))
	if err != nil {
		return nil, false, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		if _, reason, ok := proto.DecodeV2ErrPayload(resp.Payload); ok && reason == "notfound" {
			return nil, false, nil
		}
		return nil, false, errFromErrResp(resp)
	}
	_, _, payload, _, ok := proto.DecodeVersionEntry(resp.Payload)
	if !ok {
		return nil, false, fmt.Errorf("响应负载无法解析")
	}
	return payload, true, nil
}

// ListVersions 实现对 LIST_VERSIONS opcode 的调用（既有 opcode，不是新增；
// 只在审计/测试路径用，Reconciler 的热路径只用 PutVersioned/GetLatest）。
// 供集成测试直接对真实账本断言版本数——"结果与账本一致"这条验收标准要求
// 验证的是 service.TemporalStore 的真实版本序列，不是 Store 接口某个内存
// 模拟实现之下的版本序列。
func (s *V2Store) ListVersions(logicalKey string) ([]proto.VersionEntryView, error) {
	resp, err := s.roundTrip(codec.OpcodeListVersions, proto.EncodeKeyOnlyFrame([]byte(logicalKey)))
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

// Close 关闭底层连接（进程退出前清理）。
func (s *V2Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}
