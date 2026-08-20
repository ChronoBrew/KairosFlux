// Package negotiate 实现 BANLV v1/v2 的 HELLO 协商（docs/rfc/BANLV-2.md
// §5/§5.1）：v2 客户端连接后先发一个 v1 格式的 HELLO 帧，按是否在超时内
// 收到 v2 格式的响应判断对端版本，零破坏地与 v1 共存于同一条连接。
//
// 本包是新增的构建块，不修改 bannet/transport、service/router.go 等任何
// 现有生产路径——真实的服务端/客户端接线（把这里的能力接进
// bannet.Server/client.Client）是后续阶段的工作，本阶段只交付协商逻辑本身
// 并用真实的 v1 生产服务端（service.Router）与本包提供的最小 v2 响应
// 函数验证它确实可用，而不是假设可用。
//
// 依赖 bannet/codec 里的 v1 DataPack（编码探测帧要用 v1 格式，见 §5.1）与
// v2 DataPackV2（解码/编码响应要用 v2 格式），以及 proto 的 MsgHello 常量
// ——这也是本包与 bannet/codec 分开的原因：codec 包的既有定位是"只认字节，
// 不认 msgID 该分派给谁"（见 bannet/codec/message.go 包注释），而协商逻辑
// 恰恰需要认识"HELLO"这个 msgID 语义，放进 codec 会违反它自己的分层。
package negotiate

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/proto"
)

// Version 是协商结果：对端实际使用的 BANLV 协议版本。
type Version uint8

const (
	VersionUnknown Version = iota
	VersionV1
	VersionV2
)

func (v Version) String() string {
	switch v {
	case VersionV1:
		return "v1"
	case VersionV2:
		return "v2"
	default:
		return "unknown"
	}
}

// AckTier 是 §11.2 的 ack 三档；本阶段客户端恒发送 AckEvery、服务端恒回复
// AckEvery——AckWindow/AckNone 的编号已保留，行为留给后续阶段实现，见
// §5.1 的说明。
type AckTier uint8

const (
	AckEvery  AckTier = 0
	AckWindow AckTier = 1 // §11 交互模式，第二阶段实现——本阶段不发送/不生效
	AckNone   AckTier = 2 // 同上
)

// ErrProtocol 标记协商过程中收到的字节不符合 §5.1 定义的格式——如收到了
// 响应但既不是合法的 v2 帧、也不匹配预期的 magic。
var ErrProtocol = errors.New("negotiate: protocol error")

// ConnPropertyAckTier 是协商成功后，服务端把连接的确认 ack 档位挂在
// handler.Conn 属性袋（SetProperty/Property）上使用的 key——RouterV2
// （service 包）据此读出当前连接应该按 every/window/none 哪一档处理写帧。
// 放在本包（AckTier 类型的权威定义处）而不是 service 包，避免两处各自
// 定义这个字符串常量而漂移。
const ConnPropertyAckTier = "banlv.v2.ackTier"

// encodeHelloProbe 按 §5.1 编码 v2 客户端的探测帧：v1 帧格式
// （msgID="HELLO"），负载 [version u8][ackPref u8]。
func encodeHelloProbe(want AckTier) ([]byte, error) {
	payload := []byte{codec.VersionV2, byte(want)}
	return codec.NewDataPack().Pack(codec.NewMessage(proto.MsgHello, payload))
}

// BuildHelloResponseV2 按 §5.1 构造 v2 服务端对探测帧的响应：v2 帧格式，
// opcode=OK、type=0、corr_id=0，负载 [version u8][ackPref u8]（服务端选定
// 版本 + 确认的 ack 档位）。保留本阶段之前的签名（恒确认 AckEvery）不变，
// 只是转发到 BuildHelloResponseV2WithAck——第一阶段的调用方
// （negotiate_test.go 的最小 v2 服务端）与本函数的行为不受第二阶段新增的
// "ackPref 真正生效"影响。
func BuildHelloResponseV2() ([]byte, error) {
	return BuildHelloResponseV2WithAck(AckEvery)
}

// BuildHelloResponseV2WithAck 按 §5.1 构造 v2 服务端响应，确认档位为 ack
// （第二阶段：真正按客户端请求的 ackPref 协商，见 ServerHandleProbe）。
func BuildHelloResponseV2WithAck(ack AckTier) ([]byte, error) {
	payload := []byte{codec.VersionV2, byte(ack)}
	msg := codec.NewMessageV2(codec.HeaderV2{
		Opcode: codec.OpcodeOK,
		Type:   codec.TypeUnspecified,
		CorrID: 0,
	}, payload)
	return codec.NewDataPackV2().Pack(msg)
}

// isValidAckTier 判断 ackPref 字节是否是本实现认识的三档之一（§11.2）。
func isValidAckTier(v uint8) bool {
	return v == uint8(AckEvery) || v == uint8(AckWindow) || v == uint8(AckNone)
}

// ServerHandleProbe 是服务端侧协商能力的核心（第二阶段生产接线用）：给定
// 一个刚用 v1 codec 解出的帧，判断它是不是 §5.1 描述的 HELLO 探测帧；若是，
// 解析客户端请求的 ackPref 并按其协商（§11.2/§5.1"ackPref 生效"，与本阶段
// 之前"恒回 AckEvery、忽略请求值"的行为不同），构造 v2 格式响应返回。
//
// 返回 respond 是要写回连接的完整帧字节（isProbe=true 时才有意义）；ack
// 是协商确定的档位，调用方应把它挂到该连接的 ConnPropertyAckTier 属性上。
// isProbe=false 表示这不是 HELLO 探测帧，调用方应该把 msg 交给正常的 v1
// 分派路径处理，不消耗它。
//
// 未定义/非法的 ackPref 字节按 AckEvery 处理并原样回声这一决定（不是拒绝
// 连接）——协议对"客户端发了个协商阶段还不认识的档位"这类情况选择宽容
// 降级而非报错，与 RFC §5.1 本阶段之前"服务端忽略非 0x00 取值"的精神一致，
// 只是现在"忽略"变成了"按 AckEvery 确认"而不是硬编码为 AckEvery。
func ServerHandleProbe(msg *codec.Message) (respond []byte, ack AckTier, isProbe bool, err error) {
	if msg.MsgID() != proto.MsgHello {
		return nil, AckEvery, false, nil
	}
	payload := msg.Payload()
	requested := AckEvery
	if len(payload) >= 2 && isValidAckTier(payload[1]) {
		requested = AckTier(payload[1])
	}
	respond, err = BuildHelloResponseV2WithAck(requested)
	if err != nil {
		return nil, AckEvery, true, fmt.Errorf("negotiate: 构造 v2 HELLO 响应失败: %w", err)
	}
	return respond, requested, true, nil
}

// ClientNegotiate 是 v2 客户端一侧的协商入口：向 conn 写入 §5.1 的探测帧，
// 在 timeout 内等待响应，返回协商结果。
//
// 三种读取结果分别处理（不能塌缩成"读失败就当 v1"，那样会把"连接中途
// 出了真实故障"悄悄误判成"降级成功"）：
//
//  1. 在读到任何字节之前就超时：这是 RFC §5 描述的正常降级信号——对端是
//     v1 服务端，静默不响应 HELLO 帧。返回 (VersionV1, AckEvery, nil)，
//     不是错误，conn 上的读超时会被清除，调用方可以把同一条连接接着当
//     v1 连接用（v1 服务端的读循环逐帧独立读取，见 §5 原文）。
//  2. 超时或提前 EOF，但已经读到了部分字节：响应帧被半消费，conn 已经
//     不可信（后续字节要么错位、要么永远读不完整），返回错误，调用方
//     不应该复用这条连接。
//  3. 收到完整的 2 字节 magic+ver 探测：按 SniffVersion 分派——是 v2 则
//     继续读完整个响应帧，是"其他"（理论上不该发生：真正的 v1 服务端
//     根本不会发送任何字节）则报协议错误，不静默当 v1。
//
// 调用方需自行处理 conn 的关闭；ClientNegotiate 只负责协商这一步的读写与
// 期间的读超时设置/清除。
func ClientNegotiate(conn net.Conn, timeout time.Duration) (Version, AckTier, error) {
	return ClientNegotiateWithAck(conn, timeout, AckEvery)
}

// ClientNegotiateWithAck 与 ClientNegotiate 相同，额外让调用方指定 §11.2
// 期望的 ack 档位（第二阶段：ackPref 真正生效，见 ServerHandleProbe）。
// ClientNegotiate 保留旧签名不变、恒请求 AckEvery，是这个函数在
// want=AckEvery 时的特化——第一阶段遗留的四个协商测试因此不需要改一行就
// 继续验证 every 路径不变。
func ClientNegotiateWithAck(conn net.Conn, timeout time.Duration, want AckTier) (Version, AckTier, error) {
	probe, err := encodeHelloProbe(want)
	if err != nil {
		return VersionUnknown, AckEvery, fmt.Errorf("negotiate: 编码探测帧失败: %w", err)
	}
	if _, err := conn.Write(probe); err != nil {
		return VersionUnknown, AckEvery, fmt.Errorf("negotiate: 写探测帧失败: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return VersionUnknown, AckEvery, fmt.Errorf("negotiate: 设置读超时失败: %w", err)
	}
	// 无论走哪条返回路径都要清掉读超时，不能让协商阶段设置的临时超时
	// 泄漏到调用方后续复用这条连接（无论按 v1 还是 v2）的正常读写里。
	defer conn.SetReadDeadline(time.Time{})

	magicVer := make([]byte, 2)
	n, err := io.ReadFull(conn, magicVer)
	if err != nil {
		if n == 0 && isTimeout(err) {
			// 情形 1：一个字节都没读到就超时——真实的 v1 服务端不会回应
			// HELLO 帧，这是预期中的正常降级路径，不是错误。
			return VersionV1, AckEvery, nil
		}
		// 情形 2（n>0 的超时/EOF）与其他任何非超时错误，一律是真实故障，
		// 不得回退为"当作 v1"——连接可能已经被半消费，交由调用方决定
		// 是否丢弃这条连接。
		return VersionUnknown, AckEvery, fmt.Errorf("negotiate: 读响应前 2 字节失败: %w", err)
	}

	switch codec.SniffVersion(magicVer) {
	case codec.SniffV2:
		return finishReadV2Response(conn, magicVer)
	case codec.SniffUnsupportedVersion:
		return VersionUnknown, AckEvery, fmt.Errorf("%w: 对端 magic 匹配但版本号不受支持", ErrProtocol)
	default:
		// SniffV1：收到了字节，但不携带 v2 magic——按 §5 的设计，真正的
		// v1 服务端根本不该发送任何响应，收到不认识的字节是协议错误，
		// 不是"降级为 v1"（降级只应该由"完全没收到字节"触发）。
		return VersionUnknown, AckEvery, fmt.Errorf("%w: 收到非预期的响应字节（既非超时也非 v2 magic）", ErrProtocol)
	}
}

// finishReadV2Response 在已经读到头 2 字节（携带 v2 magic）之后，读完
// 剩余的 12 字节头部与负载，解出服务端确认的版本/ack 档位。
func finishReadV2Response(conn net.Conn, magicVer2Bytes []byte) (Version, AckTier, error) {
	rest := make([]byte, codec.HeaderV2Len-2)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return VersionUnknown, AckEvery, fmt.Errorf("negotiate: 读响应剩余头部失败: %w", err)
	}
	head := append(append([]byte{}, magicVer2Bytes...), rest...)
	h, err := codec.NewDataPackV2().UnPack(head)
	if err != nil {
		return VersionUnknown, AckEvery, fmt.Errorf("negotiate: 解析 v2 响应头失败: %w", err)
	}
	if h.Opcode != codec.OpcodeOK {
		return VersionUnknown, AckEvery, fmt.Errorf("%w: 响应 opcode=%#x，期望 OK(0x80)", ErrProtocol, h.Opcode)
	}
	payload := make([]byte, h.DataLen)
	if h.DataLen > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			return VersionUnknown, AckEvery, fmt.Errorf("negotiate: 读响应负载失败: %w", err)
		}
	}
	if len(payload) < 2 {
		return VersionUnknown, AckEvery, fmt.Errorf("%w: 响应负载长度=%d，期望至少 2 字节 [version][ackPref]", ErrProtocol, len(payload))
	}
	serverVersion := payload[0]
	if serverVersion != codec.VersionV2 {
		return VersionUnknown, AckEvery, fmt.Errorf("%w: 服务端选定版本=%#x，本实现只认识 %#x", ErrProtocol, serverVersion, codec.VersionV2)
	}
	// ackPref (payload[1])：第二阶段服务端按客户端请求的档位协商并回声确认
	// 值（见 ServerHandleProbe），不再恒为 AckEvery——客户端应以服务端回声
	// 的这个值为准（而不是自己请求的值），两者理论上应该相等（本实现的
	// 服务端总是原样确认合法档位），但协议层面客户端不应假设这一点。
	ack := AckTier(payload[1])
	if !isValidAckTier(payload[1]) {
		return VersionUnknown, AckEvery, fmt.Errorf("%w: 服务端确认的 ackPref=%#x 不是本实现认识的档位", ErrProtocol, payload[1])
	}
	return VersionV2, ack, nil
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
