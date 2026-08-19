package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// 报文格式:
//   [dataLen u32 LE][msgIDLen u16 LE][msgID bytes][data bytes]
// 头部固定 6 字节; msgID 与 data 是变长 trailing 部分。

// Codec 是帧编解码的抽象契约，供上层（transport）依赖，不依赖具体实现。
type Codec interface {
	HeadLen() uint32
	Pack(msg *Message) ([]byte, error)
	UnPack([]byte) (*Message, error)
}

type DataPack struct{}

var _ Codec = &DataPack{}

func NewDataPack() *DataPack { return &DataPack{} }

func (dp *DataPack) HeadLen() uint32 {
	return 6 // dataLen u32 + msgIDLen u16
}

// Pack 编码一帧。按最终长度一次性分配并直接写入定长头部，不经 bytes.Buffer 的增量扩容，
// 也不经 binary.Write 的反射路径——Pack 位于每个响应的必经路径上。
func (dp *DataPack) Pack(msg *Message) ([]byte, error) {
	id := msg.MsgID()
	if len(id) > 0xFFFF {
		return nil, fmt.Errorf("msgID too long: %d", len(id))
	}
	data := msg.Payload()

	head := int(dp.HeadLen())
	buf := make([]byte, head+len(id)+len(data))
	binary.LittleEndian.PutUint32(buf[0:4], msg.MsgLen())
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(id)))
	copy(buf[head:], id)
	copy(buf[head+len(id):], data)
	return buf, nil
}

// UnPack 只解析定长头部（6 字节），返回带 DataLen 与 IDLen 的占位 Message；
// 调用方拿到 IDLen 后，还需要从连接读取 IDLen+DataLen 字节填充 ID 与 Data——
// 这一步现由 Decode 统一完成（见下），UnPack 保留原有的"只解头部"语义不变，
// 是 Decode 的构建块，也供只需要解头部的调用方（如测试）单独使用。
//
// 它不校验帧长上限：那是策略而非编解码，由 Decode 的 maxSize 参数或调用方执行。
func (dp *DataPack) UnPack(data []byte) (*Message, error) {
	if len(data) < int(dp.HeadLen()) {
		return nil, errors.New("head too short")
	}

	// 直接按小端解析定长头部，不经 bytes.Reader + binary.Read 的反射路径。
	msg := &Message{}
	msg.DataLen = binary.LittleEndian.Uint32(data[0:4])
	idLen := binary.LittleEndian.Uint16(data[4:6])

	// 借用 Id 暂存 IDLen 信息: 调用方先从 MsgID() 拿不到东西, 通过头部之后另读 IDLen 字节填回。
	// 这里用 SetMsgLen 仅保留 DataLen 不冲突, IDLen 通过返回的 Message.IDLen 提供。
	msg.IDLen = idLen
	return msg, nil
}

// ErrFrameTooLarge 标记 Decode 因帧声明长度超过生效上限而拒绝的情形，供调用方
// 用 errors.Is 区分"对端违反长度约定"（通常该记 Warn，且值得单独计数/告警）与
// 其他 I/O 错误（读超时、连接被关闭等，通常只需 Debug/Error）。
var ErrFrameTooLarge = errors.New("codec: frame exceeds max size")

// hardMaxPackageSize 是帧长上限的绝对安全兜底：调用方（transport）配置的
// maxSize 若为 0（运维本意常是"不限制"），Decode 用这个常量兜底而不是真的不设
// 上限——攻击者只需发 6 字节头部声称 dataLen=0xFFFFFFFF（近 4GiB）就能让服务端
// 在读负载前先做一次 4GiB 的 make([]byte,...)，这是 TLV 解析器最经典的内存
// 放大 DoS，配置项的"0=不限"语义本身就是这个漏洞的入口。真的需要更大帧的部署
// 应显式调高上限，而不是设 0。
const hardMaxPackageSize = 256 << 20 // 256MiB

// EffectiveMaxSize 把调用方配置的帧长上限（0 表示"未配置/不限制"）转换成实际
// 生效的上限：0 时退回 hardMaxPackageSize。供 transport 在调用 Decode 前计算
// 要传入的 maxSize，也可以直接由 Decode 内部处理（Decode 对 maxSize==0 做同样
// 的兜底），这里单独导出是为了让调用方能在日志里打印"实际生效的上限是多少"。
func EffectiveMaxSize(configured uint32) uint32 {
	if configured == 0 {
		return hardMaxPackageSize
	}
	return configured
}

// Decode 从 r 里读出一条完整的帧：定长头部 → （校验 dataLen 不超过 maxSize）→
// msgID 字节 → 负载字节，返回组装完整的 Message。
//
// 这是本次重构从 transport 层（原 connection.go 的 StartReader 循环）合并进
// 编解码层的逻辑——此前 UnPack 只解头部，"读 msgID、读负载、校验帧长上限"这几步
// 散落在 transport 的读循环里，是分层不完整的直接证据（见
// docs/rfc/bannet-重构.md B.3）。合并后 transport 只管"喂字节给 Decode、把
// Decode 吐出的 Message 转交上层"，不再需要知道 BANLV 帧格式的任何细节。
//
// maxSize<=0 时退回 hardMaxPackageSize（见 EffectiveMaxSize），不是真的不限制。
//
// beforeRead 在每一次阻塞读（头部/msgID/负载，最多三次）之前被调用一次，
// 可以为 nil。存在的意义：transport 需要在每个逻辑读取单元开始前重设读超时
// （resetReadDeadline），一个正常但慢的大帧不该被腰斩，只有"某一步完全不发了"
// 才应该触发超时——这个语义必须在真正阻塞之前生效，Decode 把这一步做成一个
// 回调钩子，而不是要求调用方在 Decode 内部再重新实现一遍读取循环。
func (dp *DataPack) Decode(r io.Reader, maxSize uint32, beforeRead func()) (*Message, error) {
	effMax := EffectiveMaxSize(maxSize)

	if beforeRead != nil {
		beforeRead()
	}
	headData := make([]byte, dp.HeadLen())
	if _, err := io.ReadFull(r, headData); err != nil {
		return nil, fmt.Errorf("decode: read header: %w", err)
	}

	msg, err := dp.UnPack(headData)
	if err != nil {
		return nil, fmt.Errorf("decode: unpack header: %w", err)
	}

	if msg.MsgLen() > effMax {
		return nil, fmt.Errorf("decode: %w: dataLen=%d max=%d", ErrFrameTooLarge, msg.MsgLen(), effMax)
	}

	if msg.IDLen > 0 {
		if beforeRead != nil {
			beforeRead()
		}
		idBuf := make([]byte, msg.IDLen)
		if _, err := io.ReadFull(r, idBuf); err != nil {
			return nil, fmt.Errorf("decode: read msgID: %w", err)
		}
		msg.SetMsgID(string(idBuf))
	}

	if msg.MsgLen() > 0 {
		if beforeRead != nil {
			beforeRead()
		}
		data := make([]byte, msg.MsgLen())
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("decode: read body: %w", err)
		}
		msg.SetData(data)
	}

	return msg, nil
}
