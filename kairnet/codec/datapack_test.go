package codec

// Decode 是重构第一步新增的合并点（原本"读 msgID/负载/校验帧长上限"散落在
// connection.go 的读循环里，见 docs/rfc/bannet-重构.md B.3），它现在是每一帧
// 入站数据的唯一入口，此前只靠 kairnet 包里端到端的 oversized_frame_test.go /
// malformed_frame_test.go 间接验证，本文件补上 codec 包自身的直接单元测试，
// 尤其是 beforeRead 回调"每个逻辑读取单元恰好调用一次"这个承诺——这是
// resetReadDeadline 语义是否被保留的唯一可验证点，此前没有任何测试断言过
// 调用次数。

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// buildFrame 按 Kair v1 布局拼一帧：[dataLen u32 LE][idLen u16 LE][id][data]。
func buildFrame(id string, data []byte) []byte {
	head := make([]byte, 6)
	binary.LittleEndian.PutUint32(head[0:4], uint32(len(data)))
	binary.LittleEndian.PutUint16(head[4:6], uint16(len(id)))
	buf := append(head, []byte(id)...)
	buf = append(buf, data...)
	return buf
}

func TestDecodeHappyPath(t *testing.T) {
	dp := NewDataPack()
	frame := buildFrame("put", []byte("hello"))

	var calls int
	msg, err := dp.Decode(bytes.NewReader(frame), 1024, func() { calls++ })
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if msg.MsgID() != "put" {
		t.Errorf("MsgID = %q, want %q", msg.MsgID(), "put")
	}
	if string(msg.Payload()) != "hello" {
		t.Errorf("Payload = %q, want %q", msg.Payload(), "hello")
	}
	// 头部 + msgID + 负载：三个逻辑读取单元都非空，beforeRead 应恰好调用 3 次。
	if calls != 3 {
		t.Errorf("beforeRead 调用次数 = %d, want 3（头部/msgID/负载各一次）", calls)
	}
}

func TestDecodeBeforeReadCallCount(t *testing.T) {
	dp := NewDataPack()

	cases := []struct {
		name      string
		id        string
		data      []byte
		wantCalls int
	}{
		{"无msgID无负载", "", nil, 1},
		{"仅msgID", "x", nil, 2},
		{"仅负载", "", []byte("y"), 2},
		{"msgID与负载都有", "put", []byte("v"), 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame := buildFrame(tc.id, tc.data)
			var calls int
			if _, err := dp.Decode(bytes.NewReader(frame), 1024, func() { calls++ }); err != nil {
				t.Fatalf("Decode 失败: %v", err)
			}
			if calls != tc.wantCalls {
				t.Errorf("beforeRead 调用次数 = %d, want %d", calls, tc.wantCalls)
			}
		})
	}
}

func TestDecodeBeforeReadNilIsOptional(t *testing.T) {
	dp := NewDataPack()
	frame := buildFrame("put", []byte("v"))
	// beforeRead 为 nil 必须被安全跳过，不panic——transport 之外的调用方
	// （如本测试、未来可能的工具）不一定关心读超时重设。
	if _, err := dp.Decode(bytes.NewReader(frame), 1024, nil); err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
}

func TestDecodeHeaderEOF(t *testing.T) {
	dp := NewDataPack()
	// 空 Reader：对端在帧边界正常断开，Decode 必须原样传出 io.EOF（外层
	// connection.go 靠 errors.Is(err, io.EOF) 区分"正常断开"与其它错误，
	// 这个契约必须成立）。
	_, err := dp.Decode(bytes.NewReader(nil), 1024, nil)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want wraps io.EOF", err)
	}
}

func TestDecodeTruncatedMsgID(t *testing.T) {
	dp := NewDataPack()
	frame := buildFrame("put", []byte("v"))
	// 只送出头部 + 半个 msgID：io.ReadFull 在读到部分字节后遇到 EOF，
	// 返回 io.ErrUnexpectedEOF，而不是 io.EOF——这是"帧中途断开"与
	// "帧边界正常断开"的关键区分点。
	truncated := frame[:6+1] // 头部(6) + msgID 的前 1 字节（"put" 共 3 字节）
	_, err := dp.Decode(bytes.NewReader(truncated), 1024, nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want wraps io.ErrUnexpectedEOF", err)
	}
}

func TestDecodeTruncatedBody(t *testing.T) {
	dp := NewDataPack()
	frame := buildFrame("put", []byte("hello"))
	truncated := frame[:len(frame)-2] // 掐掉负载最后 2 字节
	_, err := dp.Decode(bytes.NewReader(truncated), 1024, nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want wraps io.ErrUnexpectedEOF", err)
	}
}

func TestDecodeFrameTooLarge(t *testing.T) {
	dp := NewDataPack()
	frame := buildFrame("put", make([]byte, 100))
	// maxSize=10 但帧声明负载 100 字节：必须在读负载之前就拒绝（否则测试会
	// 因为 Reader 里没有 100 字节数据而以另一种错误失败，而不是我们要断言
	// 的 ErrFrameTooLarge——用一个只包含头部+msgID、不含负载字节的 Reader
	// 更能证明"确实没有尝试读负载"）。
	noPayload := frame[:6+len("put")]
	_, err := dp.Decode(bytes.NewReader(noPayload), 10, nil)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want wraps ErrFrameTooLarge", err)
	}
}

func TestEffectiveMaxSizeFallsBackToHardCapOnZero(t *testing.T) {
	if got := EffectiveMaxSize(0); got != hardMaxPackageSize {
		t.Errorf("EffectiveMaxSize(0) = %d, want %d（硬上限兜底）", got, hardMaxPackageSize)
	}
	if got := EffectiveMaxSize(4096); got != 4096 {
		t.Errorf("EffectiveMaxSize(4096) = %d, want 4096（非零值原样透传）", got)
	}
}

func TestDecodeMaxSizeZeroUsesHardCapNotUnlimited(t *testing.T) {
	dp := NewDataPack()
	// maxSize=0 必须解释成"用硬上限兜底"，而不是"不限制"——否则一个声称
	// 巨大负载的帧头会在这里被当作合法帧放行到下一步（读负载），重新打开
	// TLV 内存放大 DoS 的入口。用一个刚好超过硬上限的声称长度、且 Reader
	// 里不提供任何负载字节来验证：如果 Decode 真的把 0 当"不限"，就会去
	// 尝试读负载并因为 Reader 耗尽而返回 io.ErrUnexpectedEOF，而不是我们
	// 期望的 ErrFrameTooLarge——两种错误可以明确区分，不会误判。
	over := buildFrame("id", nil)
	binary.LittleEndian.PutUint32(over[0:4], hardMaxPackageSize+1)
	noPayload := over[:6+2]
	_, err := dp.Decode(bytes.NewReader(noPayload), 0, nil)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want wraps ErrFrameTooLarge（maxSize=0 应退回硬上限而非不限制）", err)
	}

	// 对照：一个远小于硬上限的声称长度，在 maxSize=0 下应该被正常接受
	// （即走到"读负载"这一步，而不是被误判为超限）。
	small := buildFrame("id", []byte("ok"))
	_, err = dp.Decode(bytes.NewReader(small), 0, nil)
	if err != nil {
		t.Fatalf("小帧在 maxSize=0 下不应被拒绝: %v", err)
	}
}

func TestUnPackHeadTooShort(t *testing.T) {
	dp := NewDataPack()
	if _, err := dp.UnPack([]byte{1, 2, 3}); err == nil {
		t.Fatal("头部不足 6 字节应报错")
	}
}

func TestPackMsgIDTooLong(t *testing.T) {
	dp := NewDataPack()
	longID := make([]byte, 0x10000)
	if _, err := dp.Pack(NewMessage(string(longID), nil)); err == nil {
		t.Fatal("msgID 超过 uint16 上限应报错")
	}
}
