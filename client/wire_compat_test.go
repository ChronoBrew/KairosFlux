package client

import (
	"bytes"
	"errors"
	"testing"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/pkg/proto"
)

// TestFrameEncodingMatchesServer 交叉校验 SDK 的帧编码与服务端实现逐字节一致。
//
// SDK 自行实现线格式而不导入 bannet（后者是服务端实现包，含监听、连接管理与 worker 池，
// 不应进入客户端依赖图）。代价是两份实现可能各自演进而漂移，本测试即为此设的护栏：
// 一旦任一侧改动帧布局，这里立刻失败。
//
// 注意本测试位于包内（package client）而非 _test 外部包，故它引用 bannet 只影响测试
// 二进制，不会成为 SDK 的对外依赖。
func TestFrameEncodingMatchesServer(t *testing.T) {
	cases := []struct {
		name  string
		msgID string
		data  []byte
	}{
		{"典型 PUT", proto.MsgPut, []byte("some-payload")},
		{"空负载", proto.MsgGet, nil},
		{"空负载与空 msgID", "", nil},
		{"二进制负载", proto.MsgDelete, []byte{0x00, 0xFF, 0x7F, 0x80}},
		{"较长 msgID", proto.MsgScan, bytes.Repeat([]byte("x"), 300)},
	}

	dp := bannet.NewDataPack()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mine := encodeFrame(tc.msgID, tc.data)

			theirs, err := dp.Pack(bannet.NewMessage(tc.msgID, tc.data))
			if err != nil {
				t.Fatalf("服务端 Pack 失败: %v", err)
			}

			if !bytes.Equal(mine, theirs) {
				t.Fatalf("帧编码与服务端不一致\n SDK  = %v\n 服务端 = %v", mine, theirs)
			}
		})
	}
}

// TestFrameHeadLenMatchesServer 固定帧头长度常量与服务端一致。
func TestFrameHeadLenMatchesServer(t *testing.T) {
	if got, want := frameHeadLen, int(bannet.NewDataPack().HeadLen()); got != want {
		t.Fatalf("frameHeadLen = %d, 服务端 HeadLen = %d", got, want)
	}
}

// TestStatusErrorMapping 固定状态码到哨兵错误的映射，尤其是「不存在」不得被当作故障。
func TestStatusErrorMapping(t *testing.T) {
	tests := []struct {
		status    string
		wantErr   error
		wantRetry bool
	}{
		{proto.StatusOK, nil, false},
		{proto.StatusNotFound, ErrKeyNotFound, false},
		{proto.StatusOverloaded, ErrOverloaded, true},
		{proto.StatusDropped, ErrDropped, false},
		{proto.StatusError, ErrServer, true},
	}
	for _, tc := range tests {
		got := statusError(tc.status)
		if tc.wantErr == nil {
			if got != nil {
				t.Fatalf("status %q 应为成功, 实际 %v", tc.status, got)
			}
			continue
		}
		if got == nil || !errors.Is(got, tc.wantErr) {
			t.Fatalf("status %q 应映射为 %v, 实际 %v", tc.status, tc.wantErr, got)
		}
		if retryable(got) != tc.wantRetry {
			t.Fatalf("status %q 的可重试判定应为 %v", tc.status, tc.wantRetry)
		}
	}
}
