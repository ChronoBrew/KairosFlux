package proto

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPutFrameRoundTrip(t *testing.T) {
	cases := []struct {
		key, value []byte
	}{
		{[]byte("imu:dev0:1"), []byte(`{"az":9.8}`)},
		{[]byte(""), []byte("")}, // 空 key/value 合法
		{[]byte("k"), nil},       // nil value 与空 slice 编解码后均得空 slice
		{[]byte("quote:2026-08-19:600000"), []byte(`{"code":"600000"}`)},
	}
	for _, c := range cases {
		frame := EncodePutFrame(c.key, c.value)
		key, value, ok := DecodePutFrame(frame)
		if !ok {
			t.Fatalf("解码应成功: key=%q value=%q", c.key, c.value)
		}
		if !bytes.Equal(key, c.key) {
			t.Fatalf("key 不符: got %q want %q", key, c.key)
		}
		if len(value) != len(c.value) || !bytes.Equal(value, c.value) {
			t.Fatalf("value 不符: got %q want %q", value, c.value)
		}
	}
}

func TestDecodePutFrame_TooShort(t *testing.T) {
	if _, _, ok := DecodePutFrame([]byte{1, 2, 3}); ok {
		t.Fatal("不足 8 字节的头部应解析失败")
	}
}

func TestDecodePutFrame_Incomplete(t *testing.T) {
	frame := EncodePutFrame([]byte("k"), []byte("v"))
	// 声明的 keyLen/valueLen 超出实际数据长度：截断负载模拟半个帧。
	if _, _, ok := DecodePutFrame(frame[:len(frame)-1]); ok {
		t.Fatal("声明长度超出实际数据的负载应解析失败")
	}
}

func TestDecodeKeyFrame(t *testing.T) {
	// GET/DELETE 负载格式：keyLen(u32 LE)+key，无 value 部分。
	buf := make([]byte, 4+3)
	binary.LittleEndian.PutUint32(buf[0:4], 3)
	copy(buf[4:], "abc")
	key, ok := DecodeKeyFrame(buf)
	if !ok {
		t.Fatal("解码应成功")
	}
	if !bytes.Equal(key, []byte("abc")) {
		t.Fatalf("key 不符: got %q", key)
	}
}

func TestDecodeKeyFrame_TooShort(t *testing.T) {
	if _, ok := DecodeKeyFrame([]byte{1, 2, 3}); ok {
		t.Fatal("不足 4 字节的头部应解析失败")
	}
}

func TestDecodeKeyFrame_Incomplete(t *testing.T) {
	buf := make([]byte, 4)
	buf[0] = 10 // 声明 keyLen=10，但负载只有头部 4 字节
	if _, ok := DecodeKeyFrame(buf); ok {
		t.Fatal("声明长度超出实际数据的负载应解析失败")
	}
}
