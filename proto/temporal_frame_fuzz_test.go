package proto

import "testing"

// FuzzDecodeListWritesRequest 对 LIST_WRITES 请求解码跑随机字节——全新
// opcode（0x0D，时态内核 M2），没有 M0 遗留格式要兼容，但请求负载有四个
// 攻击者可控的长度字段（prefixLen/两个定长时间戳之间没有长度字段/sourceLen），
// 服务端在 RouterV2.handleListWrites 里对未经任何前置校验的网络字节直接调用
// 这个函数。
func FuzzDecodeListWritesRequest(f *testing.F) {
	f.Add([]byte{})
	f.Add(EncodeListWritesRequest([]byte("quote:2026-08-17:"), 100, 200, []byte("job-a")))
	f.Add(EncodeListWritesRequest(nil, 0, 0, nil))
	huge := make([]byte, 4)
	huge[0] = 0xFF
	huge[1] = 0xFF
	huge[2] = 0xFF
	huge[3] = 0xFF // prefixLen 声明 42 亿
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		prefix, _, _, source, ok := DecodeListWritesRequest(data)
		if !ok {
			return
		}
		if len(prefix) > len(data) || len(source) > len(data) {
			t.Fatalf("解码出的字段长度超过了原始 payload 长度: prefix=%d source=%d payload=%d",
				len(prefix), len(source), len(data))
		}
	})
}

// FuzzDecodeWriteEnvelopeEntry 对 LIST_WRITES 响应体单条信封解码跑随机字节
// ——这条记录里的 payload 直接来自磁盘上的版本键 value（经
// service.TemporalStore.ListWrites 转发），任意四个长度字段
// （logicalKeyLen/sourceLen/hashLen/payloadLen）都可能因为上游数据损坏而
// 携带异常值。
func FuzzDecodeWriteEnvelopeEntry(f *testing.F) {
	f.Add([]byte{})
	f.Add(EncodeWriteEnvelopeEntry("quote:2026-08-17:600000", 3, 999, "job-a", 2, "deadbeef", []byte("payload"), true))
	f.Add(EncodeWriteEnvelopeEntry("", 0, 0, "", 0, "", nil, false))

	f.Fuzz(func(t *testing.T, data []byte) {
		v, consumed, ok := DecodeWriteEnvelopeEntry(data)
		if !ok {
			return
		}
		if consumed > len(data) {
			t.Fatalf("consumed=%d 超过了输入长度=%d", consumed, len(data))
		}
		if len(v.LogicalKey) > len(data) || len(v.Source) > len(data) ||
			len(v.PayloadHash) > len(data) || len(v.Payload) > len(data) {
			t.Fatalf("解码出的字段长度超过了原始输入长度: %+v (输入长度=%d)", v, len(data))
		}
	})
}

// FuzzDecodeReplayFingerprintRequest 对 REPLAY_FINGERPRINT 请求解码跑随机
// 字节。M2 升级后这个函数按精确剩余长度（0 或 8 字节）分流到无界/带上界两条
// 路径，是本次改动新增的长度判断分支，需要覆盖。
func FuzzDecodeReplayFingerprintRequest(f *testing.F) {
	f.Add([]byte{})
	f.Add(EncodeKeyOnlyFrame([]byte("prefix:")))                     // 旧格式：无上界
	f.Add(EncodeReplayFingerprintRequest([]byte("prefix:"), 123456)) // 新格式：带上界
	f.Add(EncodeKeyOnlyFrame(nil))

	f.Fuzz(func(t *testing.T, data []byte) {
		prefix, _, ok := DecodeReplayFingerprintRequest(data)
		if !ok {
			return
		}
		if len(prefix) > len(data) {
			t.Fatalf("prefix 长度超过了原始 payload 长度: prefix=%d payload=%d", len(prefix), len(data))
		}
	})
}

// FuzzDecodePutVersionedFrame 对 PUT_VERSIONED 请求解码跑随机字节——新增的
// 可选 source 尾部字段引入了一个新的长度判断分支（是否存在、sourceLen 是否
// 与剩余字节精确匹配）。
func FuzzDecodePutVersionedFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add(EncodePutFrame([]byte("k1"), []byte("v1")))                           // 旧格式：无 source
	f.Add(EncodePutVersionedFrame([]byte("k1"), []byte("v1"), "quantbrew-job")) // 新格式：带 source

	f.Fuzz(func(t *testing.T, data []byte) {
		key, value, source, ok := DecodePutVersionedFrame(data)
		if !ok {
			return
		}
		if len(key) > len(data) || len(value) > len(data) || len(source) > len(data) {
			t.Fatalf("解码出的字段长度超过了原始 payload 长度: key=%d value=%d source=%d payload=%d",
				len(key), len(value), len(source), len(data))
		}
	})
}
