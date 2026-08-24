package temporal

import "testing"

// FuzzDecodeVersionRecord 对 M2 信封解码跑随机字节。这是 REPLAY_FINGERPRINT/
// LIST_WRITES/GET 透明回退路径在生产环境会对"未经任何前置校验的磁盘字节"
// 直接调用的函数（版本键的 value 本身没有外部校验层，SSTable/WAL 只保证字节
// 完整落盘，不保证内容合法）——sourceLen/schemaVer/hashLen 全部是从磁盘读出
// 的、理论上可被位翻转损坏的字段，是长度前缀解析最容易越界/panic 的地方。
// 只断言不 panic；成功解码时额外校验各字段没有越过原始输入长度。
func FuzzDecodeVersionRecord(f *testing.F) {
	// 种子：空、过短、legacy 格式（bit63=0）、完整的 M2 信封、以及标记位置位但
	// 其余字段被截断/声明超长的畸形输入。
	f.Add([]byte{})
	f.Add([]byte("short"))
	f.Add(EncodeVersionValue(12345, []byte("legacy payload")))
	f.Add(EncodeVersionRecord(Version{
		WriteNanos: 100, Source: "job", SchemaVer: 2, PersistedHash: "ab", Payload: []byte("pl"),
	}))
	f.Add(EncodeVersionRecord(Version{})) // 全零值信封
	// 标记位置位、但 sourceLen 声明为 0xFFFFFFFF（声明的长度远超实际字节）。
	huge := EncodeVersionRecord(Version{Source: "x"})
	if len(huge) >= 20 {
		huge[16], huge[17], huge[18], huge[19] = 0xFF, 0xFF, 0xFF, 0xFF
	}
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		v, ok := DecodeVersionRecord(data)
		if !ok {
			return // 拒绝是预期路径，只要不 panic
		}
		if len(v.Source) > len(data) || len(v.PersistedHash) > len(data) || len(v.Payload) > len(data) {
			t.Fatalf("解码出的字段长度超过了原始输入长度，说明发生了越界: source=%d hash=%d payload=%d input=%d",
				len(v.Source), len(v.PersistedHash), len(v.Payload), len(data))
		}
	})
}
