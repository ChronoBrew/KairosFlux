package ingesthook

import "testing"

// FuzzParsePut 对 PUT 负载解析（keyLen+valueLen+key+value）跑随机字节。这是
// ingesthook.Filter.Handle 在 kairnet 帧解出的 data 上直接调用的函数——两个长度
// 字段都是攻击者可控的 u32，是判断"畸形帧"的第一道关卡，必须对任意输入都不 panic。
func FuzzParsePut(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 8)) // keyLen=0, valueLen=0，恰好 8 字节头部
	f.Add(encodePut([]byte("k"), []byte("v")))
	f.Add(encodePut(nil, nil))

	huge := make([]byte, 8)
	huge[0] = 0xFF
	huge[1] = 0xFF
	huge[2] = 0xFF
	huge[3] = 0xFF // keyLen 声明 42 亿，实际只有 8 字节
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		key, value, ok := parsePut(data)
		if !ok {
			return
		}
		if len(key) > len(data) || len(value) > len(data) {
			t.Fatalf("解出的 key/value 长度超过原始 data 长度，说明发生了越界: keyLen=%d valueLen=%d dataLen=%d",
				len(key), len(value), len(data))
		}
		// 解析成功时应能重新编码并保持内容一致（round-trip 不变式）。
		reencoded := encodePut(key, value)
		rekey, revalue, reok := parsePut(reencoded)
		if !reok {
			t.Fatalf("重新编码后的负载应能再次解析成功")
		}
		if string(rekey) != string(key) || string(revalue) != string(value) {
			t.Fatalf("round-trip 后 key/value 不一致")
		}
	})
}
