package bannet

import "testing"

// FuzzUnPack 对帧头解析跑随机字节，只断言不 panic（UnPack 本身应该对任意输入
// 要么返回合法 *Message、要么返回 error，两者都不该崩）。种子覆盖已知边界：
// 空、过短、全零、全 0xFF（dataLen/idLen 都拉满）、恰好 6 字节。
func FuzzUnPack(f *testing.F) {
	dp := NewDataPack()
	seeds := [][]byte{
		{},
		{0x00},
		{0x00, 0x00, 0x00, 0x00, 0x00},       // 5 字节，差 1 字节到头部长度
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 恰好 6 字节，全零
		{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // dataLen/idLen 都是最大值
		{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, // 超过头部长度（多余字节应被忽略）
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		msg, err := dp.UnPack(data)
		if err != nil {
			if msg != nil {
				t.Fatalf("UnPack 返回了 error 又返回了非 nil *Message: err=%v msg=%+v", err, msg)
			}
			return
		}
		// 成功路径的不变式：DataLen/IDLen 只是从字节里原样解出来的数字，不该有任何
		// 解析结果导致 panic 的隐患（这里没有可供越界的切片操作，纯粹是运行不崩即通过）。
		_ = msg.DataLen
		_ = msg.IDLen
	})
}
