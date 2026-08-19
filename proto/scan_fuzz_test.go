package proto

import (
	"encoding/binary"
	"testing"

	"github.com/NeverENG/BanDB/predicate"
)

// FuzzDecodeScanRequest 对 SCAN 请求解码跑随机字节。这是服务端在 Router.handleScan
// 里直接对未经任何前置校验的网络字节调用的函数——请求负载里的三个长度字段
// （startLen/endLen/fieldLen）全部是攻击者可控的 u32，是 TLV 解析器最容易出
// 越界/放大问题的地方。只断言不 panic；成功解码时额外校验切片没有越过原始
// payload（不应该发生，但这是"如实检验"而不是假设）。
func FuzzDecodeScanRequest(f *testing.F) {
	// 种子：合法请求、空、只有部分头部、长度字段声明超过实际字节、长度字段为 0xFFFFFFFF。
	f.Add([]byte{})
	f.Add(make([]byte, scanReqHeaderLen)) // 恰好头部长度，三个长度字段都是 0
	seedReq := EncodeScanRequest(ScanRequest{
		Start: []byte("a"), End: []byte("z"),
		Pred: predicate.Predicate{Field: "az", Op: predicate.OpGT, Operand: "9.9"},
	})
	f.Add(seedReq)

	huge := make([]byte, scanReqHeaderLen)
	binary.LittleEndian.PutUint32(huge[0:4], 0xFFFFFFFF) // startLen 声明 42 亿
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		req, err := DecodeScanRequest(data)
		if err != nil {
			return // 拒绝是预期路径，只要不 panic
		}
		if len(req.Start) > len(data) || len(req.End) > len(data) || len(req.Pred.Field) > len(data) {
			t.Fatalf("解码出的字段长度超过了原始 payload 长度，说明发生了越界: start=%d end=%d field=%d payload=%d",
				len(req.Start), len(req.End), len(req.Pred.Field), len(data))
		}
	})
}

// FuzzDecodeScanResponse 对 SCAN 响应解码跑随机字节。这是客户端对"服务端"
// （或任何能在网络上冒充服务端的角色）发来的字节直接调用的函数——count 字段
// 是攻击者可控的 u32，此前会被直接拿去做 make([]ScanEntry, 0, count) 的预分配
// 容量（已修复，见 DecodeScanResponse 的注释）。这里用 fuzzing 复核修复后不再
// 有其它同类问题。
func FuzzDecodeScanResponse(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})        // 只有 statusLen=0，没有 count
	f.Add(make([]byte, 1+2+4)) // status="", count=0
	huge := make([]byte, 1+2+4)
	huge[0] = 2
	binary.LittleEndian.PutUint32(huge[3:7], 0xFFFFFFFF) // count 声明 42 亿但 payload 很短
	f.Add(huge)

	seed := EncodeScanResponse(StatusOK, []ScanEntry{{Key: []byte("k"), Value: []byte("v")}})
	f.Add(seed)

	f.Fuzz(func(t *testing.T, payload []byte) {
		_, entries, err := DecodeScanResponse(payload)
		if err != nil {
			return
		}
		for i, e := range entries {
			if len(e.Key) > len(payload) || len(e.Value) > len(payload) {
				t.Fatalf("entry %d 的 key/value 长度超过原始 payload 长度，说明发生了越界", i)
			}
		}
	})
}
