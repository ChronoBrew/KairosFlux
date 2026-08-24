package kairnet_test

// Kair 协议跨语言测试向量的 Go 侧锚点。加载 docs/kair/vectors.json（由本包
// kairnet.DataPack 生成，见 docs/Kair-协议规范.md 附录与该文件生成时用的
// 一次性脚本说明），验证 kairnet.DataPack.Pack 对每条向量都能重新生成完全
// 相同的字节序列。
//
// Python 侧对应测试见 client/python/test_bandb_client.py，读同一份 vectors.json。
// 这是防止 Go/Python 两个实现悄悄分叉的锚点：以后任何第三语言客户端接入
// Kair 协议，也应以这份向量为准。

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ChronoBrew/KairosFlux/kairnet"
)

type protocolVector struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	MsgID       string `json:"msg_id"`
	DataHex     string `json:"data_hex"`
	FrameHex    string `json:"frame_hex"`
}

func loadVectors(t *testing.T) []protocolVector {
	t.Helper()
	path := filepath.Join("..", "docs", "kair", "vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取向量文件失败 %s: %v", path, err)
	}
	var vecs []protocolVector
	if err := json.Unmarshal(raw, &vecs); err != nil {
		t.Fatalf("解析向量文件失败: %v", err)
	}
	if len(vecs) == 0 {
		t.Fatal("向量文件为空")
	}
	return vecs
}

// TestVectors_FramePackMatchesGolden 验证 DataPack.Pack 对每条向量的 (msgID, data)
// 都能重新生成与冻结样本完全一致的整帧字节——锁定 Kair 帧编码不被意外改动。
func TestVectors_FramePackMatchesGolden(t *testing.T) {
	for _, v := range loadVectors(t) {
		v := v
		t.Run(v.Name, func(t *testing.T) {
			data, err := hex.DecodeString(v.DataHex)
			if err != nil {
				t.Fatalf("data_hex 不是合法十六进制: %v", err)
			}
			wantFrame, err := hex.DecodeString(v.FrameHex)
			if err != nil {
				t.Fatalf("frame_hex 不是合法十六进制: %v", err)
			}

			gotFrame, err := kairnet.NewDataPack().Pack(kairnet.NewMessage(v.MsgID, data))
			if err != nil {
				t.Fatalf("Pack 失败: %v", err)
			}
			if hex.EncodeToString(gotFrame) != hex.EncodeToString(wantFrame) {
				t.Fatalf("帧字节与向量不一致\n  向量: %x\n  实际: %x", wantFrame, gotFrame)
			}
		})
	}
}

// TestVectors_HeaderUnpackMatchesGolden 验证 DataPack.UnPack 从每条向量的整帧里
// 解出的 (DataLen, IDLen) 与向量的 data/msgID 长度一致——锁定头部解析语义。
func TestVectors_HeaderUnpackMatchesGolden(t *testing.T) {
	for _, v := range loadVectors(t) {
		v := v
		t.Run(v.Name, func(t *testing.T) {
			frame, err := hex.DecodeString(v.FrameHex)
			if err != nil {
				t.Fatalf("frame_hex 不是合法十六进制: %v", err)
			}
			data, err := hex.DecodeString(v.DataHex)
			if err != nil {
				t.Fatalf("data_hex 不是合法十六进制: %v", err)
			}

			dp := kairnet.NewDataPack()
			head := frame[:dp.HeadLen()]
			msg, err := dp.UnPack(head)
			if err != nil {
				t.Fatalf("UnPack 失败: %v", err)
			}
			if int(msg.DataLen) != len(data) {
				t.Fatalf("DataLen=%d，期望 %d", msg.DataLen, len(data))
			}
			if int(msg.IDLen) != len(v.MsgID) {
				t.Fatalf("IDLen=%d，期望 %d（msgID=%q）", msg.IDLen, len(v.MsgID), v.MsgID)
			}

			// 头部之后紧跟 msgID 与 data，按 IDLen/DataLen 切片应分别还原它们，
			// 与直接从帧头解析出的字段自洽（交叉核验 Pack 与 UnPack 互逆）。
			rest := frame[dp.HeadLen():]
			gotMsgID := string(rest[:msg.IDLen])
			gotData := rest[msg.IDLen:]
			if gotMsgID != v.MsgID {
				t.Fatalf("msgID=%q，期望 %q", gotMsgID, v.MsgID)
			}
			if hex.EncodeToString(gotData) != v.DataHex {
				t.Fatalf("data=%x，期望 %s", gotData, v.DataHex)
			}
		})
	}
}

// TestVectors_FrameHeadLenIsSixBytes 锁定当前协议头部固定 6 字节、无版本/魔数位——
// docs/Kair-协议规范.md 记录这是 v1 的已知限制，本测试确保该假设不被静默打破。
func TestVectors_FrameHeadLenIsSixBytes(t *testing.T) {
	if got := kairnet.NewDataPack().HeadLen(); got != 6 {
		t.Fatalf("帧头长度=%d，期望 6（dataLen u32 + idLen u16，无版本字段）", got)
	}
}

// TestVectors_QuoteKeyLayoutIsDateFirst 冒烟校验行情快照向量的 key 布局约定
// （quote:<日期>:<代码>，日期在前）在向量文件里没有被写反——这个布局决定了
// 投递/retention 机制是否需要改动，见 docs/Kair-协议规范.md 与 service/
// ingesthook/schema/quote.go 的注释。
func TestVectors_QuoteKeyLayoutIsDateFirst(t *testing.T) {
	for _, v := range loadVectors(t) {
		if v.Name != "put_request_quote" {
			continue
		}
		data, err := hex.DecodeString(v.DataHex)
		if err != nil {
			t.Fatalf("data_hex 不是合法十六进制: %v", err)
		}
		if len(data) < 8 {
			t.Fatal("PUT 负载过短")
		}
		keyLen := binary.LittleEndian.Uint32(data[0:4])
		key := string(data[8 : 8+keyLen])
		const want = "quote:2026-08-17:600000"
		if key != want {
			t.Fatalf("quote key=%q，期望 %q（日期在前的布局）", key, want)
		}
		return
	}
	t.Fatal("向量文件缺少 put_request_quote")
}
