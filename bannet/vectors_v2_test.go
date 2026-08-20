package bannet_test

// BANLV v2 协议跨语言测试向量的 Go 侧锚点，与 vectors_test.go（v1）并存不合并——
// 见 docs/rfc/BANLV-2.md §8 迁移方案第 4 条："两套协议版本的测试意图不同，合并
// 会让测试文件同时承担'锁定 v1 不变'与'验证 v2 新增'两个不同职责"。
//
// 加载 docs/banlv/vectors-v2.json（手工推导生成，不是本实现自己 Pack 出来再
// 存档——见该文件顶部 _comment 说明的方法论考量）。Python 侧对应测试见
// client/python/test_bandb_client_v2.py，读同一份 JSON。

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeverENG/BanDB/bannet/codec"
	"github.com/NeverENG/BanDB/proto"
)

type frameVectorV2 struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Flags       uint8  `json:"flags"`
	Opcode      uint8  `json:"opcode"`
	Type        uint16 `json:"type"`
	CorrID      uint32 `json:"corr_id"`
	DataHex     string `json:"data_hex"`
	FrameHex    string `json:"frame_hex"`
}

type headerOnlyVectorV2 struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Flags       uint8  `json:"flags"`
	Opcode      uint8  `json:"opcode"`
	Type        uint16 `json:"type"`
	CorrID      uint32 `json:"corr_id"`
	DataLen     uint32 `json:"data_len"`
	HeaderHex   string `json:"header_hex"`
}

type negotiationVectorV2 struct {
	Name             string `json:"name"`
	Description      string `json:"description"`
	MsgID            string `json:"msg_id"`
	PayloadHex       string `json:"payload_hex"`
	Flags            uint8  `json:"flags"`
	Opcode           uint8  `json:"opcode"`
	Type             uint16 `json:"type"`
	CorrID           uint32 `json:"corr_id"`
	DataHex          string `json:"data_hex"`
	FrameHex         string `json:"frame_hex"`
	ExpectNoResponse bool   `json:"expect_no_response"`
}

// windowStatByeVectorV2 覆盖 RFC §11.2.2/§11.2.3/§11.4 新增的 opcode/响应体
// 格式：FLUSH/STAT/BYE 请求帧，以及 WINDOW_ACK/STAT_ACK 响应帧（含响应体
// 字段的黄金值，供 proto.DecodeV2AckBody/proto.V2AckBody 双向校验）。
type windowStatByeVectorV2 struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Flags       uint8  `json:"flags"`
	Opcode      uint8  `json:"opcode"`
	Type        uint16 `json:"type"`
	CorrID      uint32 `json:"corr_id"`
	DataHex     string `json:"data_hex"`
	FrameHex    string `json:"frame_hex"`

	// 以下字段仅 WINDOW_ACK/STAT_ACK 向量非零：响应体（RFC §11.2.2）的黄金
	// 字段值，DataHex 即这些字段编码后的字节。
	BodyCorrID     *uint32 `json:"body_corr_id"`
	Received       *uint32 `json:"received"`
	Accepted       *uint32 `json:"accepted"`
	Rejected       *uint32 `json:"rejected"`
	FirstErrCode   *uint16 `json:"first_err_code"`
	FirstErrReason *string `json:"first_err_reason"`
}

func (v windowStatByeVectorV2) isAck() bool { return v.BodyCorrID != nil }

type vectorsV2File struct {
	Frames        []frameVectorV2         `json:"frames"`
	HeaderOnly    []headerOnlyVectorV2    `json:"header_only"`
	Negotiation   []negotiationVectorV2   `json:"negotiation"`
	WindowStatBye []windowStatByeVectorV2 `json:"window_stat_bye"`
}

func loadVectorsV2(t *testing.T) vectorsV2File {
	t.Helper()
	path := filepath.Join("..", "docs", "banlv", "vectors-v2.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取向量文件失败 %s: %v", path, err)
	}
	var vecs vectorsV2File
	if err := json.Unmarshal(raw, &vecs); err != nil {
		t.Fatalf("解析向量文件失败: %v", err)
	}
	if len(vecs.Frames) == 0 {
		t.Fatal("frames 分区为空")
	}
	return vecs
}

// TestVectorsV2_FramePackMatchesGolden 验证 DataPackV2.Pack 对每条 frames
// 向量都能重新生成与手工推导的黄金字节完全一致的整帧——锁定 v2 帧编码。
func TestVectorsV2_FramePackMatchesGolden(t *testing.T) {
	for _, v := range loadVectorsV2(t).Frames {
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

			msg := codec.NewMessageV2(codec.HeaderV2{
				Flags:  v.Flags,
				Opcode: v.Opcode,
				Type:   v.Type,
				CorrID: v.CorrID,
			}, data)
			gotFrame, err := codec.NewDataPackV2().Pack(msg)
			if err != nil {
				t.Fatalf("Pack 失败: %v", err)
			}
			if hex.EncodeToString(gotFrame) != hex.EncodeToString(wantFrame) {
				t.Fatalf("帧字节与向量不一致\n  向量: %x\n  实际: %x", wantFrame, gotFrame)
			}
		})
	}
}

// TestVectorsV2_HeaderUnpackMatchesGolden 验证 DataPackV2.UnPack 从每条
// frames 向量的整帧里解出的头部字段（flags/opcode/type/corr_id/DataLen）
// 与向量描述的字段一致，且负载切片与 data_hex 一致——交叉核验 Pack 与
// UnPack 互逆。
func TestVectorsV2_HeaderUnpackMatchesGolden(t *testing.T) {
	dp := codec.NewDataPackV2()
	for _, v := range loadVectorsV2(t).Frames {
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

			h, err := dp.UnPack(frame[:dp.HeadLen()])
			if err != nil {
				t.Fatalf("UnPack 失败: %v", err)
			}
			if h.Flags != v.Flags {
				t.Fatalf("Flags=%#x，期望 %#x", h.Flags, v.Flags)
			}
			if h.Opcode != v.Opcode {
				t.Fatalf("Opcode=%#x，期望 %#x", h.Opcode, v.Opcode)
			}
			if h.Type != v.Type {
				t.Fatalf("Type=%d，期望 %d", h.Type, v.Type)
			}
			if h.CorrID != v.CorrID {
				t.Fatalf("CorrID=%d，期望 %d", h.CorrID, v.CorrID)
			}
			if int(h.DataLen) != len(data) {
				t.Fatalf("DataLen=%d，期望 %d", h.DataLen, len(data))
			}
			gotData := frame[dp.HeadLen():]
			if hex.EncodeToString(gotData) != v.DataHex {
				t.Fatalf("data=%x，期望 %s", gotData, v.DataHex)
			}
		})
	}
}

// TestVectorsV2_HeaderOnlyDataLenMatchesGolden 验证 header_only 分区（如声明
// dataLen=0xFFFFFFFF 的场景）：只解析头部 14 字节即可得到正确的字段，不需要、
// 也不应该尝试物化对应体量的负载字节。
func TestVectorsV2_HeaderOnlyDataLenMatchesGolden(t *testing.T) {
	dp := codec.NewDataPackV2()
	for _, v := range loadVectorsV2(t).HeaderOnly {
		v := v
		t.Run(v.Name, func(t *testing.T) {
			head, err := hex.DecodeString(v.HeaderHex)
			if err != nil {
				t.Fatalf("header_hex 不是合法十六进制: %v", err)
			}
			if uint32(len(head)) != dp.HeadLen() {
				t.Fatalf("header_hex 长度=%d，期望 %d", len(head), dp.HeadLen())
			}
			h, err := dp.UnPack(head)
			if err != nil {
				t.Fatalf("UnPack 失败: %v", err)
			}
			if h.Opcode != v.Opcode {
				t.Fatalf("Opcode=%#x，期望 %#x", h.Opcode, v.Opcode)
			}
			if h.Type != v.Type {
				t.Fatalf("Type=%d，期望 %d", h.Type, v.Type)
			}
			if h.CorrID != v.CorrID {
				t.Fatalf("CorrID=%d，期望 %d", h.CorrID, v.CorrID)
			}
			if h.DataLen != v.DataLen {
				t.Fatalf("DataLen=%d，期望 %d", h.DataLen, v.DataLen)
			}
		})
	}
}

// TestVectorsV2_HeaderLenIsFourteenBytes 锁定 v2 帧头固定 14 字节——RFC §2
// 明文定长头部，本测试确保该假设不被静默打破。
func TestVectorsV2_HeaderLenIsFourteenBytes(t *testing.T) {
	if got := codec.NewDataPackV2().HeadLen(); got != codec.HeaderV2Len {
		t.Fatalf("v2 帧头长度=%d，期望 %d（RFC §2 定长头部）", got, codec.HeaderV2Len)
	}
	if codec.HeaderV2Len != 14 {
		t.Fatalf("HeaderV2Len=%d，期望 14", codec.HeaderV2Len)
	}
}

// TestVectorsV2_MagicByteOrderIsVersionThenMagic 直接断言帧最前 2 字节的绝对
// 位置——不通过 Pack/UnPack 往返自洽（那样测的是"实现内部一致"，测不出
// "两侧实现一致地搞反字节序"这种系统性错误），而是逐字节核对
// v2_magic_byte_order_lock 向量：wire 上第 0 字节必须是 version(0x02)、
// 第 1 字节必须是 magic(0xBA)。见 codec.EncodeMagicVer 的文档与
// docs/banlv/vectors-v2.json 该向量的 description。
func TestVectorsV2_MagicByteOrderIsVersionThenMagic(t *testing.T) {
	var target *frameVectorV2
	for _, v := range loadVectorsV2(t).Frames {
		if v.Name == "v2_magic_byte_order_lock" {
			v := v
			target = &v
			break
		}
	}
	if target == nil {
		t.Fatal("向量文件缺少 v2_magic_byte_order_lock")
	}
	frame, err := hex.DecodeString(target.FrameHex)
	if err != nil {
		t.Fatalf("frame_hex 不是合法十六进制: %v", err)
	}
	if len(frame) < 2 {
		t.Fatal("帧过短")
	}
	if frame[0] != 0x02 {
		t.Fatalf("frame[0]=%#x，期望 0x02（version，LE 存储下的低字节先出现）", frame[0])
	}
	if frame[1] != codec.MagicV2 {
		t.Fatalf("frame[1]=%#x，期望 %#x（magic）", frame[1], codec.MagicV2)
	}

	// 与实现交叉核验：DataPackV2.Pack 对同一条向量重新编码，字节序必须一致。
	data, _ := hex.DecodeString(target.DataHex)
	msg := codec.NewMessageV2(codec.HeaderV2{
		Flags:  target.Flags,
		Opcode: target.Opcode,
		Type:   target.Type,
		CorrID: target.CorrID,
	}, data)
	got, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		t.Fatalf("Pack 失败: %v", err)
	}
	if got[0] != 0x02 || got[1] != codec.MagicV2 {
		t.Fatalf("实现输出的字节序与向量不一致: got[0]=%#x got[1]=%#x", got[0], got[1])
	}
}

// TestVectorsV2_SniffVersionDispatch 验证 SniffVersion 对三种情形的判定：
// v2 帧首 2 字节 → SniffV2；magic 不匹配（借用 v1 vectors.json 里任意一条
// v1 帧的首 2 字节，其 magic 字节几乎不可能等于 0xBA）→ SniffV1；magic
// 匹配但 version 不对 → SniffUnsupportedVersion（不应与 SniffV1 混淆，见
// SniffVersion 的文档）。
func TestVectorsV2_SniffVersionDispatch(t *testing.T) {
	v2Frame, err := hex.DecodeString(loadVectorsV2(t).Frames[0].FrameHex)
	if err != nil {
		t.Fatalf("解码 v2 向量失败: %v", err)
	}
	if got := codec.SniffVersion(v2Frame[0:2]); got != codec.SniffV2 {
		t.Fatalf("SniffVersion(v2首2字节)=%v，期望 SniffV2", got)
	}

	// 借用 vectors.json 的 v1 向量首 2 字节（dataLen 的低 2 字节，几乎不可能
	// 恰好命中 magic 高字节 0xBA）。
	v1Frame, err := hex.DecodeString("0c000000030050555402000000020000006b317631") // put_request_basic
	if err != nil {
		t.Fatalf("解码 v1 向量失败: %v", err)
	}
	if got := codec.SniffVersion(v1Frame[0:2]); got != codec.SniffV1 {
		t.Fatalf("SniffVersion(v1首2字节)=%v，期望 SniffV1", got)
	}

	// magic 匹配、version 不对（构造 magic=0xBA, version=0x03，不是本实现
	// 支持的 VersionV2=0x02）。
	unsupported := []byte{0x03, codec.MagicV2}
	if got := codec.SniffVersion(unsupported); got != codec.SniffUnsupportedVersion {
		t.Fatalf("SniffVersion(magic匹配/version不匹配)=%v，期望 SniffUnsupportedVersion", got)
	}
}

// TestVectorsV2_NegotiationProbeIsV1Format 验证 §5 协商探测帧
// （v2_negotiation_client_probe_v1_format）确实是 v1 帧格式：能被 v1 的
// codec.DataPack.UnPack 正常解出 msgID="HELLO"，且首 2 字节不携带 v2
// magic（SniffVersion 判定为 SniffV1）——这条向量存在的意义就是"v2 客户端
// 探测帧必须让真正的 v1 服务端读起来像一条普通消息"，两个断言合起来才
// 完整覆盖这句话。
func TestVectorsV2_NegotiationProbeIsV1Format(t *testing.T) {
	var probe *negotiationVectorV2
	for _, v := range loadVectorsV2(t).Negotiation {
		if v.Name == "v2_negotiation_client_probe_v1_format" {
			v := v
			probe = &v
			break
		}
	}
	if probe == nil {
		t.Fatal("向量文件缺少 v2_negotiation_client_probe_v1_format")
	}
	frame, err := hex.DecodeString(probe.FrameHex)
	if err != nil {
		t.Fatalf("frame_hex 不是合法十六进制: %v", err)
	}

	if got := codec.SniffVersion(frame[0:2]); got != codec.SniffV1 {
		t.Fatalf("协商探测帧首 2 字节被判定为 %v，期望 SniffV1（探测帧必须是 v1 格式）", got)
	}

	dp := codec.NewDataPack()
	head := frame[:dp.HeadLen()]
	hmsg, err := dp.UnPack(head)
	if err != nil {
		t.Fatalf("按 v1 UnPack 头部失败: %v", err)
	}
	rest := frame[dp.HeadLen():]
	gotMsgID := string(rest[:hmsg.IDLen])
	if gotMsgID != probe.MsgID {
		t.Fatalf("msgID=%q，期望 %q", gotMsgID, probe.MsgID)
	}
	gotPayload := rest[hmsg.IDLen:]
	wantPayload, err := hex.DecodeString(probe.PayloadHex)
	if err != nil {
		t.Fatalf("payload_hex 不是合法十六进制: %v", err)
	}
	if hex.EncodeToString(gotPayload) != hex.EncodeToString(wantPayload) {
		t.Fatalf("payload=%x，期望 %x", gotPayload, wantPayload)
	}
}

// TestVectorsV2_NegotiationResponseIsV2Format 验证 §5 协商响应帧
// （v2_negotiation_server_response_v2_format）是 v2 帧格式，且 corr_id=0
// （探测请求是 v1 帧、没有 corr_id 字段可回带，§5.1 固定为 0），opcode
// 复用既有的 OK（0x80），不是一个单独定义的 HELLO 响应 opcode。
func TestVectorsV2_NegotiationResponseIsV2Format(t *testing.T) {
	var resp *negotiationVectorV2
	for _, v := range loadVectorsV2(t).Negotiation {
		if v.Name == "v2_negotiation_server_response_v2_format" {
			v := v
			resp = &v
			break
		}
	}
	if resp == nil {
		t.Fatal("向量文件缺少 v2_negotiation_server_response_v2_format")
	}
	frame, err := hex.DecodeString(resp.FrameHex)
	if err != nil {
		t.Fatalf("frame_hex 不是合法十六进制: %v", err)
	}
	if got := codec.SniffVersion(frame[0:2]); got != codec.SniffV2 {
		t.Fatalf("协商响应帧首 2 字节被判定为 %v，期望 SniffV2", got)
	}
	dp := codec.NewDataPackV2()
	h, err := dp.UnPack(frame[:dp.HeadLen()])
	if err != nil {
		t.Fatalf("UnPack 失败: %v", err)
	}
	if h.Opcode != codec.OpcodeOK {
		t.Fatalf("Opcode=%#x，期望 %#x（OK）", h.Opcode, codec.OpcodeOK)
	}
	if h.CorrID != 0 {
		t.Fatalf("CorrID=%d，期望 0", h.CorrID)
	}
}

// TestVectorsV2_WindowStatByeFramesMatchGolden 验证 window_stat_bye 分区里
// 的每一条请求帧（FLUSH/STAT/BYE，无响应体）都能被 DataPackV2 正确解出
// opcode/type/corr_id/负载，且与 Pack 后重新编码的字节与向量完全一致。
func TestVectorsV2_WindowStatByeFramesMatchGolden(t *testing.T) {
	dp := codec.NewDataPackV2()
	for _, v := range loadVectorsV2(t).WindowStatBye {
		if v.isAck() {
			continue // 带响应体的向量由下面的 TestVectorsV2_AckBodyMatchesGolden 单独覆盖
		}
		v := v
		t.Run(v.Name, func(t *testing.T) {
			wantFrame, err := hex.DecodeString(v.FrameHex)
			if err != nil {
				t.Fatalf("frame_hex 不是合法十六进制: %v", err)
			}
			data, err := hex.DecodeString(v.DataHex)
			if err != nil {
				t.Fatalf("data_hex 不是合法十六进制: %v", err)
			}

			h, err := dp.UnPack(wantFrame[:dp.HeadLen()])
			if err != nil {
				t.Fatalf("UnPack 失败: %v", err)
			}
			if h.Opcode != v.Opcode || h.Type != v.Type || h.CorrID != v.CorrID {
				t.Fatalf("头部字段=%+v，期望 opcode=%#x type=%d corr_id=%d", h, v.Opcode, v.Type, v.CorrID)
			}

			msg := codec.NewMessageV2(codec.HeaderV2{Flags: v.Flags, Opcode: v.Opcode, Type: v.Type, CorrID: v.CorrID}, data)
			gotFrame, err := dp.Pack(msg)
			if err != nil {
				t.Fatalf("Pack 失败: %v", err)
			}
			if hex.EncodeToString(gotFrame) != hex.EncodeToString(wantFrame) {
				t.Fatalf("帧字节与向量不一致\n  向量: %x\n  实际: %x", wantFrame, gotFrame)
			}
		})
	}
}

// TestVectorsV2_AckBodyMatchesGolden 验证 WINDOW_ACK/STAT_ACK 响应体
// （RFC §11.2.2，corr_id 在帧头与响应体内各出现一次）双向编解码：
// proto.DecodeV2AckBody 从向量的 data_hex 解出的字段与黄金值一致，
// proto.V2AckBody 用黄金字段重新编码的字节与向量的 data_hex 完全一致。
func TestVectorsV2_AckBodyMatchesGolden(t *testing.T) {
	for _, v := range loadVectorsV2(t).WindowStatBye {
		if !v.isAck() {
			continue
		}
		v := v
		t.Run(v.Name, func(t *testing.T) {
			wantBody, err := hex.DecodeString(v.DataHex)
			if err != nil {
				t.Fatalf("data_hex 不是合法十六进制: %v", err)
			}

			fields, ok := proto.DecodeV2AckBody(wantBody)
			if !ok {
				t.Fatalf("DecodeV2AckBody 解析失败: %x", wantBody)
			}
			if fields.CorrID != *v.BodyCorrID {
				t.Fatalf("body corr_id=%d，期望 %d", fields.CorrID, *v.BodyCorrID)
			}
			if fields.Received != *v.Received || fields.Accepted != *v.Accepted || fields.Rejected != *v.Rejected {
				t.Fatalf("received/accepted/rejected=%d/%d/%d，期望 %d/%d/%d",
					fields.Received, fields.Accepted, fields.Rejected, *v.Received, *v.Accepted, *v.Rejected)
			}
			if fields.FirstErrCode != *v.FirstErrCode {
				t.Fatalf("firstErrCode=%#x，期望 %#x", fields.FirstErrCode, *v.FirstErrCode)
			}
			if fields.FirstErrReason != *v.FirstErrReason {
				t.Fatalf("firstErrReason=%q，期望 %q", fields.FirstErrReason, *v.FirstErrReason)
			}

			gotBody := proto.V2AckBody(*v.BodyCorrID, *v.Received, *v.Accepted, *v.Rejected, *v.FirstErrCode, *v.FirstErrReason)
			if hex.EncodeToString(gotBody) != hex.EncodeToString(wantBody) {
				t.Fatalf("重新编码的响应体与向量不一致\n  向量: %x\n  实际: %x", wantBody, gotBody)
			}

			// 帧头本身也断言一遍：opcode/type/corr_id，以及帧头 corr_id 与
			// 响应体 corr_id 在本向量里取值相同（RFC 要求两处都存在，不要求
			// 两处相等——本向量恰好相等只是"关闭的就是这个窗口/回带的就是
			// 这次 STAT"这个业务事实的体现，不是协议强制的相等关系）。
			wantFrame, err := hex.DecodeString(v.FrameHex)
			if err != nil {
				t.Fatalf("frame_hex 不是合法十六进制: %v", err)
			}
			dp := codec.NewDataPackV2()
			h, err := dp.UnPack(wantFrame[:dp.HeadLen()])
			if err != nil {
				t.Fatalf("UnPack 失败: %v", err)
			}
			if h.Opcode != v.Opcode || h.Type != v.Type || h.CorrID != v.CorrID {
				t.Fatalf("头部字段=%+v，期望 opcode=%#x type=%d corr_id=%d", h, v.Opcode, v.Type, v.CorrID)
			}
		})
	}
}
