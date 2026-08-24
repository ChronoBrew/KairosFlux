package temporal

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestVersionStorageKeyRoundTrip(t *testing.T) {
	storage := VersionStorageKey("quote:2026-08-17:600000", 3)
	logical, seq, ok := ParseVersionStorageKey(storage)
	if !ok {
		t.Fatal("应能解析")
	}
	if logical != "quote:2026-08-17:600000" || seq != 3 {
		t.Fatalf("解析结果不符: logical=%q seq=%d", logical, seq)
	}
}

func TestVersionStorageKeyLexOrderEqualsNumericOrder(t *testing.T) {
	a := VersionStorageKey("k", 1)
	b := VersionStorageKey("k", 2)
	c := VersionStorageKey("k", 10)
	if !(a < b && b < c) {
		t.Fatal("定宽版本键的字典序必须等于数值序，否则 LSM 扫描顺序会错")
	}
}

func TestParseVersionStorageKeyRejectsMalformed(t *testing.T) {
	for _, bad := range []string{
		"",
		"quote:2026-08-17:600000",
		"k:v1",
		"k:v0000000000000000x",
	} {
		if _, _, ok := ParseVersionStorageKey(bad); ok {
			t.Fatalf("非法键 %q 不应解析成功", bad)
		}
	}
}

func TestLatestPicksHighestSeq(t *testing.T) {
	versions := []Version{
		{LogicalKey: "k", Seq: 1, WriteNanos: 100, Payload: []byte("a")},
		{LogicalKey: "k", Seq: 3, WriteNanos: 300, Payload: []byte("c")},
		{LogicalKey: "k", Seq: 2, WriteNanos: 200, Payload: []byte("b")},
	}
	got, ok := Latest(versions)
	if !ok || got.Seq != 3 || string(got.Payload) != "c" {
		t.Fatalf("Latest 应选 seq=3，得到 %+v ok=%v", got, ok)
	}
}

func TestAsOfOnlySeesPastWrites(t *testing.T) {
	versions := []Version{
		{LogicalKey: "k", Seq: 1, WriteNanos: 100, Payload: []byte("v1")},
		{LogicalKey: "k", Seq: 2, WriteNanos: 200, Payload: []byte("v2")},
		{LogicalKey: "k", Seq: 3, WriteNanos: 300, Payload: []byte("v3")},
	}
	got, ok := AsOf(versions, 200)
	if !ok || got.Seq != 2 || string(got.Payload) != "v2" {
		t.Fatalf("as_of=200 应选 v2，得到 %+v ok=%v", got, ok)
	}
	if _, ok := AsOf(versions, 50); ok {
		t.Fatal("as_of 早于所有写入时应 not found")
	}
}

func TestAsOfTieBreaksBySeq(t *testing.T) {
	versions := []Version{
		{LogicalKey: "k", Seq: 1, WriteNanos: 100, Payload: []byte("v1")},
		{LogicalKey: "k", Seq: 2, WriteNanos: 100, Payload: []byte("v2")},
	}
	got, ok := AsOf(versions, 100)
	if !ok || got.Seq != 2 {
		t.Fatalf("同写入时刻应选 seq 更大的版本，得到 %+v", got)
	}
}

func TestFingerprintDeterministicAndOrderIndependent(t *testing.T) {
	entries := []Entry{
		{LogicalKey: "b", Seq: 1, Payload: []byte("x")},
		{LogicalKey: "a", Seq: 2, Payload: []byte("y")},
	}
	reversed := []Entry{
		{LogicalKey: "a", Seq: 2, Payload: []byte("y")},
		{LogicalKey: "b", Seq: 1, Payload: []byte("x")},
	}
	f1 := Fingerprint(entries)
	f2 := Fingerprint(reversed)
	if f1 != f2 {
		t.Fatal("指纹应与输入顺序无关")
	}
	if Fingerprint(entries) != f1 {
		t.Fatal("同一输入两次指纹必须一致")
	}
}

func TestFingerprintLengthPrefixPreventsAmbiguity(t *testing.T) {
	a := []Entry{{LogicalKey: "a", Seq: 1, Payload: []byte("bc")}}
	b := []Entry{{LogicalKey: "ab", Seq: 1, Payload: []byte("c")}}
	if Fingerprint(a) == Fingerprint(b) {
		t.Fatal("边界模糊的两组条目指纹不得相同")
	}
}

func TestEncodeDecodeVersionValueRoundTrip(t *testing.T) {
	raw := EncodeVersionValue(12345, []byte("hello"))
	nanos, payload, ok := DecodeVersionValue(raw)
	if !ok || nanos != 12345 || string(payload) != "hello" {
		t.Fatalf("round trip 失败: nanos=%d payload=%q ok=%v", nanos, payload, ok)
	}
}

func TestDecodeVersionValueRejectsTooShort(t *testing.T) {
	if _, _, ok := DecodeVersionValue([]byte("short")); ok {
		t.Fatal("不足 8 字节应解析失败")
	}
}

func TestEncodeDecodeCurrentValueRoundTrip(t *testing.T) {
	cv := CurrentValue{Seq: 7, PayloadHash: "deadbeef"}
	raw := EncodeCurrentValue(cv)
	got, ok := DecodeCurrentValue(raw)
	if !ok || got != cv {
		t.Fatalf("round trip 失败: got=%+v ok=%v", got, ok)
	}
}

func TestDecodeCurrentValueRejectsMalformed(t *testing.T) {
	for _, bad := range [][]byte{
		nil,
		{1, 2, 3},
		EncodeCurrentValue(CurrentValue{Seq: 1, PayloadHash: "ab"})[:9], // 声明了 hashLen 但截断
	} {
		if _, ok := DecodeCurrentValue(bad); ok {
			t.Fatalf("畸形 current value %v 不应解析成功", bad)
		}
	}
}

func TestVersionStorageKeyBoundsCoverAllSeqAndOnlyThisLogicalKey(t *testing.T) {
	lower := VersionStorageKeyLowerBound("quote:2026-08-17:600000")
	upper := VersionStorageKeyUpperBound("quote:2026-08-17:600000")
	mid := VersionStorageKey("quote:2026-08-17:600000", 42)
	if !(lower <= mid && mid <= upper) {
		t.Fatalf("中间版本键应落在 [lower,upper] 内: lower=%q mid=%q upper=%q", lower, mid, upper)
	}
	// 另一逻辑键（数字续接，':'(0x3a) 比任何数字字符都大，故不会落入区间）不应被区间覆盖。
	other := VersionStorageKey("quote:2026-08-17:6000000", 1)
	if lower <= other && other <= upper {
		t.Fatalf("另一逻辑键 %q 不应落入 %q 的版本区间", other, "quote:2026-08-17:600000")
	}
}

func TestIsCurrentStorageKey(t *testing.T) {
	if !IsCurrentStorageKey(CurrentStorageKey("k")) {
		t.Fatal("CurrentStorageKey 的输出应被识别为 current 键")
	}
	if IsCurrentStorageKey("k") || IsCurrentStorageKey(":current") {
		t.Fatal("裸后缀或无逻辑键前缀不应被识别为 current 键")
	}
}

// TestEncodeVersionRecordRoundTrip 验证 M2 操作元数据信封的编解码互逆——
// LogicalKey/Seq 不参与编码（已在存储键里），解码结果不应该填充它们。
func TestEncodeVersionRecordRoundTrip(t *testing.T) {
	in := Version{
		WriteNanos:    1_700_000_000_000_000_000,
		Source:        "quantbrew-job-daily",
		SchemaVer:     3,
		PersistedHash: HashPayload([]byte(`{"code":"600000"}`)),
		Payload:       []byte(`{"code":"600000"}`),
	}
	raw := EncodeVersionRecord(in)
	got, ok := DecodeVersionRecord(raw)
	if !ok {
		t.Fatal("应能解码")
	}
	if got.LogicalKey != "" || got.Seq != 0 {
		t.Fatalf("信封不应携带 LogicalKey/Seq（应来自存储键）: %+v", got)
	}
	if got.WriteNanos != in.WriteNanos || got.Source != in.Source ||
		got.SchemaVer != in.SchemaVer || got.PersistedHash != in.PersistedHash ||
		string(got.Payload) != string(in.Payload) {
		t.Fatalf("round trip 不符: got=%+v want=%+v", got, in)
	}
}

// TestDecodeVersionRecordFallsBackForLegacyValues 验证"懒迁移"：从未被信封化
// 重写过的 M0 存量版本值（EncodeVersionValue 编码），DecodeVersionRecord 必须
// 照样能解出 WriteNanos/Payload，且元数据字段（Source/SchemaVer/PersistedHash）
// 取零值——这是历史真相，不是解析失败。
func TestDecodeVersionRecordFallsBackForLegacyValues(t *testing.T) {
	legacy := EncodeVersionValue(12345, []byte("hello"))
	got, ok := DecodeVersionRecord(legacy)
	if !ok {
		t.Fatal("旧格式应能被兼容解析，不应失败")
	}
	if got.WriteNanos != 12345 || string(got.Payload) != "hello" {
		t.Fatalf("旧格式 WriteNanos/Payload 不符: %+v", got)
	}
	if got.Source != "" || got.SchemaVer != 0 || got.PersistedHash != "" {
		t.Fatalf("旧格式的元数据字段应为零值（历史真相，不是伪造默认值）: %+v", got)
	}
}

// TestDecodeVersionRecordRejectsTruncatedEnvelope 验证信封标记位已置位、但
// 定长字段区（marker/writeNanos/sourceLen/source/schemaVer/hashLen/hash）被
// 截断的畸形字节不会 panic，只是解析失败——REPLAY_FINGERPRINT/LIST_WRITES 在
// 生产环境会对未经额外校验的磁盘字节调用这个函数（配合 FuzzDecodeVersionRecord
// 做更广的输入覆盖）。截断到 payloadStart 之后（进入 payload 区）不在此列：
// payload 与 EncodeVersionValue 同样的设计——不带长度前缀、就是"缓冲区剩下的
// 全部字节"，截到那个区间产生的是一个合法但更短的 payload，不是可判定的
// 畸形输入（无法区分"被截断"与"payload 原本就这么短"，这是格式本身的固有
// 局限，不是本函数的漏洞）。
func TestDecodeVersionRecordRejectsTruncatedEnvelope(t *testing.T) {
	source, hash, payload := "s", "h", []byte("p")
	payloadStart := 8 + 8 + 4 + len(source) + 4 + 2 + len(hash)
	full := EncodeVersionRecord(Version{WriteNanos: 1, Source: source, SchemaVer: 1, PersistedHash: hash, Payload: payload})
	if len(full) != payloadStart+len(payload) {
		t.Fatalf("测试自身的偏移量计算不符: len(full)=%d payloadStart=%d", len(full), payloadStart)
	}
	for n := 0; n < payloadStart; n++ {
		if _, ok := DecodeVersionRecord(full[:n]); ok {
			t.Fatalf("截断到 %d 字节（尚未进入 payload 区）仍解析成功，不应该", n)
		}
	}
}

// TestEncodeVersionRecordGoldenHex 是信封编码的黄金字节向量（编码器漂移哨兵）：
// 谁若重排/改动 EncodeVersionRecord 的字段顺序或长度前缀宽度，这条测试会先
// 炸，而不是等到跨语言互通测试才发现字节不对——与"注入单字节漂移→指纹报警"
// 那条测的是数据完整性不是同一件事，这条测的是编码格式本身的稳定性。
func TestEncodeVersionRecordGoldenHex(t *testing.T) {
	v := Version{
		WriteNanos:    100,
		Source:        "job",
		SchemaVer:     2,
		PersistedHash: "ab",
		Payload:       []byte("pl"),
	}
	got := hex.EncodeToString(EncodeVersionRecord(v))
	const want = "0100000000000080" + // marker: envelopeMarkerBit | envelopeVersion1
		"6400000000000000" + // writeNanos=100
		"03000000" + "6a6f62" + // sourceLen=3, "job"
		"02000000" + // schemaVer=2
		"0200" + "6162" + // hashLen=2, "ab"
		"706c" // payload "pl"
	if got != want {
		t.Fatalf("信封编码字节漂移:\n got=%s\nwant=%s", got, want)
	}
}

func TestPayloadHashStable(t *testing.T) {
	v := Version{LogicalKey: "k", Seq: 1, WriteNanos: 1, Payload: []byte("payload")}
	if v.PayloadHash() != v.PayloadHash() {
		t.Fatal("同一负载指纹必须稳定")
	}
	if v.PayloadHash() == "" || strings.Contains(v.PayloadHash(), "payload") {
		t.Fatal("指纹应为十六进制摘要")
	}
}
