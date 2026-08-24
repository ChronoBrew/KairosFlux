package temporal

import (
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

func TestPayloadHashStable(t *testing.T) {
	v := Version{LogicalKey: "k", Seq: 1, WriteNanos: 1, Payload: []byte("payload")}
	if v.PayloadHash() != v.PayloadHash() {
		t.Fatal("同一负载指纹必须稳定")
	}
	if v.PayloadHash() == "" || strings.Contains(v.PayloadHash(), "payload") {
		t.Fatal("指纹应为十六进制摘要")
	}
}
