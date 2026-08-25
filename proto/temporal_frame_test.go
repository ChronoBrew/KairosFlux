package proto

import (
	"bytes"
	"testing"
)

func TestAsOfFrameRoundTrip(t *testing.T) {
	frame := EncodeAsOfFrame([]byte("quote:2026-08-17:600000"), -12345)
	key, asOf, ok := DecodeAsOfFrame(frame)
	if !ok || !bytes.Equal(key, []byte("quote:2026-08-17:600000")) || asOf != -12345 {
		t.Fatalf("round trip 失败: key=%q asOf=%d ok=%v", key, asOf, ok)
	}
}

func TestDecodeAsOfFrame_TooShort(t *testing.T) {
	for _, bad := range [][]byte{nil, {1, 2, 3}, EncodeAsOfFrame([]byte("k"), 1)[:4]} {
		if _, _, ok := DecodeAsOfFrame(bad); ok {
			t.Fatalf("畸形帧 %v 不应解析成功", bad)
		}
	}
}

func TestVersionEntryRoundTrip(t *testing.T) {
	frame := EncodeVersionEntry(3, 999, []byte("payload"))
	seq, nanos, payload, consumed, ok := DecodeVersionEntry(frame)
	if !ok || seq != 3 || nanos != 999 || !bytes.Equal(payload, []byte("payload")) || consumed != len(frame) {
		t.Fatalf("round trip 失败: seq=%d nanos=%d payload=%q consumed=%d ok=%v", seq, nanos, payload, consumed, ok)
	}
}

func TestListVersionsResponseRoundTrip(t *testing.T) {
	entries := [][]byte{
		EncodeVersionEntry(1, 100, []byte("v1")),
		EncodeVersionEntry(2, 200, []byte("v2")),
		EncodeVersionEntry(3, 300, []byte("v3")),
	}
	body := EncodeListVersionsResponse(entries)
	got, ok := DecodeListVersionsResponse(body)
	if !ok || len(got) != 3 {
		t.Fatalf("解析失败或数量不符: got=%+v ok=%v", got, ok)
	}
	for i, want := range []struct {
		seq     uint64
		nanos   int64
		payload string
	}{{1, 100, "v1"}, {2, 200, "v2"}, {3, 300, "v3"}} {
		if got[i].Seq != want.seq || got[i].WriteNanos != want.nanos || string(got[i].Payload) != want.payload {
			t.Fatalf("第 %d 条不符: got=%+v", i, got[i])
		}
	}
}

func TestListVersionsResponseEmptyIsValid(t *testing.T) {
	body := EncodeListVersionsResponse(nil)
	got, ok := DecodeListVersionsResponse(body)
	if !ok || len(got) != 0 {
		t.Fatalf("空列表应解析为合法的零条记录: got=%+v ok=%v", got, ok)
	}
}

func TestReplayFingerprintResponseRoundTrip(t *testing.T) {
	body := EncodeReplayFingerprintResponse(5, 2, "deadbeef", []string{"a", "bb"})
	got, ok := DecodeReplayFingerprintResponse(body)
	if !ok {
		t.Fatal("解析应成功")
	}
	if got.KeyCount != 5 || got.MismatchCount != 2 || got.Fingerprint != "deadbeef" {
		t.Fatalf("字段不符: %+v", got)
	}
	if len(got.MismatchKeys) != 2 || got.MismatchKeys[0] != "a" || got.MismatchKeys[1] != "bb" {
		t.Fatalf("mismatch 列表不符: %+v", got.MismatchKeys)
	}
}

func TestReplayFingerprintResponseNoMismatch(t *testing.T) {
	body := EncodeReplayFingerprintResponse(3, 0, "abc123", nil)
	got, ok := DecodeReplayFingerprintResponse(body)
	if !ok || got.MismatchCount != 0 || len(got.MismatchKeys) != 0 {
		t.Fatalf("零不一致应解析为空列表: %+v ok=%v", got, ok)
	}
}

// TestDecodeReplayFingerprintResponse_LegacyWithoutBoundedByteDefaultsFalse
// 验证 M0 时期没有尾部 bounded 字节的旧响应体（EncodeReplayFingerprintResponse，
// 不是 V2 版本）解析后 Bounded=false——这是"这次调用没有 asOf 上界，mismatch
// 就是核对过的结果"这一 M0 既有语义的直接体现，零回归。
func TestDecodeReplayFingerprintResponse_LegacyWithoutBoundedByteDefaultsFalse(t *testing.T) {
	body := EncodeReplayFingerprintResponse(1, 0, "fp", nil)
	got, ok := DecodeReplayFingerprintResponse(body)
	if !ok || got.Bounded {
		t.Fatalf("旧格式响应体应解出 Bounded=false: %+v ok=%v", got, ok)
	}
}

func TestReplayFingerprintResponseV2RoundTripBoundedTrue(t *testing.T) {
	body := EncodeReplayFingerprintResponseV2(1, 0, "fp", nil, true)
	got, ok := DecodeReplayFingerprintResponse(body)
	if !ok || !got.Bounded || got.KeyCount != 1 || got.Fingerprint != "fp" {
		t.Fatalf("round trip 失败: %+v ok=%v", got, ok)
	}
}

func TestReplayFingerprintResponseV2RoundTripBoundedFalse(t *testing.T) {
	body := EncodeReplayFingerprintResponseV2(2, 1, "fp2", []string{"a"}, false)
	got, ok := DecodeReplayFingerprintResponse(body)
	if !ok || got.Bounded || got.KeyCount != 2 || got.MismatchCount != 1 || len(got.MismatchKeys) != 1 {
		t.Fatalf("round trip 失败: %+v ok=%v", got, ok)
	}
}

// TestDecodeReplayFingerprintRequest_LegacyBareKeyOnlyFrameIsUnbounded 验证
// M0 时期的裸 EncodeKeyOnlyFrame 请求（没有尾部 asOfNanos）解出 asOfNanos=0
// （无界），与 DecodeKeyFrame 直接解出的 prefix 一致——这是 REPLAY_FINGERPRINT
// M2 服务化升级对既有调用方零回归的直接证据。
func TestDecodeReplayFingerprintRequest_LegacyBareKeyOnlyFrameIsUnbounded(t *testing.T) {
	legacy := EncodeKeyOnlyFrame([]byte("prefix:"))
	prefix, asOf, ok := DecodeReplayFingerprintRequest(legacy)
	if !ok || string(prefix) != "prefix:" || asOf != 0 {
		t.Fatalf("旧格式请求应解出 asOf=0（无界）: prefix=%q asOf=%d ok=%v", prefix, asOf, ok)
	}
}

func TestReplayFingerprintRequestRoundTripBounded(t *testing.T) {
	frame := EncodeReplayFingerprintRequest([]byte("prefix:"), 123456)
	prefix, asOf, ok := DecodeReplayFingerprintRequest(frame)
	if !ok || string(prefix) != "prefix:" || asOf != 123456 {
		t.Fatalf("round trip 失败: prefix=%q asOf=%d ok=%v", prefix, asOf, ok)
	}
}

func TestReplayFingerprintRequestUnboundedMatchesLegacyKeyOnlyFrameByteForByte(t *testing.T) {
	got := EncodeReplayFingerprintRequest([]byte("prefix:"), 0)
	want := EncodeKeyOnlyFrame([]byte("prefix:"))
	if !bytes.Equal(got, want) {
		t.Fatalf("asOfNanos<=0 时应生成与 EncodeKeyOnlyFrame 逐字节相同的请求: got=%x want=%x", got, want)
	}
}

// TestDecodePutVersionedFrame_LegacyPutFrameHasEmptySource 验证 M0 时期只有
// key/value 的老调用（proto.EncodePutFrame）解出 source=""（未声明，不是
// 错误），这是 PUT_VERSIONED 请求帧向后兼容的直接证据。
func TestDecodePutVersionedFrame_LegacyPutFrameHasEmptySource(t *testing.T) {
	legacy := EncodePutFrame([]byte("k1"), []byte("v1"))
	key, value, source, ok := DecodePutVersionedFrame(legacy)
	if !ok || string(key) != "k1" || string(value) != "v1" || source != "" {
		t.Fatalf("旧格式请求应解出 source=\"\": key=%q value=%q source=%q ok=%v", key, value, source, ok)
	}
}

func TestPutVersionedFrameRoundTripWithSource(t *testing.T) {
	frame := EncodePutVersionedFrame([]byte("k1"), []byte("v1"), "quantbrew-job")
	key, value, source, ok := DecodePutVersionedFrame(frame)
	if !ok || string(key) != "k1" || string(value) != "v1" || source != "quantbrew-job" {
		t.Fatalf("round trip 失败: key=%q value=%q source=%q ok=%v", key, value, source, ok)
	}
}

func TestPutVersionedFrameEmptySourceMatchesLegacyPutFrameByteForByte(t *testing.T) {
	got := EncodePutVersionedFrame([]byte("k1"), []byte("v1"), "")
	want := EncodePutFrame([]byte("k1"), []byte("v1"))
	if !bytes.Equal(got, want) {
		t.Fatalf("source==\"\" 时应生成与 EncodePutFrame 逐字节相同的请求: got=%x want=%x", got, want)
	}
}

func TestListWritesRequestRoundTrip(t *testing.T) {
	frame := EncodeListWritesRequest([]byte("quote:2026-08-17:"), 100, 200, []byte("job-a"))
	prefix, tFrom, tTo, source, ok := DecodeListWritesRequest(frame)
	if !ok || string(prefix) != "quote:2026-08-17:" || tFrom != 100 || tTo != 200 || string(source) != "job-a" {
		t.Fatalf("round trip 失败: prefix=%q tFrom=%d tTo=%d source=%q ok=%v", prefix, tFrom, tTo, source, ok)
	}
}

func TestListWritesRequestUnboundedNoSourceFilter(t *testing.T) {
	frame := EncodeListWritesRequest([]byte("p:"), 0, 0, nil)
	prefix, tFrom, tTo, source, ok := DecodeListWritesRequest(frame)
	if !ok || string(prefix) != "p:" || tFrom != 0 || tTo != 0 || len(source) != 0 {
		t.Fatalf("round trip 失败: prefix=%q tFrom=%d tTo=%d source=%q ok=%v", prefix, tFrom, tTo, source, ok)
	}
}

func TestListWritesCursorRoundTrip(t *testing.T) {
	payload := EncodeListWritesCursor("quote:2026-08-17:600000", 42)
	lk, seq, ok := DecodeListWritesCursor(payload)
	if !ok || lk != "quote:2026-08-17:600000" || seq != 42 {
		t.Fatalf("round trip 失败: logicalKey=%q seq=%d ok=%v", lk, seq, ok)
	}
}

func TestDecodeListWritesCursor_TooShort(t *testing.T) {
	if _, _, ok := DecodeListWritesCursor([]byte{1, 2, 3}); ok {
		t.Fatal("长度不足 4 的负载不应解析成功")
	}
}

func TestDecodeListWritesCursor_BadLength(t *testing.T) {
	// logicalKeyLen 声明与实际不符
	if _, _, ok := DecodeListWritesCursor([]byte{9, 0, 0, 0, 'a'}); ok {
		t.Fatal("长度声明与实际不符的负载不应解析成功")
	}
}

func TestListWritesRequestV2RoundTripWithPagination(t *testing.T) {
	cursor := EncodeListWritesCursor("quote:2026-08-17:600000", 7)
	frame := EncodeListWritesRequestV2([]byte("quote:2026-08-17:"), 100, 200, []byte("job-a"), cursor, 500)
	prefix, tFrom, tTo, source, after, limit, ok := DecodeListWritesRequestV2(frame)
	if !ok || string(prefix) != "quote:2026-08-17:" || tFrom != 100 || tTo != 200 ||
		string(source) != "job-a" || string(after) != string(cursor) || limit != 500 {
		t.Fatalf("round trip 失败: prefix=%q tFrom=%d tTo=%d source=%q after=%q limit=%d ok=%v",
			prefix, tFrom, tTo, source, after, limit, ok)
	}
	lk, seq, curOK := DecodeListWritesCursor(after)
	if !curOK || lk != "quote:2026-08-17:600000" || seq != 7 {
		t.Fatalf("游标内容不符: logicalKey=%q seq=%d ok=%v", lk, seq, curOK)
	}
}

func TestListWritesRequestV2FirstPageNoCursor(t *testing.T) {
	frame := EncodeListWritesRequestV2([]byte("p"), 0, 0, nil, nil, 100)
	prefix, tFrom, tTo, source, after, limit, ok := DecodeListWritesRequestV2(frame)
	if !ok || string(prefix) != "p" || tFrom != 0 || tTo != 0 || len(source) != 0 ||
		len(after) != 0 || limit != 100 {
		t.Fatalf("round trip 失败: prefix=%q tFrom=%d tTo=%d source=%q after=%q limit=%d ok=%v",
			prefix, tFrom, tTo, source, after, limit, ok)
	}
}

func TestListWritesRequestV2NoPaginationMatchesLegacyByteForByte(t *testing.T) {
	// 空游标 + limit=0 的 V2 编码必须与 M2 时期 EncodeListWritesRequest
	// 逐字节相同——"只追加向量、既有向量字节不变"红线的请求侧落地。
	legacy := EncodeListWritesRequest([]byte("p"), 1, 2, []byte("s"))
	v2 := EncodeListWritesRequestV2([]byte("p"), 1, 2, []byte("s"), nil, 0)
	if string(legacy) != string(v2) {
		t.Fatalf("V2 无分页编码应与旧格式逐字节一致\n  旧: %x\n  V2: %x", legacy, v2)
	}
}

func TestDecodeListWritesRequestV2_LegacyRequestDecodesAsNoPagination(t *testing.T) {
	legacy := EncodeListWritesRequest([]byte("p"), 1, 2, []byte("s"))
	prefix, tFrom, tTo, source, after, limit, ok := DecodeListWritesRequestV2(legacy)
	if !ok || string(prefix) != "p" || tFrom != 1 || tTo != 2 || string(source) != "s" ||
		after != nil || limit != 0 {
		t.Fatalf("旧格式请求应解出 after=nil limit=0: prefix=%q tFrom=%d tTo=%d source=%q after=%q limit=%d ok=%v",
			prefix, tFrom, tTo, source, after, limit, ok)
	}
}

func TestDecodeListWritesRequestV2_TruncatedTailIsMalformed(t *testing.T) {
	base := EncodeListWritesRequest([]byte("p"), 1, 2, []byte("s"))
	// 尾段只有 7 字节（缺 limit 的 4 字节），精确长度判定必须拒绝。
	bad := append(append([]byte{}, base...), 0, 0, 0, 0, 1, 2, 3)
	if _, _, _, _, _, _, ok := DecodeListWritesRequestV2(bad); ok {
		t.Fatal("截断的尾段不应解析成功")
	}
}

func TestListWritesResponseV2RoundTripWithNextCursor(t *testing.T) {
	entries := [][]byte{
		EncodeWriteEnvelopeEntry("k1", 1, 100, "job-a", 1, "h1", []byte("v1"), true),
		EncodeWriteEnvelopeEntry("k2", 2, 200, "job-b", 1, "h2", []byte("v2"), false),
	}
	next := EncodeListWritesCursor("k2", 2)
	body := EncodeListWritesResponseV2(entries, []string{"job-a", "job-b"}, []uint32{1, 1}, next)
	gotEntries, gotCounts, gotNext, ok := DecodeListWritesResponseV2(body)
	if !ok || len(gotEntries) != 2 || len(gotCounts) != 2 || string(gotNext) != string(next) {
		t.Fatalf("round trip 失败: entries=%d counts=%d next=%q ok=%v", len(gotEntries), len(gotCounts), gotNext, ok)
	}
	lk, seq, curOK := DecodeListWritesCursor(gotNext)
	if !curOK || lk != "k2" || seq != 2 {
		t.Fatalf("next_cursor 内容不符: logicalKey=%q seq=%d ok=%v", lk, seq, curOK)
	}
}

func TestListWritesResponseV2EmptyNextCursor(t *testing.T) {
	entries := [][]byte{EncodeWriteEnvelopeEntry("k1", 1, 100, "", 1, "h1", []byte("v1"), true)}
	body := EncodeListWritesResponseV2(entries, nil, nil, nil)
	gotEntries, gotCounts, gotNext, ok := DecodeListWritesResponseV2(body)
	if !ok || len(gotEntries) != 1 || len(gotCounts) != 0 || len(gotNext) != 0 {
		t.Fatalf("空 next_cursor 应解出零长度游标: entries=%d counts=%d next=%q ok=%v",
			len(gotEntries), len(gotCounts), gotNext, ok)
	}
}

func TestDecodeListWritesResponseV2_LegacyBodyDecodesWithoutTail(t *testing.T) {
	entries := [][]byte{EncodeWriteEnvelopeEntry("k1", 1, 100, "job-a", 1, "h1", []byte("v1"), true)}
	legacy := EncodeListWritesResponse(entries, []string{"job-a"}, []uint32{1})
	gotEntries, gotCounts, gotNext, ok := DecodeListWritesResponseV2(legacy)
	if !ok || len(gotEntries) != 1 || len(gotCounts) != 1 || gotNext != nil {
		t.Fatalf("旧格式响应体应解出 nextCursor=nil: entries=%d counts=%d next=%q ok=%v",
			len(gotEntries), len(gotCounts), gotNext, ok)
	}
}

func TestDecodeListWritesResponseV2_LegacyDecoderIgnoresTail(t *testing.T) {
	// 反向兼容：M2 时期老解码器（DecodeListWritesResponse）解 V2 响应体，
	// 应照常解出 entries/counts、忽略尾段（老解码器按字段精确消费）。
	entries := [][]byte{EncodeWriteEnvelopeEntry("k1", 1, 100, "job-a", 1, "h1", []byte("v1"), true)}
	next := EncodeListWritesCursor("k1", 1)
	body := EncodeListWritesResponseV2(entries, []string{"job-a"}, []uint32{1}, next)
	gotEntries, gotCounts, ok := DecodeListWritesResponse(body)
	if !ok || len(gotEntries) != 1 || len(gotCounts) != 1 || gotEntries[0].LogicalKey != "k1" {
		t.Fatalf("老解码器应忽略 next_cursor 尾段照常解析: entries=%+v counts=%+v ok=%v", gotEntries, gotCounts, ok)
	}
}

func TestWriteEnvelopeEntryRoundTrip(t *testing.T) {
	entry := EncodeWriteEnvelopeEntry("quote:2026-08-17:600000", 3, 999, "job-a", 2, "deadbeef", []byte("payload"), true)
	got, consumed, ok := DecodeWriteEnvelopeEntry(entry)
	if !ok || consumed != len(entry) {
		t.Fatalf("解析失败或未消费全部字节: consumed=%d len=%d ok=%v", consumed, len(entry), ok)
	}
	if got.LogicalKey != "quote:2026-08-17:600000" || got.Seq != 3 || got.WriteNanos != 999 ||
		got.Source != "job-a" || got.SchemaVer != 2 || got.PayloadHash != "deadbeef" ||
		string(got.Payload) != "payload" || !got.HashOK {
		t.Fatalf("字段不符: %+v", got)
	}
}

func TestWriteEnvelopeEntryHashOKFalseRoundTrips(t *testing.T) {
	entry := EncodeWriteEnvelopeEntry("k", 1, 1, "", 0, "abc", []byte("p"), false)
	got, _, ok := DecodeWriteEnvelopeEntry(entry)
	if !ok || got.HashOK {
		t.Fatalf("hashOK=false 应原样解出: %+v ok=%v", got, ok)
	}
}

func TestListWritesResponseRoundTripWithSourceBreakdown(t *testing.T) {
	entries := [][]byte{
		EncodeWriteEnvelopeEntry("k1", 1, 100, "job-a", 1, "h1", []byte("v1"), true),
		EncodeWriteEnvelopeEntry("k2", 1, 200, "job-b", 1, "h2", []byte("v2"), false),
	}
	body := EncodeListWritesResponse(entries, []string{"job-a", "job-b"}, []uint32{1, 1})
	gotEntries, gotCounts, ok := DecodeListWritesResponse(body)
	if !ok || len(gotEntries) != 2 {
		t.Fatalf("解析失败或数量不符: entries=%+v ok=%v", gotEntries, ok)
	}
	if gotEntries[0].LogicalKey != "k1" || gotEntries[0].Source != "job-a" || !gotEntries[0].HashOK {
		t.Fatalf("第 0 条不符: %+v", gotEntries[0])
	}
	if gotEntries[1].LogicalKey != "k2" || gotEntries[1].Source != "job-b" || gotEntries[1].HashOK {
		t.Fatalf("第 1 条不符: %+v", gotEntries[1])
	}
	if len(gotCounts) != 2 || gotCounts[0] != (SourceCountView{Source: "job-a", Count: 1}) ||
		gotCounts[1] != (SourceCountView{Source: "job-b", Count: 1}) {
		t.Fatalf("来源聚合不符: %+v", gotCounts)
	}
}

func TestListWritesResponseEmptyIsValid(t *testing.T) {
	body := EncodeListWritesResponse(nil, nil, nil)
	entries, counts, ok := DecodeListWritesResponse(body)
	if !ok || len(entries) != 0 || len(counts) != 0 {
		t.Fatalf("空结果应解析为合法的零条记录: entries=%+v counts=%+v ok=%v", entries, counts, ok)
	}
}
