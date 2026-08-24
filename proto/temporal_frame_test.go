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
