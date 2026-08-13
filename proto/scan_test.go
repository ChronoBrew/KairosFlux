package proto

import (
	"bytes"
	"testing"

	"github.com/NeverENG/BanDB/predicate"
)

func TestScanRequestRoundTrip(t *testing.T) {
	cases := []ScanRequest{
		{Start: []byte("imu:dev0:100"), End: []byte("imu:dev0:200"),
			Pred: predicate.Predicate{Field: "az", Op: predicate.OpGT, Operand: "9.9"}},
		{Start: nil, End: nil, Pred: predicate.Predicate{Op: predicate.OpNone}}, // 全量无谓词
		{Start: []byte("a"), End: nil, Pred: predicate.Predicate{Field: "s", Op: predicate.OpEQ, Operand: "ok"}},
	}
	for _, want := range cases {
		got, err := DecodeScanRequest(EncodeScanRequest(want))
		if err != nil {
			t.Fatalf("decode 失败: %v", err)
		}
		if !bytes.Equal(got.Start, want.Start) || !bytes.Equal(got.End, want.End) {
			t.Fatalf("范围不符: got start=%q end=%q", got.Start, got.End)
		}
		if got.Pred != want.Pred {
			t.Fatalf("谓词不符: got %+v want %+v", got.Pred, want.Pred)
		}
	}
}

func TestDecodeScanRequest_Truncated(t *testing.T) {
	if _, err := DecodeScanRequest([]byte{1, 2, 3}); err == nil {
		t.Fatal("过短负载应报错")
	}
}

func TestScanResponseRoundTrip(t *testing.T) {
	want := []ScanEntry{
		{Key: []byte("imu:dev0:101"), Value: []byte(`{"az":9.91}`)},
		{Key: []byte("imu:dev0:150"), Value: []byte(`{"az":10.2}`)},
	}
	status, got, err := DecodeScanResponse(EncodeScanResponse(StatusOK, want))
	if err != nil {
		t.Fatalf("decode 失败: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("status=%q", status)
	}
	if len(got) != len(want) {
		t.Fatalf("条目数 %d，期望 %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("条目 %d 不符: %q=%q", i, got[i].Key, got[i].Value)
		}
	}
}

func TestScanResponseEmpty(t *testing.T) {
	status, got, err := DecodeScanResponse(EncodeScanResponse(StatusOK, nil))
	if err != nil || status != StatusOK || len(got) != 0 {
		t.Fatalf("空结果应正常: status=%q n=%d err=%v", status, len(got), err)
	}
}
