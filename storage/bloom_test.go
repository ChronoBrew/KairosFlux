package storage

import (
	"fmt"
	"math"
	"testing"
)

// TestBloomNoFalseNegative 已插入的 key 必须全部命中（布隆过滤器不允许漏报）。
func TestBloomNoFalseNegative(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	for i := 0; i < 1000; i++ {
		bf.Add([]byte(fmt.Sprintf("key-%d", i)))
	}
	for i := 0; i < 1000; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("false negative for key-%d", i)
		}
	}
}

// TestBloomOptimalParams 校验 m、k 按最优公式计算。
func TestBloomOptimalParams(t *testing.T) {
	n, p := 1000, 0.01
	bf := NewBloomFilter(n, p)
	wantM := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
	wantK := uint64(math.Round(float64(wantM) / float64(n) * math.Ln2))
	if bf.m != wantM {
		t.Errorf("m = %d, want %d", bf.m, wantM)
	}
	if bf.k != wantK {
		t.Errorf("k = %d, want %d", bf.k, wantK)
	}
	// 1% 误判率下 k 理论上应为 7。
	if bf.k != 7 {
		t.Errorf("k = %d, expected 7 for p=0.01", bf.k)
	}
}

// TestBloomFalsePositiveRate 经验误判率应接近目标值（留 3x 统计余量）。
func TestBloomFalsePositiveRate(t *testing.T) {
	n, p := 10000, 0.01
	bf := NewBloomFilter(n, p)
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("present-%d", i)))
	}
	falsePos, trials := 0, 100000
	for i := 0; i < trials; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			falsePos++
		}
	}
	rate := float64(falsePos) / float64(trials)
	t.Logf("empirical false-positive rate: %.4f (target %.4f)", rate, p)
	if rate > p*3 {
		t.Errorf("false-positive rate %.4f far exceeds target %.4f", rate, p)
	}
}

// TestBloomEncodeDecode 序列化往返后行为一致。
func TestBloomEncodeDecode(t *testing.T) {
	bf := NewBloomFilter(500, 0.02)
	for i := 0; i < 500; i++ {
		bf.Add([]byte(fmt.Sprintf("k%d", i)))
	}
	got, err := DecodeBloomFilter(bf.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.m != bf.m || got.k != bf.k {
		t.Fatalf("params mismatch: got m=%d k=%d, want m=%d k=%d", got.m, got.k, bf.m, bf.k)
	}
	for i := 0; i < 500; i++ {
		if !got.MayContain([]byte(fmt.Sprintf("k%d", i))) {
			t.Fatalf("decoded filter missing k%d", i)
		}
	}
}

func TestDecodeBloomCorrupt(t *testing.T) {
	if _, err := DecodeBloomFilter([]byte{1, 2, 3}); err == nil {
		t.Error("expected error for short data")
	}
	if _, err := DecodeBloomFilter(make([]byte, 16)); err == nil {
		t.Error("expected error for zero m/k")
	}
}
