package storage

import (
	"fmt"
	"testing"
)

func TestSplitNamespace(t *testing.T) {
	cases := []struct {
		key     string
		wantNS  string
		wantSuf string
	}{
		{"log:2026-05-31", "log", "2026-05-31"},
		{"order:10086", "order", "10086"},
		{"order:a:b", "order", "a:b"}, // 只按首个分隔符拆分
		{"nocolon", "", "nocolon"},    // 无前缀 → 默认仓库
		{":empty", "", "empty"},
	}
	for _, c := range cases {
		ns, suf := splitNamespace([]byte(c.key), ':')
		if ns != c.wantNS || string(suf) != c.wantSuf {
			t.Errorf("split(%q) = (%q, %q), want (%q, %q)", c.key, ns, suf, c.wantNS, c.wantSuf)
		}
	}
}

// TestPartitionedNoFalseNegative 各仓库 key 插入后必须命中。
func TestPartitionedNoFalseNegative(t *testing.T) {
	pb := NewPartitionedBloom(DefaultNamespaceSep, 1000, 0.01)
	for i := 0; i < 1000; i++ {
		pb.Add([]byte(fmt.Sprintf("log:%d", i)))
		pb.Add([]byte(fmt.Sprintf("order:%d", i)))
	}
	for i := 0; i < 1000; i++ {
		if !pb.MayContain([]byte(fmt.Sprintf("log:%d", i))) {
			t.Fatalf("false negative for log:%d", i)
		}
		if !pb.MayContain([]byte(fmt.Sprintf("order:%d", i))) {
			t.Fatalf("false negative for order:%d", i)
		}
	}
}

// TestPartitionedNamespaceIsolation 未建立的仓库立即返回 false，
// 且同后缀不同仓库互不污染。
func TestPartitionedNamespaceIsolation(t *testing.T) {
	pb := NewPartitionedBloom(DefaultNamespaceSep, 1000, 0.01)
	pb.Add([]byte("log:abc"))

	// log 仓库存在该后缀
	if !pb.MayContain([]byte("log:abc")) {
		t.Error("log:abc should be present")
	}
	// order 仓库从未建立 → 必定 false（不会被 log 的 abc 污染）
	if pb.MayContain([]byte("order:abc")) {
		t.Error("order:abc must be false: namespace never created")
	}
	if len(pb.Namespaces()) != 1 {
		t.Errorf("expected 1 namespace, got %d", len(pb.Namespaces()))
	}
}

func TestPartitionedEncodeDecode(t *testing.T) {
	pb := NewPartitionedBloom(DefaultNamespaceSep, 500, 0.01)
	for i := 0; i < 500; i++ {
		pb.Add([]byte(fmt.Sprintf("log:%d", i)))
		pb.Add([]byte(fmt.Sprintf("order:%d", i)))
	}
	got, err := DecodePartitionedBloom(pb.Encode(), 500, 0.01)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Namespaces()) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(got.Namespaces()))
	}
	for i := 0; i < 500; i++ {
		if !got.MayContain([]byte(fmt.Sprintf("log:%d", i))) {
			t.Fatalf("decoded missing log:%d", i)
		}
		if !got.MayContain([]byte(fmt.Sprintf("order:%d", i))) {
			t.Fatalf("decoded missing order:%d", i)
		}
	}
}
