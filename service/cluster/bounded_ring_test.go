package cluster

import (
	"fmt"
	"math"
	"testing"
)

// maxLoad 返回负载分布中的峰值。
func maxLoad(loads map[string]int) int {
	m := 0
	for _, v := range loads {
		if v > m {
			m = v
		}
	}
	return m
}

// TestBoundedRing_BoundsHotspotUnderSkew：倾斜负载（80% 请求打同一热点 key）下，
// vanilla 一致性哈希会把热点 key 的全部负载压到一个节点，而有界负载环把峰值负载限制在
// ⌈(1+ε)·M/N⌉ 内、把溢出均衡到邻居。
func TestBoundedRing_BoundsHotspotUnderSkew(t *testing.T) {
	nodes := []string{"n0", "n1", "n2", "n3"}
	const M = 10000
	const eps = 0.25

	// vanilla：每个请求按 NodeFor 落到主节点。
	ring := NewHashRing(nodes, 128)
	vanilla := map[string]int{}
	// bounded：满载溢出到邻居。
	bounded := NewBoundedRing(nodes, 128, eps)

	for i := 0; i < M; i++ {
		var key []byte
		if i%10 < 8 { // 80% 打同一热点 key
			key = []byte("HOT")
		} else {
			key = []byte(fmt.Sprintf("k-%d", i))
		}
		vanilla[ring.NodeFor(key)]++
		bounded.Assign(key) // 不 Release：度量 M 个请求的静态分布
	}

	vMax := maxLoad(vanilla)
	bMax := maxLoad(bounded.Loads())
	capBound := int(math.Ceil((1 + eps) * float64(M) / float64(len(nodes))))

	t.Logf("倾斜负载 M=%d N=%d ε=%.2f：vanilla 峰值=%d，bounded 峰值=%d，理论上界=%d",
		M, len(nodes), eps, vMax, bMax, capBound)

	if bMax > capBound {
		t.Fatalf("bounded 峰值应 ≤ 上界：%d > %d", bMax, capBound)
	}
	if bMax >= vMax {
		t.Fatalf("bounded 应显著低于 vanilla 峰值（消除热点）：bounded=%d vanilla=%d", bMax, vMax)
	}
}

// TestBoundedRing_ReleaseFreesCapacity：Release 归还负载后容量重新可用。
func TestBoundedRing_ReleaseFreesCapacity(t *testing.T) {
	b := NewBoundedRing([]string{"n0", "n1"}, 64, 0.0) // ε=0 → 容量=⌈总负载/2⌉
	assigned := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		n, ok := b.Assign([]byte("x")) // 同一 key，反复分配 → 溢出到两个节点
		if !ok {
			t.Fatal("assign should succeed")
		}
		assigned = append(assigned, n)
	}
	// 两个节点应都被用到（容量上限迫使溢出）。
	distinct := map[string]bool{}
	for _, n := range assigned {
		distinct[n] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("bounded load should spread same key across nodes, got %v", assigned)
	}
	// 全部归还后总负载归零。
	for _, n := range assigned {
		b.Release(n)
	}
	for n, l := range b.Loads() {
		if l != 0 {
			t.Fatalf("node %s load should be 0 after release, got %d", n, l)
		}
	}
}
