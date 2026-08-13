package cluster

import (
	"fmt"
	"testing"
)

// TestNodeForStable 验证同一 key 多次查询归属稳定。
func TestNodeForStable(t *testing.T) {
	ring := NewHashRing([]string{"a", "b", "c", "d"}, 0)
	key := []byte("some-key-42")
	first := ring.NodeFor(key)
	if first == "" {
		t.Fatal("非空环 NodeFor 不应返回空")
	}
	for i := 0; i < 100; i++ {
		if got := ring.NodeFor(key); got != first {
			t.Fatalf("NodeFor 不稳定：第一次=%q 第%d次=%q", first, i, got)
		}
	}
}

// TestNodeForEmptyRing 验证空环返回 ""。
func TestNodeForEmptyRing(t *testing.T) {
	ring := NewHashRing(nil, 0)
	if got := ring.NodeFor([]byte("x")); got != "" {
		t.Fatalf("空环应返回 \"\"，得到 %q", got)
	}
}

// TestDistribution 验证 key 大致均匀分布到各节点（一致性哈希 + 虚拟节点的目的）。
func TestDistribution(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	ring := NewHashRing(nodes, 256)
	const n = 50000
	counts := make(map[string]int, len(nodes))
	for i := 0; i < n; i++ {
		counts[ring.NodeFor([]byte(fmt.Sprintf("key-%d", i)))]++
	}
	mean := float64(n) / float64(len(nodes))
	// 虚拟节点数有限，允许 ±35% 的偏差，避免统计抖动导致偶发失败。
	lo, hi := mean*0.65, mean*1.35
	for _, node := range nodes {
		c := float64(counts[node])
		if c < lo || c > hi {
			t.Errorf("节点 %q 分布 %v 超出 [%.0f, %.0f]（均值 %.0f）", node, counts[node], lo, hi, mean)
		}
	}
}

// TestRemoveNodeConsistency 验证 RemoveNode 后一致性哈希核心性质：
// 只有原属于被删节点的 key 会改变归属，其余 key 归属不变；被迁移量接近 1/N。
func TestRemoveNodeConsistency(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	ring := NewHashRing(nodes, 256)
	const n = 50000
	keys := make([][]byte, n)
	before := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		before[i] = ring.NodeFor(keys[i])
	}

	const removed = "c"
	ring.RemoveNode(removed)

	moved := 0
	for i := 0; i < n; i++ {
		after := ring.NodeFor(keys[i])
		if before[i] == removed {
			// 被删节点的 key 必须改归其他节点，且不能再归到被删节点。
			if after == removed {
				t.Fatalf("被删节点的 key 仍归属 %q", removed)
			}
			moved++
			continue
		}
		// 非被删节点的 key 归属必须保持不变——这是一致性哈希的严格性质。
		if after != before[i] {
			t.Fatalf("key %s 本不该迁移：%q -> %q", keys[i], before[i], after)
		}
	}

	// 被迁移量应接近理论份额 1/N；放宽到 2 倍公平份额作上界，避免抖动误报。
	fair := float64(n) / float64(len(nodes))
	if float64(moved) > 2*fair {
		t.Errorf("迁移量 %d 过大，超过 2×公平份额 %.0f", moved, fair)
	}
	if moved == 0 {
		t.Errorf("被删节点存在 key，迁移量不应为 0")
	}
}

// TestShardOf 验证分片编号在范围内、稳定，且非法 shardCount 返回 0。
func TestShardOf(t *testing.T) {
	if got := ShardOf([]byte("x"), 0); got != 0 {
		t.Errorf("shardCount<=0 应返回 0，得到 %d", got)
	}
	if got := ShardOf([]byte("x"), -3); got != 0 {
		t.Errorf("shardCount<0 应返回 0，得到 %d", got)
	}
	const shards = 16
	key := []byte("stable-key")
	first := ShardOf(key, shards)
	for i := 0; i < 100; i++ {
		if got := ShardOf(key, shards); got != first {
			t.Fatalf("ShardOf 不稳定：%d != %d", got, first)
		}
	}
	for i := 0; i < 1000; i++ {
		s := ShardOf([]byte(fmt.Sprintf("k-%d", i)), shards)
		if s < 0 || s >= shards {
			t.Fatalf("分片编号 %d 越界 [0,%d)", s, shards)
		}
	}
}

// TestAddNodeConsistency 验证 AddNode 只迁移少量 key（对称于 RemoveNode）。
func TestAddNodeConsistency(t *testing.T) {
	ring := NewHashRing([]string{"a", "b", "c", "d"}, 256)
	const n = 30000
	keys := make([][]byte, n)
	before := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		before[i] = ring.NodeFor(keys[i])
	}
	ring.AddNode("e")
	moved := 0
	for i := 0; i < n; i++ {
		if ring.NodeFor(keys[i]) != before[i] {
			moved++
		}
	}
	// 新增第 5 个节点，理论迁移份额约 1/5；放宽到 2 倍作上界。
	fair := float64(n) / 5.0
	if float64(moved) > 2*fair {
		t.Errorf("新增节点迁移量 %d 过大，超过 2×公平份额 %.0f", moved, fair)
	}
}

// TestShardReplicas 验证副本集放置：返回 rf 个互异节点、确定且各次一致、与副本数上界正确。
func TestShardReplicas(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	ring := NewHashRing(nodes, 0)
	const rf = 3

	for sid := 0; sid < 20; sid++ {
		rs := ShardReplicas(ring, sid, rf)
		if len(rs) != rf {
			t.Fatalf("shard %d: 期望 %d 个副本，得到 %d", sid, rf, len(rs))
		}
		seen := map[string]bool{}
		for _, n := range rs {
			if seen[n] {
				t.Fatalf("shard %d: 副本集含重复节点 %q", sid, n)
			}
			seen[n] = true
		}
		// 确定性：同一分片重复计算结果完全一致（各节点据此独立算出一致的 peers 顺序）。
		if again := ShardReplicas(ring, sid, rf); fmt.Sprint(again) != fmt.Sprint(rs) {
			t.Fatalf("shard %d: 副本集不确定，%v vs %v", sid, rs, again)
		}
	}
}

// TestShardReplicasBounds 验证 rf 越界与空环的边界处理。
func TestShardReplicasBounds(t *testing.T) {
	ring := NewHashRing([]string{"a", "b", "c"}, 0)
	if got := ShardReplicas(ring, 0, 10); len(got) != 3 {
		t.Fatalf("rf 超过节点数应取全部 3，得到 %d", len(got))
	}
	if got := ShardReplicas(ring, 0, 0); len(got) != 1 {
		t.Fatalf("rf<=0 应视为 1，得到 %d", len(got))
	}
	empty := NewHashRing(nil, 0)
	if got := ShardReplicas(empty, 0, 2); got != nil {
		t.Fatalf("空环应返回 nil，得到 %v", got)
	}
}
