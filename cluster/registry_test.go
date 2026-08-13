package cluster

import (
	"testing"
	"time"
)

// fakeClock 是可控的假时钟，供测试确定性地推进时间。
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.t = c.t.Add(d)
}

// TestRegistryTTL 验证：初始存活、心跳刷新、超过 ttl 判死、再次心跳复活。
func TestRegistryTTL(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	reg := NewRegistry([]string{"a", "b"}, 10*time.Second).WithClock(clk.now)

	// 初始时刻：NewRegistry 用真实时钟置 lastSeen=创建时刻，注入假时钟后需先心跳
	// 使 lastSeen 落在假时钟坐标系；先验证初始（真实时钟）视角下的存活。
	reg.Heartbeat("a")
	reg.Heartbeat("b")

	if !reg.IsAlive("a") || !reg.IsAlive("b") {
		t.Fatal("心跳后应存活")
	}

	// 推进到 ttl 边界内（== ttl 视为存活）。
	clk.advance(10 * time.Second)
	if !reg.IsAlive("a") {
		t.Fatal("now-lastSeen==ttl 应仍存活")
	}

	// 越过 ttl：判死。
	clk.advance(1 * time.Nanosecond)
	if reg.IsAlive("a") {
		t.Fatal("超过 ttl 应判死")
	}

	// 心跳复活。
	reg.Heartbeat("a")
	if !reg.IsAlive("a") {
		t.Fatal("心跳后应复活")
	}
}

// TestRegistryAliveNodes 验证 AliveNodes 有序且随 TTL 变化。
func TestRegistryAliveNodes(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	reg := NewRegistry([]string{"c", "a", "b"}, 5*time.Second).WithClock(clk.now)
	reg.Heartbeat("a")
	reg.Heartbeat("b")
	reg.Heartbeat("c")

	got := reg.AliveNodes()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("AliveNodes 数量 %v != %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("AliveNodes 未按升序返回：%v", got)
		}
	}

	// 只给 b 续心跳，推进越过 ttl，只有 b 存活。
	clk.advance(4 * time.Second)
	reg.Heartbeat("b")
	clk.advance(2 * time.Second) // a、c 距上次心跳 6s > 5s；b 距上次 2s
	got = reg.AliveNodes()
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("仅 b 应存活，得到 %v", got)
	}
}

// TestRegistryUnknownNode 验证未知节点不存活。
func TestRegistryUnknownNode(t *testing.T) {
	reg := NewRegistry([]string{"a"}, time.Second)
	if reg.IsAlive("ghost") {
		t.Fatal("未知节点不应存活")
	}
}
