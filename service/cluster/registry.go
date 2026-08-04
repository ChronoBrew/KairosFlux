package cluster

import (
	"sort"
	"sync"
	"time"
)

// Registry 管理集群的静态 peers 集合与基于心跳 TTL 的存活判定。
//
// 为什么这么设计：分片集群需要一份「谁还活着」的视图供放置控制面做故障转移。
// 这里采用最朴素但真实可用的心跳 TTL 模型——节点周期性上报心跳刷新 lastSeen，
// 超过 ttl 未上报即判死。为了可测试（不依赖真实墙钟），时钟通过 now 字段注入，
// 测试可直接替换或经 WithClock 注入假时钟。
//
// 并发安全：读操作持 RLock，Heartbeat 持 Lock。
type Registry struct {
	mu       sync.RWMutex
	ttl      time.Duration
	lastSeen map[string]time.Time

	// now 提供当前时间，默认 time.Now；测试可注入假时钟以确定性地验证 TTL 行为。
	now func() time.Time
}

// NewRegistry 构造注册表。初始时所有节点视为存活（lastSeen 置为创建时刻）。
// ttl 为心跳存活窗口：now-lastSeen<=ttl 判活。
func NewRegistry(nodes []string, ttl time.Duration) *Registry {
	r := &Registry{
		ttl:      ttl,
		lastSeen: make(map[string]time.Time, len(nodes)),
		now:      time.Now,
	}
	start := r.now()
	for _, n := range nodes {
		r.lastSeen[n] = start
	}
	return r
}

// WithClock 注入自定义时钟（返回自身以便链式调用）。主要供测试注入假时钟。
// 注入后不会回填已有节点的 lastSeen，调用方如需可重新 Heartbeat。
func (r *Registry) WithClock(now func() time.Time) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if now != nil {
		r.now = now
	}
	return r
}

// Heartbeat 刷新 node 的 lastSeen 为当前时刻。可用于让已判死的节点「复活」。
func (r *Registry) Heartbeat(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSeen[node] = r.now()
}

// IsAlive 判定 node 是否存活：now-lastSeen<=ttl。未知节点视为不存活。
func (r *Registry) IsAlive(node string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	last, ok := r.lastSeen[node]
	if !ok {
		return false
	}
	return r.now().Sub(last) <= r.ttl
}

// AliveNodes 返回当前所有存活节点，按节点名升序排列（便于确定性输出与测试）。
func (r *Registry) AliveNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()
	out := make([]string, 0, len(r.lastSeen))
	for n, last := range r.lastSeen {
		if now.Sub(last) <= r.ttl {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}
