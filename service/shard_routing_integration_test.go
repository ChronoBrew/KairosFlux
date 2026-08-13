package service

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/cluster"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/pkg/predicate"
	"github.com/NeverENG/BanDB/pkg/proto"
)

// memKV 是隔离的内存 KV，用作每个节点的本地 store——从而在一个进程内起多节点、
// 用「哪个节点的 store 里有这个 key」证明请求确实被路由/转发到了属主节点。
type memKV struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemKV() *memKV { return &memKV{m: map[string][]byte{}} }

func (s *memKV) Write(cmd Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Type {
	case CommandPut:
		s.m[string(cmd.Key)] = append([]byte(nil), cmd.Value...)
	case CommandDelete:
		delete(s.m, string(cmd.Key))
	}
	return nil
}

func (s *memKV) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return v, nil
}

func (s *memKV) Scan(start, end []byte, pred predicate.Predicate) []proto.ScanEntry { return nil }

func (s *memKV) has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[key]
	return ok
}

// TestShardRouting_MultiNode 在一个进程内起 3 个真实 BanNet 节点，验证：
// 写入非本节点 key 会经 BanNet TCP 转发到属主、只落在属主的 store（数据分片）、
// 从任一节点都能读到（读转发）。
func TestShardRouting_MultiNode(t *testing.T) {
	peers := []string{"127.0.0.1:19311", "127.0.0.1:19312", "127.0.0.1:19313"}
	vnodes := config.G.VNodes
	stores := make([]*memKV, len(peers))

	for i, addr := range peers {
		store := newMemKV()
		stores[i] = store
		router := NewRouterWithStore(store)
		placement := cluster.NewClusterFromPeers(peers, vnodes, time.Hour)
		pool := cluster.NewPeerPool(2 * time.Second)
		router.SetRouting(placement, addr, pool)

		srv := bannet.NewServer()
		host, portStr, _ := net.SplitHostPort(addr)
		port, _ := strconv.Atoi(portStr)
		srv.IP = host
		srv.Port = port
		srv.AddRouter(proto.MsgPut, router)
		srv.AddRouter(proto.MsgGet, router)
		srv.AddRouter(proto.MsgDelete, router)
		srv.SetConnStartFunc(router.OnConnStart)
		srv.SetConnStopFunc(router.OnConnStop)
		srv.Start()
		defer srv.Stop()
	}
	time.Sleep(300 * time.Millisecond) // 等监听就绪

	ring := cluster.NewHashRing(peers, vnodes)

	// 找一个属主为 peers[1]（非入口）的 key，用 peers[0] 作为入口写入。
	entry := peers[0]
	var key []byte
	var ownerIdx int
	for i := 0; i < 100000; i++ {
		k := []byte(fmt.Sprintf("obj:%d", i))
		owner := ring.NodeFor(k)
		if owner != entry {
			key = k
			ownerIdx = indexOf(peers, owner)
			break
		}
	}
	if key == nil {
		t.Fatal("could not find a key owned by a non-entry node")
	}
	value := []byte("payload-v1")

	// 经入口节点写入 → 应被转发到属主。
	c := bannet.NewClient(entry, 2*time.Second)
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Put(key, value); err != nil {
		t.Fatalf("put via entry failed: %v", err)
	}

	// 分片证据：只有属主的 store 有该 key，入口节点没有。
	if !stores[ownerIdx].has(string(key)) {
		t.Fatalf("owner node %s should have key %q after forward", peers[ownerIdx], key)
	}
	if stores[0].has(string(key)) {
		t.Fatalf("entry node should NOT store forwarded key %q (data must be sharded)", key)
	}

	// 从入口读（转发读）→ 命中。
	got, found, err := c.Get(key)
	if err != nil || !found {
		t.Fatalf("get via entry: found=%v err=%v", found, err)
	}
	if string(got) != string(value) {
		t.Fatalf("get via entry = %q, want %q", got, value)
	}

	// 从第三个节点（既非入口也非属主）读 → 仍应转发到属主命中。
	third := indexOfOther(peers, 0, ownerIdx)
	c3 := bannet.NewClient(peers[third], 2*time.Second)
	if err := c3.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	got3, found3, err := c3.Get(key)
	if err != nil || !found3 || string(got3) != string(value) {
		t.Fatalf("get via third node: got=%q found=%v err=%v", got3, found3, err)
	}

	// 删除经入口转发 → 属主 store 不再有该 key。
	if err := c.Delete(key); err != nil {
		t.Fatalf("delete via entry failed: %v", err)
	}
	if stores[ownerIdx].has(string(key)) {
		t.Fatalf("owner store should not have key %q after forwarded delete", key)
	}
}

func indexOf(ss []string, s string) int {
	for i, x := range ss {
		if x == s {
			return i
		}
	}
	return -1
}

// indexOfOther 返回既不是 a 也不是 b 的第一个下标。
func indexOfOther(ss []string, a, b int) int {
	for i := range ss {
		if i != a && i != b {
			return i
		}
	}
	return -1
}
