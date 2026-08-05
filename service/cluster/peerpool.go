package cluster

import (
	"sync"
	"time"

	"github.com/NeverENG/BanDB/network/banNet"
)

// PeerPool 维护到各节点的 BanNet 客户端连接（懒建、缓存），供分片转发复用。
//
// 并发正确性：单条 BanNet 连接是请求-响应式，不能被多个 goroutine 交错读写（否则帧
// 错位）。故每个 peer 用一把锁串行化其连接上的调用；出错则丢弃连接、下次重连。
// 需要更高并发时可扩为每 peer 一个连接池，此处先保正确。
type PeerPool struct {
	mu      sync.Mutex
	timeout time.Duration
	peers   map[string]*peerConn
}

type peerConn struct {
	mu     sync.Mutex
	addr   string
	client *banNet.Client
}

// NewPeerPool 创建一个转发连接池。timeout 为每次拨号/读写的超时。
func NewPeerPool(timeout time.Duration) *PeerPool {
	return &PeerPool{timeout: timeout, peers: map[string]*peerConn{}}
}

// conn 取（或创建）某 peer 的连接槽。
func (p *PeerPool) conn(addr string) *peerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	pc, ok := p.peers[addr]
	if !ok {
		pc = &peerConn{addr: addr}
		p.peers[addr] = pc
	}
	return pc
}

// withClient 在 peer 连接上串行执行 fn；懒建连接，fn 出错则丢弃连接以便下次重连。
func (p *PeerPool) withClient(addr string, fn func(*banNet.Client) error) error {
	pc := p.conn(addr)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.client == nil {
		c := banNet.NewClient(addr, p.timeout)
		if err := c.Connect(); err != nil {
			return err
		}
		pc.client = c
	}
	if err := fn(pc.client); err != nil {
		_ = pc.client.Close()
		pc.client = nil // 下次重连
		return err
	}
	return nil
}

// Put 转发 PUT 到 addr 节点。
func (p *PeerPool) Put(addr string, key, value []byte) error {
	return p.withClient(addr, func(c *banNet.Client) error { return c.Put(key, value) })
}

// Get 转发 GET 到 addr 节点，返回 value 与是否命中。
func (p *PeerPool) Get(addr string, key []byte) ([]byte, bool, error) {
	var value []byte
	var found bool
	err := p.withClient(addr, func(c *banNet.Client) error {
		v, ok, err := c.Get(key)
		value, found = v, ok
		return err
	})
	return value, found, err
}

// Delete 转发 DELETE 到 addr 节点。
func (p *PeerPool) Delete(addr string, key []byte) error {
	return p.withClient(addr, func(c *banNet.Client) error { return c.Delete(key) })
}

// Close 关闭所有缓存连接。
func (p *PeerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.peers {
		pc.mu.Lock()
		if pc.client != nil {
			_ = pc.client.Close()
			pc.client = nil
		}
		pc.mu.Unlock()
	}
}
