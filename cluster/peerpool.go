package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux/client"
)

// peerMaxRetries 是转发到属主节点时的重试次数，取 0（不重试）。
//
// 转发发生在「客户端 → 入口节点 → 属主节点」链路的第二跳，而入口侧的客户端 SDK 自身
// 已带重试。若此处再重试，过载时两级会相乘放大请求量，正好在最不该加压的时刻加压。
// 网络瞬时故障由客户端那一层的重试覆盖即可。
const peerMaxRetries = -1 // client.Options 中负值表示不重试

// PeerPool 维护到各 peer 节点的客户端，供分片转发复用。
//
// 每个 peer 一个 client.Client：KairNet 是请求-响应协议，一条连接必须收到响应才能发下
// 一帧，故对同一 peer 的并发转发由 SDK 内部的连接池以多条连接承担，而非串行排队。
type PeerPool struct {
	mu      sync.Mutex
	timeout time.Duration
	peers   map[string]*kairosflux.Client
}

// NewPeerPool 创建一个转发连接池。timeout 为每次拨号/请求的超时。
func NewPeerPool(timeout time.Duration) *PeerPool {
	return &PeerPool{timeout: timeout, peers: map[string]*kairosflux.Client{}}
}

// client 取（或惰性创建）到该 peer 的客户端。
func (p *PeerPool) client(addr string) (*kairosflux.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.peers[addr]; ok {
		return c, nil
	}
	c, err := kairosflux.New(kairosflux.Options{
		Addrs:          []string{addr},
		DialTimeout:    p.timeout,
		RequestTimeout: p.timeout,
		MaxRetries:     peerMaxRetries,
	})
	if err != nil {
		return nil, err
	}
	p.peers[addr] = c
	return c, nil
}

// ctx 返回一个受 timeout 约束的上下文。
func (p *PeerPool) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), p.timeout)
}

// Put 转发 PUT 到 addr 节点。
func (p *PeerPool) Put(addr string, key, value []byte) error {
	c, err := p.client(addr)
	if err != nil {
		return err
	}
	ctx, cancel := p.ctx()
	defer cancel()
	return c.Put(ctx, key, value)
}

// Get 转发 GET 到 addr 节点，返回 value 与是否命中。
//
// 「未命中」与「属主节点出错」严格区分：仅 ErrKeyNotFound 记为未命中，其余错误原样
// 上抛。二者混同会让上游把远端故障当成「这个 key 不存在」，从而返回错误的空结果。
func (p *PeerPool) Get(addr string, key []byte) ([]byte, bool, error) {
	c, err := p.client(addr)
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := p.ctx()
	defer cancel()

	value, err := c.Get(ctx, key)
	switch {
	case errors.Is(err, kairosflux.ErrKeyNotFound):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	return value, true, nil
}

// Delete 转发 DELETE 到 addr 节点。
func (p *PeerPool) Delete(addr string, key []byte) error {
	c, err := p.client(addr)
	if err != nil {
		return err
	}
	ctx, cancel := p.ctx()
	defer cancel()
	return c.Delete(ctx, key)
}

// Close 关闭所有缓存的客户端。
func (p *PeerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, c := range p.peers {
		_ = c.Close()
		delete(p.peers, addr)
	}
}
