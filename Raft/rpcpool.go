package Raft

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

// rpcPool 是节点间 net/rpc 客户端连接池：每个对端地址缓存并复用一个 *rpc.Client。
//
// 为什么可行且划算：net/rpc 的 Client 本身并发安全（内部按 seq 号在一条连接上多路复用
// 并发调用），故一个缓存 client 就能被所有调用方/所有 Raft 组并发复用——省掉每次调用
// 的 TCP 三次握手 + 连接建立 + 关闭。断连（ErrShutdown/网络错误）时丢弃该 client 并重拨。
//
// Multi-Raft 下同一节点上多个组拨向相同的对端地址集，共享一个池即让每对端只维持一条连接、
// 服务全部组的 RPC。
type rpcPool struct {
	mu      sync.Mutex
	clients map[string]*rpc.Client
	timeout time.Duration
}

func newRPCPool(timeout time.Duration) *rpcPool {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &rpcPool{clients: make(map[string]*rpc.Client), timeout: timeout}
}

// get 取（或建）到 addr 的客户端。
func (p *rpcPool) get(addr string) (*rpc.Client, error) {
	p.mu.Lock()
	if c, ok := p.clients[addr]; ok {
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()

	// 带超时拨号，避免不可达对端把调用挂死；rpc.NewClient 在此连接上跑协议。
	conn, err := net.DialTimeout("tcp", addr, p.timeout)
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)

	p.mu.Lock()
	// 竞态下他人可能已放入一个，复用之，关掉本次多建的。
	if existing, ok := p.clients[addr]; ok {
		p.mu.Unlock()
		c.Close()
		return existing, nil
	}
	p.clients[addr] = c
	p.mu.Unlock()
	return c, nil
}

// drop 丢弃并关闭某地址当前缓存的 client（仅当仍是同一个，避免误删他人重建的）。
func (p *rpcPool) drop(addr string, c *rpc.Client) {
	p.mu.Lock()
	if cur, ok := p.clients[addr]; ok && cur == c {
		delete(p.clients, addr)
		c.Close()
	}
	p.mu.Unlock()
}

// call 复用连接发起一次 RPC；若因连接失效（ErrShutdown/网络错误）失败，丢弃并重拨重试一次。
// 注意：应用级失败（如"not leader"）由 reply 承载，不会以 Call error 返回，故不会误重试。
func (p *rpcPool) call(addr, method string, args, reply any) error {
	c, err := p.get(addr)
	if err != nil {
		return err
	}
	err = c.Call(method, args, reply)
	if err == rpc.ErrShutdown {
		// 缓存连接已关闭：丢弃重拨重试一次（这类失败是纯传输层的，安全重试）。
		p.drop(addr, c)
		c2, err2 := p.get(addr)
		if err2 != nil {
			return err2
		}
		return c2.Call(method, args, reply)
	}
	if err != nil {
		// 其它错误也可能是连接半死；丢弃以便下次重建，但不自动重试（可能非幂等）。
		p.drop(addr, c)
	}
	return err
}

// callTimeout 复用连接发起一次带超时的 RPC：超时返回错误但不丢弃连接（连接仍可用，只是本次
// 调用慢——net/rpc 在同一连接上多路复用，慢调用不影响其它调用）。用于 Propose 这类可能在
// leader 侧等待提交而变慢的调用，避免挂死调用方。
func (p *rpcPool) callTimeout(addr, method string, args, reply any, timeout time.Duration) error {
	c, err := p.get(addr)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error == rpc.ErrShutdown {
			p.drop(addr, c)
		}
		return call.Error
	case <-time.After(timeout):
		return errRPCTimeout
	}
}

// errRPCTimeout 表示 RPC 调用超时。
var errRPCTimeout = &timeoutError{}

type timeoutError struct{}

func (*timeoutError) Error() string { return "rpc call timeout" }

// defaultRPCPool 是 Raft 节点间 RPC 的共享连接池：所有 Send*/Propose 复用之，
// 使每个对端地址只维持一条被多路复用的连接。
var defaultRPCPool = newRPCPool(5 * time.Second)

// PooledCall 用共享连接池对 addr 发起一次 RPC。导出以便同进程内其它节点间 RPC（如 shardkv
// 的副本读 ShardRead.Get）复用同一批多路复用连接——net/rpc 一条连接可服务多个已注册服务，
// 故读 RPC 与 Raft RPC 共享每对端那一条连接。
func PooledCall(addr, method string, args, reply any) error {
	return defaultRPCPool.call(addr, method, args, reply)
}
