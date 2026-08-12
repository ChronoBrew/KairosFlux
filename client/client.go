// Package client 是 BanDB 的 Go 客户端 SDK。
//
// 它与 cmd/ban-cli 里的交互式演示客户端不同：后者是单连接、无超时、无重试的示例代码，
// 不可被引用（package main）。本包提供可引用的库，并补齐生产使用所必需的三件事：
//
//   - 连接池：BanNet 是严格的请求-响应协议，一条连接必须收到响应才能发下一帧，
//     故并发只能由多条连接提供；池化同时免去每次请求的 TCP 握手。
//   - context：超时与取消经 context 传入，映射为连接的读写 deadline。
//   - 有界重试：服务端在准入过载时专门回 overloaded 状态以示「可重试」。SDK 据此对可
//     重试错误做指数退避，次数受 MaxRetries 与 context 截止时间双重约束。
//
// 错误一律以哨兵形式暴露（见 errors.go），调用方用 errors.Is 判别；其中
// ErrKeyNotFound 是正常查询结果而非故障。
//
// 用法：
//
//	c, err := client.New(client.Options{Addrs: []string{"127.0.0.1:8080"}})
//	if err != nil { return err }
//	defer c.Close()
//
//	if err := c.Put(ctx, []byte("k"), []byte("v")); err != nil { return err }
//	v, err := c.Get(ctx, []byte("k"))
//	switch {
//	case errors.Is(err, client.ErrKeyNotFound): // 正常的「查不到」
//	case err != nil:                            // 真实故障
//	}
package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeverENG/BanDB/pkg/predicate"
	"github.com/NeverENG/BanDB/pkg/proto"
)

// 默认参数。均可经 Options 覆盖。
const (
	defaultDialTimeout    = 5 * time.Second
	defaultRequestTimeout = 5 * time.Second
	defaultPoolSize       = 8
	defaultMaxRetries     = 2
	defaultRetryBackoff   = 20 * time.Millisecond
)

// Options 是客户端构造参数。零值字段一律取默认值，故 Options{Addrs: ...} 即可用。
type Options struct {
	// Addrs 是服务端地址列表，至少一个。多个地址时按轮询选择，使请求分摊到多个节点。
	Addrs []string

	// DialTimeout 建立连接的超时，默认 5s。
	DialTimeout time.Duration

	// RequestTimeout 单次请求的超时，在调用方未给 context 设置截止时间时生效，默认 5s。
	// 调用方 context 的截止时间更早时以其为准。
	RequestTimeout time.Duration

	// PoolSize 连接池上限，默认 8。它同时决定客户端侧的最大并发请求数。
	PoolSize int

	// MaxRetries 可重试错误的额外重试次数（不含首次尝试），默认 2；负值表示不重试。
	MaxRetries int

	// RetryBackoff 首次重试前的等待，其后指数增长，默认 20ms。
	RetryBackoff time.Duration
}

func (o *Options) applyDefaults() {
	if o.DialTimeout <= 0 {
		o.DialTimeout = defaultDialTimeout
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = defaultRequestTimeout
	}
	if o.PoolSize <= 0 {
		o.PoolSize = defaultPoolSize
	}
	if o.MaxRetries == 0 {
		o.MaxRetries = defaultMaxRetries
	}
	if o.MaxRetries < 0 {
		o.MaxRetries = 0
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = defaultRetryBackoff
	}
}

// Client 是并发安全的 BanDB 客户端。可被多个 goroutine 共用，应作为长生命周期对象复用
// 而非每次请求新建——新建会丢掉连接池的全部收益。
type Client struct {
	opts Options

	// idle 是空闲连接池。以带缓冲 channel 实现：取连接即接收，归还即发送，
	// 容量即池上限，天然提供「并发请求数不超过 PoolSize」的背压。
	idle chan *conn

	// live 记录已建立的连接数，用于在池未满时按需新建而非预先全部建立。
	live atomic.Int64

	rr     atomic.Uint64 // 轮询地址的游标
	closed atomic.Bool
	mu     sync.Mutex // 仅保护 Close 期间的连接排空
}

// New 创建客户端。它不立即建立连接（惰性建连），故不会因服务端暂时不可达而失败；
// 首个请求才会触发拨号。
func New(opts Options) (*Client, error) {
	if len(opts.Addrs) == 0 {
		return nil, errors.New("bandb: Options.Addrs 至少需要一个地址")
	}
	opts.applyDefaults()
	return &Client{
		opts: opts,
		idle: make(chan *conn, opts.PoolSize),
	}, nil
}

// Close 关闭客户端与池中全部连接。Close 后的调用一律返回 ErrClosed。可重复调用。
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		select {
		case cn := <-c.idle:
			_ = cn.close()
			c.live.Add(-1)
		default:
			return nil
		}
	}
}

// nextAddr 轮询取下一个地址，使请求在多节点间分摊。
func (c *Client) nextAddr() string {
	if len(c.opts.Addrs) == 1 {
		return c.opts.Addrs[0]
	}
	i := c.rr.Add(1) - 1
	return c.opts.Addrs[i%uint64(len(c.opts.Addrs))]
}

// acquire 取一条可用连接：优先复用空闲连接，池未满则新建，否则等待归还
// （受 context 约束，不会无限等待）。
func (c *Client) acquire(ctx context.Context) (*conn, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	select {
	case cn := <-c.idle:
		return cn, nil
	default:
	}

	// 池未满：按需新建。CompareAndSwap 循环保证并发下不超过 PoolSize。
	for {
		n := c.live.Load()
		if n >= int64(c.opts.PoolSize) {
			break
		}
		if c.live.CompareAndSwap(n, n+1) {
			cn, err := dial(c.nextAddr(), c.opts.DialTimeout)
			if err != nil {
				c.live.Add(-1)
				return nil, err
			}
			return cn, nil
		}
	}

	// 池已满：等待归还或 context 结束。
	select {
	case cn := <-c.idle:
		return cn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// release 归还连接。已损坏的连接直接关闭而不放回，避免残留响应与后续请求串话。
func (c *Client) release(cn *conn) {
	if cn.broken || c.closed.Load() {
		_ = cn.close()
		c.live.Add(-1)
		return
	}
	select {
	case c.idle <- cn:
	default: // 池已满（Close 竞态等）：直接关闭
		_ = cn.close()
		c.live.Add(-1)
	}
}

// deadline 由 context 与 RequestTimeout 共同决定，取更早者。
func (c *Client) deadline(ctx context.Context) time.Time {
	d := time.Now().Add(c.opts.RequestTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(d) {
		return ctxDeadline
	}
	return d
}

// do 执行一次请求，并对可重试错误做指数退避重试。
// 每次尝试都重新取连接：上一次失败可能正是该连接不可用所致。
func (c *Client) do(ctx context.Context, msgID string, data []byte) ([]byte, error) {
	backoff := c.opts.RetryBackoff
	var lastErr error

	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			// 退避期间也要响应 context 取消，且退避不得越过截止时间。
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w (最后一次错误: %v)", ctx.Err(), lastErr)
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		payload, err := c.attempt(ctx, msgID, data)
		if err == nil {
			return payload, nil
		}
		lastErr = err
		if !retryable(err) {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w (最后一次错误: %v)", ctx.Err(), lastErr)
		}
	}
	return nil, lastErr
}

// attempt 是单次尝试：取连接 → 一次往返 → 解析状态 → 归还连接。
func (c *Client) attempt(ctx context.Context, msgID string, data []byte) ([]byte, error) {
	// 先判 context：连接池有空位时 acquire 会走无阻塞快路径，若不在此处检查，
	// 一个已取消（或已超时）的 context 仍会照常发出请求。
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cn, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.release(cn)

	// 让请求中途的取消也能生效：阻塞在 read/write 的系统调用无法被 context 直接打断，
	// 唯一可移植的办法是把连接 deadline 置为「立即」，使其带 timeout 错误返回。
	// 此时响应可能只读了一半，故连接标记为损坏、不再放回池中。
	if ctx.Done() != nil {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				cn.broken = true
				_ = cn.nc.SetDeadline(time.Now())
			case <-stop:
			}
		}()
	}

	_, respData, err := cn.roundTrip(msgID, data, c.deadline(ctx))
	if err != nil {
		// 若是被上面的看门狗打断，向调用方报告 context 的原因而非 I/O 超时。
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	status, rest, err := parseStatus(respData)
	if err != nil {
		cn.broken = true // 无法解析响应：该连接的后续字节不可信
		return nil, err
	}
	if err := statusError(status); err != nil {
		return nil, err
	}
	return rest, nil
}

// Put 写入键值。返回 nil 时数据已在服务端 fsync 落盘。
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	data := make([]byte, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(value)))
	copy(data[8:], key)
	copy(data[8+len(key):], value)

	_, err := c.do(ctx, proto.MsgPut, data)
	return err
}

// Get 读取键值。key 不存在时返回 ErrKeyNotFound——那是正常结果，不是故障。
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	data := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(key)))
	copy(data[4:], key)

	rest, err := c.do(ctx, proto.MsgGet, data)
	if err != nil {
		return nil, err
	}
	if len(rest) < 4 {
		return nil, fmt.Errorf("%w: GET 响应缺少 value 长度", ErrProtocol)
	}
	n := int(binary.LittleEndian.Uint32(rest[0:4]))
	if len(rest) < 4+n {
		return nil, fmt.Errorf("%w: GET 响应 value 长度 %d 超出负载", ErrProtocol, n)
	}
	// 拷贝而非切片引用：rest 指向本次响应的整块缓冲，直接返回会让其常驻。
	out := make([]byte, n)
	copy(out, rest[4:4+n])
	return out, nil
}

// Delete 删除键。删除是幂等盲写：删除不存在的 key 同样返回 nil。
func (c *Client) Delete(ctx context.Context, key []byte) error {
	data := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(key)))
	copy(data[4:], key)

	_, err := c.do(ctx, proto.MsgDelete, data)
	return err
}

// Entry 是 Scan 返回的一条键值。
type Entry struct {
	Key   []byte
	Value []byte
}

// Scan 在 [start,end] 闭区间内按谓词筛选并返回命中条目，筛选在服务端完成，
// 只回传命中部分。start/end 为空分别表示下界/上界不限。
func (c *Client) Scan(ctx context.Context, start, end []byte, pred predicate.Predicate) ([]Entry, error) {
	req := proto.ScanRequest{Start: start, End: end, Pred: pred}
	rest, err := c.do(ctx, proto.MsgScan, proto.EncodeScanRequest(req))
	if err != nil {
		return nil, err
	}

	// Scan 响应的状态字段已由 do 剥离并校验，但 DecodeScanResponse 期望完整负载，
	// 故此处重新拼回状态头。保持与服务端编码函数成对使用，避免各写一份解析。
	full := append(statusPrefix(proto.StatusOK), rest...)
	_, entries, err := proto.DecodeScanResponse(full)
	if err != nil {
		return nil, fmt.Errorf("%w: 解析 SCAN 响应失败: %v", ErrProtocol, err)
	}
	out := make([]Entry, len(entries))
	for i, e := range entries {
		out[i] = Entry{Key: e.Key, Value: e.Value}
	}
	return out, nil
}

// statusPrefix 编码 [statusLen u8][status bytes]。
func statusPrefix(status string) []byte {
	buf := make([]byte, 1+len(status))
	buf[0] = byte(len(status))
	copy(buf[1:], status)
	return buf
}
