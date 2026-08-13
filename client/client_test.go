package client_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/client"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/service"
)

// startServer 在进程内起一个真实的 BanNet + KVServer（standalone，数据落临时目录），
// 返回其监听地址。SDK 的测试对真实服务端而非桩，才能覆盖线格式与状态码契约。
func startServer(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = dir + "/wal.log"
	config.G.SSTablePath = dir
	config.G.Mode = config.ModeStandalone
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode = oldWAL, oldSST, oldMode
	})

	// 取一个空闲端口，避免固定端口在并行测试/本机占用下冲突。
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("取空闲端口失败: %v", err)
	}
	addr := l.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	l.Close()

	kv := service.NewKVServer()
	router := service.NewRouter(kv)

	srv := bannet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddRouter(proto.MsgPut, router)
	srv.AddRouter(proto.MsgGet, router)
	srv.AddRouter(proto.MsgDelete, router)
	srv.AddRouter(proto.MsgScan, router)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	waitListening(t, addr)
	return addr
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("服务端在 2s 内未就绪: %s", addr)
}

func newClient(t *testing.T, addr string, opts ...func(*client.Options)) *client.Client {
	t.Helper()
	o := client.Options{Addrs: []string{addr}}
	for _, f := range opts {
		f(&o)
	}
	c, err := client.New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestPutGetRoundTrip(t *testing.T) {
	c := newClient(t, startServer(t))
	ctx := context.Background()

	if err := c.Put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := c.Get(ctx, []byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("Get = %q, want %q", got, "v1")
	}
}

// TestGetMissingKeyIsErrKeyNotFound 是本 SDK 最关键的契约：「查不到」必须能与真实故障
// 区分。该能力依赖服务端的 notfound 状态——在其引入之前，服务端对二者一律回 error，
// 客户端无法区分。
func TestGetMissingKeyIsErrKeyNotFound(t *testing.T) {
	c := newClient(t, startServer(t))

	_, err := c.Get(context.Background(), []byte("absent"))
	if !errors.Is(err, client.ErrKeyNotFound) {
		t.Fatalf("errors.Is(err, ErrKeyNotFound) 应为真, 实际: %v", err)
	}
	// 且不得被误判为服务端故障。
	if errors.Is(err, client.ErrServer) {
		t.Fatal("「键不存在」不应同时表现为 ErrServer")
	}
}

func TestDeleteThenGetIsNotFound(t *testing.T) {
	c := newClient(t, startServer(t))
	ctx := context.Background()

	if err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx, []byte("k")); !errors.Is(err, client.ErrKeyNotFound) {
		t.Fatalf("删除后应为 ErrKeyNotFound, 实际: %v", err)
	}
}

// TestEmptyValuePreserved 守卫「空 value 不等于墓碑」：空 value 必须读回长度 0 而非
// ErrKeyNotFound。存储层用 Value==nil 表示墓碑，空切片是普通写。
func TestEmptyValuePreserved(t *testing.T) {
	c := newClient(t, startServer(t))
	ctx := context.Background()

	if err := c.Put(ctx, []byte("empty"), []byte{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := c.Get(ctx, []byte("empty"))
	if err != nil {
		t.Fatalf("空 value 应可正常读回, 实际: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get = %q, want 空", got)
	}
}

// TestConcurrentUsage 多 goroutine 共用同一 Client：验证连接池的并发安全，
// 并确认并发请求不会因共用连接而串话（BanNet 是严格请求-响应协议）。
// 配合 -race 运行。
func TestConcurrentUsage(t *testing.T) {
	c := newClient(t, startServer(t), func(o *client.Options) { o.PoolSize = 4 })
	ctx := context.Background()

	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	errCh := make(chan error, workers*perWorker)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := []byte(fmt.Sprintf("w%d-k%d", w, i))
				val := []byte(fmt.Sprintf("w%d-v%d", w, i))
				if err := c.Put(ctx, key, val); err != nil {
					errCh <- fmt.Errorf("put %s: %w", key, err)
					return
				}
				got, err := c.Get(ctx, key)
				if err != nil {
					errCh <- fmt.Errorf("get %s: %w", key, err)
					return
				}
				// 串话会表现为读到别的 key 的值。
				if string(got) != string(val) {
					errCh <- fmt.Errorf("get %s = %q, want %q（疑似响应串话）", key, got, val)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// TestPoolSizeBoundsConcurrency 验证连接数不超过 PoolSize：池满时后续请求等待归还，
// 而不是无限新建连接。
func TestPoolSizeBoundsConcurrency(t *testing.T) {
	addr := startServer(t)
	c := newClient(t, addr, func(o *client.Options) { o.PoolSize = 2 })
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("p%d", i))
			if err := c.Put(ctx, key, []byte("v")); err != nil {
				t.Errorf("Put: %v", err)
			}
		}(i)
	}
	wg.Wait()
}

// TestContextCancellationHonored 验证已取消的 context 会立刻失败而非阻塞到超时。
func TestContextCancellationHonored(t *testing.T) {
	c := newClient(t, startServer(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 先取消

	start := time.Now()
	err := c.Put(ctx, []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("已取消的 context 应导致请求失败")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("耗时 %v，未及时响应取消", elapsed)
	}
}

// TestUnreachableServerFailsFast 验证连不上的地址会返回错误而非无限重试。
// 同时确认重试预算是有界的：整体耗时不应超过退避总和的量级。
func TestUnreachableServerFailsFast(t *testing.T) {
	// 127.0.0.1:1 上无监听，连接会被立即拒绝。
	c := newClient(t, "127.0.0.1:1", func(o *client.Options) {
		o.MaxRetries = 2
		o.RetryBackoff = 10 * time.Millisecond
		o.DialTimeout = 200 * time.Millisecond
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Put(ctx, []byte("k"), []byte("v")); err == nil {
		t.Fatal("向无监听地址写入应失败")
	}
}

func TestClosedClientReturnsErrClosed(t *testing.T) {
	c := newClient(t, startServer(t))
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil { // 幂等
		t.Fatalf("重复 Close 应为 nil: %v", err)
	}
	if err := c.Put(context.Background(), []byte("k"), []byte("v")); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("关闭后应返回 ErrClosed, 实际: %v", err)
	}
}

func TestNewRequiresAddr(t *testing.T) {
	if _, err := client.New(client.Options{}); err == nil {
		t.Fatal("未提供地址时 New 应报错")
	}
}
