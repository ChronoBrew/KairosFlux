package admission

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runLoad 用 workers 个 goroutine 在 dur 内持续打一个「延迟随并发上升」的模拟服务（排队模型：
// 处理延迟 = base×(1+当前并发/10)）。l!=nil 时经限流器准入。返回峰值并发、shed、完成数、平均延迟。
func runLoad(l *Limiter, workers int, dur time.Duration) (maxInflight int, shed, done int64, avgLat time.Duration) {
	const base = time.Millisecond
	var inflight, maxIF, totalLat int64
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				var start time.Time
				if l != nil {
					st, ok := l.Acquire()
					if !ok {
						atomic.AddInt64(&shed, 1)
						continue
					}
					start = st
				}
				cur := atomic.AddInt64(&inflight, 1)
				for {
					m := atomic.LoadInt64(&maxIF)
					if cur <= m || atomic.CompareAndSwapInt64(&maxIF, m, cur) {
						break
					}
				}
				lat := base * time.Duration(1+cur/10) // 并发越高越慢（排队）
				time.Sleep(lat)
				atomic.AddInt64(&inflight, -1)
				if l != nil {
					l.Release(start)
				}
				atomic.AddInt64(&totalLat, int64(lat))
				atomic.AddInt64(&done, 1)
			}
		}()
	}
	wg.Wait()
	if done > 0 {
		avgLat = time.Duration(totalLat / done)
	}
	return int(maxIF), shed, done, avgLat
}

// TestLimiter_ClosedLoopBoundsConcurrencyUnderOverload：闭环压测证明自适应准入的价值——
// 过载下（200 并发打容量有限的服务），限流器把并发/延迟压住并 shed 超额，而无限流则并发/延迟失控。
func TestLimiter_ClosedLoopBoundsConcurrencyUnderOverload(t *testing.T) {
	const workers = 200
	const dur = 500 * time.Millisecond

	maxNo, _, doneNo, latNo := runLoad(nil, workers, dur)

	l := New(Config{InitialLimit: 50, MinLimit: 1, MaxLimit: 2000})
	maxLim, shed, doneLim, latLim := runLoad(l, workers, dur)

	t.Logf("无限流:   峰值并发=%d 完成=%d 平均延迟=%v", maxNo, doneNo, latNo)
	t.Logf("自适应准入: 峰值并发=%d 完成=%d 平均延迟=%v shed=%d 收敛limit=%d", maxLim, doneLim, latLim, shed, l.Limit())

	if maxLim >= maxNo {
		t.Fatalf("限流器应把并发压到无限流之下：%d vs %d", maxLim, maxNo)
	}
	if shed == 0 {
		t.Fatal("过载下应发生 shed")
	}
	if latLim >= latNo {
		t.Fatalf("限流器应把平均延迟压到无限流之下：%v vs %v", latLim, latNo)
	}
}
