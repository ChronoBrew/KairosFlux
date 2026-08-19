// package delivery_test（外部测试包，而非 delivery）：governance 反向 import
// delivery，本文件又要 import governance，只能放外部测试包，否则与 delivery
// 内部测试文件互相 import 成环（governance -> delivery -> [本文件] -> governance）。
package delivery_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/NeverENG/BanDB/service/delivery"
	"github.com/NeverENG/BanDB/service/delivery/governance"
)

// fileRecord 与 delivery.FileSink 内部未导出的同名类型形状一致（key/value 两个
// []byte 字段，json 标签 "key"/"value"），供本测试直接解析 FileSink 写出的 JSONL——
// 外部测试包访问不到未导出类型，这里按已知的落盘格式重新声明一份只读用的镜像。
type fileRecord struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// TestClickHouseRouter_FaultInjection_FallsBackToFileThenRecovers 是 ClickHouse 主 +
// FileSink 兜底路由的故障注入测试：用真实的 ClickHouseSink（对一个会先返回 5xx、
// 之后恢复 200 的 mock HTTP 服务器）+ 真实的 FileSink（写临时文件），验证：
//  1. ClickHouse mock 返回 5xx 期间，记录被投递到 FileSink（可在文件里读到）；
//  2. ClickHouse mock 恢复健康后，后续投递自动切回 ClickHouse（不再落文件）。
//
// 这是「投递路由 = ClickHouse 主 + FileSink 兜底,CH 不健康自动降级落文件、
// 恢复后继续」这条验收标准的直接实现。
func TestClickHouseRouter_FaultInjection_FallsBackToFileThenRecovers(t *testing.T) {
	var chHealthy atomic.Bool // 由测试主动控制 mock ClickHouse 的行为
	var chCalls, chFailedCalls atomic.Int32

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chCalls.Add(1)
		if !chHealthy.Load() {
			chFailedCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("DB::Exception: mock ClickHouse down"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mock.Close()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "fallback.jsonl")

	chSink := NewClickHouseSink("clickhouse", mock.URL, "default", "quote_snapshot", "", "",
		200*time.Millisecond, 1 /* maxRetries=1：不在 sink 内部重试，让 Router 的熔断/降级逻辑接管 */, 0)
	fileSink, err := NewFileSink("file", filePath)
	if err != nil {
		t.Fatalf("构造 FileSink 失败: %v", err)
	}
	defer fileSink.Close()

	// failThreshold=1：ClickHouse 第一次失败即触发熔断，立即降级，不必等多次失败——
	// 生产上通常不会这么激进，但故障注入测试要的是「确定性地立刻观测到降级」。
	router := governance.NewPriorityRouter([]Sink{chSink, fileSink}, 1, time.Minute)

	batch := func(key string) []Record {
		return []Record{{Key: []byte(key), Value: []byte(`{"code":"600000","close":10.2}`)}}
	}

	// 阶段一：ClickHouse 故障，写入应降级落文件。
	chHealthy.Store(false)
	if err := router.Send(context.Background(), batch("k1")); err != nil {
		t.Fatalf("ClickHouse 故障期间应成功降级到 FileSink，得到错误: %v", err)
	}
	if got := chFailedCalls.Load(); got != 1 {
		t.Fatalf("应恰好尝试 ClickHouse 一次并失败，得到 %d 次", got)
	}
	assertFileContainsKey(t, filePath, "k1")

	// 再发一批：ClickHouse 仍不健康（熔断器已 open，openTimeout=1 分钟远未到期），
	// Router 应直接跳过 ClickHouse（不再尝试），继续走文件。
	if err := router.Send(context.Background(), batch("k2")); err != nil {
		t.Fatalf("熔断期间应继续走 FileSink，得到错误: %v", err)
	}
	if got := chCalls.Load(); got != 1 {
		t.Fatalf("熔断 open 期间不应再调用 ClickHouse，累计调用应仍为 1，得到 %d", got)
	}
	assertFileContainsKey(t, filePath, "k2")

	// 阶段二：ClickHouse 恢复健康。熔断器 openTimeout 未到期，用一个新的 Router
	// （openTimeout 极短）模拟"运维介入/等待恢复窗口后重试"更贴近真实时间流逝，
	// 而不是在测试里等一分钟——故这里改用短 openTimeout 复现半开探测转正常。
	shortRouter := governance.NewPriorityRouter([]Sink{chSink, fileSink}, 1, 10*time.Millisecond)
	chHealthy.Store(false)
	if err := shortRouter.Send(context.Background(), batch("k3")); err != nil {
		t.Fatalf("第一次故障应降级成功，得到错误: %v", err)
	}
	assertFileContainsKey(t, filePath, "k3")

	// 等熔断器进入 half-open 窗口，同时让 mock ClickHouse 恢复健康。
	time.Sleep(20 * time.Millisecond)
	chHealthy.Store(true)
	if err := shortRouter.Send(context.Background(), batch("k4")); err != nil {
		t.Fatalf("ClickHouse 恢复后应能成功投递（半开探测成功），得到错误: %v", err)
	}

	// 验证「恢复后继续」：紧接着再投递一批，应直接走 ClickHouse 主链路（熔断器已
	// 因上一次成功探测转回 closed），不应再落文件——k5 不应出现在文件里。
	if err := shortRouter.Send(context.Background(), batch("k5")); err != nil {
		t.Fatalf("恢复后的常规投递应成功，得到错误: %v", err)
	}
	assertFileDoesNotContainKey(t, filePath, "k5")
}

func assertFileContainsKey(t *testing.T, path, key string) {
	t.Helper()
	if !fileContainsKey(t, path, key) {
		t.Fatalf("期望文件 %s 包含 key %q，未找到", path, key)
	}
}

func assertFileDoesNotContainKey(t *testing.T, path, key string) {
	t.Helper()
	if fileContainsKey(t, path, key) {
		t.Fatalf("期望文件 %s 不包含 key %q（应已走 ClickHouse 主链路），但找到了", path, key)
	}
}

func fileContainsKey(t *testing.T, path, key string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec fileRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		if string(rec.Key) == key {
			return true
		}
	}
	return false
}
