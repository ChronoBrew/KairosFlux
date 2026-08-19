package delivery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClickHouseSink_SendSuccessEncodesJSONEachRowWithKey(t *testing.T) {
	var gotBody []byte
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("query")
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewClickHouseSink("ch", srv.URL, "default", "quote_snapshot", "", "", 0, 0, 0)
	batch := []Record{
		{Key: []byte("quote:2026-08-17:600000"), Value: []byte(`{"code":"600000","close":10.2}`)},
	}
	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatalf("Send 失败: %v", err)
	}

	if !strings.Contains(gotQuery, "INSERT INTO default.quote_snapshot FORMAT JSONEachRow") {
		t.Fatalf("query 不含预期的 INSERT 语句: %q", gotQuery)
	}

	lines := strings.Split(strings.TrimSpace(string(gotBody)), "\n")
	if len(lines) != 1 {
		t.Fatalf("应恰好一行 JSONEachRow，得到 %d 行: %s", len(lines), gotBody)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
		t.Fatalf("行不是合法 JSON: %v", err)
	}
	if row["code"] != "600000" {
		t.Fatalf("原始字段应保留，得到: %v", row)
	}
	if row["_key"] != "quote:2026-08-17:600000" {
		t.Fatalf("_key 字段应为原始 key，得到: %v", row["_key"])
	}

	if h := s.Health(); !h.Healthy {
		t.Fatalf("成功后应健康，得到: %+v", h)
	}
}

func TestClickHouseSink_EmptyBatchNoOp(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	s := NewClickHouseSink("ch", srv.URL, "default", "quote_snapshot", "", "", 0, 0, 0)
	if err := s.Send(context.Background(), nil); err != nil {
		t.Fatalf("空批次应直接返回 nil，得到: %v", err)
	}
	if called {
		t.Fatal("空批次不应发起 HTTP 请求")
	}
}

func TestClickHouseSink_NonJSONValueRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewClickHouseSink("ch", srv.URL, "default", "quote_snapshot", "", "", 0, 0, 0)
	batch := []Record{{Key: []byte("k"), Value: []byte("not-json")}}
	if err := s.Send(context.Background(), batch); err == nil {
		t.Fatal("非 JSON value 应报错")
	}
	// Health() 不随 Send 成败波动（见 Health 的设计说明：避免与 governance.Router
	// 的熔断门槛互锁死），编码失败后 sink 仍报告结构性健康——错误信息在 Reason 里
	// 供观测，调用方应据 Send 的返回错误而非 Health() 判断本次是否成功。
	if h := s.Health(); !h.Healthy {
		t.Fatalf("Health() 应保持结构性健康，得到: %+v", h)
	}
}

// TestClickHouseSink_5xxRetriesThenReturnsErrorWithoutFlippingHealth 验证：下游
// 持续返回 5xx 时，Send 按 maxRetries 重试后仍失败并把错误详情记进 Reason；但
// Health().Healthy 保持 true——是否该继续尝试该 sink 是 governance.Router 的
// 熔断器的职责，不是 Sink 自己的（见 Health 方法的设计说明）。
func TestClickHouseSink_5xxRetriesThenReturnsErrorWithoutFlippingHealth(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("DB::Exception: mock failure"))
	}))
	defer srv.Close()

	s := NewClickHouseSink("ch", srv.URL, "default", "quote_snapshot", "", "", time.Second, 3, 10*time.Millisecond)
	batch := []Record{{Key: []byte("k"), Value: []byte(`{"a":1}`)}}

	err := s.Send(context.Background(), batch)
	if err == nil {
		t.Fatal("持续 5xx 应最终返回错误")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("Send 返回的错误应包含状态码，得到: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("应恰好重试到 maxRetries=3 次，实际调用 %d 次", got)
	}
	h := s.Health()
	if !h.Healthy {
		t.Fatalf("Healthy 不应随 Send 失败翻转，得到: %+v", h)
	}
	if !strings.Contains(h.Reason, "500") {
		t.Fatalf("Reason 仍应记录最近一次错误详情（供观测），得到: %q", h.Reason)
	}
}

// TestClickHouseSink_RecoversAfterTransientFailure 验证：先失败后成功（模拟瞬时故障
// 恢复），最终一次尝试成功即整体成功，Health 恢复为健康。
func TestClickHouseSink_RecoversAfterTransientFailure(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewClickHouseSink("ch", srv.URL, "default", "quote_snapshot", "", "", time.Second, 5, 5*time.Millisecond)
	batch := []Record{{Key: []byte("k"), Value: []byte(`{"a":1}`)}}

	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatalf("第 3 次尝试应成功，得到错误: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("应恰好尝试 3 次（前两次失败+第三次成功），实际 %d 次", got)
	}
	if h := s.Health(); !h.Healthy {
		t.Fatalf("最终成功后应恢复健康，得到: %+v", h)
	}
}

func TestClickHouseSink_BasicAuthSentWhenConfigured(t *testing.T) {
	var gotUser, gotPass string
	var gotOK bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, gotOK = r.BasicAuth()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewClickHouseSink("ch", srv.URL, "default", "quote_snapshot", "alice", "secret", 0, 0, 0)
	batch := []Record{{Key: []byte("k"), Value: []byte(`{"a":1}`)}}
	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatalf("Send 失败: %v", err)
	}
	if !gotOK || gotUser != "alice" || gotPass != "secret" {
		t.Fatalf("应发送 Basic Auth alice/secret，得到 ok=%v user=%q pass=%q", gotOK, gotUser, gotPass)
	}
}
