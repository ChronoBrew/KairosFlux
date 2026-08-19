package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ClickHouseSink 通过 ClickHouse 的 HTTP 接口把一批记录编码为 JSONEachRow、
// POST 到 `INSERT INTO <database>.<table> FORMAT JSONEachRow`。
//
// 幂等/去重不在这一层做：Sink 接口只承诺「重复投递不破坏下游正确性」，本实现
// 靠 ClickHouse 表本身的 ReplacingMergeTree（按业务主键排序，重复插入在合并时
// 折叠）达成——配合上层 offset 机制的 at-least-once 语义，效果是「最终去重」。
// 建表 DDL 与调优注释见 docs/clickhouse-schema.md。
//
// Record.Value 必须是 JSON 对象（ingesthook 的 schema 校验已经保证了这一点）；
// Record.Key 会作为 `_key` 字段一并写入，供审计/人工排查用（不是去重键，去重键
// 是表的 ORDER BY，通常是业务字段如 code+date）。
type ClickHouseSink struct {
	name       string
	addr       string // 如 "http://127.0.0.1:8123"
	database   string
	table      string
	username   string
	password   string
	httpClient *http.Client

	maxRetries   int
	retryBackoff time.Duration

	mu      sync.Mutex
	lastErr string // 最近一次 Send 失败的原因，仅供观测；不影响 Healthy 判定（见 Health 注释）
}

// NewClickHouseSink 构造一个 ClickHouse sink。timeout<=0 取 5s；maxRetries<=0 取 1
// （不重试）；retryBackoff<=0 取 200ms。
func NewClickHouseSink(name, addr, database, table, username, password string, timeout time.Duration, maxRetries int, retryBackoff time.Duration) *ClickHouseSink {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if maxRetries <= 0 {
		maxRetries = 1
	}
	if retryBackoff <= 0 {
		retryBackoff = 200 * time.Millisecond
	}
	return &ClickHouseSink{
		name:         name,
		addr:         addr,
		database:     database,
		table:        table,
		username:     username,
		password:     password,
		httpClient:   &http.Client{Timeout: timeout},
		maxRetries:   maxRetries,
		retryBackoff: retryBackoff,
	}
}

func (s *ClickHouseSink) Name() string { return s.name }

// Health 只反映结构性可用性（是否配置了 addr/database/table），不随 Send 的成败
// 波动——这是刻意的设计决定，不是遗漏。
//
// 教训（故障注入测试挖出的真实 bug，见 clickhouse_router_test.go）：最初实现让
// Healthy 跟随最近一次 Send 结果翻转，结果与 governance.Router 的健康门槛互锁死：
// Router.Send 用 `!sink.Health().Healthy || !breaker.Allow()` 作为是否尝试该 sink
// 的前置条件；一旦 Send 失败一次，Healthy 变 false，Router 从此不再对它调用 Send——
// 包括熔断器已经转入 half-open、本该放行一次探测的时候。探测请求根本发不出去，
// Healthy 永远没有机会被下一次成功的 Send 翻回 true，sink 永久卡在「兜底」，即使
// 下游早已恢复。
//
// 结论：「本次调用是否该重试」是 governance.Breaker 的职责（它有完整的
// closed/open/half-open 状态机），不该由 sink 自己再实现一套会跟 Breaker 打架的
// 影子熔断。Send 失败的详情仍记录在 lastErr，供 Reason 做观测用，但不驱动 Healthy。
func (s *ClickHouseSink) Health() SinkHealth {
	s.mu.Lock()
	reason := s.lastErr
	s.mu.Unlock()

	if s.addr == "" || s.database == "" || s.table == "" {
		return SinkHealth{Healthy: false, Reason: "clickhouse sink 未正确配置：addr/database/table 不能为空"}
	}
	return SinkHealth{Healthy: true, Reason: reason}
}

func (s *ClickHouseSink) recordErr(reason string) {
	s.mu.Lock()
	s.lastErr = reason
	s.mu.Unlock()
}

// Send 把 batch 编码为 JSONEachRow 并 POST 到 ClickHouse，失败按 maxRetries 固定
// 退避重试。整批要么全部成功（一次 INSERT 请求），要么整批失败——不做部分成功的
// 拆分重试，保持与 at-least-once 语义的边界清晰（上层按整批提交/回滚 offset）。
func (s *ClickHouseSink) Send(ctx context.Context, batch []Record) error {
	if len(batch) == 0 {
		return nil
	}

	body, err := encodeJSONEachRow(batch)
	if err != nil {
		s.recordErr(err.Error())
		return fmt.Errorf("clickhouse: 编码批次失败: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				s.recordErr(ctx.Err().Error())
				return ctx.Err()
			case <-time.After(s.retryBackoff):
			}
		}
		if err := s.doInsert(ctx, body); err != nil {
			lastErr = err
			continue
		}
		s.recordErr("")
		return nil
	}
	s.recordErr(lastErr.Error())
	return fmt.Errorf("clickhouse: 插入失败（重试 %d 次后）: %w", s.maxRetries, lastErr)
}

// encodeJSONEachRow 把每条 Record 编码为一行 JSON：原始 Value 的字段 + 注入的
// `_key` 字段（原始 key 的字符串形式，供审计用，不是去重键）。
func encodeJSONEachRow(batch []Record) ([]byte, error) {
	var buf bytes.Buffer
	for _, r := range batch {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(r.Value, &fields); err != nil {
			return nil, fmt.Errorf("记录 %q 的 value 不是 JSON 对象: %w", r.Key, err)
		}
		if fields == nil {
			fields = make(map[string]json.RawMessage)
		}
		keyJSON, err := json.Marshal(string(r.Key))
		if err != nil {
			return nil, err
		}
		fields["_key"] = keyJSON

		line, err := json.Marshal(fields)
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// doInsert 执行一次 HTTP 插入请求。ClickHouse HTTP 接口的约定：查询串放在
// URL 的 `query` 参数里，请求体是 FORMAT 子句声明的数据本身。
func (s *ClickHouseSink) doInsert(ctx context.Context, body []byte) error {
	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", s.database, s.table)
	reqURL := s.addr + "/?query=" + url.QueryEscape(query)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("构造请求失败: %w", err)
	}
	if s.username != "" {
		req.SetBasicAuth(s.username, s.password)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, errBody)
	}
	// 排空响应体，使连接可被 http.Client 的 transport 复用。
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
