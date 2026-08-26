package jobctl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ChronoBrew/KairosFlux/proto"
)

// fakeStore 是 Store 的内存实现，语义与 service.TemporalStore 对齐（每次
// PutVersioned 追加一条新版本、seq 从 1 严格递增，GetLatest 返回最新版本），
// 只为单元测试而存在——不需要为了验证"reconcile 决策幂等"这件事真的起一个
// TCP 服务端跑一万次（那件事由 v2store_integration_test.go 单独、少量地
// 覆盖：证明 V2Store 确实把这套决策接到了真实的 PUT_VERSIONED/GET_AS_OF
// opcode 上）。
//
// 用 map 存版本历史不违反"禁止依赖 map 迭代序"——本类型从不遍历 versions
// 这个 map 产出任何结果，只做单键查找/追加，遍历顺序从未参与任何决策。
type fakeStore struct {
	mu       sync.Mutex
	versions map[string][][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{versions: make(map[string][][]byte)}
}

func (s *fakeStore) PutVersioned(logicalKey string, payload []byte, source string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]byte(nil), payload...)
	s.versions[logicalKey] = append(s.versions[logicalKey], cp)
	return uint64(len(s.versions[logicalKey])), nil
}

func (s *fakeStore) GetLatest(logicalKey string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := s.versions[logicalKey]
	if len(vs) == 0 {
		return nil, false, nil
	}
	return vs[len(vs)-1], true, nil
}

// versionCount 返回某逻辑键累计写入过的版本数（不是"当前值"，是历史长度），
// 供测试断言"账本没有随重跑次数膨胀"。
func (s *fakeStore) versionCount(logicalKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.versions[logicalKey])
}

// ListVersions 实现 Store 接口：按版本序（seq 升序）返回完整历史，语义与
// service.TemporalStore 的 LIST_VERSIONS 一致。供启动恢复扫描测试
// （recovery_test.go）读取事件账本。
func (s *fakeStore) ListVersions(logicalKey string) ([]proto.VersionEntryView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := s.versions[logicalKey]
	out := make([]proto.VersionEntryView, 0, len(vs))
	for i, payload := range vs {
		out = append(out, proto.VersionEntryView{Seq: uint64(i + 1), Payload: payload})
	}
	return out, nil
}

// fixedTestTime 是测试用的固定时刻，避免每个测试文件各自拼一个
// time.Date(...) 字面量。
func fixedTestTime() time.Time {
	return time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
}

// fakeClock 是 Clock 的测试实现：完全由测试代码驱动，不含真实 time.Sleep。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// countingExecutor 是 Executor 的测试实现：不 fork 任何真实子进程，只记录
// 被调用的次数与参数，返回预先配置好的结果——一万次重跑幂等性测试关心的是
// "reconcile 决策层是否幂等"，不是"os/exec 本身是否幂等"（那由
// executor_test.go 单独覆盖真实 CmdExecutor）。
type countingExecutor struct {
	mu       sync.Mutex
	calls    int
	results  []ExecResult // 按调用顺序取；用完最后一个重复返回
	fixed    ExecResult
	useFixed bool
}

func newCountingExecutor(result ExecResult) *countingExecutor {
	return &countingExecutor{fixed: result, useFixed: true}
}

func (e *countingExecutor) Run(ctx context.Context, spec JobSpec) ExecResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.useFixed {
		return e.fixed
	}
	if len(e.results) == 0 {
		panic(fmt.Sprintf("countingExecutor: 第 %d 次调用但没有预设结果", e.calls))
	}
	idx := e.calls - 1
	if idx >= len(e.results) {
		idx = len(e.results) - 1
	}
	return e.results[idx]
}

func (e *countingExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// nullAlertSink 什么都不做，测试里不想看到告警日志噪音时用它替换
// LogAlertSink。
type nullAlertSink struct{ alerts []Event }

func (s *nullAlertSink) Alert(jobName string, ev Event) {
	s.alerts = append(s.alerts, ev)
}
