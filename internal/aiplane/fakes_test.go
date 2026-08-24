package aiplane

import (
	"sort"
	"sync"

	"github.com/ChronoBrew/KairosFlux/proto"
)

// fakeReadWriter 是 ReadWriter 的内存实现，语义与生产环境的 jobctl.V2Store
// （backed by service.TemporalStore）刻意保持一致：
//   - PutVersioned 永不覆盖，每次调用产生一个新的递增 seq；
//   - GetAsOf 返回"写入时刻（这里用调用方显式传入的 fakeClock 逻辑时钟，
//     不是真实墙钟）<= asOfNanos"的版本中 seq 最大的一条；
//   - ListPrefix 返回该前缀下**全部历史版本**（不折叠），与真实
//     LIST_WRITES 的审计语义一致——折叠交给 LatestPerLogicalKey（生产/
//     测试共用同一份折叠实现，见 store.go）。
//
// 与 internal/jobctl/fakes_test.go 的 fakeStore 同一模式（内存 map + 互斥
// 锁），本包独立维护一份而不是导出复用 jobctl 内部的 fakeStore：那是
// jobctl 包的测试专用类型（未导出），且缺少 WriteNanos/前缀扫描能力，直接
// 复用需要先改 jobctl 测试代码，超出本次任务"新增能力不动无关代码"的范围。
type fakeReadWriter struct {
	mu    sync.Mutex
	clock int64 // 逻辑写入时钟：每次 PutVersioned 自增 1，不是真实纳秒
	// versions[logicalKey] 是该逻辑键的全部历史版本，按写入顺序追加。
	versions map[string][]fakeVersion
}

type fakeVersion struct {
	seq        uint64
	writeNanos int64
	payload    []byte
}

func newFakeReadWriter() *fakeReadWriter {
	return &fakeReadWriter{versions: make(map[string][]fakeVersion)}
}

func (s *fakeReadWriter) PutVersioned(logicalKey string, payload []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock++
	seq := uint64(len(s.versions[logicalKey]) + 1)
	cp := append([]byte(nil), payload...)
	s.versions[logicalKey] = append(s.versions[logicalKey], fakeVersion{seq: seq, writeNanos: s.clock, payload: cp})
	return seq, nil
}

func (s *fakeReadWriter) GetAsOf(logicalKey string, asOfNanos int64) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best fakeVersion
	found := false
	for _, v := range s.versions[logicalKey] {
		if v.writeNanos <= asOfNanos && (!found || v.seq > best.seq) {
			best, found = v, true
		}
	}
	if !found {
		return nil, false, nil
	}
	return best.payload, true, nil
}

func (s *fakeReadWriter) ListPrefix(prefix string, asOfNanos int64) ([]proto.WriteEnvelopeView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []proto.WriteEnvelopeView
	for key, vs := range s.versions {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		for _, v := range vs {
			if v.writeNanos > asOfNanos {
				continue
			}
			out = append(out, proto.WriteEnvelopeView{
				LogicalKey: key,
				Seq:        v.seq,
				WriteNanos: v.writeNanos,
				Payload:    v.payload,
			})
		}
	}
	// map 迭代序不确定：返回前按 (LogicalKey, Seq) 排序，与生产
	// service.TemporalStore.ListWrites 的输出顺序约定一致（该函数文档承诺
	// "按 (LogicalKey,Seq) 升序排好"）。
	sort.Slice(out, func(i, j int) bool {
		if out[i].LogicalKey != out[j].LogicalKey {
			return out[i].LogicalKey < out[j].LogicalKey
		}
		return out[i].Seq < out[j].Seq
	})
	return out, nil
}

// currentClock 是测试断言用的辅助方法：返回当前逻辑写入时钟值——供测试
// 精确构造"as_of 恰好在某次写入之前"的边界，而不是用一个随意选的大数（那样
// 会把测试意图想排除的"未来写入"也算进 as_of 可见范围，起不到验证效果）。
func (s *fakeReadWriter) currentClock() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clock
}

// versionCount 是测试断言用的辅助方法：某逻辑键当前累计了多少条版本。
func (s *fakeReadWriter) versionCount(logicalKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.versions[logicalKey])
}

// putRaw 是测试装配用的辅助方法：绕开 WriteAsAgent/WriteAsEngine 的权限
// 判断，直接以"引擎/系统初始化"身份写入任意键——用于测试里预置
// strategy:index 等既有对象，不代表本包认可"任意直写"这个操作模式（生产
// 代码路径必须经过 WriteAsAgent/WriteAsEngine）。
func (s *fakeReadWriter) putRaw(logicalKey string, payload []byte) {
	_, _ = s.PutVersioned(logicalKey, payload)
}
