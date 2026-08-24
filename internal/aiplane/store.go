package aiplane

import (
	"sort"

	"github.com/ChronoBrew/KairosFlux/internal/jobctl"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// Writer 是本包写路径依赖的最小能力：与 jobctl.Store 同形状（结构化类型，
// Go 接口按方法集隐式满足，jobctl.V2Store/jobctl 测试用的 fakeStore 都天然
// 满足本接口，不需要额外的适配层）。之所以在本包重新声明一遍而不是直接
// 引用 jobctl.Store 类型，是因为 aiplane 的写路径调用方（agent/engine）不
// 应该被迫依赖 jobctl 包里"Job 控制面"这个不相关的语义命名——接口属于消费者
// 这条 Go 惯例的直接体现，两个接口结构相同是巧合，不是耦合。
type Writer interface {
	PutVersioned(logicalKey string, payload []byte) (seq uint64, err error)
}

// AsOfReader 是 Context API/证据图谱查询依赖的确定性读能力：给定 as_of
// 时间点读某个逻辑键当时可见的最新版本。由 jobctl.V2Store.GetAsOf 在生产
// 环境实现（见 internal/jobctl/v2store.go 的新增方法）。
type AsOfReader interface {
	GetAsOf(logicalKey string, asOfNanos int64) (payload []byte, found bool, err error)
}

// PrefixLister 是证据图谱查询/Context API"测过哪些因子"依赖的前缀扫描能力：
// 返回某前缀下所有逻辑键的**全部历史版本**（与 service.TemporalStore.
// ListWrites 的审计语义一致，不是"每个逻辑键的最新版本"），调用方用
// LatestPerLogicalKey 折叠成"每个逻辑键的最新版本"。asOfNanos 是写入时间
// 上界，与 AsOfReader 语义一致——两者组合起来保证 Context API 整体是
// as-of 语义（不掺入 as_of 之后发生的写入）。
type PrefixLister interface {
	ListPrefix(prefix string, asOfNanos int64) ([]proto.WriteEnvelopeView, error)
}

// ReadWriter 组合 Writer/AsOfReader/PrefixLister，是本包大多数装配函数
// （SubmitProposal/AdmitFactorEvidence/BuildContext 等）的参数类型——调用方
// 只需要构造一个同时满足三者的对象（生产用 *jobctl.V2Store，测试用本包的
// fakeReadWriter）。
type ReadWriter interface {
	Writer
	AsOfReader
	PrefixLister
}

// 编译期断言：jobctl.V2Store 满足 ReadWriter（GetAsOf/ListPrefix 是本次
// M4 任务新增的方法，PutVersioned 是既有方法）。
var _ ReadWriter = (*jobctl.V2Store)(nil)

// LatestPerLogicalKey 把 PrefixLister 返回的"全部历史版本"折叠成"每个逻辑键
// 的最新版本"（按 Seq 取最大值）。折叠逻辑只在这一处实现——测试用的
// fakeReadWriter 与生产用的 jobctl.V2Store 都不自己折叠，统一交给这个函数，
// 避免"折叠规则在两个地方各写一遍、悄悄分叉"。返回按 LogicalKey 升序排序
// （确定性：Context API/证据图谱查询的输出顺序不依赖 map 迭代序）。
func LatestPerLogicalKey(entries []proto.WriteEnvelopeView) []proto.WriteEnvelopeView {
	best := make(map[string]proto.WriteEnvelopeView, len(entries))
	for _, e := range entries {
		cur, ok := best[e.LogicalKey]
		if !ok || e.Seq > cur.Seq {
			best[e.LogicalKey] = e
		}
	}
	keys := make([]string, 0, len(best))
	for k := range best {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]proto.WriteEnvelopeView, 0, len(keys))
	for _, k := range keys {
		out = append(out, best[k])
	}
	return out
}
