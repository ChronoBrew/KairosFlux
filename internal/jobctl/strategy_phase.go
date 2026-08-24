package jobctl

import "fmt"

// StrategyPhase 是策略生命周期状态机（契约原文，任务书第 5 项）：
//
//	Hypothesis -> Gate -> Candidate -> Paper -> Live/Retired
//
// 状态转移全枚举、非法转移结构化拒绝——不用字符串匹配承载语义，调用方判断
// 转移是否合法只调用 CanTransition/Transition，不自己拼字符串比较。
type StrategyPhase string

const (
	// StrategyHypothesis：刚提出的想法，还没有跑过任何 gate 检验。
	StrategyHypothesis StrategyPhase = "hypothesis"
	// StrategyGate：正在/已经过统计检验关（对应 QuantBrew 侧的 gate2 等
	// 显著性检验）。
	StrategyGate StrategyPhase = "gate"
	// StrategyCandidate：通过检验，进入候选池，等待接入模拟盘。
	StrategyCandidate StrategyPhase = "candidate"
	// StrategyPaper：模拟盘运行中。
	StrategyPaper StrategyPhase = "paper"
	// StrategyLive：已获批实盘（QuantBrew CLAUDE.md 现状：实盘执行走独立
	// qb-exec 层，本状态只是"策略生命周期"里的一个标记，不代表本仓库本身
	// 触发下单）。
	StrategyLive StrategyPhase = "live"
	// StrategyRetired：终态，退役——可能来自 Gate 未通过、Candidate 被否、
	// Paper 表现不佳、或 Live 后被下线，四条路径都允许直接进 Retired（见
	// strategyTransitions）。
	StrategyRetired StrategyPhase = "retired"
)

// strategyTransitions 是全部合法转移的显式枚举表：key 是起点，value 是该
// 起点允许到达的终点集合。用 slice 而不是 map[StrategyPhase]map[...]bool
// 是因为集合很小（每个起点最多 2 个合法终点），slice 线性查找足够、且不
// 涉及"遍历 map 输出"的确定性问题（本函数只做成员判断，不遍历整个表）。
var strategyTransitions = map[StrategyPhase][]StrategyPhase{
	StrategyHypothesis: {StrategyGate},
	StrategyGate:       {StrategyCandidate, StrategyRetired},
	StrategyCandidate:  {StrategyPaper, StrategyRetired},
	StrategyPaper:      {StrategyLive, StrategyRetired},
	StrategyLive:       {StrategyRetired},
	StrategyRetired:    {}, // 终态：Retired 之后不允许再转移到任何状态
}

// IllegalTransitionError 是非法转移的结构化拒绝——字段是类型化的
// StrategyPhase 常量，不是拼出来的错误字符串子串，调用方要做程序化判断
// （比如"这是不是从终态转出"）可以直接比较 From/To，不必解析 Error()。
type IllegalTransitionError struct {
	From StrategyPhase
	To   StrategyPhase
}

func (e *IllegalTransitionError) Error() string {
	return fmt.Sprintf("非法状态转移: %s -> %s", e.From, e.To)
}

// CanTransition 报告 from -> to 是否是合法转移。
func CanTransition(from, to StrategyPhase) bool {
	for _, allowed := range strategyTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// Transition 执行一次状态转移：合法则返回 to；非法返回结构化
// *IllegalTransitionError（调用方按契约"非法转移结构化拒绝"处理，不 panic、
// 不静默放行）。
func Transition(from, to StrategyPhase) (StrategyPhase, error) {
	if !CanTransition(from, to) {
		return from, &IllegalTransitionError{From: from, To: to}
	}
	return to, nil
}
