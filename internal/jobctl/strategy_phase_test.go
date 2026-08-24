package jobctl

import "testing"

// TestStrategyTransition_LegalEdgesAllAllowed 覆盖状态机原文枚举的每一条
// 合法边：Hypothesis -> Gate -> Candidate -> Paper -> Live/Retired，以及
// Gate/Candidate/Paper 各自到 Retired 的旁路。
func TestStrategyTransition_LegalEdgesAllAllowed(t *testing.T) {
	legal := []struct{ from, to StrategyPhase }{
		{StrategyHypothesis, StrategyGate},
		{StrategyGate, StrategyCandidate},
		{StrategyGate, StrategyRetired},
		{StrategyCandidate, StrategyPaper},
		{StrategyCandidate, StrategyRetired},
		{StrategyPaper, StrategyLive},
		{StrategyPaper, StrategyRetired},
		{StrategyLive, StrategyRetired},
	}
	for _, tc := range legal {
		if !CanTransition(tc.from, tc.to) {
			t.Errorf("应允许 %s -> %s", tc.from, tc.to)
		}
		got, err := Transition(tc.from, tc.to)
		if err != nil {
			t.Errorf("Transition(%s, %s) 返回错误: %v", tc.from, tc.to, err)
		}
		if got != tc.to {
			t.Errorf("Transition(%s, %s) 返回 %s，期望 %s", tc.from, tc.to, got, tc.to)
		}
	}
}

// TestStrategyTransition_RejectsIllegalTransitions 枚举一批非法转移
// （跳级、逆转、Retired 终态转出），断言全部被结构化拒绝——
// *IllegalTransitionError，不是裸字符串错误。
func TestStrategyTransition_RejectsIllegalTransitions(t *testing.T) {
	illegal := []struct{ from, to StrategyPhase }{
		{StrategyHypothesis, StrategyCandidate}, // 跳级：不经过 Gate
		{StrategyHypothesis, StrategyPaper},
		{StrategyHypothesis, StrategyLive},
		{StrategyHypothesis, StrategyRetired}, // Hypothesis 本身没有直接退役的合法边
		{StrategyGate, StrategyLive},          // 跳级
		{StrategyGate, StrategyHypothesis},    // 逆转
		{StrategyCandidate, StrategyGate},     // 逆转
		{StrategyCandidate, StrategyLive},     // 跳级：不经过 Paper
		{StrategyPaper, StrategyCandidate},    // 逆转
		{StrategyPaper, StrategyHypothesis},
		{StrategyLive, StrategyPaper}, // 逆转
		{StrategyLive, StrategyGate},
		{StrategyRetired, StrategyHypothesis}, // 终态转出
		{StrategyRetired, StrategyLive},
		{StrategyRetired, StrategyRetired}, // 终态转到自己也不在允许列表里
	}
	for _, tc := range illegal {
		if CanTransition(tc.from, tc.to) {
			t.Errorf("不应允许 %s -> %s", tc.from, tc.to)
		}
		_, err := Transition(tc.from, tc.to)
		if err == nil {
			t.Fatalf("Transition(%s, %s) 应返回错误", tc.from, tc.to)
		}
		illegalErr, ok := err.(*IllegalTransitionError)
		if !ok {
			t.Fatalf("Transition(%s, %s) 返回的错误类型应为 *IllegalTransitionError，实际 %T", tc.from, tc.to, err)
		}
		if illegalErr.From != tc.from || illegalErr.To != tc.to {
			t.Errorf("IllegalTransitionError 字段应为 %s/%s，实际 %s/%s", tc.from, tc.to, illegalErr.From, illegalErr.To)
		}
	}
}
