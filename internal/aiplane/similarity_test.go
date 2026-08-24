package aiplane

import (
	"math"
	"testing"
)

func TestPearsonCorrelation_PerfectPositiveAndNegative(t *testing.T) {
	a := []float64{0.01, 0.02, -0.01, 0.03, 0.00}
	b := []float64{0.02, 0.04, -0.02, 0.06, 0.00} // b = 2*a，完全正相关
	corr, err := PearsonCorrelation(a, b)
	if err != nil {
		t.Fatalf("计算失败: %v", err)
	}
	if math.Abs(corr-1.0) > 1e-9 {
		t.Fatalf("完全正相关应=1.0，实际 %v", corr)
	}

	c := []float64{-0.02, -0.04, 0.02, -0.06, 0.00} // c = -b，完全负相关
	corr2, err := PearsonCorrelation(a, c)
	if err != nil {
		t.Fatalf("计算失败: %v", err)
	}
	if math.Abs(corr2+1.0) > 1e-9 {
		t.Fatalf("完全负相关应=-1.0，实际 %v", corr2)
	}
}

func TestPearsonCorrelation_RejectsMismatchedLengthAndConstantSeries(t *testing.T) {
	if _, err := PearsonCorrelation([]float64{1, 2}, []float64{1}); err == nil {
		t.Fatal("长度不一致应报错")
	}
	if _, err := PearsonCorrelation([]float64{1}, []float64{1}); err == nil {
		t.Fatal("长度<2应报错")
	}
	if _, err := PearsonCorrelation([]float64{1, 1, 1}, []float64{1, 2, 3}); err == nil {
		t.Fatal("常数序列（方差为0）应报错")
	}
}

func TestSimilarityKey_OrderIndependent(t *testing.T) {
	if SimilarityKey("amihud_illiq", "turnover") != SimilarityKey("turnover", "amihud_illiq") {
		t.Fatal("SimilarityKey 应与调用方传参顺序无关（无向边）")
	}
}

// TestAdmitFactorEvidence_RejectsFactorWithCorrelationAboveThreshold 是任务书
// 验收标准第二条的黄金测试："相似因子（相关性>0.7）无法进入证据关"。
func TestAdmitFactorEvidence_RejectsFactorWithCorrelationAboveThreshold(t *testing.T) {
	store := newFakeReadWriter()
	const asOf = int64(1000)

	// factorA 先进证据关。
	if err := AdmitFactorEvidence(store, asOf, "factorA", "expA"); err != nil {
		t.Fatalf("factorA 首次进证据关不应失败: %v", err)
	}

	// factorA/factorB 收益序列高度相关（corr > 0.7）。
	returnsA := []float64{0.01, 0.02, -0.01, 0.03, 0.015, -0.02, 0.04}
	returnsB := []float64{0.011, 0.021, -0.009, 0.031, 0.014, -0.019, 0.041} // 与 A 几乎同向
	edge, err := RecordSimilarity(store, "factorA", "factorB", returnsA, returnsB)
	if err != nil {
		t.Fatalf("记录相似度失败: %v", err)
	}
	if edge.Correlation <= SimilarityThreshold {
		t.Fatalf("测试前提不成立：构造的收益序列相关性应 > %v，实际 %v", SimilarityThreshold, edge.Correlation)
	}
	if !edge.SuspectDuplicate {
		t.Fatal("相关性超阈值应标记 SuspectDuplicate=true")
	}

	err = AdmitFactorEvidence(store, asOf, "factorB", "expB")
	if err == nil {
		t.Fatal("factorB 与已入证据关的 factorA 相关性超阈值，应被拒绝进证据关")
	}
	dup, ok := err.(*SuspectDuplicateError)
	if !ok {
		t.Fatalf("错误类型应为 *SuspectDuplicateError，实际 %T(%v)", err, err)
	}
	if dup.Factor != "factorB" || dup.ConflictingFactor != "factorA" {
		t.Fatalf("拒绝原因字段不符: %+v", dup)
	}
	if dup.Threshold != SimilarityThreshold {
		t.Fatalf("Threshold 字段应为 %v，实际 %v", SimilarityThreshold, dup.Threshold)
	}

	// 确认 factorB 确实没有被写入证据关。
	chain, err := QueryEvidenceChain(store, "factorB", asOf)
	if err != nil {
		t.Fatalf("查询证据图谱失败: %v", err)
	}
	if len(chain.Experiments) != 0 {
		t.Fatalf("factorB 应未进入证据关，实际查到 %d 条关联实验", len(chain.Experiments))
	}
}

// TestAdmitFactorEvidence_DoesNotRetroactivelyBlockIncumbent 证明相似度门禁
// 只拒绝"后进入证据关的一方"：factorA 先入证据关，之后才记录 A~B 高相关
// 的相似度边（顺序与上一条测试相反）；factorA 对一个新的实验再次调用
// AdmitFactorEvidence 时不应该被自己涉及的这条可疑边追溯拒绝——它是先手，
// 不是后来者。
func TestAdmitFactorEvidence_DoesNotRetroactivelyBlockIncumbent(t *testing.T) {
	store := newFakeReadWriter()
	const asOf = int64(1000)

	if err := AdmitFactorEvidence(store, asOf, "factorA", "expA1"); err != nil {
		t.Fatalf("factorA 首次进证据关不应失败: %v", err)
	}

	returnsA := []float64{0.01, 0.02, -0.01, 0.03, 0.015, -0.02, 0.04}
	returnsB := []float64{0.011, 0.021, -0.009, 0.031, 0.014, -0.019, 0.041}
	edge, err := RecordSimilarity(store, "factorA", "factorB", returnsA, returnsB)
	if err != nil {
		t.Fatalf("记录相似度失败: %v", err)
	}
	if !edge.SuspectDuplicate {
		t.Fatalf("测试前提不成立：构造的收益序列应触发 SuspectDuplicate，实际相关性 %v", edge.Correlation)
	}

	// factorA 是先手（已经在 evidence:factor: 里），对新实验再次准入不应
	// 被自己涉及的这条可疑边追溯拒绝。
	if err := AdmitFactorEvidence(store, asOf, "factorA", "expA2"); err != nil {
		t.Fatalf("先手因子不应被追溯拒绝: %v", err)
	}

	// factorB 是后来者，仍应被拒绝——证明门禁本身没有失效，只是不再对称
	// 拒绝先手。
	err = AdmitFactorEvidence(store, asOf, "factorB", "expB")
	if _, ok := err.(*SuspectDuplicateError); !ok {
		t.Fatalf("后来者 factorB 仍应被拒绝，实际: %v", err)
	}
}

func TestAdmitFactorEvidence_AllowsUnrelatedFactor(t *testing.T) {
	store := newFakeReadWriter()
	const asOf = int64(1000)

	if err := AdmitFactorEvidence(store, asOf, "factorA", "expA"); err != nil {
		t.Fatalf("factorA 进证据关不应失败: %v", err)
	}

	// 低相关的收益序列。
	returnsA := []float64{0.01, -0.02, 0.03, -0.01, 0.02}
	returnsC := []float64{-0.03, 0.01, -0.01, 0.02, -0.02}
	edge, err := RecordSimilarity(store, "factorA", "factorC", returnsA, returnsC)
	if err != nil {
		t.Fatalf("记录相似度失败: %v", err)
	}
	if edge.SuspectDuplicate {
		t.Skip("构造的收益序列意外呈高相关，跳过（非本测试关注点）")
	}

	if err := AdmitFactorEvidence(store, asOf, "factorC", "expC"); err != nil {
		t.Fatalf("低相关因子应允许进证据关: %v", err)
	}
}
