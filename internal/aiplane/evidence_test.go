package aiplane

import (
	"encoding/json"
	"testing"

	"github.com/ChronoBrew/KairosFlux/internal/jobctl"
)

// putExperimentRecord 是测试装配辅助：直接把一条 ExperimentRecord 编码成
// strategy:index:{fingerprint} 的原始 payload（镜像 QuantBrew registry.jsonl
// 的 {"verdict": {...}} 顶层结构），绕开 jobctl.RegistryImporter 从文件导入
// 这一步，专注测试 aiplane 侧的读取/组装逻辑。
func putExperimentRecord(store *fakeReadWriter, rec ExperimentRecord) {
	raw, _ := json.Marshal(registryVerdictEnvelope{Verdict: rec})
	store.putRaw(jobctl.RegistryIndexKey(rec.Fingerprint), raw)
}

// TestQueryEvidenceChain_FullChainResolves 证明"factor -> experiment ->
// strategy -> paper -> review 一次查询走完"在全部四跳都已登记时能完整
// 解析出来（任务书第 4 项）。真实 QuantBrew 数据目前还没有走到这一步（见
// integration_test.go 的端到端测试展示"如实降级"的那一半），这里用合成
// 数据证明"能力本身是完整的"。
func TestQueryEvidenceChain_FullChainResolves(t *testing.T) {
	store := newFakeReadWriter()
	const asOf = int64(1_000_000)

	putExperimentRecord(store, ExperimentRecord{
		Fingerprint:       "exp_amihud_1",
		Kind:              "factor_gate",
		HypothesisSummary: "factors=[amihud_illiq(lb=20,w=1)]",
		AsOf:              "2026-08-21",
		TestIndex:         1,
		Level:             "pass_raw",
	})

	if err := AdmitFactorEvidence(store, asOf, "amihud_illiq", "exp_amihud_1"); err != nil {
		t.Fatalf("AdmitFactorEvidence 失败: %v", err)
	}
	if err := LinkExperimentToStrategy(store, "exp_amihud_1", "candidate1"); err != nil {
		t.Fatalf("LinkExperimentToStrategy 失败: %v", err)
	}
	if err := RegisterStrategyObject(store, StrategyObject{Name: "candidate1", Phase: jobctl.StrategyPaper}); err != nil {
		t.Fatalf("RegisterStrategyObject 失败: %v", err)
	}
	if err := LinkStrategyToPaper(store, "candidate1", "paper_10"); err != nil {
		t.Fatalf("LinkStrategyToPaper 失败: %v", err)
	}
	if err := RegisterPaperAccountObject(store, PaperAccountObject{Name: "paper_10", StrategyName: "candidate1", Status: "running"}); err != nil {
		t.Fatalf("RegisterPaperAccountObject 失败: %v", err)
	}
	if err := LinkPaperToReview(store, "paper_10", "review_2026w34"); err != nil {
		t.Fatalf("LinkPaperToReview 失败: %v", err)
	}
	if err := RegisterReview(store, ReviewObject{ID: "review_2026w34", PaperName: "paper_10", Verdict: "符合预期"}); err != nil {
		t.Fatalf("RegisterReview 失败: %v", err)
	}

	chain, err := QueryEvidenceChain(store, "amihud_illiq", asOf)
	if err != nil {
		t.Fatalf("QueryEvidenceChain 失败: %v", err)
	}
	if len(chain.Degraded) != 0 {
		t.Fatalf("全链路都已登记时不应有降级项，实际: %v", chain.Degraded)
	}
	if len(chain.Experiments) != 1 {
		t.Fatalf("应查到 1 条关联实验，实际 %d", len(chain.Experiments))
	}
	exp := chain.Experiments[0]
	if exp.Experiment == nil || exp.Experiment.Level != "pass_raw" {
		t.Fatalf("实验记录应能解析出 level=pass_raw，实际 %+v", exp.Experiment)
	}
	if len(exp.Strategies) != 1 || exp.Strategies[0].Name != "candidate1" {
		t.Fatalf("应关联到 candidate1 策略，实际 %+v", exp.Strategies)
	}
	strat := exp.Strategies[0]
	if strat.Object == nil || strat.Object.Phase != jobctl.StrategyPaper {
		t.Fatalf("策略对象应能解析出 phase=paper，实际 %+v", strat.Object)
	}
	if len(strat.PaperAccounts) != 1 || strat.PaperAccounts[0].Name != "paper_10" {
		t.Fatalf("应关联到 paper_10 模拟盘，实际 %+v", strat.PaperAccounts)
	}
	paper := strat.PaperAccounts[0]
	if paper.Object == nil || paper.Object.Status != "running" {
		t.Fatalf("模拟盘对象应能解析出 status=running，实际 %+v", paper.Object)
	}
	if len(paper.Reviews) != 1 || paper.Reviews[0].Verdict != "符合预期" {
		t.Fatalf("应关联到 1 条复盘记录，实际 %+v", paper.Reviews)
	}
}

// TestQueryEvidenceChain_DegradesHonestlyWhenLinksMissing 证明"查不到关联的
// 环节如实降级回答，禁编造"：只有 factor->experiment 一跳有数据，后续三跳
// 都没有登记时，Degraded 里必须显式说明，而不是伪造/省略。
func TestQueryEvidenceChain_DegradesHonestlyWhenLinksMissing(t *testing.T) {
	store := newFakeReadWriter()
	const asOf = int64(1_000_000)

	putExperimentRecord(store, ExperimentRecord{
		Fingerprint:       "exp_amihud_1",
		HypothesisSummary: "factors=[amihud_illiq(lb=20,w=1)]",
		Level:             "pass_raw",
	})
	if err := AdmitFactorEvidence(store, asOf, "amihud_illiq", "exp_amihud_1"); err != nil {
		t.Fatalf("AdmitFactorEvidence 失败: %v", err)
	}

	chain, err := QueryEvidenceChain(store, "amihud_illiq", asOf)
	if err != nil {
		t.Fatalf("QueryEvidenceChain 失败: %v", err)
	}
	if len(chain.Experiments) != 1 {
		t.Fatalf("应查到 1 条关联实验，实际 %d", len(chain.Experiments))
	}
	if len(chain.Experiments[0].Strategies) != 0 {
		t.Fatal("未登记 Strategy 时不应凭空生成策略关联")
	}
	if len(chain.Degraded) == 0 {
		t.Fatal("应有降级说明记录'实验尚未关联任何 Strategy 对象'")
	}
}

func TestQueryEvidenceChain_UnknownFactorDegradesWithoutError(t *testing.T) {
	store := newFakeReadWriter()
	chain, err := QueryEvidenceChain(store, "never_tested_factor", 1000)
	if err != nil {
		t.Fatalf("查询未知因子不应报错: %v", err)
	}
	if len(chain.Experiments) != 0 {
		t.Fatal("未知因子不应查到任何实验")
	}
	if len(chain.Degraded) != 1 {
		t.Fatalf("应有恰好 1 条降级说明，实际 %v", chain.Degraded)
	}
}
