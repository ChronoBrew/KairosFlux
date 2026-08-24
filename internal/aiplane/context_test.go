package aiplane

import (
	"bytes"
	"encoding/json"
	"testing"
)

// repoContractsDir/repoRedlinesPath 指向仓库真实的 contracts/、
// riskredlines/ 目录——测试用真实文件而不是临时构造的 fixture，因为
// ContractsDigest/RiskRedlinesDigest 存在的意义就是"配置漂移在字节上可见"，
// 用真实文件测试才能顺带验证这两份契约文件本身是合法可解析的 JSON。
//
// 注意：repoContractsDir 指向的是 service/ingesthook/schema.LoadContracts
// 同一个 contracts/ 顶层目录（当前只有 quote.schema.json 一份"数据集"契约）
// ——不是 contracts/aiplane/ 下的 proposal/context 协议契约。两者刻意分开：
// LoadContracts 对 contracts/ 目录做非递归、DisallowUnknownFields 的严格
// 扫描（每个 *.schema.json 必须能注册到一个真实 Go Validator），把
// proposal/context 这两份"AI 数据平面协议契约"放进同一个目录会破坏那条
// 既有的强校验（见 service/ingesthook/schema/contracts_test.go）。Context
// API 的 dataset_contracts 字段语义是"数据集版本"，本来就该只反映 quote
// 这类真正的数据契约，不应该把"agent 请求/响应协议长什么样"也混进同一个
// 列表——放进子目录、不出现在 DatasetContracts 里，是符合语义的选择，不是
// 绕开测试的权宜之计。
const (
	repoContractsDir = "../../contracts"
	repoRedlinesPath = "../../riskredlines/redlines.json"
)

// TestBuildContext_DeterministicAcrossCalls 验证 Context API 的核心确定性
// 契约："同请求两跑逐字节相同"：同一个 ReadWriter 状态、同一个
// ContextRequest，两次调用 BuildContext 的 JSON 序列化结果必须逐字节相等
// （不是逐字段比较——那会漏掉"字段顺序/多余空白"这类不影响 struct 相等
// 但影响"逐字节相同"这条硬性要求的差异）。
func TestBuildContext_DeterministicAcrossCalls(t *testing.T) {
	store := newFakeReadWriter()
	putExperimentRecord(store, ExperimentRecord{
		Fingerprint:       "exp1",
		HypothesisSummary: "factors=[amihud_illiq(lb=20,w=1)]",
		Level:             "pass_raw",
		AsOf:              "2026-08-21",
		TestIndex:         1,
	})
	if err := RegisterStrategyObject(store, StrategyObject{Name: "candidate1", Phase: "gate"}); err != nil {
		t.Fatalf("RegisterStrategyObject 失败: %v", err)
	}

	// 精确取"当前写入时钟"作为 as_of 边界，而不是一个随意选的大数——后面
	// 要验证"这个 as_of 之后发生的写入不应改变结果"，as_of 必须恰好卡在
	// setup 写入之后、额外写入之前。
	req := ContextRequest{AsOfNanos: store.currentClock()}

	b1, err := BuildContext(store, req, repoContractsDir, repoRedlinesPath)
	if err != nil {
		t.Fatalf("第一次 BuildContext 失败: %v", err)
	}
	b2, err := BuildContext(store, req, repoContractsDir, repoRedlinesPath)
	if err != nil {
		t.Fatalf("第二次 BuildContext 失败: %v", err)
	}

	j1, err := json.Marshal(b1)
	if err != nil {
		t.Fatalf("序列化 b1 失败: %v", err)
	}
	j2, err := json.Marshal(b2)
	if err != nil {
		t.Fatalf("序列化 b2 失败: %v", err)
	}
	if !bytes.Equal(j1, j2) {
		t.Fatalf("同请求两次调用应逐字节相同:\n第一次: %s\n第二次: %s", j1, j2)
	}
	if b1.ContextFingerprint == "" {
		t.Fatal("ContextFingerprint 不应为空")
	}

	// 在 as_of 之后追加一次写入：as_of 不变的情况下，第三次调用必须与前两次
	// 仍然逐字节相同——这是"as_of 语义"的核心断言，不是"从不变化"的平凡情况。
	if err := RegisterStrategyObject(store, StrategyObject{Name: "candidate2", Phase: "hypothesis"}); err != nil {
		t.Fatalf("追加写入失败: %v", err)
	}
	b3, err := BuildContext(store, req, repoContractsDir, repoRedlinesPath)
	if err != nil {
		t.Fatalf("第三次 BuildContext 失败: %v", err)
	}
	j3, err := json.Marshal(b3)
	if err != nil {
		t.Fatalf("序列化 b3 失败: %v", err)
	}
	if !bytes.Equal(j1, j3) {
		t.Fatalf("as_of 早于新写入时间点时，账本追加写入不应改变 Context 结果:\nas_of前: %s\nas_of后(应相同): %s", j1, j3)
	}
}

func TestBuildContext_AsOfExcludesFutureWrites(t *testing.T) {
	store := newFakeReadWriter()
	// clock 从写入操作次数递增（见 fakeReadWriter.PutVersioned），第一次写入
	// writeNanos=1。
	if err := RegisterStrategyObject(store, StrategyObject{Name: "early", Phase: "hypothesis"}); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	early, err := BuildContext(store, ContextRequest{AsOfNanos: 1}, repoContractsDir, repoRedlinesPath)
	if err != nil {
		t.Fatalf("BuildContext(as_of=1) 失败: %v", err)
	}
	if len(early.StrategyStates) != 1 {
		t.Fatalf("as_of=1 应看到 1 个策略状态，实际 %d", len(early.StrategyStates))
	}

	if err := RegisterStrategyObject(store, StrategyObject{Name: "later", Phase: "hypothesis"}); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	stillEarly, err := BuildContext(store, ContextRequest{AsOfNanos: 1}, repoContractsDir, repoRedlinesPath)
	if err != nil {
		t.Fatalf("BuildContext(as_of=1，第二次) 失败: %v", err)
	}
	if len(stillEarly.StrategyStates) != 1 {
		t.Fatalf("as_of=1 不应看到 as_of 之后写入的 'later'，实际看到 %d 个", len(stillEarly.StrategyStates))
	}

	later, err := BuildContext(store, ContextRequest{AsOfNanos: 2}, repoContractsDir, repoRedlinesPath)
	if err != nil {
		t.Fatalf("BuildContext(as_of=2) 失败: %v", err)
	}
	if len(later.StrategyStates) != 2 {
		t.Fatalf("as_of=2 应能看到两次写入，实际 %d", len(later.StrategyStates))
	}
}

func TestBuildContext_ContractsDigestReflectsRealContractFiles(t *testing.T) {
	store := newFakeReadWriter()
	b, err := BuildContext(store, ContextRequest{AsOfNanos: 1}, repoContractsDir, repoRedlinesPath)
	if err != nil {
		t.Fatalf("BuildContext 失败: %v", err)
	}
	if b.ContractsDigest == "" {
		t.Fatal("ContractsDigest 不应为空")
	}
	// contracts/ 顶层目录当前只有 quote.schema.json 一份真正的数据集契约
	// （proposal/context 是 contracts/aiplane/ 下的协议契约，不计入
	// DatasetContracts，见 repoContractsDir 的文档）。
	if len(b.DatasetContracts) < 1 {
		t.Fatalf("仓库 contracts/ 目录应至少有 quote 契约，实际解析出 %d 份", len(b.DatasetContracts))
	}
	foundQuote := false
	for _, c := range b.DatasetContracts {
		if c.Name == "quote" {
			foundQuote = true
		}
	}
	if !foundQuote {
		t.Fatalf("应能解析出 quote 契约，实际: %+v", b.DatasetContracts)
	}
	if len(b.RiskRedlines) == 0 {
		t.Fatal("应能读到风控红线")
	}
}
