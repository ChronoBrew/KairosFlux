package service_test

// kairosflux 双模式引擎（service/kairosflux.go）的验收测试：embedded 全流程、
// Open 重启恢复、server 网络壳（真实 v2 线协议往返）、Proposal/Context 访问口。
//
// 测试全部用独立临时数据目录，不触碰进程级 config.G 与仓库 log/ 目录。

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/internal/jobctl"
	"github.com/ChronoBrew/KairosFlux/service"
)

// mustDataDir 建一个本测试独占的临时数据目录。
func mustDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "data")
}

// TestEngineEmbeddedFullFlow 覆盖 embedded 模式全链路：PUT_VERSIONED →
// GET_AS_OF（含 as-of 定点语义）→ LIST_VERSIONS → REPLAY_FINGERPRINT（无界
// 对账 + 确定性）→ LIST_WRITES（审计 + 按来源计数）。
func TestEngineEmbeddedFullFlow(t *testing.T) {
	e, err := service.NewEmbedded(service.Options{DataDir: mustDataDir(t)})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer e.Close()

	base := int64(1_700_000_000) * 1e9 // 固定基准时间戳（纳秒）
	// 三次版本化写入，write_ts 用调用方控制的时间戳——as-of 定点语义的可测前提。
	for i, payload := range []string{"v1", "v2", "v3"} {
		seq, err := e.PutVersioned("quote:2026-08-17:510300", []byte(payload), base+int64(i)*1e9, "job-a", 2)
		if err != nil {
			t.Fatalf("PutVersioned #%d: %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("seq = %d, want %d", seq, i+1)
		}
	}

	// as-of：t0+0.5s 时刻只能看到第 1 个版本，绝不看到未来写入。
	v, found, err := e.GetAsOf("quote:2026-08-17:510300", base+int64(0.5e9))
	if err != nil {
		t.Fatalf("GetAsOf: %v", err)
	}
	if !found || string(v.Payload) != "v1" {
		t.Fatalf("as-of(t0+0.5s) = found=%v payload=%q, want v1", found, v.Payload)
	}
	// t0+2.5s 时刻看到第 3 个版本。
	v, found, err = e.GetAsOf("quote:2026-08-17:510300", base+int64(2.5e9))
	if err != nil || !found || string(v.Payload) != "v3" {
		t.Fatalf("as-of(t0+2.5s) = found=%v payload=%q err=%v, want v3", found, v.Payload, err)
	}
	// t0-1s：写入之前，不可见。
	if _, found, err = e.GetAsOf("quote:2026-08-17:510300", base-1e9); err != nil || found {
		t.Fatalf("as-of(t0-1s) 应不可见: found=%v err=%v", found, err)
	}

	versions, err := e.ListVersions("quote:2026-08-17:510300")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 3 || versions[2].Seq != 3 {
		t.Fatalf("ListVersions = %d 条, want 3（seq 升序, 最新 seq=3）", len(versions))
	}

	r1, err := e.ReplayFingerprint("quote:", 0)
	if err != nil {
		t.Fatalf("ReplayFingerprint: %v", err)
	}
	if r1.KeyCount != 1 || len(r1.Mismatches) != 0 {
		t.Fatalf("replay: keyCount=%d mismatches=%v, want 1 key 0 mismatches", r1.KeyCount, r1.Mismatches)
	}
	r2, err := e.ReplayFingerprint("quote:", 0)
	if err != nil {
		t.Fatalf("ReplayFingerprint #2: %v", err)
	}
	if r2.Fingerprint != r1.Fingerprint {
		t.Fatalf("同一账本两次重放指纹不一致: %s != %s（确定性违反）", r1.Fingerprint, r2.Fingerprint)
	}

	writes, err := e.ListWrites("quote:", 0, 0, "")
	if err != nil {
		t.Fatalf("ListWrites: %v", err)
	}
	if len(writes.Entries) != 3 {
		t.Fatalf("ListWrites = %d 条, want 3", len(writes.Entries))
	}
	for _, we := range writes.Entries {
		if !we.HashOK {
			t.Fatalf("写入信封 hash 自检失败: %+v", we)
		}
	}
	if len(writes.BySource) != 1 || writes.BySource[0].Source != "job-a" || writes.BySource[0].Count != 3 {
		t.Fatalf("BySource = %+v, want 单一来源 job-a x3", writes.BySource)
	}
}

// TestEngineOpenRestartRecoversData 验证 Open 的"重启恢复"语义：写入 → Close
// （排空并关闭 WAL）→ 同目录重新 Open → 数据完整回到（as-of 可读、指纹一致）。
func TestEngineOpenRestartRecoversData(t *testing.T) {
	dir := mustDataDir(t)

	e1, err := service.NewEmbedded(service.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	now := time.Now().UnixNano()
	if _, err := e1.PutVersioned("bar:2026-08-17:000001", []byte(`{"close":3.14}`), now, "job-b", 0); err != nil {
		t.Fatalf("PutVersioned: %v", err)
	}
	fp1, err := e1.ReplayFingerprint("bar:", 0)
	if err != nil {
		t.Fatalf("ReplayFingerprint: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	e2, err := service.Open(service.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("Open 同一数据目录: %v", err)
	}
	defer e2.Close()

	v, found, err := e2.GetAsOf("bar:2026-08-17:000001", now+1)
	if err != nil || !found || string(v.Payload) != `{"close":3.14}` {
		t.Fatalf("重启后 GetAsOf = found=%v payload=%q err=%v, want 原 payload", found, v.Payload, err)
	}
	fp2, err := e2.ReplayFingerprint("bar:", 0)
	if err != nil {
		t.Fatalf("重启后 ReplayFingerprint: %v", err)
	}
	if fp2.Fingerprint != fp1.Fingerprint {
		t.Fatalf("重启前后指纹不一致: %s != %s", fp1.Fingerprint, fp2.Fingerprint)
	}
	if len(fp2.Mismatches) != 0 {
		t.Fatalf("重启后出现 :current 对账不一致: %v", fp2.Mismatches)
	}
}

// TestEngineOpenRequiresExistingDir 验证 Open 对"目录不存在"的结构化拒绝。
func TestEngineOpenRequiresExistingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := service.Open(service.Options{DataDir: missing}); err == nil {
		t.Fatal("Open 不存在的目录应报错")
	}
}

// freePort 找一个当前空闲的端口（listen:0 后立即关闭，与 kairnet 测试同一
// 模式——短暂竞态窗口可接受，测试只要求大概率不冲突）。
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// TestEngineServeNetworkShell 验证 server 模式 = 同一 API 的网络壳：真实 v2
// 线协议（协商 + PUT_VERSIONED + GET_AS_OF + LIST_VERSIONS）往返，与
// embedded 模式同数据、同语义。
func TestEngineServeNetworkShell(t *testing.T) {
	port := freePort(t)
	e, err := service.Serve(service.Options{DataDir: mustDataDir(t), Port: port})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer e.Close()
	addr := e.Addr()
	if addr == "" {
		t.Fatal("Serve 后 Addr() 为空")
	}

	// 以外部消费者身份（jobctl.V2Store，纯 v2 网络客户端）写读。quote: 前缀
	// 走内建契约校验（ingesthook/schema 的 QuoteSnapshot，注册于包 init），
	// 负载必须是合法行情快照。
	quoteJSON := []byte(`{"code":"510300","date":"2026-08-17","open":1.0,"high":1.1,"low":0.9,"close":1.05,"volume":100}`)
	store := jobctl.NewV2Store(addr, 5*time.Second)
	seq, err := store.PutVersioned("quote:2026-08-17:510300", quoteJSON, "job-net")
	if err != nil {
		t.Fatalf("v2 网络 PUT_VERSIONED: %v", err)
	}
	if seq != 1 {
		t.Fatalf("网络写入 seq = %d, want 1", seq)
	}
	payload, found, err := store.GetAsOf("quote:2026-08-17:510300", time.Now().UnixNano()+1e9)
	if err != nil || !found || string(payload) != string(quoteJSON) {
		t.Fatalf("v2 网络 GET_AS_OF = found=%v payload=%q err=%v", found, payload, err)
	}

	// 同一数据在 embedded 视角可见（同一引擎，同语义）。
	vv, found, err := e.GetAsOf("quote:2026-08-17:510300", time.Now().UnixNano()+1e9)
	if err != nil || !found || string(vv.Payload) != string(quoteJSON) {
		t.Fatalf("embedded 视角读网络写入 = found=%v payload=%q err=%v", found, vv.Payload, err)
	}
}

// TestEngineServeRejectsNonPositivePort 验证 Serve 对 Port<=0 的拒绝。
func TestEngineServeRejectsNonPositivePort(t *testing.T) {
	if _, err := service.Serve(service.Options{DataDir: mustDataDir(t)}); err == nil {
		t.Fatal("Serve 无端口应报错")
	}
}

// TestEngineProposalAndContext 验证 Proposal/Context 访问口：agent 提交提议
// （角色强制 + 指纹确定性），随后组装确定性上下文包（契约文件 + 红线文件
// 摘要 + 策略状态读取全走 as-of）。
func TestEngineProposalAndContext(t *testing.T) {
	e, err := service.NewEmbedded(service.Options{DataDir: mustDataDir(t)})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer e.Close()

	fp, seq, err := e.SubmitProposal(service.Proposal{
		Kind:        service.ProposalFactor,
		SubmittedBy: "quantscout-researcher-v1",
		FactorName:  "amihud_illiq",
		Summary:     "流动性因子复测提议",
	})
	if err != nil {
		t.Fatalf("SubmitProposal: %v", err)
	}
	if seq != 1 || fp == "" {
		t.Fatalf("SubmitProposal seq=%d fp=%q, want seq=1 非空指纹", seq, fp)
	}

	// 写一条策略状态（strategy:obj: 前缀），供 BuildContext 的 StrategyStates 读取。
	if _, err := e.PutVersioned("strategy:obj:momentum_10", []byte(`{"name":"momentum_10","phase":"paper"}`), time.Now().UnixNano(), "engine", 0); err != nil {
		t.Fatalf("PutVersioned strategy: %v", err)
	}

	// 临时契约目录 + 红线文件（与仓库 contracts/riskredlines 同形状的最小集）。
	contractsDir := filepath.Join(t.TempDir(), "contracts")
	if err := os.MkdirAll(contractsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	contractJSON := `{"type_id": 1, "name": "quote", "schema_version": 2}`
	if err := os.WriteFile(filepath.Join(contractsDir, "quote.schema.json"), []byte(contractJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	redlinesPath := filepath.Join(t.TempDir(), "redlines.json")
	if err := os.WriteFile(redlinesPath, []byte(`{"version":1,"redlines":[{"id":"r1","description":"agent 只写 proposal","applies_to_phase":"gate"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}

	req := service.ContextRequest{AsOfNanos: time.Now().UnixNano() + 1e9}
	b1, err := e.BuildContext(req, contractsDir, redlinesPath)
	if err != nil {
		t.Fatalf("BuildContext: %v", err)
	}
	b2, err := e.BuildContext(req, contractsDir, redlinesPath)
	if err != nil {
		t.Fatalf("BuildContext #2: %v", err)
	}
	if b1.ContextFingerprint != b2.ContextFingerprint {
		t.Fatal("同一请求两次 BuildContext 指纹不一致（确定性违反）")
	}
	if len(b1.DatasetContracts) != 1 || b1.DatasetContracts[0].Name != "quote" {
		t.Fatalf("DatasetContracts = %+v, want quote 契约", b1.DatasetContracts)
	}
	if len(b1.RiskRedlines) != 1 || b1.RiskRedlines[0].ID != "r1" {
		t.Fatalf("RiskRedlines = %+v, want r1", b1.RiskRedlines)
	}
	if len(b1.StrategyStates) != 1 || b1.StrategyStates[0].Name != "momentum_10" {
		t.Fatalf("StrategyStates = %+v, want momentum_10", b1.StrategyStates)
	}
	if b1.ContractsDigest == "" || b1.RiskRedlinesDigest == "" {
		t.Fatal("契约/红线摘要不应为空")
	}
}
