package aiplane

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ChronoBrew/KairosFlux/internal/jobctl"
)

// ExperimentRecord 是 strategy:index:{fingerprint}（M3 jobctl.RegistryImporter
// 从 QuantBrew experiments/registry.jsonl 导入的实验/verdict 记录，见
// doc.go"已知边界"关于这个键名与方案对象模型不完全对齐的说明）解析出的
// 结构化视图。字段对应 QuantBrew SPEC-M31 的 verdict 结构（本仓库不拥有
// 这份 schema，只读取其中与 Context API/证据图谱查询相关的字段；
// CoreMetrics 原样保留 json.RawMessage，不逐字段重新建模——那是 QuantBrew
// 侧会随 SPEC 演进的内部结构，重新建模一遍属于"依赖对方内部细节"，字段
// 变了要跟着改两处）。
type ExperimentRecord struct {
	Fingerprint       string          `json:"fingerprint"`
	Kind              string          `json:"kind"`
	HypothesisSummary string          `json:"hypothesis_summary"`
	AsOf              string          `json:"as_of"`
	TestIndex         int             `json:"test_index"`
	Level             string          `json:"level"`
	PRaw              float64         `json:"p_raw"`
	PGlobal           float64         `json:"p_global"`
	ArtifactPath      string          `json:"artifact_path"`
	Cached            bool            `json:"cached"`
	CoreMetrics       json.RawMessage `json:"core_metrics,omitempty"`
}

// registryVerdictEnvelope 镜像 QuantBrew registry.jsonl 一行的顶层结构
// （{"verdict": {...}}），只用于解析，不对外暴露。
type registryVerdictEnvelope struct {
	Verdict ExperimentRecord `json:"verdict"`
}

// parseExperimentRecord 从 strategy:index:{fingerprint} 的原始 payload
// （即 registry.jsonl 该行原始字节，见 jobctl.RegistryImporter 的导入
// 逻辑）解析出 ExperimentRecord。
func parseExperimentRecord(raw []byte) (ExperimentRecord, bool) {
	var env registryVerdictEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return ExperimentRecord{}, false
	}
	return env.Verdict, true
}

// LookupExperiment 按 fingerprint 读取单条实验记录。
func LookupExperiment(reader AsOfReader, fingerprint string, asOfNanos int64) (ExperimentRecord, bool, error) {
	raw, found, err := reader.GetAsOf(jobctl.RegistryIndexKey(fingerprint), asOfNanos)
	if err != nil {
		return ExperimentRecord{}, false, err
	}
	if !found {
		return ExperimentRecord{}, false, nil
	}
	rec, ok := parseExperimentRecord(raw)
	if !ok {
		return ExperimentRecord{}, false, fmt.Errorf("实验记录解码失败(fingerprint=%s)", fingerprint)
	}
	return rec, true, nil
}

// ListExperiments 枚举 strategy:index: 前缀下全部已导入的实验记录（每个
// fingerprint 的最新版本），按 Fingerprint 字典序排序——Context API"测过
// 哪些因子"依赖的基础数据源。解析失败的记录跳过，不中断整体查询（与
// jobctl.RegistryImporter 导入阶段"单行出错不影响其它行"同一原则）。
func ListExperiments(lister PrefixLister, asOfNanos int64) ([]ExperimentRecord, error) {
	entries, err := lister.ListPrefix(jobctl.RegistryIndexPrefix(), asOfNanos)
	if err != nil {
		return nil, fmt.Errorf("扫描 strategy:index 失败: %w", err)
	}
	latest := LatestPerLogicalKey(entries)
	out := make([]ExperimentRecord, 0, len(latest))
	for _, e := range latest {
		rec, ok := parseExperimentRecord(e.Payload)
		if !ok {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Fingerprint < out[j].Fingerprint })
	return out, nil
}

// SearchExperimentsByMention 是"agent 想知道某个因子测过几次/判决是什么"
// 这类问答依赖的查询工具：对 HypothesisSummary 做大小写不敏感的子串匹配。
//
// 这是刻意的文本检索，不是语义分类：QuantBrew 的 registry.jsonl（M31 SPEC
// 拥有的 schema）目前没有一个结构化的"这条实验测的是哪个/哪些因子"字段——
// 因子名只出现在 hypothesis_summary/core_metrics.winning_params_summary
// 这类拼接好的展示字符串里（如 "factors=[amihud_illiq(lb=20,w=1)]"）。
// 本仓库 CLAUDE.md 明令禁止"用字符串匹配承载语义"，但那条禁令针对的是
// "决策/裁决输入"（如本包 AdmitFactorEvidence 的 factor 参数，必须来自
// Proposal.FactorName 等结构化字段，见 evidence.go 文档）；这里是一个
// 只读的、明确标注为"关键词提及检索"的搜索工具，检索结果不反向驱动任何
// 准入判定，属于诚实标注的近似能力，不是被禁止的那类胶水。
//
// 发现的歧义（写进最终报告，不在本次任务里修正）：理想情况下 QuantBrew
// 的 registry.jsonl 应该新增一个结构化的 factors []string 字段，届时本函数
// 应该改为精确匹配该字段，而不是继续做文本检索。
func SearchExperimentsByMention(lister PrefixLister, asOfNanos int64, keyword string) ([]ExperimentRecord, error) {
	all, err := ListExperiments(lister, asOfNanos)
	if err != nil {
		return nil, err
	}
	needle := strings.ToLower(keyword)
	var matched []ExperimentRecord
	for _, rec := range all {
		if strings.Contains(strings.ToLower(rec.HypothesisSummary), needle) {
			matched = append(matched, rec)
		}
	}
	return matched, nil
}
