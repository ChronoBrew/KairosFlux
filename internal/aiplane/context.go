package aiplane

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Package aiplane 的 Context API（任务书第 1 项）：为研究员 agent 组装
// 确定性上下文包——同一请求（同一 ContextRequest.AsOfNanos + 同一份底层
// 账本/契约文件/风控红线文件内容）两次调用 BuildContext 产生逐字节相同的
// JSON（json.Marshal(bundle1) == json.Marshal(bundle2)）。
//
// 确定性的三个来源，缺一不可：
//  1. 账本读取全部走 AsOfReader/PrefixLister 的 asOfNanos 参数，不调用
//     time.Now()（调用方显式传入 as_of，见 ContextRequest.AsOfNanos）；
//  2. 契约文件（contracts/*.schema.json）与风控红线文件按文件名排序读取，
//     其原始字节被摘要进 ContractsDigest/RiskRedlinesDigest——文件内容
//     变化会体现在这两个摘要上，不会悄悄改变其它字段却不留痕迹；
//  3. 全部集合类字段（FactorsTested/StrategyStates/DatasetContracts）在
//     组装时显式排序，不依赖 map 迭代序或文件系统返回顺序。

// ContextRequest 是 BuildContext 的唯一输入参数（As-of 语义：本请求看到的
// 账本状态截止到 AsOfNanos，不包含之后发生的写入）。
type ContextRequest struct {
	AsOfNanos int64
}

// DatasetContractInfo 是 Context 包里"数据集版本"这一项的最小结构化表示，
// 直接取自 contracts/*.schema.json（见 contracts/quote.schema.json 的既有
// 字段）。
type DatasetContractInfo struct {
	TypeID        int    `json:"type_id"`
	Name          string `json:"name"`
	SchemaVersion int    `json:"schema_version"`
}

// RiskRedline 是一条风控红线规则（结构化字段，不是自由文本段落——agent 需要
// 能程序化判断"这条红线适用于哪个阶段"，见 AppliesToPhase）。
type RiskRedline struct {
	ID             string `json:"id"`
	Description    string `json:"description"`
	AppliesToPhase string `json:"applies_to_phase,omitempty"`
}

// riskRedlinesDoc 镜像风控红线配置文件（见 riskredlines/redlines.json）的
// 顶层结构。
type riskRedlinesDoc struct {
	Version  int           `json:"version"`
	Redlines []RiskRedline `json:"redlines"`
}

// FactorTestSummary 是"测过哪些因子/最近判决"这一项的一条记录，直接来自
// ExperimentRecord（strategy:index 导入的 QuantBrew 实验记录）。
type FactorTestSummary struct {
	ExperimentFingerprint string `json:"experiment_fingerprint"`
	HypothesisSummary     string `json:"hypothesis_summary"`
	Level                 string `json:"level"`
	AsOf                  string `json:"as_of"`
	TestIndex             int    `json:"test_index"`
}

// ContextBundle 是 BuildContext 的输出（契约文件 contracts/aiplane/context.schema.json
// 是本结构体面向跨仓调用方的协议描述）。
type ContextBundle struct {
	AsOfNanos          int64                 `json:"as_of_nanos"`
	DatasetContracts   []DatasetContractInfo `json:"dataset_contracts"`
	ContractsDigest    string                `json:"contracts_digest"`
	FactorsTested      []FactorTestSummary   `json:"factors_tested"`
	StrategyStates     []StrategyObject      `json:"strategy_states"`
	RiskRedlines       []RiskRedline         `json:"risk_redlines"`
	RiskRedlinesDigest string                `json:"risk_redlines_digest"`
	// ContextFingerprint 是对本结构体除自身之外全部字段的 sha256 摘要
	// （canonicalBundleForFingerprint，不含 ContextFingerprint 字段本身，
	// 否则"指纹依赖于包含指纹的字节"是一个无法闭合的自指定义）。
	ContextFingerprint string `json:"context_fingerprint"`
}

// canonicalBundleForFingerprint 与 ContextBundle 字段一一对应，唯独不含
// ContextFingerprint——序列化这个结构体得到的字节是指纹的输入。
type canonicalBundleForFingerprint struct {
	AsOfNanos          int64                 `json:"as_of_nanos"`
	DatasetContracts   []DatasetContractInfo `json:"dataset_contracts"`
	ContractsDigest    string                `json:"contracts_digest"`
	FactorsTested      []FactorTestSummary   `json:"factors_tested"`
	StrategyStates     []StrategyObject      `json:"strategy_states"`
	RiskRedlines       []RiskRedline         `json:"risk_redlines"`
	RiskRedlinesDigest string                `json:"risk_redlines_digest"`
}

// loadContracts 按文件名排序读取 contractsDir 下全部 *.schema.json，解析出
// DatasetContractInfo 列表，并返回全部文件原始字节（按同一顺序拼接）的
// sha256 摘要——摘要覆盖整份文件内容（不只是本函数解析出的三个字段），
// 契约文件里任何字段变化都会反映在这个摘要上。
func loadContracts(contractsDir string) ([]DatasetContractInfo, string, error) {
	pattern := filepath.Join(contractsDir, "*.schema.json")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, "", fmt.Errorf("枚举契约文件失败: %w", err)
	}
	sort.Strings(paths)

	infos := make([]DatasetContractInfo, 0, len(paths))
	h := sha256.New()
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, "", fmt.Errorf("读契约文件失败(%s): %w", p, err)
		}
		h.Write(raw)
		h.Write([]byte{'\n'})

		var info DatasetContractInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return nil, "", fmt.Errorf("契约文件 JSON 解析失败(%s): %w", p, err)
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].TypeID < infos[j].TypeID })
	return infos, hex.EncodeToString(h.Sum(nil)), nil
}

// loadRiskRedlines 读取风控红线配置文件，返回排序后的规则列表与文件原始
// 字节的 sha256 摘要。
func loadRiskRedlines(path string) ([]RiskRedline, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("读风控红线文件失败(%s): %w", path, err)
	}
	var doc riskRedlinesDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, "", fmt.Errorf("风控红线文件 JSON 解析失败(%s): %w", path, err)
	}
	sort.Slice(doc.Redlines, func(i, j int) bool { return doc.Redlines[i].ID < doc.Redlines[j].ID })
	sum := sha256.Sum256(raw)
	return doc.Redlines, hex.EncodeToString(sum[:]), nil
}

// listStrategyStates 扫描 strategy:obj: 前缀下全部已登记的 StrategyObject，
// 按 Name 排序。
func listStrategyStates(lister PrefixLister, asOfNanos int64) ([]StrategyObject, error) {
	entries, err := lister.ListPrefix("strategy:obj:", asOfNanos)
	if err != nil {
		return nil, fmt.Errorf("扫描 strategy:obj 失败: %w", err)
	}
	latest := LatestPerLogicalKey(entries)
	out := make([]StrategyObject, 0, len(latest))
	for _, e := range latest {
		var obj StrategyObject
		if json.Unmarshal(e.Payload, &obj) == nil {
			out = append(out, obj)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// BuildContext 组装一份确定性上下文包。contractsDir 通常是仓库的
// contracts/ 目录，redlinesPath 通常是 riskredlines/redlines.json（两者都
// 是调用方传入的路径，本函数不硬编码；生产用法见
// cmd/kairosflux-jobctl 或测试里的固定路径）。
func BuildContext(rw ReadWriter, req ContextRequest, contractsDir, redlinesPath string) (ContextBundle, error) {
	contracts, contractsDigest, err := loadContracts(contractsDir)
	if err != nil {
		return ContextBundle{}, err
	}
	redlines, redlinesDigest, err := loadRiskRedlines(redlinesPath)
	if err != nil {
		return ContextBundle{}, err
	}
	experiments, err := ListExperiments(rw, req.AsOfNanos)
	if err != nil {
		return ContextBundle{}, err
	}
	factorsTested := make([]FactorTestSummary, 0, len(experiments))
	for _, e := range experiments {
		factorsTested = append(factorsTested, FactorTestSummary{
			ExperimentFingerprint: e.Fingerprint,
			HypothesisSummary:     e.HypothesisSummary,
			Level:                 e.Level,
			AsOf:                  e.AsOf,
			TestIndex:             e.TestIndex,
		})
	}
	strategyStates, err := listStrategyStates(rw, req.AsOfNanos)
	if err != nil {
		return ContextBundle{}, err
	}

	canon := canonicalBundleForFingerprint{
		AsOfNanos:          req.AsOfNanos,
		DatasetContracts:   contracts,
		ContractsDigest:    contractsDigest,
		FactorsTested:      factorsTested,
		StrategyStates:     strategyStates,
		RiskRedlines:       redlines,
		RiskRedlinesDigest: redlinesDigest,
	}
	canonBytes, err := json.Marshal(canon)
	if err != nil {
		return ContextBundle{}, fmt.Errorf("规范化编码失败: %w", err)
	}
	sum := sha256.Sum256(canonBytes)

	return ContextBundle{
		AsOfNanos:          canon.AsOfNanos,
		DatasetContracts:   canon.DatasetContracts,
		ContractsDigest:    canon.ContractsDigest,
		FactorsTested:      canon.FactorsTested,
		StrategyStates:     canon.StrategyStates,
		RiskRedlines:       canon.RiskRedlines,
		RiskRedlinesDigest: canon.RiskRedlinesDigest,
		ContextFingerprint: hex.EncodeToString(sum[:]),
	}, nil
}
