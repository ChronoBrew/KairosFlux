package aiplane

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ChronoBrew/KairosFlux/internal/jobctl"
)

// 证据图谱键布局（任务书第 4 项："factor -> experiment -> strategy -> paper
// -> review 一次查询走完...对象间引用走逻辑键,BTree 前缀扫描实现,不引图
// 数据库"）。每一跳都是一个独立的前缀命名空间，边的"目标"编码在键的尾段，
// 值本身只是一个占位负载（谁在何时建的边），查询时用 PrefixLister 按前缀
// 扫描取出所有尾段——这就是"BTree 前缀扫描"在本包里的具体落地：LSM/存储层
// 本来就按键的字典序排列，[prefix, prefix+0xFF) 范围扫描等价于对一棵有序
// 索引（B树/LSM 皆可）做前缀查找，不需要专门的图数据库邻接表结构。
func evidenceFactorPrefix(factor string) string { return "evidence:factor:" + factor + ":" }
func evidenceFactorKey(factor, experimentFingerprint string) string {
	return evidenceFactorPrefix(factor) + experimentFingerprint
}

func evidenceExperimentPrefix(experimentFingerprint string) string {
	return "evidence:experiment:" + experimentFingerprint + ":"
}
func evidenceExperimentKey(experimentFingerprint, strategyName string) string {
	return evidenceExperimentPrefix(experimentFingerprint) + strategyName
}

func evidenceStrategyPrefix(strategyName string) string {
	return "evidence:strategy:" + strategyName + ":"
}
func evidenceStrategyKey(strategyName, paperName string) string {
	return evidenceStrategyPrefix(strategyName) + paperName
}

func evidencePaperPrefix(paperName string) string { return "evidence:paper:" + paperName + ":" }
func evidencePaperKey(paperName, reviewID string) string {
	return evidencePaperPrefix(paperName) + reviewID
}

func strategyObjectKey(name string) string     { return "strategy:obj:" + name }
func paperAccountObjectKey(name string) string { return "paper:obj:" + name }
func reviewObjectKey(id string) string         { return "review:obj:" + id }

// edgeMarker 是证据边的落盘负载：只记录边两端的名字，供人工审计核对
// （边的"是否存在"这一事实已经由键本身表达，负载不需要携带更多信息）。
type edgeMarker struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func encodeEdgeMarker(from, to string) []byte {
	out, _ := json.Marshal(edgeMarker{From: from, To: to})
	return out
}

// putEdgeIfChanged 以引擎身份幂等写一条证据边：内容（这里恒定为
// {from,to}）与已存版本相同则跳过，避免重复调用产生冗余版本。
func putEdgeIfChanged(rw ReadWriter, kind ObjectKind, key string, payload []byte) error {
	existing, found, err := rw.GetAsOf(key, nowUnboundedAsOf)
	if err != nil {
		return fmt.Errorf("读现有证据边失败: %w", err)
	}
	if found && string(existing) == string(payload) {
		return nil
	}
	if _, err := WriteAsEngine(rw, kind, key, payload); err != nil {
		return fmt.Errorf("写证据边失败: %w", err)
	}
	return nil
}

// AdmitFactorEvidence 是"进证据关"的唯一入口（任务书第 3 项："相关性>0.7
// 自动打 suspect_duplicate 标记并拒绝进证据关"）。factor 必须是调用方
// 显式传入的结构化标识（如 Proposal.FactorName），不能是从任何自由文本
// （hypothesis_summary 等）解析出来的派生值——见 experiment.go
// SearchExperimentsByMention 的文档，那是查询/展示用途，与本函数的准入
// 判定职责严格分开。
//
// 判定依据：factor 是否与某个"已经先进了证据关"的因子存在
// SuspectDuplicate==true 的相似度边（FindSuspectDuplicate，见
// similarity.go；只拒绝后进入的一方，不追溯拒绝已经先入证据关的一方，见该
// 函数文档与 riskredlines/redlines.json 的 factor_similarity_gate）。命中
// 则返回结构化 *SuspectDuplicateError，不写入 evidence:factor: 边。未命中
// 则以引擎身份幂等写入 evidence:factor:{factor}:{experimentFingerprint}。
func AdmitFactorEvidence(rw ReadWriter, asOfNanos int64, factor, experimentFingerprint string) error {
	if factor == "" {
		return fmt.Errorf("factor 不能为空")
	}
	if experimentFingerprint == "" {
		return fmt.Errorf("experimentFingerprint 不能为空")
	}
	dup, edge, err := FindSuspectDuplicate(rw, factor, asOfNanos)
	if err != nil {
		return err
	}
	if dup {
		conflicting := edge.FactorA
		if conflicting == factor {
			conflicting = edge.FactorB
		}
		return &SuspectDuplicateError{
			Factor:            factor,
			ConflictingFactor: conflicting,
			Correlation:       edge.Correlation,
			Threshold:         SimilarityThreshold,
		}
	}
	key := evidenceFactorKey(factor, experimentFingerprint)
	return putEdgeIfChanged(rw, KindEvidenceEdge, key, encodeEdgeMarker(factor, experimentFingerprint))
}

// LinkExperimentToStrategy/LinkStrategyToPaper/LinkPaperToReview 是引擎裁决
// 管道把后续几跳证据边落账的入口——与 AdmitFactorEvidence 不同，这几跳不
// 涉及 FactorSimilarity 门禁（相似度门禁只发生在"因子第一次进证据关"这一
// 步），只是"引擎已经确认了这条引用关系，落成可查询的边"。
func LinkExperimentToStrategy(rw ReadWriter, experimentFingerprint, strategyName string) error {
	key := evidenceExperimentKey(experimentFingerprint, strategyName)
	return putEdgeIfChanged(rw, KindEvidenceEdge, key, encodeEdgeMarker(experimentFingerprint, strategyName))
}

func LinkStrategyToPaper(rw ReadWriter, strategyName, paperName string) error {
	key := evidenceStrategyKey(strategyName, paperName)
	return putEdgeIfChanged(rw, KindEvidenceEdge, key, encodeEdgeMarker(strategyName, paperName))
}

func LinkPaperToReview(rw ReadWriter, paperName, reviewID string) error {
	key := evidencePaperKey(paperName, reviewID)
	return putEdgeIfChanged(rw, KindEvidenceEdge, key, encodeEdgeMarker(paperName, reviewID))
}

// StrategyObject/PaperAccountObject/ReviewObject 是证据图谱后三跳的节点内容
// （方案 §3.2 对象模型表 Strategy/PaperAccount kind 的最小落地——见 doc.go
// "已知边界"：这与 M3 已有的 strategy:index:{fingerprint}（实验/verdict
// 记录）是两个不同的键空间，本次任务不合并、不改名，只在报告里指出这个
// 既有的命名歧义）。

type StrategyObject struct {
	Name  string               `json:"name"`
	Phase jobctl.StrategyPhase `json:"phase"`
}

type PaperAccountObject struct {
	Name         string `json:"name"`
	StrategyName string `json:"strategy_name"`
	Status       string `json:"status"`
}

type ReviewObject struct {
	ID        string `json:"id"`
	PaperName string `json:"paper_name"`
	Verdict   string `json:"verdict"`
}

func RegisterStrategyObject(rw ReadWriter, obj StrategyObject) error {
	out, _ := json.Marshal(obj)
	return putEdgeIfChanged(rw, KindStrategyObject, strategyObjectKey(obj.Name), out)
}

func RegisterPaperAccountObject(rw ReadWriter, obj PaperAccountObject) error {
	out, _ := json.Marshal(obj)
	return putEdgeIfChanged(rw, KindPaperAccount, paperAccountObjectKey(obj.Name), out)
}

func RegisterReview(rw ReadWriter, obj ReviewObject) error {
	out, _ := json.Marshal(obj)
	return putEdgeIfChanged(rw, KindReview, reviewObjectKey(obj.ID), out)
}

// EvidenceChain 是 QueryEvidenceChain 的返回结果："factor -> experiment ->
// strategy -> paper -> review 一次查询走完"里的"一次查询"：调用方只发起
// 一次 QueryEvidenceChain 调用，函数内部按需对每一跳分别做前缀扫描（见
// 包级文档），把结果结构化组装成一棵树；某一跳没有命中不是错误，落进
// Degraded（如实降级，不编造）。
type EvidenceChain struct {
	Factor      string               `json:"factor"`
	Experiments []ExperimentEvidence `json:"experiments"`
	// Degraded 记录哪些环节没有查到关联对象（人类可读诊断信息，纯展示用途，
	// 不参与任何后续裁决——与 AdmitFactorEvidence 的判定输入严格区分）。
	Degraded []string `json:"degraded"`
}

type ExperimentEvidence struct {
	ExperimentFingerprint string             `json:"experiment_fingerprint"`
	Experiment            *ExperimentRecord  `json:"experiment,omitempty"`
	Strategies            []StrategyEvidence `json:"strategies"`
}

type StrategyEvidence struct {
	Name          string          `json:"name"`
	Object        *StrategyObject `json:"object,omitempty"`
	PaperAccounts []PaperEvidence `json:"paper_accounts"`
}

type PaperEvidence struct {
	Name    string              `json:"name"`
	Object  *PaperAccountObject `json:"object,omitempty"`
	Reviews []ReviewObject      `json:"reviews"`
}

// prefixTargets 列出某前缀下所有边的"尾段"（即目标节点名），按字典序排序、
// 去重。tailAfter 是要剥离的前缀本身长度。
func prefixTargets(rw ReadWriter, prefix string, asOfNanos int64) ([]string, error) {
	entries, err := rw.ListPrefix(prefix, asOfNanos)
	if err != nil {
		return nil, err
	}
	latest := LatestPerLogicalKey(entries)
	seen := make(map[string]bool, len(latest))
	for _, e := range latest {
		if !strings.HasPrefix(e.LogicalKey, prefix) {
			continue
		}
		tail := e.LogicalKey[len(prefix):]
		if tail == "" {
			continue
		}
		seen[tail] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// QueryEvidenceChain 见 EvidenceChain 文档。asOfNanos 贯穿全部四跳扫描与
// 单键读取，保证整棵树是同一个 as-of 时间点的一致快照。
func QueryEvidenceChain(rw ReadWriter, factor string, asOfNanos int64) (EvidenceChain, error) {
	chain := EvidenceChain{Factor: factor}

	experimentFPs, err := prefixTargets(rw, evidenceFactorPrefix(factor), asOfNanos)
	if err != nil {
		return EvidenceChain{}, fmt.Errorf("查 factor->experiment 边失败: %w", err)
	}
	if len(experimentFPs) == 0 {
		chain.Degraded = append(chain.Degraded, fmt.Sprintf("factor=%s 未查到任何已入证据关的 experiment（尚未 AdmitFactorEvidence，或该因子从未被测试过）", factor))
		return chain, nil
	}

	for _, fp := range experimentFPs {
		ee := ExperimentEvidence{ExperimentFingerprint: fp}

		rec, found, err := LookupExperiment(rw, fp, asOfNanos)
		if err != nil {
			return EvidenceChain{}, fmt.Errorf("查 experiment 记录失败(fp=%s): %w", fp, err)
		}
		if !found {
			chain.Degraded = append(chain.Degraded, fmt.Sprintf("experiment_fingerprint=%s 在 strategy:index 中未登记记录（证据边指向的对象缺失）", fp))
		} else {
			ee.Experiment = &rec
		}

		strategyNames, err := prefixTargets(rw, evidenceExperimentPrefix(fp), asOfNanos)
		if err != nil {
			return EvidenceChain{}, fmt.Errorf("查 experiment->strategy 边失败(fp=%s): %w", fp, err)
		}
		if len(strategyNames) == 0 {
			chain.Degraded = append(chain.Degraded, fmt.Sprintf("experiment_fingerprint=%s 尚未关联任何 Strategy 对象（该实验还没有进入策略/尚未登记）", fp))
		}
		for _, sname := range strategyNames {
			se := StrategyEvidence{Name: sname}
			if raw, ok, err := rw.GetAsOf(strategyObjectKey(sname), asOfNanos); err != nil {
				return EvidenceChain{}, fmt.Errorf("查 strategy 对象失败(%s): %w", sname, err)
			} else if ok {
				var obj StrategyObject
				if json.Unmarshal(raw, &obj) == nil {
					se.Object = &obj
				}
			} else {
				chain.Degraded = append(chain.Degraded, fmt.Sprintf("strategy=%s 有证据边引用但未登记 StrategyObject", sname))
			}

			paperNames, err := prefixTargets(rw, evidenceStrategyPrefix(sname), asOfNanos)
			if err != nil {
				return EvidenceChain{}, fmt.Errorf("查 strategy->paper 边失败(%s): %w", sname, err)
			}
			if len(paperNames) == 0 {
				chain.Degraded = append(chain.Degraded, fmt.Sprintf("strategy=%s 尚未关联任何 PaperAccount（模拟盘尚未接入/尚未登记）", sname))
			}
			for _, pname := range paperNames {
				pe := PaperEvidence{Name: pname}
				if raw, ok, err := rw.GetAsOf(paperAccountObjectKey(pname), asOfNanos); err != nil {
					return EvidenceChain{}, fmt.Errorf("查 paper 对象失败(%s): %w", pname, err)
				} else if ok {
					var obj PaperAccountObject
					if json.Unmarshal(raw, &obj) == nil {
						pe.Object = &obj
					}
				} else {
					chain.Degraded = append(chain.Degraded, fmt.Sprintf("paper=%s 有证据边引用但未登记 PaperAccountObject", pname))
				}

				reviewIDs, err := prefixTargets(rw, evidencePaperPrefix(pname), asOfNanos)
				if err != nil {
					return EvidenceChain{}, fmt.Errorf("查 paper->review 边失败(%s): %w", pname, err)
				}
				for _, rid := range reviewIDs {
					raw, ok, err := rw.GetAsOf(reviewObjectKey(rid), asOfNanos)
					if err != nil {
						return EvidenceChain{}, fmt.Errorf("查 review 对象失败(%s): %w", rid, err)
					}
					if !ok {
						chain.Degraded = append(chain.Degraded, fmt.Sprintf("review=%s 有证据边引用但未登记 ReviewObject", rid))
						continue
					}
					var obj ReviewObject
					if json.Unmarshal(raw, &obj) == nil {
						pe.Reviews = append(pe.Reviews, obj)
					}
				}
				se.PaperAccounts = append(se.PaperAccounts, pe)
			}
			ee.Strategies = append(ee.Strategies, se)
		}
		chain.Experiments = append(chain.Experiments, ee)
	}
	return chain, nil
}
