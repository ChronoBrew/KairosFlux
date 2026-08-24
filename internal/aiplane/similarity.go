package aiplane

import (
	"encoding/json"
	"fmt"
	"math"
)

// SimilarityThreshold 是"相关性>0.7 自动打 suspect_duplicate"的阈值常量
// （方案原文任务书第 3 项）。用具名常量而不是散落的字面量 0.7，判断改成
// >=/> 之类的边界调整只需要改一处。严格大于——方案原文写"相关性>0.7"，
// 恰好等于 0.7 不触发（本包对此保持字面精确，不做"约等于也算"的模糊化）。
const SimilarityThreshold = 0.7

// SimilarityEdge 是两个因子之间的收益相关性边（方案 §3.2 对象模型
// FactorSimilarity kind）。FactorA/FactorB 落盘时按字符串字典序排列，
// 使同一对因子无论调用方以什么顺序传参，都落在同一个逻辑键、编码结果
// 逐字节相同（无向边）。
type SimilarityEdge struct {
	FactorA          string  `json:"factor_a"`
	FactorB          string  `json:"factor_b"`
	Correlation      float64 `json:"correlation"`
	SuspectDuplicate bool    `json:"suspect_duplicate"`
}

// SimilarityKey 返回 factor:similarity:{a}|{b} 逻辑键，a/b 按字典序排列。
// 用 "|" 而不是 ":" 分隔两个因子名，避免因子名本身含 ":" 时产生切分歧义
// （因子名目前的实际取值如 "amihud_illiq" 不含 "|"，但键布局本身的分隔符
// 选择不应该依赖"目前恰好没有"这种偶然事实）。
func SimilarityKey(factorA, factorB string) string {
	a, b := factorA, factorB
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("factor:similarity:%s|%s", a, b)
}

// PearsonCorrelation 计算两条等长收益序列的皮尔逊相关系数。方案原文明确
// "相关性计算输入=因子收益序列（由调用方提交,KairosFlux 存边与判定,不在
// 库内重算因子——职责分离）"：调用方已经算好两个因子的收益序列，本函数只
// 负责"给定两条数值序列，算出相关系数"这一步确定性数学运算，不做因子收益
// 本身的计算（那是 QuantBrew 的职责）。
//
// 长度不等、长度<2、或任一序列方差为 0（常数序列，相关系数无定义）返回
// 结构化错误，不返回 NaN——NaN 参与后续 >阈值 比较的结果未定义，让上层
// 拿到一个"看起来合法"的 float64 却其实毫无意义，比显式报错更危险。
func PearsonCorrelation(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("两条收益序列长度不一致: len(a)=%d len(b)=%d", len(a), len(b))
	}
	if len(a) < 2 {
		return 0, fmt.Errorf("收益序列长度必须>=2，实际=%d", len(a))
	}
	n := float64(len(a))
	var sumA, sumB float64
	for i := range a {
		sumA += a[i]
		sumB += b[i]
	}
	meanA, meanB := sumA/n, sumB/n

	var cov, varA, varB float64
	for i := range a {
		da, db := a[i]-meanA, b[i]-meanB
		cov += da * db
		varA += da * da
		varB += db * db
	}
	if varA == 0 || varB == 0 {
		return 0, fmt.Errorf("收益序列方差为 0（常数序列），相关系数无定义")
	}
	corr := cov / math.Sqrt(varA*varB)
	return corr, nil
}

// SuspectDuplicateError 是"相关性>阈值，拒绝进证据关"的结构化拒绝（任务书
// 验收标准第二条："相似因子（相关性>0.7）无法进入证据关"）。reason 机读：
// 调用方可以直接读 Correlation/ConflictingFactor 字段，不必解析错误文本。
type SuspectDuplicateError struct {
	Factor            string
	ConflictingFactor string
	Correlation       float64
	Threshold         float64
}

func (e *SuspectDuplicateError) Error() string {
	return fmt.Sprintf(
		"因子 %q 与已入证据关的因子 %q 相关性=%.4f 超过阈值 %.2f，标记 suspect_duplicate，拒绝进证据关",
		e.Factor, e.ConflictingFactor, e.Correlation, e.Threshold,
	)
}

// RecordSimilarity 计算 factorA/factorB 的皮尔逊相关系数并以引擎身份存边
// （factor:similarity:{a}|{b}，见 SimilarityKey）。幂等：与已存的边内容
// 完全相同则跳过写入（不产生新版本），与 jobctl.RegistryImporter 的幂等
// 判据同一模式。
func RecordSimilarity(rw ReadWriter, factorA, factorB string, returnsA, returnsB []float64) (SimilarityEdge, error) {
	if factorA == factorB {
		return SimilarityEdge{}, fmt.Errorf("factorA 与 factorB 不能相同: %q", factorA)
	}
	corr, err := PearsonCorrelation(returnsA, returnsB)
	if err != nil {
		return SimilarityEdge{}, fmt.Errorf("计算相关系数失败: %w", err)
	}

	a, b := factorA, factorB
	if a > b {
		a, b = b, a
	}
	edge := SimilarityEdge{FactorA: a, FactorB: b, Correlation: corr, SuspectDuplicate: corr > SimilarityThreshold}
	payload := encodeSimilarityEdge(edge)
	key := SimilarityKey(factorA, factorB)

	existing, found, err := rw.GetAsOf(key, nowUnboundedAsOf)
	if err != nil {
		return SimilarityEdge{}, fmt.Errorf("读现有相似度边失败: %w", err)
	}
	if found && string(existing) == string(payload) {
		return edge, nil
	}
	if _, err := WriteAsEngine(rw, KindFactorSimilarity, key, payload); err != nil {
		return SimilarityEdge{}, fmt.Errorf("写相似度边失败: %w", err)
	}
	return edge, nil
}

// LookupSimilarity 读取 factorA/factorB 之间已记录的相似度边；未记录返回
// found=false。
func LookupSimilarity(reader AsOfReader, factorA, factorB string, asOfNanos int64) (edge SimilarityEdge, found bool, err error) {
	raw, ok, err := reader.GetAsOf(SimilarityKey(factorA, factorB), asOfNanos)
	if err != nil {
		return SimilarityEdge{}, false, err
	}
	if !ok {
		return SimilarityEdge{}, false, nil
	}
	e, decodeOK := decodeSimilarityEdge(raw)
	if !decodeOK {
		return SimilarityEdge{}, false, fmt.Errorf("相似度边解码失败: %s", SimilarityKey(factorA, factorB))
	}
	return e, true, nil
}

// FindSuspectDuplicate 扫描 factor:similarity: 前缀下所有已记录的边，判断
// factor 是否应该被"进证据关"拒绝。
//
// 判定规则（与 riskredlines/redlines.json 的 factor_similarity_gate 一致）：
// 只拒绝"后进入证据关的一方"——即 factor 与某个已标记 SuspectDuplicate 的
// 因子 other 相关性超阈值，且 other 此前已经有 AdmitFactorEvidence 成功
// 落下的 evidence:factor:{other}: 边（isFactorAdmitted）。这不是"任何一条
// 涉及 factor 的可疑边都拒绝"（早期实现是这样，存在一个自相矛盾：那种规则
// 会连"已经先进了证据关的一方"也一并拒绝——如果之后才补录相似度边，或者
// 之后才对同一个 factor 的另一个实验重新调用 AdmitFactorEvidence，会把
// 本该被放行的先入者也挡在外面，与"拒绝的是后来者"这个直觉/文档描述矛盾）。
//
// 时序结果：谁先把自己的 evidence:factor: 边落下，谁就是"先手"，之后任何
// 与它高相关的因子（不论相似度边是在先手落地之前还是之后记录的）都会被
// 挡住；先手因子本身即使在之后对新的 experimentFingerprint 重复调用
// AdmitFactorEvidence，也不会因为自己"涉及一条可疑边"而被追溯拒绝。
//
// 按逻辑键升序检查，多条命中时返回字典序最小的那条边对应的冲突因子——
// 确定性：同样的边集合两次调用返回同一个冲突因子，不依赖 map 迭代序。
func FindSuspectDuplicate(lister PrefixLister, factor string, asOfNanos int64) (found bool, edge SimilarityEdge, err error) {
	entries, err := lister.ListPrefix("factor:similarity:", asOfNanos)
	if err != nil {
		return false, SimilarityEdge{}, fmt.Errorf("扫描相似度边失败: %w", err)
	}
	latest := LatestPerLogicalKey(entries)
	for _, e := range latest {
		edge, ok := decodeSimilarityEdge(e.Payload)
		if !ok {
			continue
		}
		if !edge.SuspectDuplicate {
			continue
		}
		if edge.FactorA != factor && edge.FactorB != factor {
			continue
		}
		other := edge.FactorA
		if other == factor {
			other = edge.FactorB
		}
		admitted, err := isFactorAdmitted(lister, other, asOfNanos)
		if err != nil {
			return false, SimilarityEdge{}, fmt.Errorf("检查因子 %q 是否已入证据关失败: %w", other, err)
		}
		if admitted {
			return true, edge, nil
		}
	}
	return false, SimilarityEdge{}, nil
}

// isFactorAdmitted 报告 factor 是否已经有至少一条 evidence:factor:{factor}:
// 边（即此前 AdmitFactorEvidence 对它成功过）。
func isFactorAdmitted(lister PrefixLister, factor string, asOfNanos int64) (bool, error) {
	entries, err := lister.ListPrefix(evidenceFactorPrefix(factor), asOfNanos)
	if err != nil {
		return false, fmt.Errorf("扫描 evidence:factor 边失败: %w", err)
	}
	return len(LatestPerLogicalKey(entries)) > 0, nil
}

func encodeSimilarityEdge(e SimilarityEdge) []byte {
	out, _ := json.Marshal(e) // 全字段可编码类型，不会失败
	return out
}

func decodeSimilarityEdge(raw []byte) (SimilarityEdge, bool) {
	var e SimilarityEdge
	if err := json.Unmarshal(raw, &e); err != nil {
		return SimilarityEdge{}, false
	}
	return e, true
}
