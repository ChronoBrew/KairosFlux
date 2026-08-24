package aiplane

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// ProposalKind 枚举 Agent 能提交的提议种类（方案原文任务书第 2/5 项：
// "agent 只能提交假设/因子/实验提议""Agent 产出（推荐/复盘）落成 Proposal
// 对象"）。用枚举而不是自由字符串，SubmitProposal 的校验分支按 == 比较。
type ProposalKind string

const (
	ProposalFactor         ProposalKind = "factor"
	ProposalHypothesis     ProposalKind = "hypothesis"
	ProposalExperiment     ProposalKind = "experiment"
	ProposalRecommendation ProposalKind = "recommendation"
	ProposalReview         ProposalKind = "review"
)

// validProposalKinds 是全部合法 ProposalKind 的枚举表，Validate 用它做成员
// 判断——不用 switch 里默认分支兜底"其它字符串也算合法"，未声明的 kind
// 一律拒绝。
var validProposalKinds = map[ProposalKind]bool{
	ProposalFactor:         true,
	ProposalHypothesis:     true,
	ProposalExperiment:     true,
	ProposalRecommendation: true,
	ProposalReview:         true,
}

// Proposal 是 agent 唯一能写入的对象（契约文件 contracts/aiplane/proposal.schema.json
// 是本结构体面向跨仓调用方的协议描述，字段须保持一致）。
type Proposal struct {
	// Kind 决定这条提议的种类，见 ProposalKind 常量。
	Kind ProposalKind `json:"kind"`
	// SubmittedBy 是提交方标识（如 "quantscout-researcher-v1"），落审计用，
	// 不是权限判定依据——权限判定只看调用的是 WriteAsAgent 还是
	// WriteAsEngine，SubmittedBy 是"谁"，不是"能不能"。
	SubmittedBy string `json:"submitted_by"`
	// FactorName 是结构化的因子标识：Kind==ProposalFactor 时必填。这是
	// FactorSimilarity/证据图谱查询定位"这个提议是关于哪个因子"的唯一
	// 依据——engine 侧任何需要知道"这是什么因子"的逻辑（AdmitFactorEvidence
	// 的入参）必须读这个字段，禁止从 Summary 等自由文本里解析因子名（那是
	// 本仓库明令禁止的"字符串匹配承载语义"）。
	FactorName string `json:"factor_name,omitempty"`
	// ExperimentFingerprint 是结构化的实验指纹引用：Kind==ProposalExperiment
	// 或需要关联既有实验记录（strategy:index:{fingerprint}）时填写。
	ExperimentFingerprint string `json:"experiment_fingerprint,omitempty"`
	// Summary 是人类可读摘要，纯展示用途，不参与任何裁决判断。
	Summary string `json:"summary"`
	// Detail 是 kind 专属的结构化负载（原样透传，schema 由
	// contracts/aiplane/proposal.schema.json 按 kind 分别约束），本包不解释其内容。
	Detail json.RawMessage `json:"detail,omitempty"`
}

// ProposalValidationError 是提议字段校验失败的结构化描述（与
// internal/jobctl.ValidationError 同一设计原则：字段名+原因，不靠字符串
// 子串判断哪里错了）。
type ProposalValidationError struct {
	Field  string
	Reason string
}

func (e *ProposalValidationError) Error() string {
	return fmt.Sprintf("proposal 校验失败: 字段=%s 原因=%s", e.Field, e.Reason)
}

// Validate 校验 Proposal 必填字段。
func (p Proposal) Validate() error {
	if !validProposalKinds[p.Kind] {
		return &ProposalValidationError{Field: "kind", Reason: fmt.Sprintf("未知提议种类: %q", p.Kind)}
	}
	if p.SubmittedBy == "" {
		return &ProposalValidationError{Field: "submitted_by", Reason: "不能为空"}
	}
	if p.Summary == "" {
		return &ProposalValidationError{Field: "summary", Reason: "不能为空"}
	}
	if p.Kind == ProposalFactor && p.FactorName == "" {
		return &ProposalValidationError{Field: "factor_name", Reason: "kind=factor 时必填"}
	}
	if p.Kind == ProposalExperiment && p.ExperimentFingerprint == "" {
		return &ProposalValidationError{Field: "experiment_fingerprint", Reason: "kind=experiment 时必填"}
	}
	return nil
}

// canonicalProposal 是 Proposal 的规范化编码目标结构：字段顺序固定，Detail
// 用 json.RawMessage 原样透传（json.Marshal 不会重新格式化 RawMessage 内部
// 字节，故 Detail 内部字段顺序由提交方决定——这是提交方需要对自己提交内容
// 的指纹稳定性负责的部分，不是本包能控制的）。与 internal/jobctl.JobSpec.
// CanonicalJSON 同一模式：不依赖 struct 字段声明顺序这种偶然细节之外的东西
// （struct 字段顺序本身是 encoding/json 文档保证的确定性行为，不是偶然细节）。
type canonicalProposal struct {
	Kind                  ProposalKind    `json:"kind"`
	SubmittedBy           string          `json:"submitted_by"`
	FactorName            string          `json:"factor_name"`
	ExperimentFingerprint string          `json:"experiment_fingerprint"`
	Summary               string          `json:"summary"`
	Detail                json.RawMessage `json:"detail"`
}

// CanonicalJSON 返回 Proposal 的规范化 JSON 编码，用于 Fingerprint 与实际
// 落盘的 payload——同一份提议内容，两次调用产生逐字节相同的编码。
func (p Proposal) CanonicalJSON() []byte {
	detail := p.Detail
	if detail == nil {
		detail = json.RawMessage("null")
	}
	canon := canonicalProposal{
		Kind:                  p.Kind,
		SubmittedBy:           p.SubmittedBy,
		FactorName:            p.FactorName,
		ExperimentFingerprint: p.ExperimentFingerprint,
		Summary:               p.Summary,
		Detail:                detail,
	}
	out, _ := json.Marshal(canon) // 全字段可编码类型，不会失败
	return out
}

// Fingerprint 返回 Proposal 的确定性指纹（sha256 十六进制），既是
// proposal:{fingerprint} 键空间的键组成部分，也是"同一份提议重复提交=幂等
// 无新版本"的判据。
func (p Proposal) Fingerprint() string {
	sum := sha256.Sum256(p.CanonicalJSON())
	return hex.EncodeToString(sum[:])
}

// ProposalKey 返回 proposal:{fingerprint} 逻辑键。
func ProposalKey(fingerprint string) string {
	return "proposal:" + fingerprint
}

// SubmitProposal 是 Proposal API 的唯一入口：校验 -> 算指纹 -> 幂等写入
// proposal:{fingerprint}。刻意不在这里做 FactorSimilarity 检查——任务书
// 原文把"相关性>0.7 拒绝"限定在"进证据关"这一步（AdmitFactorEvidence，
// evidence.go），不是"能不能提交提议"这一步：agent 应该始终能够提交任何
// 假设，哪怕它后来被引擎判定为重复因子而拒绝进证据关。二者是"能提议"与
// "提议是否被采信"两件事，任务书原文的分句结构（"Proposal API：...；
// FactorSimilarity：...相关性>0.7...拒绝进证据关"）也印证了这一点。
//
// 权限：本函数内部调用 WriteAsAgent（kind 固定为 KindProposal），故只能被
// agent 身份使用；不提供 engine 直接篡改既有 Proposal 内容的路径（Proposal
// 走时态版本语义，不可变——同 fingerprint 重复提交是幂等 no-op，不是覆盖）。
func SubmitProposal(rw ReadWriter, p Proposal) (fingerprint string, seq uint64, err error) {
	if err := p.Validate(); err != nil {
		return "", 0, err
	}
	fp := p.Fingerprint()
	key := ProposalKey(fp)
	payload := p.CanonicalJSON()

	existing, found, err := rw.GetAsOf(key, nowUnboundedAsOf)
	if err != nil {
		return "", 0, fmt.Errorf("读现有 proposal 失败: %w", err)
	}
	if found && string(existing) == string(payload) {
		return fp, 0, nil // 幂等：内容完全相同，不产生新版本
	}

	newSeq, err := WriteAsAgent(rw, KindProposal, key, payload)
	if err != nil {
		return "", 0, err
	}
	return fp, newSeq, nil
}

// nowUnboundedAsOf 是"读当前最新版本"时使用的 as_of 上界——取 int64 最大值
// 而非 time.Now()：SubmitProposal 的幂等性检查关心的是"这个 fingerprint
// 之前有没有被写过、内容是否相同"，与调用发生的墙钟时刻无关（两次在不同
// 时刻提交同一份提议都应该判定为幂等）。用 math.MaxInt64 表达"不设上界"，
// 与 internal/temporal 包"以真实取值范围推出确定性判据"的一贯做法一致：
// 真实 write_ts 恒小于 MaxInt64，用它做上界不会误伤任何真实写入。
const nowUnboundedAsOf = int64(1<<63 - 1)
