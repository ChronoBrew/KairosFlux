package aiplane

import (
	"fmt"

	"github.com/ChronoBrew/KairosFlux/internal/identity"
)

// Role 是写请求发起方的身份枚举（任务书原文："agent 只能读真相、只能写
// 提议；引擎裁决"）。用枚举表达角色，不用布尔参数（如 isAgent bool）——
// 与本仓库 QuantBrew CLAUDE.md 明令禁止的"布尔参数扩散"同一条纪律：未来
// 出现第三种角色（如"审计只读客户端"）不至于变成多个 bool 参数。
//
// 类型别名到 internal/identity：Role 的定义与"source 字符串怎么编码角色"
// 现在是 service（协议层）与本包（应用层）共享的契约，挪去了零依赖的
// identity 叶子包（见其文档"import cycle"一节），这里转发而不是重新声明，
// 本包内以及既有测试对 Role/RoleAgent/RoleEngine 的引用不需要改一个字符。
type Role = identity.Role

const (
	// RoleAgent：研究员 agent（QuantScout 侧的 LLM 驱动调用方）。只能通过
	// WriteAsAgent 写 KindProposal 对象，其余 kind 一律结构化拒绝——这一条
	// 现在由 service/router_v2.go 的 handlePutVersioned 在协议层强制，不再
	// 只是本包 API 层的一道闸门（见 doc.go"已知边界"一节的更新）。
	RoleAgent = identity.RoleAgent
	// RoleEngine：确定性裁决管道自身（本包内部函数、jobctl reconcile loop
	// 等）。可以写任意已定义 kind——引擎才有权把 Proposal 裁决落成
	// Strategy/PaperAccount/证据边等正式对象。
	RoleEngine = identity.RoleEngine
)

// engineSource 是本包以引擎身份写入时落在 WriteEnvelope.source 上的标识，
// 与 internal/jobctl.EngineSource 同一模式（不同组件各自声明一个 plain
// 字符串，"没有 agent: 前缀"即可落 RoleEngine，见 identity.SourceRole）。
const engineSource = "aiplane"

// ObjectKind 是本包管理的对象种类枚举（对应方案 §3.2 对象模型表的子集：
// M4 范围只新增 Proposal/FactorSimilarity/证据边三类，其余 kind 沿用既有
// 包——Job* 归 jobctl，Experiment 记录沿用 jobctl 的 strategy:index 前缀，
// 见 doc.go "已知边界"一节）。枚举表达角色/种类，不用字符串前缀匹配承载
// 语义——WriteAsAgent 的判断只对 ObjectKind 做 == 比较，不解析 logicalKey
// 字符串本身去猜它是什么种类的对象。
type ObjectKind string

const (
	KindProposal         ObjectKind = "proposal"
	KindFactorSimilarity ObjectKind = "factor_similarity"
	KindEvidenceEdge     ObjectKind = "evidence_edge"
	KindStrategyObject   ObjectKind = "strategy_object"
	KindPaperAccount     ObjectKind = "paper_account"
	KindReview           ObjectKind = "review"
)

// UnauthorizedWriteError 是"越权写数据"的结构化拒绝（任务书验收标准第一条：
// "越权写数据被结构化拒绝"）。字段是类型化的 Role/ObjectKind，不是拼接出的
// 错误字符串子串——调用方要做程序化判断（比如统计越权尝试次数、按 Kind
// 分类告警）可以直接比较字段，不必解析 Error() 文本。
type UnauthorizedWriteError struct {
	Role   Role
	Kind   ObjectKind
	Reason string
}

func (e *UnauthorizedWriteError) Error() string {
	return fmt.Sprintf("越权写拒绝: role=%s kind=%s 原因=%s", e.Role, e.Kind, e.Reason)
}

// WriteAsAgent 是 agent 身份的唯一写入口：只允许 kind==KindProposal，其余
// kind 一律返回结构化 *UnauthorizedWriteError，不写入。agentID 是调用方
// 声明的 agent 身份（如 Proposal.SubmittedBy），编码进落盘写入的 source
// 字段（identity.AgentSource）——这不只是 API 层的措辞，是审计
// （LIST_WRITES 的 BySource 聚合）与协议层强制共用的同一个值：
// service/router_v2.go 的 handlePutVersioned 现在会按请求帧里解出的这个
// source 独立地重新做一遍这条判断（见其文档），本函数被绕过（直接调
// Writer.PutVersioned）不再意味着越权写能够得逞——写到线上的请求终会
// 落在 PUT_VERSIONED opcode 上，服务端会用同一份 identity.SourceRole/
// IsProposalKey 规则再校验一次（doc.go"已知边界"一节已更新，仍有一处
// 边界未覆盖：字面量 PUT/DEL(v1、v2 OpcodePut) 不携带 source，不受这条
// 规则约束，只有 PUT_VERSIONED 路径被强制）。
func WriteAsAgent(w Writer, kind ObjectKind, agentID, logicalKey string, payload []byte) (uint64, error) {
	if kind != KindProposal {
		return 0, &UnauthorizedWriteError{
			Role:   RoleAgent,
			Kind:   kind,
			Reason: "agent 身份只能写 Proposal 对象，不能直接写其它 kind",
		}
	}
	return w.PutVersioned(logicalKey, payload, identity.AgentSource(agentID))
}

// WriteAsEngine 是引擎裁决管道的写入口：不限制 kind（引擎需要能把 Proposal
// 裁决落成 Strategy/PaperAccount/证据边等正式对象）。之所以仍然经过一个
// 具名函数而不是调用方直接调 Writer.PutVersioned：让"这次写入以引擎身份
// 发生"在调用点显式可见，与 WriteAsAgent 对称，日后如果要接入审计（比如
// 记录哪些写入来自引擎自动裁决 vs 人工操作）有唯一的插入点。source 固定为
// engineSource（"aiplane"），与 jobctl.EngineSource（"jobctl"）同一模式——
// 两个引擎组件各自声明一个不带 "agent:" 前缀的 plain 名字，足以让
// identity.SourceRole 判定为 RoleEngine，服务端不需要维护一份组件名白名单。
func WriteAsEngine(w Writer, kind ObjectKind, logicalKey string, payload []byte) (uint64, error) {
	return w.PutVersioned(logicalKey, payload, engineSource)
}
