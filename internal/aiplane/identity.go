package aiplane

import "fmt"

// Role 是写请求发起方的身份枚举（任务书原文："agent 只能读真相、只能写
// 提议；引擎裁决"）。用枚举表达角色，不用布尔参数（如 isAgent bool）——
// 与本仓库 QuantBrew CLAUDE.md 明令禁止的"布尔参数扩散"同一条纪律：未来
// 出现第三种角色（如"审计只读客户端"）不至于变成多个 bool 参数。
type Role string

const (
	// RoleAgent：研究员 agent（QuantScout 侧的 LLM 驱动调用方）。只能通过
	// WriteAsAgent 写 KindProposal 对象，其余 kind 一律结构化拒绝。
	RoleAgent Role = "agent"
	// RoleEngine：确定性裁决管道自身（本包内部函数、jobctl reconcile loop
	// 等）。可以写任意已定义 kind——引擎才有权把 Proposal 裁决落成
	// Strategy/PaperAccount/证据边等正式对象。
	RoleEngine Role = "engine"
)

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
// kind 一律返回结构化 *UnauthorizedWriteError，不写入。
//
// 已知边界（见 doc.go）：这只是 API 层的一道闸门。任何调用方仍然可以绕过
// 本函数直接调用底层 Writer.PutVersioned 写任意键——本函数不是、也不能是
// 唯一的强制点，真正的服务端强制需要在 service/router_v2.go 的
// handlePutVersioned 里按请求帧已解出的 source 字段做校验，那需要改动
// v1/v2 协议入口（有零回归红线），本次任务不做。这里提供的是"正式接口
// 存在结构化拒绝能力"，满足任务书验收标准的字面要求。
func WriteAsAgent(w Writer, kind ObjectKind, logicalKey string, payload []byte) (uint64, error) {
	if kind != KindProposal {
		return 0, &UnauthorizedWriteError{
			Role:   RoleAgent,
			Kind:   kind,
			Reason: "agent 身份只能写 Proposal 对象，不能直接写其它 kind",
		}
	}
	return w.PutVersioned(logicalKey, payload)
}

// WriteAsEngine 是引擎裁决管道的写入口：不限制 kind（引擎需要能把 Proposal
// 裁决落成 Strategy/PaperAccount/证据边等正式对象）。之所以仍然经过一个
// 具名函数而不是调用方直接调 Writer.PutVersioned：让"这次写入以引擎身份
// 发生"在调用点显式可见，与 WriteAsAgent 对称，日后如果要接入审计（比如
// 记录哪些写入来自引擎自动裁决 vs 人工操作）有唯一的插入点。
func WriteAsEngine(w Writer, kind ObjectKind, logicalKey string, payload []byte) (uint64, error) {
	return w.PutVersioned(logicalKey, payload)
}
