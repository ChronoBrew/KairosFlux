// Package identity 是"写请求发起方角色"这一条规则的唯一真相来源：把
// internal/aiplane.Role（agent 只能写 Proposal、引擎不受限）从 API 层的一道
// 闸门升级为协议层强制（M4 上报缺口，任务书："把 M4 的 WriteAsAgent API 层
// 闸门升级为协议层强制"）时，service/router_v2.go 需要在 handlePutVersioned
// 里按解出的 source 字段做角色校验——但 service 不能 import internal/aiplane：
// internal/aiplane/integration_test.go 与 internal/jobctl/v2store_integration_test.go
// 都是同包内部测试（package aiplane / package jobctl，不是 _test 后缀的外部
// 测试包）且都 import service 起真实服务端做端到端验证，若 service 反过来
// import internal/aiplane，会在 `go test ./internal/aiplane/...` 编译测试
// 二进制时触发"import cycle not allowed in test"（已用最小复现验证过这个
// 编译期错误，不是理论风险）。
//
// 拆出这个零依赖的叶子包，是"抽象错了就改抽象，不写胶水"的直接应用：
// Role/ObjectKind 的角色语义与"source 字符串怎么编码角色""哪个键前缀是
// Proposal 对象"这两条判定规则，本质上是协议层（service）与应用层
// (aiplane) 都需要引用的同一份契约，不属于 aiplane 独占——之前把它们放在
// aiplane 包内部，是"先有 API 层闸门、协议层强制是后续工作"这一阶段性状态
// 的产物，现在协议层也要用，就必须挪到两边都能安全依赖的位置。
// internal/aiplane/identity.go 用类型别名/常量转发本包的定义，字面意义上
// 的 Role/RoleAgent/RoleEngine 未变，调用方（含既有测试）不需要改一行。
package identity

import "strings"

// Role 是写请求发起方的身份枚举（详见 internal/aiplane/identity.go 的原始
// 文档：agent 只读真相、只写提议，引擎负责裁决）。
type Role string

const (
	// RoleAgent 是研究员 agent（QuantScout 侧 LLM 驱动调用方）的角色。
	RoleAgent Role = "agent"
	// RoleEngine 是确定性裁决管道自身（jobctl/aiplane 引擎组件）的角色。
	RoleEngine Role = "engine"
)

// agentSourcePrefix 是 WriteEnvelope.source 字段里标记"这次写入以 agent 身份
// 发生"的唯一编码约定：source == agentSourcePrefix + 调用方声明的 agent
// 标识（见 AgentSource）。除此之外的任何 source 取值（包括空字符串、
// "jobctl"、"aiplane" 等引擎组件名）一律视为 RoleEngine——协议层只需要
// 区分"是不是 agent"，信任列表由"没有用 agent: 前缀自称"这一条件本身表达，
// 不需要维护一份引擎白名单。
const agentSourcePrefix = "agent:"

// AgentSource 把调用方声明的 agent 标识（如 Proposal.SubmittedBy）编码成
// 落在 WriteEnvelope.source 上的规范字符串——审计（LIST_WRITES 的 BySource
// 聚合）与协议层角色判定（SourceRole）共享同一份编码，不是两套互相不知道
// 对方存在的约定。
func AgentSource(agentID string) string {
	return agentSourcePrefix + agentID
}

// SourceRole 从 WriteEnvelope.source 反解出 Role：带 agent: 前缀的一律
// RoleAgent，其余（含空字符串）一律 RoleEngine。
func SourceRole(source string) Role {
	if strings.HasPrefix(source, agentSourcePrefix) {
		return RoleAgent
	}
	return RoleEngine
}

// AgentID 从 AgentSource 编码的 source 字符串里取回原始 agent 标识；
// ok=false 表示 source 不是 agent: 前缀格式（调用方不应该假设一定能取到）。
func AgentID(source string) (id string, ok bool) {
	if !strings.HasPrefix(source, agentSourcePrefix) {
		return "", false
	}
	return strings.TrimPrefix(source, agentSourcePrefix), true
}

// ProposalKeyPrefix 是 internal/aiplane 的 Proposal 对象唯一使用的逻辑键
// 前缀（aiplane.ProposalKey 用它拼出 "proposal:{fingerprint}"）。定义在这里
// （而不是 aiplane 包）的唯一原因是协议层强制：service/router_v2.go 需要
// 判断"agent 身份写的这个 key 是不是 Proposal 对象"，而 PUT_VERSIONED 请求帧
// 里没有独立的"kind"字段——键前缀是唯一可用的判据（与 aiplane/doc.go 记录的
// "协议层不需要注册新 schema type_id 也能工作"是同一设计：键前缀本身就是
// kind 的协议层编码）。aiplane.ProposalKey 转发本常量，不重复"proposal:"
// 这个字面量。
const ProposalKeyPrefix = "proposal:"

// IsProposalKey 判断 key 是否落在 Proposal 对象的键空间——agent 身份的
// PUT_VERSIONED 写入只有落在这里才被协议层放行。
func IsProposalKey(key string) bool {
	return strings.HasPrefix(key, ProposalKeyPrefix)
}
