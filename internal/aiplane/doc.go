// Package aiplane 实现 M4 AI 数据平面（docs/方案-BanDB-时态内核与AI数据平面.md
// §M4）：让"Agent 只读真相、只写提议、引擎裁决"成为正式接口。
//
// 范围（原文五项）：
//  1. Context API：为研究员 agent 组装确定性上下文包（数据集版本、测过哪些
//     因子、最近判决、策略状态、风控红线）——同一请求（同一 as_of + 同一
//     底层账本状态）两次调用逐字节相同。
//  2. Proposal API：agent 只能提交假设/因子/实验提议；引擎负责指纹、相似度、
//     证据关前置检查、入账（Proposal 对象走时态版本语义，不可变）。
//  3. FactorSimilarity：因子收益相关性边存储与查询；相关性 >0.7 自动打
//     suspect_duplicate 标记并拒绝进证据关（结构化拒绝，reason 机读）。
//     相关性计算输入（因子收益序列）由调用方提交，本包只存边与判定，不在
//     库内重算因子——职责分离。
//  4. 证据图谱查询：factor -> experiment -> strategy -> paper -> review
//     一次查询走完——"一次查询"指调用方只需要一次 QueryEvidenceChain 调用，
//     内部依次对每一跳做前缀扫描（BTree 前缀扫描，见 keys.go 的键布局），
//     不是底层存储只发生一次扫描，也不引入图数据库。
//  5. Agent 产出（推荐/复盘）落成 Proposal 对象，由确定性管道裁决。
//
// 依赖关系：本包依赖 internal/jobctl（复用其 Store 接口形状与 V2Store 的
// GetAsOf/ListPrefix 能力）与 internal/temporal（Fingerprint 风格一致的
// 确定性指纹方法）。不新增协议 opcode——Proposal/FactorSimilarity/证据边
// 全部走既有 PUT_VERSIONED/GET_AS_OF/LIST_WRITES opcode 上的新键空间前缀
// （proposal:/factor:similarity:/evidence:），协议层不需要注册新 schema
// type_id 也能工作（TypeUnspecified 未纳管前缀默认放行，见
// service/ingesthook/filter.go 的 validate 回退路径）。
//
// 已知边界（写进最终报告的"发现的歧义"，不在本包内自行拍板修正）：
//   - WriteAsAgent 只是本包 API 层的一道闸门：任何调用方仍可以绕过它直接调用
//     jobctl.Store.PutVersioned 写任意键。真正的强制点应该在
//     service/router_v2.go 的 handlePutVersioned 里，按 PUT_VERSIONED 请求帧
//     已经解出的 source 字段做服务端强制——但那需要改 router_v2（有 v1/v2
//     零回归红线），任务书验收标准只要求"结构化拒绝"这一条能力存在，本次
//     不做协议层改动。
//   - `strategy:index:{fingerprint}`（M3 registry 导入落的键）实际存的是
//     QuantBrew 的实验/verdict 记录，不是方案 §3.2 对象模型表里定义的
//     "Strategy 策略库存档"对象——命名与内容不对齐，是发现的既有歧义，本包
//     按"它就是 Evidence 图谱里的 Experiment 节点"来读，不改名、不改写。
package aiplane
