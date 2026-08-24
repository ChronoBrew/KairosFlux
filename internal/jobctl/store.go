package jobctl

// Store 是 reconcile loop 依赖的键空间读写能力，对应契约里"走既有 v2
// opcode（PUT_VERSIONED/GET/GET_AS_OF）操作 job: 键空间"这一条：
//   - PutVersioned 对应 PUT_VERSIONED opcode：每次调用产生一条新版本，
//     不覆盖历史（job:spec/job:status 也走这条路径而不是字面量 PUT——
//     方案原文的目标是"天然可审计、可重放"，spec/status 的变更历史本身
//     也是审计对象，不只是 events）。
//   - GetLatest 对应 GET_AS_OF opcode（以调用时刻为 as_of）：读某逻辑键
//     当前可见的最新版本。不用裸 GET，因为 PUT_VERSIONED 写的是
//     "{logical}:v{seq}" 与 "{logical}:current" 两个存储键，字面量键
//     "{logical}" 本身从未被写过，GET 会读不到。
//
// 生产实现是 V2Store（真实拨号、走 kairnet v2 协议）；测试用内存实现
// （见 reconciler_test.go 的 fakeStore）验证 reconcile 语义，不需要每次
// 都起一个真实 TCP 服务端跑一万次——10000 次幂等重跑这条验收标准关心的是
// "reconcile 决策逻辑本身幂等"，与"字节是否真的走了 TCP 线"是两件可以分开
// 验证的事：opcode 编解码的正确性由 service/router_v2_temporal_test.go
// 等既有测试覆盖，本包只需要证明"给定同样的 Store 语义，reconcile 决策
// 幂等"，注入面用接口留好，真实集成路径由 V2Store 提供、由
// v2store_integration_test.go 做少量端到端覆盖。
type Store interface {
	// PutVersioned 写入 logicalKey 的一条新版本，返回分配到的 seq。
	PutVersioned(logicalKey string, payload []byte) (seq uint64, err error)

	// GetLatest 返回 logicalKey 当前可见的最新版本负载；从未写过返回
	// found=false、err=nil（"从未写过"不是错误，与 temporal.TemporalStore
	// 的既有约定一致）。
	GetLatest(logicalKey string) (payload []byte, found bool, err error)
}
