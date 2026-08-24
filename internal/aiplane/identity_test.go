package aiplane

import (
	"errors"
	"testing"
)

// TestWriteAsAgent_RejectsDirectWriteOfNonProposalKind 是任务书验收标准
// 第一条的黄金测试："越权写数据被结构化拒绝（黄金测试：agent 身份直写
// 非 Proposal kind → 拒绝）"。
func TestWriteAsAgent_RejectsDirectWriteOfNonProposalKind(t *testing.T) {
	store := newFakeReadWriter()

	_, err := WriteAsAgent(store, KindStrategyObject, "strategy:obj:candidate1", []byte(`{"name":"candidate1","phase":"live"}`))
	if err == nil {
		t.Fatal("agent 身份直写 strategy_object kind 应被拒绝，实际未返回错误")
	}
	var uw *UnauthorizedWriteError
	if !errors.As(err, &uw) {
		t.Fatalf("错误类型应为 *UnauthorizedWriteError，实际: %T(%v)", err, err)
	}
	if uw.Role != RoleAgent {
		t.Fatalf("Role 应为 RoleAgent，实际 %s", uw.Role)
	}
	if uw.Kind != KindStrategyObject {
		t.Fatalf("Kind 应为 KindStrategyObject，实际 %s", uw.Kind)
	}
	if uw.Reason == "" {
		t.Fatal("Reason 不应为空——调用方需要机读的拒绝原因")
	}

	if store.versionCount("strategy:obj:candidate1") != 0 {
		t.Fatal("被拒绝的写入不应产生任何版本")
	}
}

// TestWriteAsAgent_AllowsProposalKind 是上一条测试的对照：kind==KindProposal
// 时应正常写入，证明拒绝逻辑只针对"非 Proposal"，不是把 agent 写死拒绝一切。
func TestWriteAsAgent_AllowsProposalKind(t *testing.T) {
	store := newFakeReadWriter()
	seq, err := WriteAsAgent(store, KindProposal, "proposal:abc123", []byte(`{"kind":"factor"}`))
	if err != nil {
		t.Fatalf("agent 写 Proposal kind 不应被拒绝: %v", err)
	}
	if seq != 1 {
		t.Fatalf("首次写入 seq 应为 1，实际 %d", seq)
	}
}

func TestUnauthorizedWriteError_ErrorStringContainsMachineReadableFields(t *testing.T) {
	err := &UnauthorizedWriteError{Role: RoleAgent, Kind: KindPaperAccount, Reason: "测试"}
	msg := err.Error()
	if msg == "" {
		t.Fatal("Error() 不应为空字符串")
	}
}
