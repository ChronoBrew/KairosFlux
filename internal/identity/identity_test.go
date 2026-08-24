package identity

import "testing"

func TestAgentSourceRoundTrip(t *testing.T) {
	source := AgentSource("quantscout-researcher-v1")
	if SourceRole(source) != RoleAgent {
		t.Fatalf("AgentSource 编码的 source 应判定为 RoleAgent，实际 %s", SourceRole(source))
	}
	id, ok := AgentID(source)
	if !ok || id != "quantscout-researcher-v1" {
		t.Fatalf("AgentID 应还原出原始标识，实际 id=%q ok=%v", id, ok)
	}
}

func TestSourceRole_EngineSourcesAreNotAgent(t *testing.T) {
	for _, source := range []string{"", "jobctl", "aiplane", "quantbrew-daily"} {
		if SourceRole(source) != RoleEngine {
			t.Fatalf("source=%q 应判定为 RoleEngine，实际 %s", source, SourceRole(source))
		}
		if _, ok := AgentID(source); ok {
			t.Fatalf("source=%q 不是 agent: 前缀，AgentID 应返回 ok=false", source)
		}
	}
}

func TestIsProposalKey(t *testing.T) {
	if !IsProposalKey("proposal:abc123") {
		t.Fatal("proposal: 前缀的键应判定为 Proposal 键")
	}
	for _, key := range []string{"strategy:obj:x", "job:spec:x", "evidence:factor:x:y", "proposalx:not-really"} {
		if IsProposalKey(key) {
			t.Fatalf("key=%q 不应判定为 Proposal 键", key)
		}
	}
}
