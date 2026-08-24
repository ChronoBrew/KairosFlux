package aiplane

import "testing"

func TestProposal_ValidateRejectsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name  string
		p     Proposal
		field string
	}{
		{"未知kind", Proposal{Kind: "not_a_kind", SubmittedBy: "x", Summary: "s"}, "kind"},
		{"缺submitted_by", Proposal{Kind: ProposalHypothesis, Summary: "s"}, "submitted_by"},
		{"缺summary", Proposal{Kind: ProposalHypothesis, SubmittedBy: "x"}, "summary"},
		{"factor缺factor_name", Proposal{Kind: ProposalFactor, SubmittedBy: "x", Summary: "s"}, "factor_name"},
		{"experiment缺fingerprint", Proposal{Kind: ProposalExperiment, SubmittedBy: "x", Summary: "s"}, "experiment_fingerprint"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if err == nil {
				t.Fatal("应返回校验错误")
			}
			ve, ok := err.(*ProposalValidationError)
			if !ok {
				t.Fatalf("错误类型应为 *ProposalValidationError，实际 %T", err)
			}
			if ve.Field != tc.field {
				t.Fatalf("Field 应为 %s，实际 %s", tc.field, ve.Field)
			}
		})
	}
}

func TestSubmitProposal_WritesAndIsIdempotentOnIdenticalContent(t *testing.T) {
	store := newFakeReadWriter()
	p := Proposal{
		Kind:        ProposalFactor,
		SubmittedBy: "quantscout-researcher-v1",
		FactorName:  "amihud_illiq",
		Summary:     "amihud 非流动性因子假设",
	}

	fp1, seq1, err := SubmitProposal(store, p)
	if err != nil {
		t.Fatalf("首次提交失败: %v", err)
	}
	if seq1 != 1 {
		t.Fatalf("首次提交 seq 应为 1，实际 %d", seq1)
	}
	if store.versionCount(ProposalKey(fp1)) != 1 {
		t.Fatalf("proposal:%s 应恰好 1 条版本", fp1)
	}

	fp2, seq2, err := SubmitProposal(store, p)
	if err != nil {
		t.Fatalf("重复提交失败: %v", err)
	}
	if fp2 != fp1 {
		t.Fatalf("相同内容应产生相同指纹: fp1=%s fp2=%s", fp1, fp2)
	}
	if seq2 != 0 {
		t.Fatalf("重复提交应幂等（seq=0 表示未产生新版本），实际 seq=%d", seq2)
	}
	if store.versionCount(ProposalKey(fp1)) != 1 {
		t.Fatal("重复提交不应产生新版本")
	}
}

func TestSubmitProposal_DifferentContentProducesDifferentFingerprint(t *testing.T) {
	store := newFakeReadWriter()
	p1 := Proposal{Kind: ProposalHypothesis, SubmittedBy: "a", Summary: "假设A"}
	p2 := Proposal{Kind: ProposalHypothesis, SubmittedBy: "a", Summary: "假设B"}

	fp1, _, err := SubmitProposal(store, p1)
	if err != nil {
		t.Fatalf("提交 p1 失败: %v", err)
	}
	fp2, _, err := SubmitProposal(store, p2)
	if err != nil {
		t.Fatalf("提交 p2 失败: %v", err)
	}
	if fp1 == fp2 {
		t.Fatal("不同内容的提议不应产生相同指纹")
	}
}

func TestSubmitProposal_RejectsInvalidProposal(t *testing.T) {
	store := newFakeReadWriter()
	_, _, err := SubmitProposal(store, Proposal{Kind: ProposalFactor, SubmittedBy: "a", Summary: "s"})
	if err == nil {
		t.Fatal("缺 factor_name 的 factor 提议应被拒绝")
	}
	if _, ok := err.(*ProposalValidationError); !ok {
		t.Fatalf("错误类型应为 *ProposalValidationError，实际 %T", err)
	}
}
