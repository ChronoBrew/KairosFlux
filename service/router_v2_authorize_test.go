package service

// 协议层角色强制的黄金测试（M4 上报缺口的修复）：PUT_VERSIONED 帧携带的
// source 由 identity.SourceRole 判定角色——agent 身份（"agent:"+agentID）只
// 允许写 Proposal 键空间（identity.IsProposalKey），写任何其它 kind 一律
// 结构化拒绝（codec.ErrCodeUnauthorizedRole=0x4001，reason=
// agent_write_forbidden_kind），且被拒绝的写入不产生版本、不计入
// metrics.Writes，只计入 metrics.DroppedUnauthorized。对照：agent 身份写
// Proposal key 通过、引擎身份（jobctl/aiplane/空 source）写任意 key 通过
// （空 source 即 M0 的旧帧字节，回归红线：既有路径零影响）。
//
// 复用 router_v2_integration_test.go 的真实服务端基建
// （startRouterV2TestServer）与 router_v2_temporal_test.go 的最小 v2 客户端
// （putVersionedWithSource/listVersions）——不新起一套测试基建。

import (
	"testing"

	"github.com/ChronoBrew/KairosFlux/internal/identity"
	"github.com/ChronoBrew/KairosFlux/internal/metrics"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// TestRouterV2_AgentSourceRejectsNonProposalKey 是修复的核心断言：绕过
// internal/aiplane.WriteAsAgent、直接用 PUT_VERSIONED 帧声明 agent 身份并写
// 非 Proposal kind，必须在协议层被结构化拒绝，且零副作用（不产生版本）。
func TestRouterV2_AgentSourceRejectsNonProposalKey(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	before := metrics.Take()

	msg := c.putVersionedWithSource(1, "reading:2026-08-25:600000", "v1", identity.AgentSource("test-agent"))
	if msg.Header.Opcode != codec.OpcodeErr {
		t.Fatalf("agent 身份写非 Proposal kind 应被拒绝, opcode=%#x", msg.Header.Opcode)
	}
	code, reason, ok := proto.DecodeV2ErrPayload(msg.Payload)
	if !ok {
		t.Fatalf("ERR 负载应能解出 V2ErrPayload: % x", msg.Payload)
	}
	if code != codec.ErrCodeUnauthorizedRole {
		t.Fatalf("错误码=%#x，期望 ErrCodeUnauthorizedRole(0x4001)", code)
	}
	if reason != "agent_write_forbidden_kind" {
		t.Fatalf("reason=%q，期望 agent_write_forbidden_kind", reason)
	}

	// 副作用为零：同一逻辑键下没有产生任何版本。
	listMsg := c.listVersions(2, "reading:2026-08-25:600000")
	if listMsg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("LIST_VERSIONS 应成功, opcode=%#x", listMsg.Header.Opcode)
	}
	versions, ok := proto.DecodeListVersionsResponse(listMsg.Payload)
	if !ok || len(versions) != 0 {
		t.Fatalf("被拒绝的写入不应产生版本: versions=%+v ok=%v", versions, ok)
	}

	after := metrics.Take()
	if got := after.DroppedUnauthorized - before.DroppedUnauthorized; got != 1 {
		t.Fatalf("metrics.DroppedUnauthorized 增量=%d，期望 1", got)
	}
	if got := after.Writes - before.Writes; got != 0 {
		t.Fatalf("被拒绝的写入不得计入 metrics.Writes，增量=%d", got)
	}
}

// TestRouterV2_AgentSourceWritesProposalKey 是对照：同一 agent 身份写
// Proposal 键空间（identity.ProposalKeyPrefix 拼键，与 aiplane.ProposalKey
// 用的是同一个前缀常量）必须放行。
func TestRouterV2_AgentSourceWritesProposalKey(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	key := identity.ProposalKeyPrefix + "fp-1234567890abcdef"
	msg := c.putVersionedWithSource(1, key, "payload", identity.AgentSource("test-agent"))
	if msg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("agent 身份写 Proposal kind 应放行, opcode=%#x", msg.Header.Opcode)
	}
	if len(msg.Payload) != 8 {
		t.Fatalf("OK 负载应是 8 字节 seq, got %d 字节", len(msg.Payload))
	}

	// 同一逻辑键确实落了一条版本，source 以 agent 身份记录。
	listMsg := c.listVersions(2, key)
	if listMsg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("LIST_VERSIONS 应成功, opcode=%#x", listMsg.Header.Opcode)
	}
	versions, ok := proto.DecodeListVersionsResponse(listMsg.Payload)
	if !ok || len(versions) != 1 {
		t.Fatalf("应恰好 1 条版本: versions=%+v ok=%v", versions, ok)
	}
	if string(versions[0].Payload) != "payload" {
		t.Fatalf("版本负载=%q，期望 payload", versions[0].Payload)
	}

	writes := c.listWrites(3, identity.ProposalKeyPrefix, 0, 1<<62, identity.AgentSource("test-agent"))
	if writes.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("LIST_WRITES 应成功, opcode=%#x", writes.Header.Opcode)
	}
	entries, _, ok := proto.DecodeListWritesResponse(writes.Payload)
	if !ok || len(entries) != 1 {
		t.Fatalf("按 agent source 过滤应恰好 1 条写入: entries=%+v ok=%v", entries, ok)
	}
}

// TestRouterV2_EngineSourceWritesAnyKey 是对照：引擎身份（带内组件名称
// jobctl/aiplane，不带 agent: 前缀）与不带 source 的旧帧（M0 字节兼容路径）
// 写任意 key 均不受 agent 限制约束。
func TestRouterV2_EngineSourceWritesAnyKey(t *testing.T) {
	addr := startRouterV2TestServer(t, DefaultV2WindowSafetyValveN)
	c := dialV2(t, addr, negotiate.AckEvery)
	defer c.close()

	for i, src := range []string{"jobctl", "aiplane", ""} {
		msg := c.putVersionedWithSource(uint32(i+1), "engine:key", "v", src)
		if msg.Header.Opcode != codec.OpcodeOK {
			t.Fatalf("引擎身份 source=%q 写任意 key 应放行, opcode=%#x", src, msg.Header.Opcode)
		}
		if len(msg.Payload) != 8 {
			t.Fatalf("OK 负载应是 8 字节 seq, got %d 字节", len(msg.Payload))
		}
	}

	// 三条写入全部落版本，seq 严格递增（1..3）。
	listMsg := c.listVersions(10, "engine:key")
	if listMsg.Header.Opcode != codec.OpcodeOK {
		t.Fatalf("LIST_VERSIONS 应成功, opcode=%#x", listMsg.Header.Opcode)
	}
	versions, ok := proto.DecodeListVersionsResponse(listMsg.Payload)
	if !ok || len(versions) != 3 {
		t.Fatalf("应恰好 3 条版本: versions=%+v ok=%v", versions, ok)
	}
	for i, v := range versions {
		if v.Seq != uint64(i+1) {
			t.Fatalf("seq=%d，期望 %d（严格递增）", v.Seq, i+1)
		}
	}
}
