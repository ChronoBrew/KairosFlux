package kvgrpc

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/service"
	"github.com/NeverENG/BanDB/service/ingesthook"
)

// newStandaloneKVServer 构造一个 standalone 模式（不启 Raft）的 KVServer，
// 每个测试用独立 WAL 文件避免互相污染，t.Cleanup 里清理配置与文件。
func newStandaloneKVServer(t *testing.T) *service.KVServer {
	t.Helper()
	oldMode := config.G.Mode
	oldWAL := config.G.WALPath
	config.G.Mode = config.ModeStandalone
	config.G.WALPath = "test_grpc_wal_" + time.Now().Format("20060102150405.000000000") + ".log"
	t.Cleanup(func() {
		os.Remove(config.G.WALPath)
		config.G.Mode = oldMode
		config.G.WALPath = oldWAL
	})
	return service.NewKVServer()
}

// TestGRPCServer_PutGetStandalone 锁定回归：standalone 模式下 KVServer.raft 为 nil，
// gRPC 的 Put 走 kv.Write() 而非直接 AppendEntry，不再 nil 指针 panic。
func TestGRPCServer_PutGetStandalone(t *testing.T) {
	srv := NewGRPCServer(newStandaloneKVServer(t), nil)

	putResp, err := srv.Put(context.Background(), &PutRequest{Key: []byte("k1"), Value: []byte("v1")})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if !putResp.Success {
		t.Fatalf("Put failed in standalone mode")
	}

	getResp, err := srv.Get(context.Background(), &GetRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !getResp.Success || string(getResp.Value) != "v1" {
		t.Fatalf("Get returned success=%v value=%q, want true/v1", getResp.Success, getResp.Value)
	}
}

// TestGRPCServer_GetMissingKeyReturnsFailure 覆盖 Get 的未命中路径（此前 6.5% 覆盖率
// 下完全没有测试到）。
func TestGRPCServer_GetMissingKeyReturnsFailure(t *testing.T) {
	srv := NewGRPCServer(newStandaloneKVServer(t), nil)

	getResp, err := srv.Get(context.Background(), &GetRequest{Key: []byte("does-not-exist")})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if getResp.Success {
		t.Fatalf("Get 未命中的 key 应返回 Success=false")
	}
}

// TestGRPCServer_DeleteRoundtrip 覆盖 Delete：写入后删除，再 Get 应未命中。
func TestGRPCServer_DeleteRoundtrip(t *testing.T) {
	srv := NewGRPCServer(newStandaloneKVServer(t), nil)
	ctx := context.Background()

	if _, err := srv.Put(ctx, &PutRequest{Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	delResp, err := srv.Delete(ctx, &DeleteRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if !delResp.Success {
		t.Fatalf("Delete 应成功")
	}
	getResp, err := srv.Get(ctx, &GetRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if getResp.Success {
		t.Fatalf("删除后 Get 应未命中")
	}
}

// TestGRPCServer_NilFilterSkipsValidation 锁定历史行为：filter=nil 时 Put 直接写入，
// 不做任何清洗（仅供不关心清洗的场景使用，生产构造必须传非 nil filter）。
func TestGRPCServer_NilFilterSkipsValidation(t *testing.T) {
	srv := NewGRPCServer(newStandaloneKVServer(t), nil)

	// 一条不满足 quote schema（缺必填字段）的记录，filter=nil 时应直接写入成功。
	resp, err := srv.Put(context.Background(), &PutRequest{
		Key:   []byte("quote:2026-08-17:600000"),
		Value: []byte(`{"open":-1}`),
	})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("filter=nil 时应跳过校验直接写入成功")
	}
}

// TestGRPCServer_PutRejectsOversizedValue 验证 gRPC Put 现在会经过与 bannet 入口
// 相同的 value 长度限制——这是清洗缺口修复后新增的行为。
func TestGRPCServer_PutRejectsOversizedValue(t *testing.T) {
	filter := ingesthook.NewFilter(nil, 4, false) // maxValueLen=4
	srv := NewGRPCServer(newStandaloneKVServer(t), filter)
	ctx := context.Background()

	resp, err := srv.Put(ctx, &PutRequest{Key: []byte("k1"), Value: []byte("toolong")})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if resp.Success {
		t.Fatalf("超长 value 应被 Filter.Validate 拒绝，Put 不应成功")
	}
	// 拒绝的记录不应落盘。
	getResp, _ := srv.Get(ctx, &GetRequest{Key: []byte("k1")})
	if getResp.Success {
		t.Fatalf("被拒绝的记录不应写入存储层")
	}
}

// TestGRPCServer_PutRejectsSchemaViolation 验证 gRPC Put 现在会经过 schema 校验：
// quote: 前缀的记录若不满足 QuoteSnapshot 规则（此处 open<=0），应被拒绝且不落盘。
func TestGRPCServer_PutRejectsSchemaViolation(t *testing.T) {
	filter := ingesthook.NewFilter(nil, 0, false)
	srv := NewGRPCServer(newStandaloneKVServer(t), filter)
	ctx := context.Background()

	key := []byte("quote:2026-08-17:600000")
	badValue := []byte(`{"code":"600000","date":"2026-08-17","open":-1,"high":10.5,"low":9.8,"close":10.2,"volume":1000000}`)

	resp, err := srv.Put(ctx, &PutRequest{Key: key, Value: badValue})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if resp.Success {
		t.Fatalf("非正价格的行情记录应被 schema 校验拒绝")
	}
	getResp, _ := srv.Get(ctx, &GetRequest{Key: key})
	if getResp.Success {
		t.Fatalf("被 schema 拒绝的记录不应写入存储层")
	}
}

// TestGRPCServer_PutAcceptsValidQuoteAndAppliesRedaction 验证合法行情记录经 gRPC 写入
// 成功，且脱敏字段（若配置）在落盘前被改写——脱敏逻辑与 bannet 入口共用同一个
// Filter.Validate，行为应一致。
func TestGRPCServer_PutAcceptsValidQuoteAndAppliesRedaction(t *testing.T) {
	filter := ingesthook.NewFilter([]string{"analyst_note"}, 0, false)
	srv := NewGRPCServer(newStandaloneKVServer(t), filter)
	ctx := context.Background()

	key := []byte("quote:2026-08-17:600000")
	value := []byte(`{"code":"600000","date":"2026-08-17","open":10,"high":10.5,"low":9.8,"close":10.2,"volume":1000000,"analyst_note":"secret"}`)

	resp, err := srv.Put(ctx, &PutRequest{Key: key, Value: value})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("合法行情记录应写入成功")
	}

	getResp, err := srv.Get(ctx, &GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !getResp.Success {
		t.Fatalf("写入成功的 key 应能读回")
	}
	if got := string(getResp.Value); !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("落盘的 value 应包含脱敏占位符，得到: %s", got)
	}
}
