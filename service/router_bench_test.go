package service

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/predicate"
	"github.com/NeverENG/BanDB/proto"
	"github.com/NeverENG/BanDB/service/ingesthook"
)

// benchKeyCardinality 是基准预生成的负载数量：足够大以避免同 key 反复覆写
// 掩盖真实的写放大特征，又足够小以便一次性生成、不占用计时区间。
const benchKeyCardinality = 1000

// benchQuotePayload 返回真实行情快照负载，字段形状对齐
// service/ingesthook/schema/quote.go 的 QuoteSnapshot 校验规则（必填
// code/date/open/high/low/close/volume，可选 prev_close），使基准跑的是
// schema 校验器真正会执行的那条路径，而不是一个未纳管类型被直接放行的空载。
func benchQuotePayload(code string) []byte {
	return []byte(fmt.Sprintf(
		`{"code":%q,"date":"2026-08-19","open":10.0,"high":10.5,"low":9.8,"close":10.2,"volume":1000000,"prev_close":10.0}`,
		code,
	))
}

// benchPutRequests 预生成 n 个 PUT 请求替身（key=quote:<date>:<编号>，value=
// 对应行情负载），复用同一个 fakeConn。请求构造（fmt.Sprintf 拼 key/value、
// 编帧、两次结构体分配）刻意放在 b.ResetTimer() 之前完成，不让这些与被测
// 逻辑无关的分配计入 ns/op、allocs/op。
func benchPutRequests(n int) []*fakePreHandleReq {
	conn := &fakeConn{}
	reqs := make([]*fakePreHandleReq, n)
	for i := 0; i < n; i++ {
		code := fmt.Sprintf("%06d", i)
		key := []byte("quote:2026-08-19:" + code)
		reqs[i] = &fakePreHandleReq{conn: conn, data: proto.EncodePutFrame(key, benchQuotePayload(code))}
	}
	return reqs
}

// setupBenchKV 按 standalone 模式构造一个独立临时目录的 KVServer，模式与
// service/scan_integration_test.go 的 TestKVServer_Scan 一致，供落盘基准复用。
// MaxMemTableSize 取大，使基准期间不触发 flush，专注测 Router 链路本身
// （PreHandle 校验/脱敏 + 帧解析 + WAL/memtable 写入），不是 flush/compaction
// 的开销（那部分已有独立 benchmark，见 storage/bench_test.go）。
func setupBenchKV(b *testing.B) *KVServer {
	b.Helper()
	dir := b.TempDir()
	oldWAL, oldSST, oldMode, oldMax := config.G.WALPath, config.G.SSTablePath, config.G.Mode, config.G.MaxMemTableSize
	config.G.Mode = config.ModeStandalone
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.MaxMemTableSize = 1 << 30
	b.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode, config.G.MaxMemTableSize = oldWAL, oldSST, oldMode, oldMax
	})
	kv := NewKVServer()
	b.Cleanup(func() { kv.Close() })
	return kv
}

// benchNopStore 是不落盘的 KVStore 实现，供 BenchmarkRouterHandlePut_NoDisk
// 隔离出「Router 全链路（PreHandle 校验/脱敏 + 帧解析）」本身的开销，把
// WAL fsync（比网关层清洗逻辑慢约三个数量级，会把后者的差异淹没在噪声里）
// 排除在计时之外。
type benchNopStore struct{}

func (benchNopStore) Write(cmd Command) error { return nil }
func (benchNopStore) Get(key []byte) ([]byte, error) {
	return nil, nil
}
func (benchNopStore) Scan(start, end []byte, pred predicate.Predicate, limit int) []proto.ScanEntry {
	return nil
}

// BenchmarkRouterHandlePut 测网关写入的真实全链路（含磁盘 fsync）：PreHandle
// （ingesthook 的 schema 校验 + 脱敏）+ Handle（Router.handlePut 解帧 +
// KVServer.Write 落盘），接线方式与生产 cmd/ban-server/server.go 一致（同一组
// redactFields/maxValueLen/dropBackward 参数，同样挂 filter.Handle 为
// PreHandle）。WAL fsync 主导此基准的 ns/op（约 3-4ms/op，比网关层清洗逻辑
// 高三个数量级），因此本基准的 ns/op 不适合用来评估网关层优化的收益——
// 那部分收益看 BenchmarkRouterHandlePut_NoDisk 与 allocs/op。
func BenchmarkRouterHandlePut(b *testing.B) {
	kv := setupBenchKV(b)
	router := NewRouter(kv)
	filter := ingesthook.NewFilter([]string{"gps", "user_id"}, 0, true)
	router.SetPreHandle(filter.Handle)
	reqs := benchPutRequests(benchKeyCardinality)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := reqs[i%benchKeyCardinality]
		if action := router.PreHandle(req); action != bannet.HookPass {
			b.Fatalf("第 %d 条基准负载被意外丢弃", i)
		}
		router.Handle(req)
	}
}

// BenchmarkRouterHandlePut_NoDisk 与 BenchmarkRouterHandlePut 走同一条
// PreHandle+Handle 全链路，唯一区别是落盘换成 benchNopStore（无 I/O）。
// 这是评估「合并双重反序列化」等网关层优化的主基准：fsync 被排除后，
// ns/op 与 allocs/op 才能真实反映 PreHandle（schema 校验 + 脱敏）+ 帧解析
// 本身的开销变化。
func BenchmarkRouterHandlePut_NoDisk(b *testing.B) {
	router := NewRouterWithStore(benchNopStore{})
	filter := ingesthook.NewFilter([]string{"gps", "user_id"}, 0, true)
	router.SetPreHandle(filter.Handle)
	reqs := benchPutRequests(benchKeyCardinality)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := reqs[i%benchKeyCardinality]
		if action := router.PreHandle(req); action != bannet.HookPass {
			b.Fatalf("第 %d 条基准负载被意外丢弃", i)
		}
		router.Handle(req)
	}
}

// BenchmarkKVServerWriteDirect 是跳过 Router 全链路（PreHandle 校验/脱敏 +
// handlePut 帧解析）、直调 KVServer.Write 落盘的对照组：与
// BenchmarkRouterHandlePut 的差值即网关层（解帧 + 清洗）本身的开销（同样受
// fsync 主导，读法与 BenchmarkRouterHandlePut 的说明一致）。
func BenchmarkKVServerWriteDirect(b *testing.B) {
	kv := setupBenchKV(b)
	keys := make([][]byte, benchKeyCardinality)
	values := make([][]byte, benchKeyCardinality)
	for i := 0; i < benchKeyCardinality; i++ {
		code := fmt.Sprintf("%06d", i)
		keys[i] = []byte("quote:2026-08-19:" + code)
		values[i] = benchQuotePayload(code)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % benchKeyCardinality
		if err := kv.Write(Command{Type: CommandPut, Key: keys[idx], Value: values[idx]}); err != nil {
			b.Fatalf("写入失败: %v", err)
		}
	}
}
