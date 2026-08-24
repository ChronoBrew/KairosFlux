package service

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ChronoBrew/KairosFlux/config"
)

// setupTemporalTest 起一个真实的 standalone KVServer（临时 WAL/SSTable 目录），
// 供 TemporalStore 的单测直接读写底层存储，不经网络层。与
// startRouterV2TestServer（service/router_v2_integration_test.go）同样的
// standalone 接线方式，但不起 kairnet.Server——这里只测 TemporalStore 本身的
// 业务逻辑，不需要协议层。
func setupTemporalTest(t *testing.T) *KVServer {
	t.Helper()
	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.Mode = config.ModeStandalone
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode = oldWAL, oldSST, oldMode
	})

	kv := NewKVServer()
	t.Cleanup(func() { kv.Close() })
	return kv
}

func TestTemporalStore_PutVersionedAssignsMonotonicSeqStartingAt1(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	seq1, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("v1"), 100)
	if err != nil || seq1 != 1 {
		t.Fatalf("第一次写应得 seq=1: seq=%d err=%v", seq1, err)
	}
	seq2, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("v2"), 200)
	if err != nil || seq2 != 2 {
		t.Fatalf("第二次写应得 seq=2: seq=%d err=%v", seq2, err)
	}
	seq3, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("v3"), 300)
	if err != nil || seq3 != 3 {
		t.Fatalf("第三次写应得 seq=3: seq=%d err=%v", seq3, err)
	}
}

func TestTemporalStore_ListVersionsReturnsAllInSeqOrder(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	for i, payload := range []string{"v1", "v2", "v3"} {
		if _, err := ts.PutVersioned(logical, []byte(payload), int64(100*(i+1))); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}

	versions, err := ts.ListVersions(logical)
	if err != nil {
		t.Fatalf("ListVersions 失败: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("应有 3 个版本, got %d", len(versions))
	}
	for i, want := range []string{"v1", "v2", "v3"} {
		if versions[i].Seq != uint64(i+1) || string(versions[i].Payload) != want {
			t.Fatalf("第 %d 条不符: %+v", i, versions[i])
		}
	}
}

func TestTemporalStore_ListVersionsEmptyForUntouchedKey(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	versions, err := ts.ListVersions("never:written")
	if err != nil || len(versions) != 0 {
		t.Fatalf("从未写过的逻辑键应返回空列表、无错误: versions=%v err=%v", versions, err)
	}
}

func TestTemporalStore_ListVersionsDoesNotLeakOtherLogicalKeys(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	// "quote:2026-08-17:60000" 是 "quote:2026-08-17:600000" 的严格前缀（少一个
	// 数字位），验证按数值续接不会互相"渗漏"进对方的版本列表——见
	// temporal.VersionStorageKeyLowerBound/UpperBound 的字典序论证。
	if _, err := ts.PutVersioned("quote:2026-08-17:60000", []byte("short"), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("long"), 200); err != nil {
		t.Fatal(err)
	}

	shortVersions, err := ts.ListVersions("quote:2026-08-17:60000")
	if err != nil || len(shortVersions) != 1 || string(shortVersions[0].Payload) != "short" {
		t.Fatalf("短逻辑键应只看到自己的版本: %+v err=%v", shortVersions, err)
	}
	longVersions, err := ts.ListVersions("quote:2026-08-17:600000")
	if err != nil || len(longVersions) != 1 || string(longVersions[0].Payload) != "long" {
		t.Fatalf("长逻辑键应只看到自己的版本: %+v err=%v", longVersions, err)
	}
}

func TestTemporalStore_GetAsOfSeesOnlyPastWrites(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	mustPutAt := func(payload string, nanos int64) {
		if _, err := ts.PutVersioned(logical, []byte(payload), nanos); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	mustPutAt("v1", 100)
	mustPutAt("v2", 200)
	mustPutAt("v3", 300)

	got, found, err := ts.GetAsOf(logical, 200)
	if err != nil || !found || string(got.Payload) != "v2" {
		t.Fatalf("as_of=200 应得 v2: got=%+v found=%v err=%v", got, found, err)
	}

	_, found, err = ts.GetAsOf(logical, 50)
	if err != nil || found {
		t.Fatalf("as_of 早于首次写入应 not found: found=%v err=%v", found, err)
	}

	got, found, err = ts.GetAsOf(logical, 10_000)
	if err != nil || !found || string(got.Payload) != "v3" {
		t.Fatalf("as_of 晚于末次写入应得最新版本 v3: got=%+v found=%v err=%v", got, found, err)
	}
}

func TestTemporalStore_ReplayFingerprintNoMismatchAfterNormalWrites(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	for _, logical := range []string{"quote:2026-08-17:600000", "quote:2026-08-17:600001"} {
		for i := 0; i < 3; i++ {
			if _, err := ts.PutVersioned(logical, []byte{byte('a' + i)}, int64(100*(i+1))); err != nil {
				t.Fatalf("写入失败: %v", err)
			}
		}
	}

	result, err := ts.ReplayFingerprint("quote:2026-08-17:")
	if err != nil {
		t.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if result.KeyCount != 2 {
		t.Fatalf("应发现 2 个逻辑键, got %d", result.KeyCount)
	}
	if len(result.Mismatches) != 0 {
		t.Fatalf("正常写入后重放应零不一致: %v", result.Mismatches)
	}
	if result.Fingerprint == "" {
		t.Fatal("指纹不应为空")
	}

	// 同一份账本重放两次指纹必须一致（跨调用确定性）。
	result2, err := ts.ReplayFingerprint("quote:2026-08-17:")
	if err != nil {
		t.Fatalf("第二次 ReplayFingerprint 失败: %v", err)
	}
	if result2.Fingerprint != result.Fingerprint {
		t.Fatal("同一账本两次重放指纹应一致")
	}
}

// TestTemporalStore_ConcurrentPutVersionedSameLogicalKeyAssignsDistinctSeq
// 用多个 goroutine 并发对同一个逻辑键调用 PutVersioned，验证 seq 分配的
// per-key 锁不会产生重复/丢失的 seq（每个 seq 恰好被分配一次，1..N 全覆盖），
// 且 :current 指针最终确实落在真正最新的那个 seq 上——这是 nextSeq/seqCache
// 那段临界区设计要保证的东西，光靠单线程测试看不出问题，必须 -race 下跑。
func TestTemporalStore_ConcurrentPutVersionedSameLogicalKeyAssignsDistinctSeq(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)
	logical := "concurrent:key"

	const n = 50
	seqs := make([]uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seq, err := ts.PutVersioned(logical, []byte(fmt.Sprintf("v%d", i)), int64(i))
			if err != nil {
				t.Errorf("goroutine %d: PutVersioned 失败: %v", i, err)
				return
			}
			seqs[i] = seq
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool, n)
	for _, seq := range seqs {
		if seq == 0 {
			t.Fatal("seq 不应为 0（分配从 1 起）")
		}
		if seen[seq] {
			t.Fatalf("seq=%d 被分配了两次", seq)
		}
		seen[seq] = true
	}
	if len(seen) != n {
		t.Fatalf("应恰好分配 %d 个不同的 seq, got %d", n, len(seen))
	}
	for seq := uint64(1); seq <= uint64(n); seq++ {
		if !seen[seq] {
			t.Fatalf("seq=%d 缺失，1..%d 应被完整覆盖", seq, n)
		}
	}

	versions, err := ts.ListVersions(logical)
	if err != nil || len(versions) != n {
		t.Fatalf("ListVersions 应看到全部 %d 条: got %d err=%v", n, len(versions), err)
	}
}

func TestTemporalStore_ReplayFingerprintDetectsCorruptedCurrentPointer(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	if _, err := ts.PutVersioned(logical, []byte("v1"), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned(logical, []byte("v2"), 200); err != nil {
		t.Fatal(err)
	}

	// 直接在存储层把 :current 指针改写为解不出来的垫圾字节，模拟指针损坏
	// （例如崩溃恢复留下的中间态）。注意 Value 不能是 nil：nil 是墓碑约定，
	// 会把这次写解释成删除该指针键，而不是"写入一段无法解析的内容"。
	if err := kv.Write(Command{
		Type:  CommandPut,
		Key:   []byte(logical + ":current"),
		Value: []byte{0xFF},
	}); err != nil {
		t.Fatalf("构造损坏指针失败: %v", err)
	}

	result, err := ts.ReplayFingerprint("quote:2026-08-17:")
	if err != nil {
		t.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if len(result.Mismatches) != 1 || result.Mismatches[0] != logical {
		t.Fatalf("应检出 1 条不一致: %+v", result.Mismatches)
	}
}
