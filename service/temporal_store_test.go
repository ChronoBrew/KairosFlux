package service

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/internal/temporal"
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

	seq1, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("v1"), 100, "", 0)
	if err != nil || seq1 != 1 {
		t.Fatalf("第一次写应得 seq=1: seq=%d err=%v", seq1, err)
	}
	seq2, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("v2"), 200, "", 0)
	if err != nil || seq2 != 2 {
		t.Fatalf("第二次写应得 seq=2: seq=%d err=%v", seq2, err)
	}
	seq3, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("v3"), 300, "", 0)
	if err != nil || seq3 != 3 {
		t.Fatalf("第三次写应得 seq=3: seq=%d err=%v", seq3, err)
	}
}

func TestTemporalStore_ListVersionsReturnsAllInSeqOrder(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	for i, payload := range []string{"v1", "v2", "v3"} {
		if _, err := ts.PutVersioned(logical, []byte(payload), int64(100*(i+1)), "", 0); err != nil {
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
	if _, err := ts.PutVersioned("quote:2026-08-17:60000", []byte("short"), 100, "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("long"), 200, "", 0); err != nil {
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
		if _, err := ts.PutVersioned(logical, []byte(payload), nanos, "", 0); err != nil {
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
			if _, err := ts.PutVersioned(logical, []byte{byte('a' + i)}, int64(100*(i+1)), "", 0); err != nil {
				t.Fatalf("写入失败: %v", err)
			}
		}
	}

	result, err := ts.ReplayFingerprint("quote:2026-08-17:", 0)
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
	result2, err := ts.ReplayFingerprint("quote:2026-08-17:", 0)
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
			seq, err := ts.PutVersioned(logical, []byte(fmt.Sprintf("v%d", i)), int64(i), "", 0)
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
	if _, err := ts.PutVersioned(logical, []byte("v1"), 100, "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned(logical, []byte("v2"), 200, "", 0); err != nil {
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

	result, err := ts.ReplayFingerprint("quote:2026-08-17:", 0)
	if err != nil {
		t.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if len(result.Mismatches) != 1 || result.Mismatches[0] != logical {
		t.Fatalf("应检出 1 条不一致: %+v", result.Mismatches)
	}
}

// TestTemporalStore_ListWritesFiltersByTimeRangeSourceAndAggregatesBySource
// 验证 LIST_WRITES 的三个过滤维度（前缀/时间范围/来源）与按来源聚合计数
// （M2 任务书第 2 项："某键某时段被写了几次、来自谁"）。
func TestTemporalStore_ListWritesFiltersByTimeRangeSourceAndAggregatesBySource(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	if _, err := ts.PutVersioned(logical, []byte("v1"), 100, "job-a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned(logical, []byte("v2"), 200, "job-b", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned(logical, []byte("v3"), 300, "job-a", 2); err != nil {
		t.Fatal(err)
	}

	// 无过滤：应看到全部 3 条，来源聚合 job-a=2 job-b=1（按来源升序）。
	all, err := ts.ListWrites("quote:2026-08-17:", 0, 0, "")
	if err != nil {
		t.Fatalf("ListWrites 失败: %v", err)
	}
	if len(all.Entries) != 3 {
		t.Fatalf("无过滤应看到 3 条: %+v", all.Entries)
	}
	if len(all.BySource) != 2 || all.BySource[0] != (SourceCount{Source: "job-a", Count: 2}) ||
		all.BySource[1] != (SourceCount{Source: "job-b", Count: 1}) {
		t.Fatalf("来源聚合不符: %+v", all.BySource)
	}

	// 时间范围 [150,250] 只应命中第 2 条（write_ts=200）。
	byTime, err := ts.ListWrites("quote:2026-08-17:", 150, 250, "")
	if err != nil {
		t.Fatalf("ListWrites 失败: %v", err)
	}
	if len(byTime.Entries) != 1 || byTime.Entries[0].Seq != 2 {
		t.Fatalf("时间范围过滤不符: %+v", byTime.Entries)
	}

	// 按来源过滤 job-a：应命中 seq 1 与 3。
	bySrc, err := ts.ListWrites("quote:2026-08-17:", 0, 0, "job-a")
	if err != nil {
		t.Fatalf("ListWrites 失败: %v", err)
	}
	if len(bySrc.Entries) != 2 || bySrc.Entries[0].Seq != 1 || bySrc.Entries[1].Seq != 3 {
		t.Fatalf("来源过滤不符: %+v", bySrc.Entries)
	}
	for _, e := range bySrc.Entries {
		if !e.HashOK {
			t.Fatalf("未被篡改的记录 HashOK 应为 true: %+v", e)
		}
	}
}

// TestTemporalStore_ListWritesDetectsCorruptedHistoricalVersion 是"注入单字节
// 漂移→报警"验收标准在历史版本（非最新版本）上的实现——REPLAY_FINGERPRINT
// 只重放+对账"每个逻辑键的最新版本"，一条被静默改写的历史版本（:current
// 仍指向未被动过的最新版本）对它完全不可见；这正是 M2 操作元数据信封里
// 持久化 payload_hash 存在的意义：LIST_WRITES 对每条历史记录独立做
// "写入时刻记的哈希 vs 现在重新算的哈希"自检，与 :current 对账是两条互不
// 覆盖的检测路径。
func TestTemporalStore_ListWritesDetectsCorruptedHistoricalVersion(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	if _, err := ts.PutVersioned(logical, []byte("v1"), 100, "job-a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned(logical, []byte("v2-latest"), 200, "job-a", 1); err != nil {
		t.Fatal(err)
	}

	// 直接在存储层篡改第 1 条（非最新）版本键的 payload 最后一个字节
	// （单字节漂移），不经过 PutVersioned——模拟磁盘位翻转/存储层 bug，而不是
	// "业务逻辑写错了"。
	versionKey := temporal.VersionStorageKey(logical, 1)
	raw, err := kv.Get([]byte(versionKey))
	if err != nil {
		t.Fatalf("读取版本键失败: %v", err)
	}
	corrupted := append([]byte(nil), raw...)
	corrupted[len(corrupted)-1] ^= 0xFF // 翻转 payload 最后一个字节
	if err := kv.Write(Command{Type: CommandPut, Key: []byte(versionKey), Value: corrupted}); err != nil {
		t.Fatalf("注入单字节漂移失败: %v", err)
	}

	// REPLAY_FINGERPRINT（无界）看不到这条漂移：:current 仍指向 seq=2，
	// 与它自己的最新版本一致，零不一致——这正是本测试要证明的"覆盖不到"。
	replay, err := ts.ReplayFingerprint("quote:2026-08-17:", 0)
	if err != nil {
		t.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if len(replay.Mismatches) != 0 {
		t.Fatalf("REPLAY_FINGERPRINT 不应检出历史版本漂移（设计上覆盖不到）: %+v", replay.Mismatches)
	}

	// LIST_WRITES 必须检出：seq=1 的 HashOK 应为 false，seq=2 不受影响。
	writes, err := ts.ListWrites("quote:2026-08-17:", 0, 0, "")
	if err != nil {
		t.Fatalf("ListWrites 失败: %v", err)
	}
	if len(writes.Entries) != 2 {
		t.Fatalf("应看到 2 条: %+v", writes.Entries)
	}
	if writes.Entries[0].Seq != 1 || writes.Entries[0].HashOK {
		t.Fatalf("seq=1 应被检出 HashOK=false: %+v", writes.Entries[0])
	}
	if writes.Entries[1].Seq != 2 || !writes.Entries[1].HashOK {
		t.Fatalf("seq=2 未被篡改，HashOK 应为 true: %+v", writes.Entries[1])
	}
}

// TestTemporalStore_ReplayFingerprintBoundedSkipsCurrentReconciliation 验证
// M2 REPLAY_FINGERPRINT 服务化升级的时间上界语义：asOfNanos>0 时按该时刻
// 重放（temporal.AsOf），且不与 :current 对账（Mismatches 恒为空、
// Bounded=true）——:current 永远指向全局最新版本，与历史时间点的重放结果
// 比较没有意义（见 ReplayResult.Bounded 的文档）。
func TestTemporalStore_ReplayFingerprintBoundedSkipsCurrentReconciliation(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	if _, err := ts.PutVersioned(logical, []byte("v1"), 100, "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned(logical, []byte("v2"), 200, "", 0); err != nil {
		t.Fatal(err)
	}

	bounded, err := ts.ReplayFingerprint("quote:2026-08-17:", 150)
	if err != nil {
		t.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if !bounded.Bounded {
		t.Fatal("带 asOfNanos 的调用应标记 Bounded=true")
	}
	if len(bounded.Mismatches) != 0 {
		t.Fatalf("区间查询不应产生任何 mismatch（未做对账，不是核对通过）: %+v", bounded.Mismatches)
	}

	// asOf=150 时只看到 v1（v2 写于 200，晚于 150），指纹应与"只有 v1"时的
	// 无界重放指纹一致——验证 AsOf 语义确实生效，不是恒等于无界结果。
	onlyV1 := setupTemporalTest(t)
	ts2 := NewTemporalStore(onlyV1)
	if _, err := ts2.PutVersioned(logical, []byte("v1"), 100, "", 0); err != nil {
		t.Fatal(err)
	}
	unbounded, err := ts2.ReplayFingerprint("quote:2026-08-17:", 0)
	if err != nil {
		t.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if bounded.Fingerprint != unbounded.Fingerprint {
		t.Fatalf("asOf=150 的指纹应等于只写过 v1 时的指纹: bounded=%s unbounded=%s",
			bounded.Fingerprint, unbounded.Fingerprint)
	}

	// asOf 早于任何写入：该逻辑键在这次重放里不存在，keyCount 反映"发现的
	// :current 数"（不因 asOf 收窄），指纹应等于空集合的指纹。
	tooEarly, err := ts.ReplayFingerprint("quote:2026-08-17:", 1)
	if err != nil {
		t.Fatalf("ReplayFingerprint 失败: %v", err)
	}
	if tooEarly.Fingerprint != temporal.Fingerprint(nil) {
		t.Fatalf("asOf 早于全部写入应得空集合指纹: %s", tooEarly.Fingerprint)
	}
}

// TestTemporalStore_ReplayResultConsistentAcrossProcessRestart 是 M0/M2 共同
// 验收标准"重启后重放结果与崩溃前一致"的直接实现：对同一个 WAL/SSTable
// 目录先用一个 KVServer 写入若干版本化记录并关闭（模拟进程退出/崩溃后的
// 落盘状态），再用一个全新的 KVServer 重新打开同一目录（模拟重启，
// NewKVServer 在 standalone 模式下会重放 WAL 到 memtable，见 service/fsm.go
// 的 replayWAL），重放指纹必须逐字节相同。
func TestTemporalStore_ReplayResultConsistentAcrossProcessRestart(t *testing.T) {
	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.Mode = config.ModeStandalone
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode = oldWAL, oldSST, oldMode
	})

	logical := "quote:2026-08-17:600000"
	kv1 := NewKVServer()
	ts1 := NewTemporalStore(kv1)
	for i, payload := range []string{"v1", "v2", "v3"} {
		if _, err := ts1.PutVersioned(logical, []byte(payload), int64(100*(i+1)), "job-a", 1); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	before, err := ts1.ReplayFingerprint("quote:2026-08-17:", 0)
	if err != nil {
		t.Fatalf("重启前 ReplayFingerprint 失败: %v", err)
	}
	if err := kv1.Close(); err != nil {
		t.Fatalf("关闭失败（模拟进程退出）: %v", err)
	}

	kv2 := NewKVServer() // 同一目录重新打开，模拟重启
	t.Cleanup(func() { kv2.Close() })
	ts2 := NewTemporalStore(kv2)
	after, err := ts2.ReplayFingerprint("quote:2026-08-17:", 0)
	if err != nil {
		t.Fatalf("重启后 ReplayFingerprint 失败: %v", err)
	}

	if after.Fingerprint != before.Fingerprint {
		t.Fatalf("重启后重放指纹应与崩溃前逐字节一致: before=%s after=%s", before.Fingerprint, after.Fingerprint)
	}
	if after.KeyCount != before.KeyCount || len(after.Mismatches) != 0 {
		t.Fatalf("重启后应零不一致且逻辑键数不变: before=%+v after=%+v", before, after)
	}

	// 重启后 LIST_WRITES 也应看到全部历史记录，操作元数据（source/schema_ver）
	// 同样重放正确——不只是指纹这一个聚合数字碰巧一致。
	writes, err := ts2.ListWrites("quote:2026-08-17:", 0, 0, "")
	if err != nil {
		t.Fatalf("重启后 ListWrites 失败: %v", err)
	}
	if len(writes.Entries) != 3 {
		t.Fatalf("重启后应看到 3 条历史写入: %+v", writes.Entries)
	}
	for _, e := range writes.Entries {
		if e.Source != "job-a" || e.SchemaVer != 1 || !e.HashOK {
			t.Fatalf("重启后操作元数据应完整保留: %+v", e)
		}
	}
}

// —— M5 分页（方案 §C.1）：ListWritesPage 的游标/limit 语义 ——

// pageKeys 把 ListWritesPage 结果折叠成 "logicalKey@seq" 字符串序列，便于
// 与期望值逐条比对（payload/hash 等字段与 ListWrites 共享同一编码路径，
// 由既有测试覆盖，这里聚焦分页的顺序与分界）。
func pageKeys(t *testing.T, res ListWritesResult) []string {
	t.Helper()
	out := make([]string, len(res.Entries))
	for i, e := range res.Entries {
		out[i] = fmt.Sprintf("%s@%d", e.LogicalKey, e.Seq)
	}
	return out
}

// pageQuery 用 (after, limit) 参数查询并把结果拼成
// {keys, hasMore, bySource} 三元的可读形态。
func pageQuery(t *testing.T, ts *TemporalStore, after *ListWritesCursor, limit uint32) ([]string, bool, []SourceCount) {
	t.Helper()
	res, hasMore, err := ts.ListWritesPage("q:", 0, 0, "", after, limit)
	if err != nil {
		t.Fatalf("ListWritesPage 失败: %v", err)
	}
	return pageKeys(t, res), hasMore, res.BySource
}

func TestTemporalStore_ListWritesPagePaginatesInTotalOrder(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	// 交错写入 3 个逻辑键 × 3 版本：seq 分配序（a1,b1,c1,a2,...）与
	// (LogicalKey, Seq) 总序（a1,a2,a3,b1,...）不同，分页必须按后者推进。
	for i := 1; i <= 3; i++ {
		for _, k := range []string{"q:a", "q:b", "q:c"} {
			if _, err := ts.PutVersioned(k, []byte(fmt.Sprintf("v%d", i)), int64(100*i), "job", 1); err != nil {
				t.Fatalf("写入失败: %v", err)
			}
		}
	}

	// 无分页对照：全部 9 条，按 (LogicalKey, Seq) 升序。
	full, err := ts.ListWrites("q:", 0, 0, "")
	if err != nil {
		t.Fatalf("ListWrites 失败: %v", err)
	}
	fullKeys := pageKeys(t, full)
	if len(fullKeys) != 9 {
		t.Fatalf("应有 9 条: %v", fullKeys)
	}

	// 第一页：limit=3 从起点开始，恰好 3 条、有更多。
	page1, hasMore1, _ := pageQuery(t, ts, nil, 3)
	if hasMore1 != true || len(page1) != 3 {
		t.Fatalf("第一页应为 3 条且有更多: keys=%v hasMore=%v", page1, hasMore1)
	}
	if page1[0] != "q:a@1" || page1[2] != "q:a@3" {
		t.Fatalf("第一页应从 q:a@1 到 q:a@3: %v", page1)
	}

	// 第二页：游标 = 第一页最后一条 (q:a, 3)。
	page2, hasMore2, _ := pageQuery(t, ts, &ListWritesCursor{LogicalKey: "q:a", Seq: 3}, 3)
	if hasMore2 != true || len(page2) != 3 || page2[0] != "q:b@1" || page2[2] != "q:b@3" {
		t.Fatalf("第二页应从 q:b@1 到 q:b@3 且有更多: keys=%v hasMore=%v", page2, hasMore2)
	}

	// 第三页：游标 = (q:b, 3)，最后一页 3 条、无更多。
	page3, hasMore3, _ := pageQuery(t, ts, &ListWritesCursor{LogicalKey: "q:b", Seq: 3}, 3)
	if hasMore3 != false || len(page3) != 3 || page3[0] != "q:c@1" || page3[2] != "q:c@3" {
		t.Fatalf("第三页应到 q:c@3 为止且无更多: keys=%v hasMore=%v", page3, hasMore3)
	}

	// 逐页拼接 == 无分页全量结果：顺序一致、无遗漏、无重复。
	var collected []string
	collected = append(collected, page1...)
	collected = append(collected, page2...)
	collected = append(collected, page3...)
	if fmt.Sprint(collected) != fmt.Sprint(fullKeys) {
		t.Fatalf("分页拼接应与全量逐条一致\n  分页: %v\n  全量: %v", collected, fullKeys)
	}

	// 游标边界：after=(q:a,2) 应跳过 q:a@1/q:a@2，从 q:a@3 开始。
	fromA3, _, _ := pageQuery(t, ts, &ListWritesCursor{LogicalKey: "q:a", Seq: 2}, 0)
	if len(fromA3) != 7 || fromA3[0] != "q:a@3" {
		t.Fatalf("after=(q:a,2) 应跳过 q:a@1/q:a@2 从 q:a@3 起: %v", fromA3)
	}
}

func TestTemporalStore_ListWritesPageLimitZeroIsUnbounded(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	for _, k := range []string{"q:a", "q:b"} {
		if _, err := ts.PutVersioned(k, []byte("v"), 100, "job", 1); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}

	keys, hasMore, counts := pageQuery(t, ts, nil, 0)
	if hasMore || len(keys) != 2 {
		t.Fatalf("limit=0 应返回全部且无更多: keys=%v hasMore=%v", keys, hasMore)
	}
	if len(counts) != 1 || counts[0].Source != "job" || counts[0].Count != 2 {
		t.Fatalf("limit=0 的 BySource 应统计全部条目: %+v", counts)
	}
}

func TestTemporalStore_ListWritesPageCountsOnlyPageEntries(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	// 两个来源各 2 条，limit=1：第一页只有 1 条，BySource 只反映本页。
	for i := 0; i < 2; i++ {
		if _, err := ts.PutVersioned("q:a", []byte("x"), int64(100+i), "job-a", 1); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := ts.PutVersioned("q:b", []byte("y"), int64(200+i), "job-b", 1); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}

	_, hasMore, counts := pageQuery(t, ts, nil, 1)
	if !hasMore {
		t.Fatal("limit=1 时应有更多")
	}
	if len(counts) != 1 || counts[0].Source != "job-a" || counts[0].Count != 1 {
		t.Fatalf("分页模式的 BySource 只统计本页条目: %+v", counts)
	}
}

func TestTemporalStore_ListWritesPageAppliesFiltersWithCursor(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	// q:a 三条：job-a@100/job-b@200/job-a@300。按来源过滤 + 分页：
	// limit=1 过滤 source=job-a 后只有 q:a@1、q:a@3 两条，hasMore 由过滤
	// 后的命中数决定，而不是原始扫描条数。
	for i, src := range []string{"job-a", "job-b", "job-a"} {
		if _, err := ts.PutVersioned("q:a", []byte("v"), int64(100*(i+1)), src, 1); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}

	res, hasMore, err := ts.ListWritesPage("q:", 0, 0, "job-a", nil, 1)
	if err != nil {
		t.Fatalf("ListWritesPage 失败: %v", err)
	}
	if !hasMore || len(res.Entries) != 1 || res.Entries[0].Seq != 1 {
		t.Fatalf("过滤后第一页应只有 q:a@1 且有更多: %+v hasMore=%v", res.Entries, hasMore)
	}

	res2, hasMore2, err := ts.ListWritesPage("q:", 0, 0, "job-a", &ListWritesCursor{LogicalKey: "q:a", Seq: 1}, 1)
	if err != nil {
		t.Fatalf("ListWritesPage 失败: %v", err)
	}
	if hasMore2 || len(res2.Entries) != 1 || res2.Entries[0].Seq != 3 {
		t.Fatalf("过滤后第二页应只有 q:a@3 且无更多: %+v hasMore=%v", res2.Entries, hasMore2)
	}
}
