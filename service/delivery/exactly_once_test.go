package delivery

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ChronoBrew/KairosFlux/proto"
)

// 本文件是「exactly-once 正确性压测台」：在崩溃点注入故障，量化重复/丢失条数，
// 对照 plain FileSink（at-least-once，会重复）与 IdempotentFileSink（effectively-once）。
//
// 结论用断言固化：plain 在崩溃下重复>0；idempotent 在同样崩溃下 0 重复 0 丢失。

// sinkSendCloser 抽象出压测台需要的 sink 能力：投递 + 重开前关闭。
type sinkSendCloser interface {
	Send(ctx context.Context, batch []Record) error
	Close() error
}

// buildScanner 造 n 条升序 key（k0000..）的记录，返回可被 KVSource 使用的扫描器与 key 全集。
func buildScanner(n int) (*fakeScanner, map[string]bool) {
	entries := make([]proto.ScanEntry, n)
	keys := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%05d", i)
		entries[i] = proto.ScanEntry{Key: []byte(k), Value: []byte(fmt.Sprintf("v%d", i))}
		keys[k] = true
	}
	return &fakeScanner{entries: entries}, keys
}

// scenarioResult 是一次压测的量化结果。
type scenarioResult struct {
	ingested  int
	delivered int // sink 文件总行数（含重复）
	unique    int // 去重后的 key 数
	dups      int // delivered - unique
	losses    int // 灌入但 sink 中缺失的 key 数
}

// analyzeSink 解析 sink JSONL，统计投递/去重/重复/丢失。
func analyzeSink(t *testing.T, path string, ingested map[string]bool) scenarioResult {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seen := map[string]int{}
	total := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for sc.Scan() {
		var r fileRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue
		}
		seen[string(r.Key)]++
		total++
	}
	dups := 0
	for _, c := range seen {
		if c > 1 {
			dups += c - 1
		}
	}
	losses := 0
	for k := range ingested {
		if seen[k] == 0 {
			losses++
		}
	}
	return scenarioResult{
		ingested:  len(ingested),
		delivered: total,
		unique:    len(seen),
		dups:      dups,
		losses:    losses,
	}
}

// runCrashAfterSendBeforeCommit 手动驱动投递循环（复刻 deliverer 的 Fetch→Send→Commit），
// 在第 crashAt 批「Send 成功后、Commit 前」注入一次崩溃：不 Commit、不推进游标，然后重开
// sink 与游标继续——正是 at-least-once 的重复窗口。newSink 每次重开构造一个 sink 实例。
func runCrashAfterSendBeforeCommit(t *testing.T, path string, n, batchSize, crashAt int, newSink func(string) sinkSendCloser) {
	t.Helper()
	scanner, _ := buildScanner(n)
	store := newMemOffsetStore()
	const sinkName = "file"

	sink := newSink(path)
	src := NewKVSource(scanner, nil)
	cursor, _ := store.Load(sinkName)
	crashed := false
	batchIdx := 0
	for {
		batch, next, err := src.Fetch(cursor, batchSize)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		if err := sink.Send(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
		if !crashed && batchIdx == crashAt {
			// 崩溃：Send 已落，但 Commit 未执行、游标未推进。重开 sink，游标从 store 重载。
			crashed = true
			_ = sink.Close()
			sink = newSink(path)
			cursor, _ = store.Load(sinkName) // 仍是崩溃批之前的游标
			continue                         // 重投崩溃批
		}
		if err := store.Commit(sinkName, next); err != nil {
			t.Fatal(err)
		}
		cursor = next
		batchIdx++
	}
	_ = sink.Close()
}

// runOffsetLossReplay 先把 n 条正常投递（带 Commit）完毕，再模拟「offset 完全丢失」
// （重启时 Load 得到 nil，从头重投）——例如 offset 读故障被 committer 吞成 nil。这检验
// sink 自身的幂等性：不依赖 offset 也不产生重复。
func runOffsetLossReplay(t *testing.T, path string, n, batchSize int, newSink func(string) sinkSendCloser) {
	t.Helper()
	scanner, _ := buildScanner(n)
	store := newMemOffsetStore()
	const sinkName = "file"

	// 第一遍：完整投递并提交游标。
	sink := newSink(path)
	src := NewKVSource(scanner, nil)
	cursor, _ := store.Load(sinkName)
	for {
		batch, next, err := src.Fetch(cursor, batchSize)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		if err := sink.Send(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
		_ = store.Commit(sinkName, next)
		cursor = next
	}
	_ = sink.Close()

	// 第二遍：offset 丢失（cursor=nil），从头重投一整轮。
	sink = newSink(path)
	cursor = nil
	for {
		batch, next, err := src.Fetch(cursor, batchSize)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		if err := sink.Send(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
		cursor = next
	}
	_ = sink.Close()
}

func TestExactlyOnce_IdempotentSink_RobustToOffsetLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem-replay.jsonl")
	_, keys := buildScanner(500)
	runOffsetLossReplay(t, path, 500, 50, func(p string) sinkSendCloser {
		s, err := NewIdempotentFileSink("file", p)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
	r := analyzeSink(t, path, keys)
	t.Logf("IdempotentFileSink (offset lost, full replay): ingested=%d delivered=%d unique=%d dups=%d losses=%d", r.ingested, r.delivered, r.unique, r.dups, r.losses)
	if r.dups != 0 || r.losses != 0 {
		t.Fatalf("idempotent sink should absorb full replay: dups=%d losses=%d", r.dups, r.losses)
	}
}

func TestExactlyOnce_PlainSink_DuplicatesOnCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plain.jsonl")
	_, keys := buildScanner(500)
	runCrashAfterSendBeforeCommit(t, path, 500, 50, 3, func(p string) sinkSendCloser {
		s, err := NewFileSink("file", p)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
	r := analyzeSink(t, path, keys)
	t.Logf("plain FileSink: ingested=%d delivered=%d unique=%d dups=%d losses=%d", r.ingested, r.delivered, r.unique, r.dups, r.losses)
	if r.dups == 0 {
		t.Fatalf("expected plain sink to duplicate the re-delivered batch, got dups=0")
	}
	if r.losses != 0 {
		t.Fatalf("plain sink should not lose data (offset durable), got losses=%d", r.losses)
	}
}

func TestExactlyOnce_IdempotentSink_NoDuplicatesOnCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.jsonl")
	_, keys := buildScanner(500)
	runCrashAfterSendBeforeCommit(t, path, 500, 50, 3, func(p string) sinkSendCloser {
		s, err := NewIdempotentFileSink("file", p)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
	r := analyzeSink(t, path, keys)
	t.Logf("IdempotentFileSink: ingested=%d delivered=%d unique=%d dups=%d losses=%d", r.ingested, r.delivered, r.unique, r.dups, r.losses)
	if r.dups != 0 {
		t.Fatalf("expected idempotent sink 0 dups on crash, got dups=%d", r.dups)
	}
	if r.losses != 0 {
		t.Fatalf("expected idempotent sink 0 losses, got losses=%d", r.losses)
	}
}
