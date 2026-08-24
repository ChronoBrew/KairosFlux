package service

import (
	"errors"
	"testing"

	"github.com/NeverENG/BanDB/predicate"
	"github.com/NeverENG/BanDB/storage"
)

// TestKVServer_GetTransparentlyResolvesVersionedKey 验证 M0 的核心兼容承诺：
// PUT_VERSIONED 从不写字面量 key，只写版本键+ :current 指针；但 v1/v2 的 GET
// （都经 KVServer.Get）必须看到最新版本的内容，仿佛这个 key 是被普通 PUT 写
// 过的一样——"GET 逻辑键走 current 指针，行为对 v1 客户端透明"。
func TestKVServer_GetTransparentlyResolvesVersionedKey(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	logical := "quote:2026-08-17:600000"
	if _, err := ts.PutVersioned(logical, []byte("v1"), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned(logical, []byte("v2"), 200); err != nil {
		t.Fatal(err)
	}

	got, err := kv.Get([]byte(logical))
	if err != nil {
		t.Fatalf("GET 应透明命中最新版本: err=%v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("应返回最新版本 v2, got %q", got)
	}
}

// TestKVServer_GetPlainPutUnaffected 验证零回归：一个从未被 PUT_VERSIONED 触碰
// 过的普通 v1 key，Get 行为与改动前完全一致（字面量直接命中，不触达任何
// :current 回退逻辑）。
func TestKVServer_GetPlainPutUnaffected(t *testing.T) {
	kv := setupTemporalTest(t)
	if err := kv.Write(Command{Type: CommandPut, Key: []byte("plain:key"), Value: []byte("plain-value")}); err != nil {
		t.Fatal(err)
	}
	got, err := kv.Get([]byte("plain:key"))
	if err != nil || string(got) != "plain-value" {
		t.Fatalf("普通 key 应直接命中: got=%q err=%v", got, err)
	}
}

// TestKVServer_GetUnversionedMissingKeyStillNotFound 验证从未写过的 key（既没
// 有字面量、也没有 :current 指针）依然返回 ErrKeyNotFound，不会因为新增的
// 回退步骤而把"彻底不存在"误判成别的结果。
func TestKVServer_GetUnversionedMissingKeyStillNotFound(t *testing.T) {
	kv := setupTemporalTest(t)
	_, err := kv.Get([]byte("never:written"))
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("应返回 ErrKeyNotFound, got %v", err)
	}
}

// TestKVServer_ScanHidesInternalTemporalKeysButKeepsPlainKeys 验证 SCAN 的
// M0 边界决策：版本键（"key:vSEQ"）与 current 指针键（"key:current"）对业务
// SCAN 不可见，普通 v1 PUT 的 key 在同一扫描范围内正常可见——SCAN 保持"一个
// 逻辑键一行"的既有契约，不会因为某个逻辑键被版本化过就在结果里裂成多条。
func TestKVServer_ScanHidesInternalTemporalKeysButKeepsPlainKeys(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)

	// 普通 v1 写入，与版本化的逻辑键共享同一个扫描范围前缀。
	if err := kv.Write(Command{Type: CommandPut, Key: []byte("quote:2026-08-17:600001"), Value: []byte(`{"plain":true}`)}); err != nil {
		t.Fatal(err)
	}
	// 版本化写入：产生 "quote:2026-08-17:600000:v...0001"/"...0002" 与
	// "quote:2026-08-17:600000:current" 三个内部存储键。
	if _, err := ts.PutVersioned("quote:2026-08-17:600000", []byte(`{"versioned":true}`), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PutVersioned("quote:2026-08-17:600000", []byte(`{"versioned":true}`), 200); err != nil {
		t.Fatal(err)
	}

	entries := kv.Scan([]byte("quote:2026-08-17:"), []byte("quote:2026-08-17:\xff"), predicate.Predicate{}, 0)

	if len(entries) != 1 {
		t.Fatalf("SCAN 应只看到 1 条普通 key（版本键/current 指针应被过滤）: got %d 条: %+v", len(entries), entries)
	}
	if string(entries[0].Key) != "quote:2026-08-17:600001" {
		t.Fatalf("SCAN 唯一可见的应是普通 key, got %q", entries[0].Key)
	}
}

// TestKVServer_ScanRawSeesInternalKeys 验证 ScanRaw（时态内核内部专用、不过滤）
// 确实能看到 Scan 特意隐藏的内部键——否则 LIST_VERSIONS/REPLAY_FINGERPRINT
// 将无法读到自己需要的数据，这是二者刻意分开的原因（见 fsm.go 注释）。
func TestKVServer_ScanRawSeesInternalKeys(t *testing.T) {
	kv := setupTemporalTest(t)
	ts := NewTemporalStore(kv)
	if _, err := ts.PutVersioned("quote:2026-08-17:600000", []byte("v1"), 100); err != nil {
		t.Fatal(err)
	}

	raw := kv.ScanRaw([]byte("quote:2026-08-17:"), []byte("quote:2026-08-17:\xff"))
	if len(raw) != 2 { // 一个版本键 + 一个 current 指针
		t.Fatalf("ScanRaw 应看到版本键与 current 指针共 2 条, got %d: %+v", len(raw), raw)
	}
}
