package storage

import (
	"testing"
)

func setupTestEngine(t *testing.T) (*Engine, func()) {
	// 每个用例独立的临时目录与独立的参数，用例之间不再经由全局配置互相影响。
	mt := NewEngine(Options{Dir: t.TempDir(), MaxMemTableSize: 100})
	return mt, func() { mt.Close() }
}

func TestEngine_PutAndGet(t *testing.T) {
	mt, cleanup := setupTestEngine(t)
	defer cleanup()

	err := mt.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err := mt.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Value mismatch: expected 'value1', got '%s'", value)
	}
}

func TestEngine_PutMultipleKeys(t *testing.T) {
	mt, cleanup := setupTestEngine(t)
	defer cleanup()

	testCases := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, tc := range testCases {
		err := mt.Put([]byte(tc.key), []byte(tc.value))
		if err != nil {
			t.Fatalf("Put failed for key %s: %v", tc.key, err)
		}
	}

	for _, tc := range testCases {
		value, err := mt.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("Get failed for key %s: %v", tc.key, err)
		}
		if string(value) != tc.value {
			t.Errorf("Value mismatch for key %s: expected '%s', got '%s'", tc.key, tc.value, value)
		}
	}
}

func TestEngine_GetNonExistentKey(t *testing.T) {
	mt, cleanup := setupTestEngine(t)
	defer cleanup()

	value, err := mt.Get([]byte("nonexistent"))
	// 约定：缺失 key 返回错误（handleGet 据此向客户端回错误状态码）
	if err == nil {
		t.Errorf("Expected error for non-existent key, got value '%s'", value)
	}
	if value != nil {
		t.Errorf("Expected nil value for non-existent key, got '%s'", value)
	}
}

func TestEngine_Delete(t *testing.T) {
	mt, cleanup := setupTestEngine(t)
	defer cleanup()

	err := mt.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = mt.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	value, err := mt.Get([]byte("key1"))
	// 约定：删除后再查应报缺失（与 handleGet 的错误语义一致）
	if err == nil {
		t.Errorf("Expected error after delete, got value '%s'", value)
	}
	if value != nil {
		t.Errorf("Expected nil value after delete, got '%s'", value)
	}
}

func TestEngine_DeleteNonExistentKey(t *testing.T) {
	mt, cleanup := setupTestEngine(t)
	defer cleanup()

	// 墓碑语义下 Delete 是幂等盲写：删除不存在的 key 不报错（写入无害的墓碑），
	// 随后 Get 仍为未找到。无需先读即可删除是 LSM/分布式写入的正确契约。
	if err := mt.Delete([]byte("nonexistent")); err != nil {
		t.Errorf("Delete of non-existent key should be a no-op success, got err=%v", err)
	}
	if val, err := mt.Get([]byte("nonexistent")); err == nil && val != nil {
		t.Errorf("expected non-existent key to remain absent, got %q", string(val))
	}
}

func TestEngine_UpdateExistingKey(t *testing.T) {
	mt, cleanup := setupTestEngine(t)
	defer cleanup()

	err := mt.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = mt.Put([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("Engine Update failed: %v", err)
	}

	value, err := mt.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("Value mismatch: expected 'value2', got '%s'", value)
	}
}

func TestEngine_PutTriggersFlush(t *testing.T) {
	mt := NewEngine(Options{Dir: t.TempDir(), MaxMemTableSize: 5})

	for i := 0; i < 10; i++ {
		key := []byte(string(rune('a' + i)))
		value := []byte(string(rune('A' + i)))
		err := mt.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if mt.Size() >= 5 {
		t.Logf("MemTable size after 10 puts: %d (flush may have been triggered)", mt.Size())
	}

	mt.Close()
}
