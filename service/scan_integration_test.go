package service

import (
	"path/filepath"
	"testing"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/pkg/predicate"
)

// TestKVServer_Scan 端到端验证边缘查询：写入若干 IMU 帧后，按时间范围 + 谓词扫描，
// 只返回命中切片（谓词下推），且越界设备帧被范围排除。
func TestKVServer_Scan(t *testing.T) {
	oldWALPath := config.G.WALPath
	oldSSTablePath := config.G.SSTablePath
	oldMode := config.G.Mode
	oldMaxSize := config.G.MaxMemTableSize

	dir := t.TempDir()
	config.G.Mode = config.ModeStandalone
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.MaxMemTableSize = 1 << 20 // 足够大，确保数据留在 MemTable 热窗口
	defer func() {
		config.G.WALPath = oldWALPath
		config.G.SSTablePath = oldSSTablePath
		config.G.Mode = oldMode
		config.G.MaxMemTableSize = oldMaxSize
	}()

	kv := NewKVServer()
	defer kv.wal.Close()

	frames := []Command{
		{Type: CommandPut, Key: []byte("imu:dev0:100"), Value: []byte(`{"az":9.8}`)},
		{Type: CommandPut, Key: []byte("imu:dev0:150"), Value: []byte(`{"az":9.95}`)}, // 命中
		{Type: CommandPut, Key: []byte("imu:dev0:200"), Value: []byte(`{"az":10.2}`)}, // 命中
		{Type: CommandPut, Key: []byte("imu:dev0:250"), Value: []byte(`{"az":9.0}`)},
		{Type: CommandPut, Key: []byte("imu:dev1:150"), Value: []byte(`{"az":11}`)}, // 设备越界
	}
	for _, c := range frames {
		if err := kv.Write(c); err != nil {
			t.Fatalf("write %s: %v", c.Key, err)
		}
	}

	pred := predicate.Predicate{Field: "az", Op: predicate.OpGT, Operand: "9.9"}
	got := kv.Scan([]byte("imu:dev0:100"), []byte("imu:dev0:299"), pred)

	want := map[string]string{
		"imu:dev0:150": `{"az":9.95}`,
		"imu:dev0:200": `{"az":10.2}`,
	}
	if len(got) != len(want) {
		t.Fatalf("命中数 %d，期望 %d: %+v", len(got), len(want), got)
	}
	for _, e := range got {
		w, ok := want[string(e.Key)]
		if !ok {
			t.Fatalf("意外命中 %s（谓词或范围未正确下推）", e.Key)
		}
		if string(e.Value) != w {
			t.Fatalf("%s 值不符: %s", e.Key, e.Value)
		}
	}
}
