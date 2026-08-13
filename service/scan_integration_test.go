package service

import (
	"fmt"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/predicate"
	"path/filepath"
	"testing"
	"time"
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

// TestScanCoversFlushedData 守护 SCAN 与下游投递的取数范围：已落盘的数据必须仍在扫描
// 结果中。
//
// 这条契约此前不成立：存储层的扫描只遍历两张内存表。后果不止 SCAN 命令少返回结果——
// 下游投递（delivery.KVSource）正是按游标反复调用本方法取数，一旦 flush 快于投递
// （阈值上万条 vs 每秒百条，必然如此），落盘的记录就再也不会被投递，即静默漏投。
func TestScanCoversFlushedData(t *testing.T) {
	dir := t.TempDir()
	oldWAL, oldSST, oldMax := config.G.WALPath, config.G.SSTablePath, config.G.MaxMemTableSize
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.MaxMemTableSize = 20 // 极小阈值：写入过程中必然多次 flush
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.MaxMemTableSize = oldWAL, oldSST, oldMax
	})

	kv := NewKVServer()
	t.Cleanup(func() { kv.Close() })

	const n = 300
	for i := 0; i < n; i++ {
		cmd := Command{
			Type:  CommandPut,
			Key:   []byte(fmt.Sprintf("k%05d", i)),
			Value: []byte(fmt.Sprintf(`{"v":%d}`, i)),
		}
		if err := kv.Write(cmd); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	// 等后台 flush 落定，使多数数据已在 SSTable 中。
	time.Sleep(500 * time.Millisecond)

	got := kv.Scan([]byte("k00000"), []byte("k99999"), predicate.Predicate{Op: predicate.OpNone})
	if len(got) != n {
		t.Fatalf("扫描应覆盖全部 %d 条（含已落盘），实际 %d 条", n, len(got))
	}
	seen := make(map[string]bool, len(got))
	for _, e := range got {
		seen[string(e.Key)] = true
	}
	for i := 0; i < n; i++ {
		if k := fmt.Sprintf("k%05d", i); !seen[k] {
			t.Fatalf("扫描结果缺少 %s", k)
		}
	}
}
