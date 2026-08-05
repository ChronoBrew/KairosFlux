package config

import "testing"

// TestNew_AppliesOptions：函数式选项在默认值上叠加指定字段，未设字段保持默认。
func TestNew_AppliesOptions(t *testing.T) {
	c := New(
		WithSSTablePath("/tmp/sst"),
		WithShardCount(4),
		WithMaxCompactionSize(8),
		WithMode(ModeStandalone),
	)
	if c.SSTablePath != "/tmp/sst" {
		t.Errorf("SSTablePath = %q, want /tmp/sst", c.SSTablePath)
	}
	if c.ShardCount != 4 {
		t.Errorf("ShardCount = %d, want 4", c.ShardCount)
	}
	if c.MaxCompactionSize != 8 {
		t.Errorf("MaxCompactionSize = %d, want 8", c.MaxCompactionSize)
	}
	if c.Mode != ModeStandalone {
		t.Errorf("Mode = %q, want %q", c.Mode, ModeStandalone)
	}
	// 未设字段保持默认。
	if c.Port != 8080 {
		t.Errorf("Port = %d, want default 8080", c.Port)
	}
}

// TestNew_NoOptionsEqualsDefaults：New() 无选项等价于默认配置。
func TestNew_NoOptionsEqualsDefaults(t *testing.T) {
	if got := New().Port; got != defaultGlobalConfig().Port {
		t.Errorf("New().Port = %d, want default %d", got, defaultGlobalConfig().Port)
	}
}

// TestNew_OptionsApplyInOrder：后施加的选项覆盖先施加的。
func TestNew_OptionsApplyInOrder(t *testing.T) {
	c := New(WithPort(1111), WithPort(2222))
	if c.Port != 2222 {
		t.Errorf("Port = %d, want last-applied 2222", c.Port)
	}
}
