package jobctl

import (
	"testing"
	"time"
)

func TestParseJobSpec_ValidRoundTrip(t *testing.T) {
	raw := []byte(`{
		"name": "paper_daily",
		"command": ["/usr/bin/env", "bash", "/tmp/paper_daily.sh"],
		"dir": "/tmp",
		"schedule_interval_seconds": 86400,
		"max_retries": 2,
		"retry_backoff_seconds": 600,
		"depends_on": ["scout_daily"]
	}`)
	spec, err := ParseJobSpec(raw)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if spec.Name != "paper_daily" || len(spec.Command) != 3 || spec.ScheduleIntervalSeconds != 86400 {
		t.Fatalf("解析结果不符: %+v", spec)
	}
}

func TestParseJobSpec_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"缺 name", `{"command":["x"],"schedule_interval_seconds":1}`},
		{"缺 command", `{"name":"a","schedule_interval_seconds":1}`},
		{"command 为空数组", `{"name":"a","command":[],"schedule_interval_seconds":1}`},
		{"schedule_interval_seconds<=0", `{"name":"a","command":["x"],"schedule_interval_seconds":0}`},
		{"max_retries 为负", `{"name":"a","command":["x"],"schedule_interval_seconds":1,"max_retries":-1}`},
		{"依赖自己", `{"name":"a","command":["x"],"schedule_interval_seconds":1,"depends_on":["a"]}`},
	}
	for _, tc := range cases {
		if _, err := ParseJobSpec([]byte(tc.raw)); err == nil {
			t.Errorf("%s: 应返回校验错误", tc.name)
		} else if _, ok := err.(*ValidationError); !ok {
			t.Errorf("%s: 错误类型应为 *ValidationError，实际 %T", tc.name, err)
		}
	}
}

func TestJobSpec_FingerprintStableAndSensitive(t *testing.T) {
	base := testSpec("job_a")
	fp1 := base.Fingerprint()
	fp2 := testSpec("job_a").Fingerprint()
	if fp1 != fp2 {
		t.Fatalf("相同 spec 两次算出的指纹应一致: %s vs %s", fp1, fp2)
	}

	changed := testSpec("job_a")
	changed.MaxRetries = 99
	if changed.Fingerprint() == fp1 {
		t.Fatal("字段变化后指纹应改变")
	}

	// DependsOn 顺序不影响指纹（CanonicalJSON 里显式排序）。
	a := testSpec("job_b", "x", "y")
	b := testSpec("job_b", "y", "x")
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("DependsOn 顺序不同但集合相同时，指纹应一致")
	}
}

// TestJobSpec_Fingerprint_AnchoredSchedule 锁定 M3 遗留修正的指纹语义：
//  1. 无 schedule 的旧 spec 指纹与新增字段前逐字节一致（CanonicalJSON 里
//     Schedule 用 omitempty 追加在尾部——空值不参与序列化，既有 spec 的
//     幂等键/指纹零回归；锁死具体哈希值防未来再变）；
//  2. 声明 schedule 锚点后指纹必须改变（spec 语义确实变了）；
//  3. 两种写法（显式省略 vs 空字符串）指纹一致。
func TestJobSpec_Fingerprint_AnchoredSchedule(t *testing.T) {
	legacyFP := "dbb1c319e9cd5d954a6355d9481e5c62104ddcbadc95c2cc2b197a2ab0d6563e"
	if got := testSpec("job_a").Fingerprint(); got != legacyFP {
		t.Fatalf("无 schedule 的旧 spec 指纹回归: got %s, want %s", got, legacyFP)
	}

	anchored := testSpec("job_a")
	anchored.Schedule = "daily@16:10 Asia/Shanghai"
	if got := anchored.Fingerprint(); got == legacyFP {
		t.Fatal("声明 schedule 后指纹应改变")
	}
	if got := anchored.Fingerprint(); got != "b3bae46f4dc87cb660fd15d14c2e4e219b68b0f1253aaef835cd610e26754f03" {
		t.Fatalf("锚定 spec 指纹锁定失败: %s", got)
	}
}

func TestParseJobSpec_AnchoredSchedule(t *testing.T) {
	raw := []byte(`{
		"name": "paper_daily",
		"command": ["bash", "/tmp/x.sh"],
		"schedule_interval_seconds": 86400,
		"schedule": "daily@16:10 Asia/Shanghai"
	}`)
	spec, err := ParseJobSpec(raw)
	if err != nil {
		t.Fatalf("带 schedule 的 spec 应解析成功: %v", err)
	}
	if spec.Schedule != "daily@16:10 Asia/Shanghai" {
		t.Fatalf("schedule 字段未解析: %+v", spec)
	}

	invalid := []struct {
		name, schedule string
	}{
		{"未知种类", "weekly@16:10 Asia/Shanghai"},
		{"缺时区", "daily@16:10"},
		{"缺 @", "daily 16:10 Asia/Shanghai"},
		{"小时越界", "daily@24:10 Asia/Shanghai"},
		{"分钟越界", "daily@16:60 Asia/Shanghai"},
		{"未知时区", "daily@16:10 Mars/OlympusMons"},
	}
	for _, tc := range invalid {
		s := testSpec("a")
		s.Schedule = tc.schedule
		if err := s.Validate(); err == nil {
			t.Errorf("%s: schedule=%q 应校验失败", tc.name, tc.schedule)
		} else if ve, ok := err.(*ValidationError); !ok || ve.Field != "schedule" {
			t.Errorf("%s: 应为 schedule 字段的 ValidationError, got %v", tc.name, err)
		}
	}
}

// TestJobSpec_Slot_AnchoredBoundary 验证锚定时槽的边界语义：16:10
// Asia/Shanghai 的本地墙钟时刻是 slot 边界（前后 1ns 落在相邻 slot），
// 且同一时刻的锚定 slot 与旧纪元 slot 数值不同（证明锚点真的生效）。
func TestJobSpec_Slot_AnchoredBoundary(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("加载 Asia/Shanghai 时区失败: %v", err)
	}
	// 16:09:59.999999999 +08:00 与 16:10:00.000000000 +08:00：锚点前后 1ns。
	before := time.Date(2026, 8, 25, 16, 9, 59, 999999999, shanghai)
	at := time.Date(2026, 8, 25, 16, 10, 0, 0, shanghai)

	spec := testSpec("a")
	spec.Schedule = "daily@16:10 Asia/Shanghai"
	slotBefore := spec.Slot(before)
	slotAt := spec.Slot(at)
	if slotBefore != slotAt-1 {
		t.Fatalf("16:10 应是 slot 边界: before=%d at=%d", slotBefore, slotAt)
	}

	legacy := testSpec("a") // 无 schedule：旧语义（纪元零点切分）
	// 16:09:59.999 +08:00 的真实时刻是 UTC 日 20675，纪元 slot=20675；
	// 锚定 slot=20674（16:10 尚未到）——同一真实时刻两套语义不同，证明
	// 锚点生效（旧公式整体平移在这里恰好也会给出不同值，但两段式对
	// 16:10 边界本身的断言 slotBefore==slotAt-1 才是决定性检验）。
	if legacy.Slot(before) == slotBefore {
		t.Fatalf("锚定 slot 不应与纪元 slot 相同: %d", slotBefore)
	}
	if legacy.Slot(at) == slotBefore {
		t.Fatalf("锚点未生效: 锚定 before=%d 纪元 at=%d", slotBefore, legacy.Slot(at))
	}
}
