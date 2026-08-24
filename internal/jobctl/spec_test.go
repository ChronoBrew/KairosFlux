package jobctl

import "testing"

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
