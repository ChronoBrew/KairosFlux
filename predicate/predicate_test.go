package predicate

import "testing"

func TestEval(t *testing.T) {
	cases := []struct {
		name  string
		pred  Predicate
		value string
		want  bool
	}{
		{"无谓词恒真", Predicate{Op: OpNone}, `{"az":1}`, true},
		{"数值大于命中", Predicate{"az", OpGT, "9.9"}, `{"az":9.91}`, true},
		{"数值大于未命中", Predicate{"az", OpGT, "9.9"}, `{"az":9.8}`, false},
		{"数值大于等于边界", Predicate{"az", OpGTE, "9.9"}, `{"az":9.9}`, true},
		{"数值小于命中", Predicate{"az", OpLT, "0"}, `{"az":-0.5}`, true},
		{"数值小于等于边界", Predicate{"az", OpLTE, "9.9"}, `{"az":9.9}`, true},
		{"字符串相等命中", Predicate{"status", OpEQ, "ok"}, `{"status":"ok"}`, true},
		{"字符串相等未命中", Predicate{"status", OpEQ, "ok"}, `{"status":"err"}`, false},
		{"不等命中", Predicate{"status", OpNE, "ok"}, `{"status":"err"}`, true},
		{"带引号数字串可比", Predicate{"az", OpGT, "9.9"}, `{"az":"9.91"}`, true},
		{"字段缺失不命中", Predicate{"az", OpGT, "9.9"}, `{"ax":1}`, false},
		{"非 JSON 不命中", Predicate{"az", OpGT, "9.9"}, `not-json`, false},
		{"操作数非数值不命中", Predicate{"az", OpGT, "abc"}, `{"az":1}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.pred.Eval([]byte(c.value)); got != c.want {
				t.Fatalf("Eval(%s) = %v, want %v", c.value, got, c.want)
			}
		})
	}
}
