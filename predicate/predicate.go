// Package predicate 提供一个最小的字段谓词，用于边缘查询的服务端下推：
// 对 JSON 值取出指定字段，与操作数按算子比较，只让命中的行通过——
// 从而「只回传命中切片」而非整段原始流。
//
// 设计刻意从简：单字段、单算子、单操作数（如 az > 9.9 / status == ok），
// 覆盖具身采集的常见查询，不引入完整表达式语言。
package predicate

import (
	"encoding/json"
	"strconv"
)

// Op 是谓词算子。
type Op uint8

const (
	OpNone Op = iota // 无谓词：全匹配
	OpGT             // >
	OpGTE            // >=
	OpLT             // <
	OpLTE            // <=
	OpEQ             // ==
	OpNE             // !=
)

// Predicate 表示「Field Op Operand」。Op==OpNone 时 Eval 恒为真。
type Predicate struct {
	Field   string
	Op      Op
	Operand string // 文本操作数：有序比较按 float64 解析，相等比较按字符串
}

// Eval 报告 value（JSON 对象）是否满足谓词。
// 无谓词恒真；value 非 JSON、字段缺失、或类型无法比较时返回 false。
func (p Predicate) Eval(value []byte) bool {
	if p.Op == OpNone {
		return true
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(value, &obj); err != nil {
		return false
	}
	raw, ok := obj[p.Field]
	if !ok {
		return false
	}

	switch p.Op {
	case OpGT, OpGTE, OpLT, OpLTE:
		fv, ok1 := asFloat(raw)
		ov, err := strconv.ParseFloat(p.Operand, 64)
		if !ok1 || err != nil {
			return false
		}
		switch p.Op {
		case OpGT:
			return fv > ov
		case OpGTE:
			return fv >= ov
		case OpLT:
			return fv < ov
		default: // OpLTE
			return fv <= ov
		}
	case OpEQ:
		return asScalar(raw) == p.Operand
	case OpNE:
		return asScalar(raw) != p.Operand
	default:
		return false
	}
}

// asFloat 把字段原始 JSON 解析为 float64：直接数字或带引号的数字串都接受。
func asFloat(raw json.RawMessage) (float64, bool) {
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return f, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// asScalar 把字段原始 JSON 归一成可比较的标量字符串：
// JSON 字符串去引号，其余（数字/布尔）按原文。
func asScalar(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}
