package jobctl

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// JobSpec 是 job:spec:{name} 的期望状态。协调者裁决：spec 声明格式用 JSON
// （方案原文任务清单写"TOML 声明"但同一节明文"spec 用 JSON 定义（Go 标准库
// 即可解析，不引入 TOML/YAML 解析依赖）"，且 §5 依赖策略表裁决"核心语义手写
// 保持纯粹"——以 JSON 为准）。只用 encoding/json，不引入任何新解析依赖。
type JobSpec struct {
	// Name 是 job 的唯一名字，job:spec:{name} 等键空间段落的 {name}。
	Name string `json:"name"`

	// Command 是执行体：显式配置的命令行（含工作目录/环境变量），跨仓只走
	// 配置声明——禁止在 Go 代码里硬编码对方仓路径（本任务的硬性边界）。
	// Command[0] 是可执行文件，Command[1:] 是参数。
	Command []string `json:"command"`

	// Dir 是执行时的工作目录（绝对路径）。空字符串表示继承调用进程的工作目录。
	Dir string `json:"dir,omitempty"`

	// Env 是额外注入的环境变量（覆盖/追加到继承的环境之上）。map 本身语义上
	// 就是无序的键值集合，序列化用 encoding/json（对 map[string]string 编码
	// 时按 key 字典序输出，Go 标准库保证），执行时构造 os/exec.Cmd.Env 前
	// 也显式按 key 排序（见 executor.go）——避免"map 迭代序"这条本仓库禁令
	// 在任何路径上被违反。
	Env map[string]string `json:"env,omitempty"`

	// DependsOn 是本 job 依赖的其它 job 名字（同一次 reconcile 逻辑时间片
	// 内，全部依赖必须已成功才允许本 job 执行）。顺序即声明顺序，不是 map，
	// 天然确定。
	DependsOn []string `json:"depends_on,omitempty"`

	// ScheduleIntervalSeconds 定义调度节律：把墙钟时间切成等长的"逻辑时间片"
	// （slot = nowNanos / (ScheduleIntervalSeconds * 1e9)），daily job 填
	// 86400。调度触发允许用墙钟（服务端场景，见任务书"墙钟仅用于调度触发与
	// 事件时间戳"），但幂等键只依赖 slot 这个整数、不直接依赖 time.Now()
	// 的具体纳秒值——同一时间片内重跑多少次，slot 不变，幂等键不变。
	ScheduleIntervalSeconds int64 `json:"schedule_interval_seconds"`

	// MaxRetries 是失败后的最大重试次数（不含首次执行）。
	MaxRetries int `json:"max_retries"`

	// RetryBackoffSeconds 是失败后到下一次重试之间的最小退避时长。
	RetryBackoffSeconds int64 `json:"retry_backoff_seconds"`
}

// ValidationError 是 spec 校验失败的结构化描述（禁止用字符串匹配承载语义：
// 调用方要判断"哪个字段错了"，走 Field，不走 Err.Error() 里的子串）。
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("job spec 校验失败: 字段=%s 原因=%s", e.Field, e.Reason)
}

// Validate 校验 JobSpec 的必填字段与取值范围，返回结构化 *ValidationError
// （字段名/原因），不 panic，与 QuantBrew 侧 qb_core::ConfigError 的设计
// 原则一致（本仓库独立实现，不共享类型——两个仓库不假设互相依赖对方内部
// 类型）。
func (s JobSpec) Validate() error {
	if s.Name == "" {
		return &ValidationError{Field: "name", Reason: "不能为空"}
	}
	if len(s.Command) == 0 {
		return &ValidationError{Field: "command", Reason: "不能为空数组"}
	}
	if s.Command[0] == "" {
		return &ValidationError{Field: "command[0]", Reason: "可执行文件路径不能为空字符串"}
	}
	if s.ScheduleIntervalSeconds <= 0 {
		return &ValidationError{Field: "schedule_interval_seconds", Reason: "必须为正整数"}
	}
	if s.MaxRetries < 0 {
		return &ValidationError{Field: "max_retries", Reason: "不能为负数"}
	}
	if s.RetryBackoffSeconds < 0 {
		return &ValidationError{Field: "retry_backoff_seconds", Reason: "不能为负数"}
	}
	for i, dep := range s.DependsOn {
		if dep == "" {
			return &ValidationError{Field: fmt.Sprintf("depends_on[%d]", i), Reason: "依赖名不能为空字符串"}
		}
		if dep == s.Name {
			return &ValidationError{Field: fmt.Sprintf("depends_on[%d]", i), Reason: "job 不能依赖自己"}
		}
	}
	return nil
}

// ParseJobSpec 从 JSON 字节解析 JobSpec 并立即 Validate。这是 spec 声明的
// 唯一解析入口——apply/reconcile loop/CLI 都走这一个函数，避免"JSON 解析
// 散落各处、校验规则各写一遍"的分叉。
func ParseJobSpec(raw []byte) (JobSpec, error) {
	var s JobSpec
	if err := json.Unmarshal(raw, &s); err != nil {
		return JobSpec{}, fmt.Errorf("job spec JSON 解析失败: %w", err)
	}
	if err := s.Validate(); err != nil {
		return JobSpec{}, err
	}
	return s, nil
}

// CanonicalJSON 返回 spec 的规范化 JSON 编码：字段顺序固定为 canonicalSpec
// 声明的字段顺序（Go struct 编码顺序即字段声明顺序，encoding/json 文档保证
// 这一点），Env 这个 map 字段编码时 encoding/json 按 key 字典序输出（Go
// 标准库保证，非"看起来大概率如此"的偶然实现细节）。用于 Fingerprint，
// 不用于对外展示。
func (s JobSpec) CanonicalJSON() []byte {
	// 显式复制一份排序后的 DependsOn 用于指纹计算，避免"声明顺序不同但语义
	// 相同的两个 spec 算出不同指纹"——声明顺序本身对 reconcile 语义无意义
	// （依赖集合是否满足只看集合关系），但保留原始字段做执行期展示。
	sortedDeps := append([]string(nil), s.DependsOn...)
	sort.Strings(sortedDeps)

	canon := struct {
		Name                    string            `json:"name"`
		Command                 []string          `json:"command"`
		Dir                     string            `json:"dir"`
		Env                     map[string]string `json:"env"`
		DependsOn               []string          `json:"depends_on"`
		ScheduleIntervalSeconds int64             `json:"schedule_interval_seconds"`
		MaxRetries              int               `json:"max_retries"`
		RetryBackoffSeconds     int64             `json:"retry_backoff_seconds"`
	}{
		Name:                    s.Name,
		Command:                 s.Command,
		Dir:                     s.Dir,
		Env:                     s.Env,
		DependsOn:               sortedDeps,
		ScheduleIntervalSeconds: s.ScheduleIntervalSeconds,
		MaxRetries:              s.MaxRetries,
		RetryBackoffSeconds:     s.RetryBackoffSeconds,
	}
	// 规范化编码不可能失败（所有字段都是 JSON 原生可编码类型），忽略 error
	// 与本仓库其它"编码函数不返回 error"的既有先例一致（如 temporal 包的
	// EncodeVersionRecord）。
	out, _ := json.Marshal(canon)
	return out
}

// Fingerprint 返回 spec 的确定性指纹（sha256 十六进制）。用途：
//  1. reconcile 判断"这次 apply 的 spec 与上次执行时的 spec 是否相同"
//     （spec 变了要重新触发首次执行，即便 slot 相同）；
//  2. 幂等键的一部分（见 reconciler.go），不依赖字段声明顺序这种偶然实现
//     细节（本仓库明令禁止的胶水点 3 的反例）。
func (s JobSpec) Fingerprint() string {
	sum := sha256.Sum256(s.CanonicalJSON())
	return hex.EncodeToString(sum[:])
}
