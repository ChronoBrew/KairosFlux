package jobctl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// registryFingerprintKey 是"对象索引"里策略/实验记录的键空间前缀。任务书
// 第 4 项："M31 注册表迁入 KairosFlux 对象索引：本任务只做 KairosFlux 侧
// 能力（对象索引 kind + 从 registry.jsonl 一次性导入工具，文件仍是 git
// 审计源，双写过渡）；QuantBrew 侧写入接线另排，不在本任务改对方仓。"——
// 本函数只读 QuantBrew 的 registry.jsonl（路径来自调用方传入的配置/CLI flag，
// 不硬编码对方仓路径），只写本仓库自己的键空间，不回写对方仓文件。
func registryFingerprintKey(fingerprint string) string {
	return "strategy:index:" + fingerprint
}

// registryLine 是 registry.jsonl 一行里我们需要用来定位记录的最小字段——
// 故意不建模完整的 verdict 结构（那是 QuantBrew 侧的契约，字段会随
// SPEC-M14/M17 等增量演进），只解出 fingerprint 作为对象索引的键，剩下的
// 原始字节整行原样存进版本 payload（导入工具的职责是"迁入索引"，不是
// "重新理解 QuantBrew 的业务字段"）。
type registryLine struct {
	Verdict struct {
		Fingerprint string `json:"fingerprint"`
	} `json:"verdict"`
}

// ImportError 是导入某一行失败的结构化记录（行号 + 原因），不是拼接进
// 一个大字符串——调用方（CLI）需要能逐条展示。
type ImportError struct {
	Line   int
	Reason string
}

// ImportResult 是一次 ImportFile 调用的结果统计。
type ImportResult struct {
	TotalLines int
	Imported   int // 写入了新版本（首次导入或内容变化）
	Unchanged  int // 已存在且内容与上次导入完全一致，跳过（幂等）
	Errors     []ImportError
}

// RegistryImporter 把 QuantBrew 的 experiments/registry.jsonl 一次性导入
// KairosFlux 对象索引（strategy:index:{fingerprint}，走 PUT_VERSIONED——
// 每次导入若内容变化会追加新版本，天然保留"这条记录曾经是什么样"的历史，
// 与"文件仍是 git 审计源，双写过渡"这条裁决一致：本工具不删、不改
// registry.jsonl 本身，只读它、只写自己的键空间）。
type RegistryImporter struct {
	Store Store
}

// ImportReader 从 r 逐行读取 JSONL 并导入。按 fingerprint 幂等：若
// strategy:index:{fingerprint} 当前最新版本的字节与本次读到的行完全相同，
// 跳过（Unchanged），不产生新版本——保证重复对同一份 registry.jsonl 跑
// 导入工具不会无限膨胀事件账本。
func (imp *RegistryImporter) ImportReader(r io.Reader) (ImportResult, error) {
	var result ImportResult
	scanner := bufio.NewScanner(r)
	// registry.jsonl 单行可能很长（一条 verdict 记录内嵌完整网格搜索结果），
	// 默认 bufio.Scanner 的 64KiB 单行上限不够用，显式放大。
	buf := make([]byte, 0, 1<<20)
	scanner.Buffer(buf, 8<<20)

	lineNo := 0
	for scanner.Scan() {
		lineNo++
		raw := scanner.Bytes()
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		result.TotalLines++

		var parsed registryLine
		if err := json.Unmarshal(raw, &parsed); err != nil {
			result.Errors = append(result.Errors, ImportError{Line: lineNo, Reason: fmt.Sprintf("JSON 解析失败: %v", err)})
			continue
		}
		if parsed.Verdict.Fingerprint == "" {
			result.Errors = append(result.Errors, ImportError{Line: lineNo, Reason: "verdict.fingerprint 为空，无法定位对象索引键"})
			continue
		}

		key := registryFingerprintKey(parsed.Verdict.Fingerprint)
		existing, found, err := imp.Store.GetLatest(key)
		if err != nil {
			result.Errors = append(result.Errors, ImportError{Line: lineNo, Reason: fmt.Sprintf("读现有索引失败: %v", err)})
			continue
		}
		rawCopy := append([]byte(nil), raw...)
		if found && bytes.Equal(existing, rawCopy) {
			result.Unchanged++
			continue
		}
		if _, err := imp.Store.PutVersioned(key, rawCopy); err != nil {
			result.Errors = append(result.Errors, ImportError{Line: lineNo, Reason: fmt.Sprintf("写入索引失败: %v", err)})
			continue
		}
		result.Imported++
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("读取 registry 失败: %w", err)
	}
	return result, nil
}
