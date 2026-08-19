// Package schema 提供「按数据类型注册校验规则」的最小注册表：每种业务数据类型
// （如行情快照）注册一个 Validator，落盘前按 key 前缀分派到对应校验器。
//
// 定位：这一层只管「一条记录内容是否合法」，不管帧格式（畸形帧/超限由
// service/ingesthook.Filter 处理）也不管传输层（bannet 裸帧 / gRPC 结构体皆可复用，
// 见 Filter.Validate 的调用方）。
//
// 分派方式刻意从简：按 key 前缀做最长匹配，一个前缀对应一个 Validator。
// 不支持的 key（无匹配前缀）视为「未纳管类型」，不做 schema 校验、直接放行——
// 新增数据类型前，未注册的旧类型不会被误伤。
package schema

import (
	"bytes"
	"sync"
)

// Validator 校验一条记录的 value 字节内容。返回 nil 表示通过；非 nil 时调用方
// 应丢弃该记录，error 内容即丢弃原因（供 metrics/日志使用）。
type Validator interface {
	Validate(value []byte) error
}

var (
	mu       sync.RWMutex
	registry = map[string]Validator{}
)

// Register 为 key 前缀 prefix 注册一个校验器。重复注册同一前缀会覆盖旧的
// （便于测试替换/热更新，生产路径通常只在启动时调用一次）。
func Register(prefix string, v Validator) {
	mu.Lock()
	defer mu.Unlock()
	registry[prefix] = v
}

// Unregister 移除 prefix 对应的校验器，主要供测试清理注册表用。
func Unregister(prefix string) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, prefix)
}

// Lookup 返回 key 命中的校验器：在所有已注册前缀中找按字节匹配的最长前缀。
// 无前缀匹配时返回 nil, false——调用方应将其视为「未纳管类型」而放行，不视为错误。
func Lookup(key []byte) (Validator, bool) {
	mu.RLock()
	defer mu.RUnlock()

	var best Validator
	bestLen := -1
	for prefix, v := range registry {
		if len(prefix) <= bestLen {
			continue
		}
		if bytes.HasPrefix(key, []byte(prefix)) {
			best = v
			bestLen = len(prefix)
		}
	}
	if bestLen < 0 {
		return nil, false
	}
	return best, true
}
