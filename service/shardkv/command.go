//go:build experimental

// 本包（Multi-Raft 分片 KV，v1）未被任何 cmd 入口接线，是自洽但游离在主干
// 构建之外的原型——见 service/shardkv/store.go 顶部注释的边界说明，以及
// docs/iteration-2026-08-19-slimdown-quant-adapt.md 的瘦身路线记录。
// 用 //go:build experimental 隔离，默认 `go build ./...` 不编译它；
// 需要时用 `go build -tags experimental ./...` 显式编译。不物理删除：
// 测试齐全（76.9% 覆盖）且体现 Multi-Raft 分片设计能力，保留供后续可能
// 的横向扩展需求或作品集参考。
package shardkv

import "encoding/json"

// command 是写进 Raft 日志、由每分片 FSM 应用的命令。
type command struct {
	Op    string `json:"op"` // "Put" | "Delete"
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func encodeCommand(c command) ([]byte, error) {
	return json.Marshal(c)
}

func decodeCommand(b []byte) (command, error) {
	var c command
	err := json.Unmarshal(b, &c)
	return c, err
}
