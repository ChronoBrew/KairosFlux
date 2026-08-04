// Package offset 是投递层的「强一致 offset」子包：把每个 sink 的投递进度（游标）
// 持久化为一条 KV，从而在进程崩溃/重启后从已提交位置续投，而非从头重投。
//
// 设计意图：offset 的写入经由 Committer 抽象接口路由到底层 KV 写路径。主体会在
// service 包提供一个把 Put 路由到 KVServer.Write 的适配器——raft 模式下该写入会经
// Raft 日志强一致复制，因此 offset 与业务数据享有同一份复制保证，不会出现「数据已
// 复制但游标丢失」的分裂。
//
// 语义边界（刻意的骨架边界）：本包只保证 at-least-once。投递流程是「Send 成功 →
// Commit 游标」，若 Send 成功后、Commit 前崩溃，重启会重投上一批——因此 exactly-once
// 需要 sink 侧幂等（按 Record.Key 去重或下游主键 upsert）来兜底，offset 层不承诺。
package offset

import "fmt"

// Committer 抽象「KV 写读能力」。定义在此以避免 offset 反向依赖 service，防止 import 环；
// 主体在 service 包提供把 Put 路由到 KVServer.Write（raft 模式经 Raft 日志强一致）的适配器。
type Committer interface {
	// Put 写入一个 key/value。返回 nil 表示写入已被底层接受（raft 模式下即已提交复制）。
	Put(key, value []byte) error
	// Get 读取 key 对应的 value。key 不存在时约定返回 (nil, nil)。
	Get(key []byte) ([]byte, error)
}

// OffsetStore 是投递游标的持久化存取抽象：按 sink 名读/写其「下一批起始游标」。
type OffsetStore interface {
	// Load 读取 sink 已提交的游标。sink 从未提交过时返回 (nil, nil)，表示从头开始。
	Load(sink string) ([]byte, error)
	// Commit 持久化 sink 的最新游标（下一批起始位置）。
	Commit(sink string, cursor []byte) error
}

// offsetKeyPrefix 是 offset KV 的 key 前缀，与业务 key 空间隔离，避免冲突。
const offsetKeyPrefix = "__offset__/"

// offsetKey 由 sink 名派生 offset 的 KV key，形如 `__offset__/<sink>`。
func offsetKey(sink string) []byte {
	return []byte(offsetKeyPrefix + sink)
}

// KVOffsetStore 是 OffsetStore 的 KV 实现：游标以 `__offset__/<sink>` 为 key 落到 Committer。
// Commit=Put(key,cursor)，Load=Get(key)。因为写走 Committer→KVServer.Write，raft 模式下
// 游标与业务数据同享 Raft 强一致复制（见包 godoc 的语义边界说明）。
type KVOffsetStore struct {
	committer Committer
}

// NewKVOffsetStore 以 committer 为底层写读端构造一个 KV offset 存储。
func NewKVOffsetStore(committer Committer) *KVOffsetStore {
	return &KVOffsetStore{committer: committer}
}

// Load 读取 sink 已提交的游标；key 不存在（Committer.Get 返回 nil,nil）时返回 (nil,nil)。
func (s *KVOffsetStore) Load(sink string) ([]byte, error) {
	v, err := s.committer.Get(offsetKey(sink))
	if err != nil {
		return nil, fmt.Errorf("offset: load %q: %w", sink, err)
	}
	return v, nil
}

// Commit 将 sink 的游标写入 KV（raft 模式经 Raft 日志强一致复制）。
func (s *KVOffsetStore) Commit(sink string, cursor []byte) error {
	if err := s.committer.Put(offsetKey(sink), cursor); err != nil {
		return fmt.Errorf("offset: commit %q: %w", sink, err)
	}
	return nil
}
