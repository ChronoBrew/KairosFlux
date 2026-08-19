//go:build experimental

// 隔离说明见同包 command.go 顶部注释。
//
// Package shardkv 是「Multi-Raft 分片 KV」集成层（v1）：把已打通的 Multi-Raft 接进 KV——
// 每个分片是一个 Raft 组、写按 key 路由到分片组的 leader、每分片一个状态机（FSM）把已提交
// 命令应用到该分片的 store。
//
// 边界（诚实标注）：
//   - 拓扑：真分片——每个分片按副本因子 rf 只放置到 rf 个节点的副本子集（一致性哈希环选点，
//     见 cluster.ShardReplicas）。rf<节点数即「数据跨节点分区 + 每分片多副本」；rf>=节点数退化
//     为全副本。与 network 层 #191 的一致性哈希分区转发（key→属主节点、无副本）仍是两套一致性
//     模型，ShardKV 自成一层、不劫持 kv.Write。
//   - 存储：每分片 FSM 用内存 store（下面的 KVStore 接口），刻意避开「每分片存储隔离 + 全局
//     配置」难点——把真 LSM 引擎插到 KVStore 边界是独立后续。
//   - 读：本节点托管则本地读、否则 P2C 在副本集间择优转发（见 read.go），均为最终一致（apply
//     异步）；线性一致的 leader 读/read-index 留后续。
package shardkv

import "sync"

// KVStore 是每分片状态机的存储边界。v1 用内存实现；真 LSM 引擎后续在此插入。
type KVStore interface {
	Put(key, value []byte)
	Get(key []byte) ([]byte, bool)
	Delete(key []byte)
}

// memStore 是并发安全的内存 KVStore（v1）。
type memStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{m: make(map[string][]byte)}
}

func (s *memStore) Put(key, value []byte) {
	s.mu.Lock()
	s.m[string(key)] = append([]byte(nil), value...)
	s.mu.Unlock()
}

func (s *memStore) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[string(key)]
	return v, ok
}

func (s *memStore) Delete(key []byte) {
	s.mu.Lock()
	delete(s.m, string(key))
	s.mu.Unlock()
}
