package service

import (
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/pkg/predicate"
	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/raft"
	"github.com/NeverENG/BanDB/storage"
)

// CommandType 是写命令的类型。具名类型 + 具名常量使拼写只存在此处一份，调用点不再重复
// "Put"/"Delete" 字面量——散落的字面量一旦拼错，switch 会落空，命令既不写入也不删除。
//
// 它不是编译期强校验：无类型字符串常量可隐式转为 CommandType，故 Type: "Pt" 仍能编译。
// 强校验需改为 int 枚举，但 Command 经 json.Marshal 写入 Raft 日志，改变数值表示会破坏
// 既有日志的兼容性，故底层类型保持字符串。
type CommandType string

const (
	CommandPut    CommandType = "Put"
	CommandDelete CommandType = "Delete"
)

type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte
}

type KVServer struct {
	raft    *raft.Raft
	storage *storage.Engine
	wal     *storage.WAL // standalone 模式的存储层 WAL；raft 模式为 nil

	// cpMu 协调写入与 WAL checkpoint：每次写用 RLock 把 wal.Append+storage.Put
	// 两步一起罩住（写之间仍并发，不影响 group commit）；checkpoint 用 Lock 独占，
	// 待所有写静默后 active+dirty 快照才必然一致，此时重写 WAL 才不丢"已落 WAL 未进
	// memtable"的在途写。
	cpMu       sync.RWMutex
	writeCount atomic.Int64
}

// NewKVServer 创建 KVServer，按运行模式初始化存储与持久化路径。
// standalone：构建存储层 WAL 并重放到 memtable，不启动 Raft。
// raft：启动 Raft，写经其日志，不使用存储层 WAL。
func NewKVServer() *KVServer {
	// 初始化存储
	kv := &KVServer{
		storage: storage.NewEngine(),
	}

	if config.G.Mode == config.ModeStandalone {
		wal, err := storage.NewWAL(config.G.WALPath)
		if err != nil {
			slog.Error("failed to open storage WAL", "path", config.G.WALPath, "error", err)
			panic("failed to open storage WAL: " + err.Error())
		}
		kv.wal = wal
		kv.replayWAL()
		return kv
	}

	// raft 模式：写经 Raft 日志
	kv.raft = raft.NewRaft(config.G.Peers, config.G.Me)
	return kv
}

// replayWAL 启动时把 WAL 中的记录重放进 memtable（幂等盲写）。
func (k *KVServer) replayWAL() {
	if err := k.wal.Replay(func(op uint8, key, value []byte) error {
		switch op {
		case storage.WALOpPut:
			return k.storage.Put(key, value)
		case storage.WALOpDelete:
			return k.storage.Delete(key)
		}
		return nil
	}); err != nil {
		slog.Error("WAL replay failed", "error", err)
	}
}

// Run 运行 FSM。standalone 模式下写已在 Write 中直接落 WAL+存储，无需 apply 循环。
func (k *KVServer) Run() {
	if k.raft == nil {
		slog.Info("KVServer started in standalone mode (no Raft apply loop)")
		return
	}
	slog.Info("KVServer started, waiting for Raft entries")
	for entry := range k.raft.ApplyCh {
		k.Apply(entry)
	}
}

// Write 统一写入入口：standalone 直接落 WAL+存储；raft 经日志提交后由 apply 循环落盘。
func (k *KVServer) Write(cmd Command) error {
	if k.raft == nil {
		return k.writeStandalone(cmd)
	}
	index, err := k.AppendEntry(cmd)
	if err != nil {
		return err
	}
	return k.WaitForCommit(index)
}

// writeStandalone 先 append+fsync WAL，再写 memtable，提供单机崩溃恢复。
// 全程持 cpMu.RLock（与 checkpoint 的 Lock 互斥），确保 append 与 Put 两步之间
// 不会被 checkpoint 抢入截断——否则会丢"已落 WAL 但尚未进 memtable"的在途写。
func (k *KVServer) writeStandalone(cmd Command) error {
	k.cpMu.RLock()
	err := k.applyStandalone(cmd)
	k.cpMu.RUnlock()
	if err != nil {
		return err
	}
	k.maybeCheckpoint()
	return nil
}

// applyStandalone 执行一条写的 WAL append + memtable 落地，调用方须持 cpMu.RLock。
func (k *KVServer) applyStandalone(cmd Command) error {
	switch cmd.Type {
	case CommandPut:
		if err := k.wal.Append(storage.WALOpPut, cmd.Key, cmd.Value); err != nil {
			return err
		}
		return k.storage.Put(cmd.Key, cmd.Value)
	case CommandDelete:
		if err := k.wal.Append(storage.WALOpDelete, cmd.Key, nil); err != nil {
			return err
		}
		return k.storage.Delete(cmd.Key)
	}
	return nil
}

// checkpointInterval 每累计多少次写触发一次 WAL 自清洁重写。取 2×MaxMemTableSize：
// 略高于单表 flush 阈值，保证 checkpoint 时通常已有历史数据落 SSTable 可回收，
// 同时把 WAL 稳态大小约束在未 flush 热数据量级。
func checkpointInterval() int64 {
	return int64(2 * config.G.MaxMemTableSize)
}

// maybeCheckpoint 累计写达到阈值时触发一次 checkpoint。Add 原子自增保证同一阈值
// 仅一个 goroutine 命中触发。
func (k *KVServer) maybeCheckpoint() {
	n := checkpointInterval()
	if n <= 0 {
		return
	}
	if k.writeCount.Add(1)%n == 0 {
		k.Checkpoint()
	}
}

// Checkpoint 独占静默所有写后，把 WAL 整体重写为未 flush 热数据(active+dirty，含墓碑)
// 快照，回收已落 SSTable 的历史 WAL，令 WAL 大小有界。standalone 专用；raft 模式
// 无存储层 WAL，为 no-op。
func (k *KVServer) Checkpoint() {
	if k.wal == nil {
		return
	}
	k.cpMu.Lock()
	defer k.cpMu.Unlock()

	live := k.storage.SnapshotLive()
	records := make([]storage.WALRecord, len(live))
	for i, e := range live {
		op := storage.WALOpPut
		if e.Value == nil { // nil=墓碑；空切片[]byte{}非 nil=普通写
			op = storage.WALOpDelete
		}
		records[i] = storage.WALRecord{Op: op, Key: e.Key, Value: e.Value}
	}
	if err := k.wal.Rewrite(records); err != nil {
		slog.Error("WAL checkpoint rewrite failed", "error", err)
	}
}

// Close 优雅停机：停止存储后台协程并关闭 standalone WAL（raft 模式 wal 为 nil）。
func (k *KVServer) Close() error {
	if k.storage != nil {
		_ = k.storage.Close()
	}
	if k.wal != nil {
		return k.wal.Close()
	}
	return nil
}

// Apply 应用日志到存储
func (k *KVServer) Apply(entry raft.LogEntry) {
	if entry.IsSnapshot {
		go k.replaySnapshot(entry)
		return
	}

	var cmd Command
	if err := json.Unmarshal(entry.Command, &cmd); err != nil {
		slog.Error("failed to unmarshal command", "error", err)
		return
	}

	switch cmd.Type {
	case CommandPut:
		if err := k.storage.Put(cmd.Key, cmd.Value); err != nil {
			slog.Error("failed to put", "error", err)
		}
	case CommandDelete:
		if err := k.storage.Delete(cmd.Key); err != nil {
			slog.Error("failed to delete", "error", err)
		}
	}
}

// replaySnapshot 异步重放快照中的日志条目到临时表并 Flush 到 SSTable
func (k *KVServer) replaySnapshot(entry raft.LogEntry) {
	entries := raft.DeserializeLogEntries(entry.Command)
	if len(entries) == 0 {
		return
	}

	kvEntries := make([]storage.LogEntry, 0, len(entries))
	for _, e := range entries {
		var cmd Command
		if err := json.Unmarshal(e.Command, &cmd); err != nil {
			continue
		}
		switch cmd.Type {
		case CommandPut:
			kvEntries = append(kvEntries, storage.LogEntry{Key: cmd.Key, Value: cmd.Value})
		case CommandDelete:
			kvEntries = append(kvEntries, storage.LogEntry{Key: cmd.Key, Value: nil})
		}
	}

	if err := k.storage.FlushToSSTable(kvEntries); err != nil {
		slog.Error("snapshot replay failed", "error", err)
	}
}

// Get 读取 key 的最新值。key 不存在时返回 storage.ErrKeyNotFound，调用方以
// errors.Is 判别，从而与读盘失败等真实故障区分开。
func (k *KVServer) Get(key []byte) ([]byte, error) {
	value, err := k.storage.Get(key)
	if value == nil && err == nil {
		return nil, storage.ErrKeyNotFound
	}
	return value, err
}

// maxScanResults 限制单次扫描返回条目数，防止无谓词大范围扫描撑爆内存。
const maxScanResults = 10000

// Scan 在 [start,end] 闭区间扫描 MemTable 热数据，对满足谓词的条目收集 key/value
// 拷贝后返回（只回传命中切片）。底层切片归 MemTable 所有，故必须拷贝。
// 达到上限时截断并告警。
func (k *KVServer) Scan(start, end []byte, pred predicate.Predicate) []proto.ScanEntry {
	out := make([]proto.ScanEntry, 0)
	k.storage.ScanRange(start, end, func(key, value []byte) bool {
		if !pred.Eval(value) {
			return true
		}
		out = append(out, proto.ScanEntry{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), value...),
		})
		if len(out) >= maxScanResults {
			slog.Warn("scan truncated at result limit", "limit", maxScanResults)
			return false
		}
		return true
	})
	return out
}

/* Put 直接写入存储（仅用于测试，生产环境应通过 Raft 写入）
func (k *KVServer) Put(key []byte, value []byte) error {
	return k.storage.Put(key, value)
}
*/

/* Delete 直接删除存储中的值（仅用于测试，生产环境应通过 Raft 写入）
func (k *KVServer) Delete(key []byte) error {
	return k.storage.Delete(key)
}
*/

// Raft 获取 Raft 实例
func (k *KVServer) Raft() *raft.Raft {
	return k.raft
}

// AppendEntry 通过 Raft 追加日志
func (k *KVServer) AppendEntry(cmd Command) (int, error) {
	cmdBytes, err := EncodeCommand(cmd)
	if err != nil {
		return -1, err
	}
	index, err := k.raft.AppendEntry(cmdBytes)
	if err != nil {
		return -1, err
	}
	return index, nil
}

// WaitForCommit 等待日志被提交
func (k *KVServer) WaitForCommit(index int) error {
	// 检查当前提交索引
	k.raft.WaitCommitIndex(index)
	return nil

}

// WaitUntilReady 在单节点集群下阻塞直到本节点成为 Leader，避免客户端端口已开放
// 但 Raft 尚未选主时写请求被拒（AppendEntry 返回 not leader，见 #86）。
// 单节点选主必达且很快，故无需超时。多节点集群直接返回不阻塞：Follower 永远不会
// 成为 Leader，且其端口需立即开放以提供本地读；多节点选主窗口内的写失败需由客户端
// 重试关闭，超出本修复范围。
func (k *KVServer) WaitUntilReady() {
	if k.raft == nil {
		return // standalone：无选主，端口可立即开放
	}
	if len(config.G.Peers) != 1 {
		return
	}
	slog.Info("single-node: waiting for leader before serving clients")
	for {
		if state, _ := k.raft.State(); state == raft.Leader {
			slog.Info("leader ready, opening client port")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// EncodeCommand 编码命令为 JSON
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}
