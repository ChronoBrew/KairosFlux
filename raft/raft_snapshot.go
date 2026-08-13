// 本文件是 Raft 的快照：触发判定、生成快照，以及日志条目的序列化编解码。
package raft

import (
	"encoding/binary"
	"fmt"
	"github.com/NeverENG/BanDB/config"
	"log/slog"
)

func (r *Raft) checkSnapshotTrigger() {
	if r.state != Leader {
		return
	}

	logLength := len(r.log)
	threshold := config.G.RaftSnapshotThreshold
	if threshold <= 0 {
		threshold = 10000
	}
	keepEntries := config.G.RaftSnapshotKeepEntries
	if keepEntries <= 0 {
		keepEntries = 100
	}

	if logLength > threshold {
		snapshotIndex := r.commitIndex - keepEntries
		if snapshotIndex > r.lastSnapshotIndex {
			slog.Info("auto-triggering snapshot", "index", snapshotIndex, "logLen", logLength, "threshold", threshold)
			// 异步调用避免持锁死锁（checkSnapshotTrigger 在持锁上下文中被调用）
			go r.TakeSnapshot(snapshotIndex)
		}
	}
}

// relIndex 把绝对日志 index 映射为 r.log 的数组下标。
//
// off-by-one 易错点：无快照时基准应为 -1（首条绝对 index 0 → 数组下标 0）；
// 但 LastIncludedIndex 初始为 0，旧代码统一用 `abs - LastIncludedIndex - 1`，在无快照场景
// 把 index 0 算成 -1 → 追加/取 term/切片全部越界或错位。因 LastIncludedIndex>0 才代表"有快照"，
// 这里无快照时用基准 -1，有快照时用 LastIncludedIndex（与旧公式一致，不影响快照路径）。
func (r *Raft) TakeSnapshot(index int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.lastSnapshotIndex {
		return fmt.Errorf("snapshot index %d is not greater than last snapshot index %d", index, r.lastSnapshotIndex)
	}

	if index > r.commitIndex {
		return fmt.Errorf("cannot snapshot uncommitted index %d, commitIndex is %d", index, r.commitIndex)
	}

	// 收集需要放入快照的日志条目（绝对索引 <= index）
	var snapshotEntries []LogEntry
	relativeEnd := index + 1 - int(r.LastIncludedIndex)
	for i := 0; i < relativeEnd && i < len(r.log); i++ {
		snapshotEntries = append(snapshotEntries, r.log[i])
	}

	// 获取最后一条的 term
	var term int
	if len(snapshotEntries) > 0 {
		term = snapshotEntries[len(snapshotEntries)-1].Term
	} else if index == int(r.LastIncludedIndex) {
		term = int(r.LastIncludedTerm)
	}

	// 序列化日志条目为快照数据
	data := SerializeLogEntries(snapshotEntries)

	// 1. 先保存快照到磁盘
	if err := r.wal.SaveSnapshot(data, int64(index), int64(term)); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// 2. 删除旧快照
	r.wal.DeleteOldSnapshots(int64(index))

	// 3. 截断 WAL 日志
	if err := r.wal.TruncateLogs(int64(index)); err != nil {
		return fmt.Errorf("failed to truncate logs: %w", err)
	}

	// 4. 清理内存中的日志并重新编号
	newLogStart := index + 1 - int(r.LastIncludedIndex)
	if newLogStart >= 0 && newLogStart <= len(r.log) {
		r.log = r.log[newLogStart:]
		for i := range r.log {
			r.log[i].Index = index + 1 + i
		}
	} else {
		r.log = []LogEntry{}
	}

	// 5. 更新元数据
	r.lastSnapshotIndex = index
	r.LastIncludedIndex = int64(index)
	r.LastIncludedTerm = int64(term)

	// 6. 通知 FSM 异步重放快照（日志条目序列化数据）
	if r.ApplyCh != nil {
		snapshotEntry := LogEntry{
			Index:      index,
			Term:       term,
			Command:    data,
			IsSnapshot: true,
		}
		select {
		case r.ApplyCh <- snapshotEntry:
			slog.Info("snapshot replay sent to FSM", "index", index, "entries", len(snapshotEntries))
		default:
			slog.Warn("ApplyCh full, snapshot replay skipped")
		}
	}

	// 7. 持久化状态（日志已由 TruncateLogs 处理）
	r.persistStateLocked()

	return nil
}

// SerializeLogEntries 序列化日志条目为字节流（快照数据格式）
func SerializeLogEntries(entries []LogEntry) []byte {
	if len(entries) == 0 {
		return nil
	}

	size := 4 // entry count
	for _, e := range entries {
		size += 8 + 8 + 8 + len(e.Command) // Index(8) + Term(8) + CmdLen(8) + Command
	}

	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(entries)))
	offset += 4
	for _, e := range entries {
		binary.BigEndian.PutUint64(buf[offset:], uint64(e.Index))
		offset += 8
		binary.BigEndian.PutUint64(buf[offset:], uint64(e.Term))
		offset += 8
		binary.BigEndian.PutUint64(buf[offset:], uint64(len(e.Command)))
		offset += 8
		copy(buf[offset:], e.Command)
		offset += len(e.Command)
	}
	return buf
}

// DeserializeLogEntries 反序列化日志条目
func DeserializeLogEntries(data []byte) []LogEntry {
	if len(data) < 4 {
		return nil
	}

	offset := 0
	count := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	entries := make([]LogEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if offset+24 > len(data) {
			break
		}
		index := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		term := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		cmdLen := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8

		if offset+cmdLen > len(data) {
			break
		}
		cmd := make([]byte, cmdLen)
		copy(cmd, data[offset:offset+cmdLen])
		offset += cmdLen

		entries = append(entries, LogEntry{
			Index:   index,
			Term:    term,
			Command: cmd,
		})
	}
	return entries
}
