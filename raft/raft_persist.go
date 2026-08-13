// 本文件是 Raft 的持久化：任期/投票状态与日志的落盘，以及重启时的读回。
package raft

import (
	"log/slog"
)

func (r *Raft) persistStateLocked() {
	if err := r.wal.SaveState(int64(r.Term), int64(r.votedFor)); err != nil {
		slog.Error("failed to persist state", "error", err)
	}
}

// persistLocked 全量持久化 Raft 状态（仅用于日志冲突截断等特殊情况）
func (r *Raft) persistLocked() {
	data := PersistData{
		CurrentTerm:       int64(r.Term),
		VotedFor:          int64(r.votedFor),
		Log:               r.log,
		LastIncludedIndex: r.LastIncludedIndex,
		LastIncludedTerm:  r.LastIncludedTerm,
	}

	if err := r.wal.SavePersist(data); err != nil {
		slog.Error("failed to persist state", "error", err)
	}
}

// readPersist 从磁盘加载 Raft 状态
func (r *Raft) readPersist() error {
	data, err := r.wal.LoadPersist()
	if err != nil {
		return err
	}

	r.Term = int(data.CurrentTerm)
	r.votedFor = int(data.VotedFor)
	r.log = data.Log
	r.LastIncludedIndex = data.LastIncludedIndex
	r.LastIncludedTerm = data.LastIncludedTerm

	if r.LastIncludedIndex > 0 {
		r.commitIndex = int(r.LastIncludedIndex)
		r.lastApplied = int(r.LastIncludedIndex)
		r.lastSnapshotIndex = int(r.LastIncludedIndex)
	}

	return nil
}
