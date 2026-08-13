// 本文件是 Raft 的日志复制：追加日志、心跳与复制、推进 commitIndex 并 apply。
package raft

import (
	"fmt"
	"log/slog"
	"time"
)

func (r *Raft) startHeartbeatLoop() {
	if r.heartbeatTicker != nil {
		r.heartbeatTicker.Stop()
	}

	r.heartbeatTicker = time.NewTicker(HeartbeatInterval)
	ticker := r.heartbeatTicker
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-r.stopCh:
				return
			case <-ticker.C:
				// 心跳即携带待复制条目的 AppendEntries：与 replicateLog 同一路径，
				// 确保 follower 落后时下一次心跳就把缺的条目补上，而不是发空包。
				r.mu.Lock()
				if r.state != Leader {
					r.mu.Unlock()
					return
				}
				r.replicateLog()
				r.mu.Unlock()
			}
		}
	}()
}

func (r *Raft) updateCommitIndex() {
	if r.state != Leader {
		return
	}

	// 从后往前遍历日志条目，找到可以提交的
	for i := len(r.log) - 1; i >= 0; i-- {
		n := r.log[i].Index
		if n <= r.commitIndex {
			continue
		}
		if r.log[i].Term != r.Term {
			continue
		}

		count := 1
		for j := range r.peers {
			if j != r.me && r.matchIndex[j] >= n {
				count++
			}
		}
		if count > len(r.peers)/2 {
			r.commitIndex = n
			r.applyCommittedLogs()
			r.commitCond.Broadcast()
			break
		}
	}
}

func (r *Raft) applyCommittedLogs() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		// 将绝对索引转换为相对数组索引
		relativeIndex := r.relIndex(r.lastApplied)
		if relativeIndex >= 0 && relativeIndex < len(r.log) {
			if r.ApplyCh != nil {
				r.ApplyCh <- r.log[relativeIndex]
			}
		}
	}

	// 检查是否需要触发快照
	r.checkSnapshotTrigger()
}

// checkSnapshotTrigger 检查是否应该触发快照
func (r *Raft) AppendEntry(command []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		slog.Warn("AppendEntry rejected, not leader", "state", r.state)
		return -1, fmt.Errorf("not leader")
	}

	// 计算绝对索引（考虑快照偏移）
	lastLogIndex := -1
	if len(r.log) > 0 {
		lastLogIndex = r.log[len(r.log)-1].Index
	} else if r.LastIncludedIndex > 0 {
		lastLogIndex = int(r.LastIncludedIndex)
	}

	entry := LogEntry{
		Index:   lastLogIndex + 1,
		Term:    r.Term,
		Command: command,
	}
	r.log = append(r.log, entry)

	// 增量持久化：仅追加一条日志
	if err := r.wal.AppendLog(entry); err != nil {
		slog.Error("failed to append log", "error", err)
	}

	// 单节点模式：立即提交
	if len(r.peers) == 1 {
		r.commitIndex = entry.Index
		r.applyCommittedLogs()
		r.commitCond.Broadcast()
	} else {
		r.replicateLog()
	}

	return entry.Index, nil
}

func (r *Raft) replicateLog() {
	if r.state != Leader {
		return
	}

	for i := range r.peers {
		if i == r.me {
			continue
		}

		prevLogIndex := r.nextIndex[i] - 1

		// 如果 follower 落后太多（prevLogIndex 在快照范围内），发送 InstallSnapshot
		if prevLogIndex < int(r.LastIncludedIndex) && r.LastIncludedIndex > 0 {
			snapshotData, _, _, err := r.wal.LoadLatestSnapshot()
			if err == nil && snapshotData != nil {
				snapArgs := &InstallSnapshotArgs{
					GroupID:           r.groupID,
					Term:              r.Term,
					LeaderID:          r.me,
					Data:              snapshotData,
					LastIncludedIndex: r.LastIncludedIndex,
					LastIncludedTerm:  r.LastIncludedTerm,
				}
				go func(peerID int, snapArgs *InstallSnapshotArgs) {
					reply, err := r.SendInstallSnapshot(r.addrMap[peerID], snapArgs)
					if err != nil {
						return
					}
					r.mu.Lock()
					defer r.mu.Unlock()
					if reply.Success {
						r.nextIndex[peerID] = int(r.LastIncludedIndex) + 1
						r.matchIndex[peerID] = int(r.LastIncludedIndex)
					} else if reply.Term > r.Term {
						r.Term = reply.Term
						r.state = Follower
						r.votedFor = -1
						r.heartbeatTicker.Stop()
					}
				}(i, snapArgs)
			}
			continue
		}

		prevLogTerm := r.getTermAt(prevLogIndex)

		// 将绝对索引转换为相对数组索引来切片日志
		var entries []LogEntry
		relativeStart := r.relIndex(r.nextIndex[i])
		if relativeStart >= 0 && relativeStart < len(r.log) {
			entries = r.log[relativeStart:]
		}

		args := &AppendEntriesArgs{
			GroupID:      r.groupID,
			Term:         r.Term,
			LeaderID:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}

		go func(peerID int, args *AppendEntriesArgs) {
			reply, err := r.SendAppendEntries(r.addrMap[peerID], args)
			if err != nil {
				r.mu.Lock()
				if r.state == Leader {
					r.nextIndex[peerID]--
				}
				r.mu.Unlock()
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.Term {
				r.Term = reply.Term
				r.state = Follower
				r.votedFor = -1
				r.heartbeatTicker.Stop()
				return
			}

			if reply.Success {
				// 只按「本次实际发送的条目」推进 matchIndex，绝不按 leader 的 last index——
				// 否则空心跳会把 follower 误标为已追平（历史 bug：空 entries 却推进到 last）。
				matched := args.PrevLogIndex + len(args.Entries)
				if matched > r.matchIndex[peerID] {
					r.matchIndex[peerID] = matched
					r.nextIndex[peerID] = matched + 1
				}
				r.updateCommitIndex()
			} else if r.nextIndex[peerID] > 0 {
				r.nextIndex[peerID]--
			}
		}(i, args)
	}
}

func (r *Raft) WaitCommitIndex(index int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.commitIndex < index {
		r.commitCond.Wait()
	}
}
