package raft

import (
	"log/slog"
	"net/rpc"
)

type RequestVoteArgs struct {
	GroupID      int // 多组分发：目标 Raft 组编号（零值=组 0，单组时兼容）
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	GroupID      int // 多组分发：目标 Raft 组编号（零值=组 0）
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type InstallSnapshotArgs struct {
	GroupID           int // 多组分发：目标 Raft 组编号（零值=组 0）
	Term              int
	LeaderID          int
	Data              []byte
	LastIncludedIndex int64
	LastIncludedTerm  int64
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

type RaftRPC struct {
	raft *Raft
}

func NewRaftRPC(raft *Raft) *RaftRPC {
	return &RaftRPC{raft: raft}
}

func (r *RaftRPC) RegisterRPC(server *rpc.Server) {
	server.Register(r)
}

func (r *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.raft.mu.Lock()
	defer r.raft.mu.Unlock()

	if args.Term < r.raft.Term {
		reply.Term = r.raft.Term
		reply.VoteGranted = false
		return nil
	}

	if args.Term > r.raft.Term {
		r.raft.Term = args.Term
		r.raft.state = Follower
		r.raft.votedFor = -1
		r.raft.persistStateLocked()
	}

	votedForMe := r.raft.votedFor == -1 || r.raft.votedFor == args.CandidateID
	logUpToDate := r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)

	if votedForMe && logUpToDate {
		r.raft.votedFor = args.CandidateID
		r.raft.persistStateLocked()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = r.raft.Term
	return nil
}

func (r *RaftRPC) isLogUpToDate(candidateLastIndex, candidateLastTerm int) bool {
	// 当前节点日志为空且无快照时，候选者日志始终是最新的
	if len(r.raft.log) == 0 && r.raft.LastIncludedIndex == 0 {
		return true
	}

	// 获取当前节点的最后日志索引和任期（考虑快照）
	lastIndex := int(r.raft.LastIncludedIndex)
	lastTerm := int(r.raft.LastIncludedTerm)
	if len(r.raft.log) > 0 {
		lastLog := r.raft.log[len(r.raft.log)-1]
		lastIndex = lastLog.Index
		lastTerm = lastLog.Term
	}

	if candidateLastTerm > lastTerm {
		return true
	}
	if candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex {
		return true
	}
	return false
}

func (r *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.raft.mu.Lock()
	defer r.raft.mu.Unlock()

	if args.Term < r.raft.Term {
		reply.Term = r.raft.Term
		reply.Success = false
		return nil
	}

	if args.Term > r.raft.Term {
		r.raft.Term = args.Term
		r.raft.votedFor = -1
		r.raft.persistStateLocked()
	}

	// 收到当前 term 合法 leader 的 AppendEntries：认其为 leader，Candidate 退回 Follower。
	r.raft.state = Follower
	r.raft.currentLeader = args.LeaderID

	// 重置选举计时器：收到合法 leader 的心跳/复制即认为 leader 存活，避免误发起选举。
	// 非阻塞发送——electionLoop 正阻塞在 heartbeatCh 上时命中并 resetElectionTimer；
	// 若其正忙则丢弃，下次心跳再重置。此前缺失此信号导致 follower 无视心跳持续改选、leader 抖动。
	select {
	case r.raft.heartbeatCh <- true:
	default:
	}

	// 检查 PrevLogIndex 是否匹配（考虑快照偏移）
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex == int(r.raft.LastIncludedIndex) && r.raft.LastIncludedIndex > 0 {
			// prevLogIndex 匹配快照，检查 term 是否一致
			if args.PrevLogTerm != int(r.raft.LastIncludedTerm) {
				reply.Success = false
				reply.Term = r.raft.Term
				return nil
			}
		} else if args.PrevLogIndex > int(r.raft.LastIncludedIndex) {
			relativeIndex := r.raft.relIndex(args.PrevLogIndex)
			if relativeIndex < 0 || relativeIndex >= len(r.raft.log) || r.raft.log[relativeIndex].Term != args.PrevLogTerm {
				reply.Success = false
				reply.Term = r.raft.Term
				return nil
			}
		} else {
			// PrevLogIndex 小于 LastIncludedIndex，日志不一致
			reply.Success = false
			reply.Term = r.raft.Term
			return nil
		}
	}

	// 追加新日志条目（增量持久化）
	needRebuild := false
	var newEntries []LogEntry
	for _, entry := range args.Entries {
		relativeIndex := r.raft.relIndex(entry.Index)
		if relativeIndex < 0 {
			continue // 已被快照覆盖的旧条目，跳过
		}
		if relativeIndex < len(r.raft.log) && r.raft.log[relativeIndex].Term != entry.Term {
			r.raft.log = r.raft.log[:relativeIndex]
			needRebuild = true
		}
		if relativeIndex >= len(r.raft.log) {
			r.raft.log = append(r.raft.log, entry)
			newEntries = append(newEntries, entry)
		}
	}

	if len(args.Entries) > 0 {
		if needRebuild {
			if err := r.raft.wal.RebuildLogFile(r.raft.log); err != nil {
				slog.Error("failed to rebuild log", "error", err)
			}
		} else {
			if err := r.raft.wal.AppendLogs(newEntries); err != nil {
				slog.Error("failed to append logs", "error", err)
			}
		}
		r.raft.persistStateLocked()
	}

	if args.LeaderCommit > r.raft.commitIndex {
		lastLogIndex := int(r.raft.LastIncludedIndex)
		if len(r.raft.log) > 0 {
			lastLogIndex = r.raft.log[len(r.raft.log)-1].Index
		}
		r.raft.commitIndex = min(args.LeaderCommit, lastLogIndex)
		r.applyCommittedLogs()
	}

	reply.Success = true
	reply.Term = r.raft.Term
	return nil
}

func (r *RaftRPC) applyCommittedLogs() {
	for r.raft.lastApplied < r.raft.commitIndex {
		r.raft.lastApplied++
		relativeIndex := r.raft.relIndex(r.raft.lastApplied)
		if relativeIndex >= 0 && relativeIndex < len(r.raft.log) {
			if r.raft.ApplyCh != nil {
				r.raft.ApplyCh <- r.raft.log[relativeIndex]
			}
		}
	}
}

// 被调用端
func (r *RaftRPC) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	r.raft.mu.Lock()
	defer r.raft.mu.Unlock()

	if args.Term < r.raft.Term {
		reply.Term = r.raft.Term
		reply.Success = false
		return nil
	}
	if args.Term > r.raft.Term {
		r.raft.Term = args.Term
		r.raft.state = Follower
		r.raft.votedFor = -1
	}

	if args.LastIncludedIndex <= r.raft.LastIncludedIndex {
		// 快照比已有的还旧，不需要应用
		reply.Success = false
		reply.Term = r.raft.Term
		return nil
	}

	// 1. 先保存快照到磁盘
	if err := r.raft.wal.SaveSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm); err != nil {
		slog.Error("failed to save snapshot", "error", err)
		reply.Success = false
		reply.Term = r.raft.Term
		return err
	}

	// 2. 删除旧快照
	r.raft.wal.DeleteOldSnapshots(args.LastIncludedIndex)

	// 3. 清理内存中的日志并重新编号（移除快照包含的条目）
	newLogStart := int(args.LastIncludedIndex) + 1 - int(r.raft.LastIncludedIndex)
	if newLogStart >= 0 && newLogStart <= len(r.raft.log) {
		r.raft.log = r.raft.log[newLogStart:]
		for i := range r.raft.log {
			r.raft.log[i].Index = int(args.LastIncludedIndex) + 1 + i
		}
	} else {
		r.raft.log = []LogEntry{}
	}

	// 4. 截断 WAL 日志
	if err := r.raft.wal.TruncateLogs(args.LastIncludedIndex); err != nil {
		slog.Error("failed to truncate logs", "error", err)
		reply.Success = false
		reply.Term = r.raft.Term
		return err
	}

	// 5. 更新元数据
	r.raft.commitIndex = int(args.LastIncludedIndex)
	r.raft.lastApplied = int(args.LastIncludedIndex)
	r.raft.lastSnapshotIndex = int(args.LastIncludedIndex)
	r.raft.LastIncludedIndex = args.LastIncludedIndex
	r.raft.LastIncludedTerm = args.LastIncludedTerm

	// 6. 通知 FSM 应用快照
	if r.raft.ApplyCh != nil {
		snapshotEntry := LogEntry{
			Index:      int(args.LastIncludedIndex),
			Term:       int(args.LastIncludedTerm),
			Command:    args.Data,
			IsSnapshot: true,
		}
		select {
		case r.raft.ApplyCh <- snapshotEntry:
			slog.Info("snapshot delivered to FSM", "index", args.LastIncludedIndex)
		default:
			slog.Warn("ApplyCh full, snapshot delivery skipped")
		}
	}

	// 7. 持久化状态（日志已由 TruncateLogs 处理）
	r.raft.persistStateLocked()

	reply.Term = r.raft.Term
	reply.Success = true
	return nil
}

func (r *Raft) SendRequestVote(serverAddr string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := defaultRPCPool.call(serverAddr, "RaftRPC.RequestVote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (r *Raft) SendAppendEntries(serverAddr string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := defaultRPCPool.call(serverAddr, "RaftRPC.AppendEntries", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (r *Raft) SendInstallSnapshot(serverAddr string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	if err := defaultRPCPool.call(serverAddr, "RaftRPC.InstallSnapshot", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
