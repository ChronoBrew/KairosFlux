// 本文件是 Raft 的类型、构造、生命周期与只读访问器。
// 选举见 raft_election.go，日志复制见 raft_replication.go，快照见 raft_snapshot.go，
// 状态与日志的持久化见 raft_persist.go。
package raft

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

const (
	MinElectionTimeout = 150 * time.Millisecond
	MaxElectionTimeout = 300 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index      int
	Term       int
	Command    []byte
	IsSnapshot bool
}

type Raft struct {
	groupID  int // 多组（Multi-Raft）编号；单组为 0。用于出站 RPC 标注与对端按组分发。
	peers    []string
	me       int
	state    State
	votedFor int
	Term     int
	mu       sync.Mutex

	electionTimeout time.Duration
	timer           *time.Timer
	heartbeatTicker *time.Ticker

	commitIndex       int
	lastApplied       int
	lastSnapshotIndex int

	nextIndex  []int
	matchIndex []int
	log        []LogEntry

	// electionCh 与 heartbeatCh 是纯信号：值不携带信息，故用 struct{}。
	electionCh  chan struct{}
	heartbeatCh chan struct{}
	ApplyCh     chan LogEntry

	LastIncludedIndex int64
	LastIncludedTerm  int64

	wal     *RaftWAL
	addrMap map[int]string

	commitCond *sync.Cond

	currentLeader int // 已知的当前 leader 的 peer 下标；-1 表示未知（供 leader-aware 路由重定向）

	stopCh   chan struct{} // 关闭以停止选举/心跳循环
	stopOnce sync.Once
}

func NewRaft(peers []string, me int) *Raft {
	return NewRaftGroup(0, peers, me, "raft_data")
}

func NewRaftWithDataDir(peers []string, me int, dataDir string) *Raft {
	return NewRaftGroup(0, peers, me, dataDir)
}

// NewRaftGroup 创建一个属于 groupID 组的 Raft 实例（Multi-Raft）。groupID 在启动选举循环
// 前设定，出站 RPC 会据此标注，对端 RaftGroupServer 按组分发。单组用 NewRaft/NewRaftWithDataDir（组 0）。
func NewRaftGroup(groupID int, peers []string, me int, dataDir string) *Raft {
	addrMap := make(map[int]string)
	for i, addr := range peers {
		addrMap[i] = addr
	}

	r := &Raft{
		groupID:         groupID,
		peers:           peers,
		me:              me,
		state:           Follower,
		votedFor:        -1,
		Term:            0,
		electionTimeout: MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout))),
		commitIndex:     -1,
		lastApplied:     -1,

		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		log:           make([]LogEntry, 0),
		electionCh:    make(chan struct{}),
		heartbeatCh:   make(chan struct{}),
		ApplyCh:       make(chan LogEntry, 100),
		addrMap:       addrMap,
		stopCh:        make(chan struct{}),
		currentLeader: -1,
	}

	wal, _ := NewRaftWAL(dataDir)

	r.wal = wal

	// 从磁盘加载持久化状态（currentTerm, votedFor, log, snapshot metadata）
	if err := r.readPersist(); err != nil {
		slog.Warn("failed to load persisted state", "error", err)
	}

	// 如果有快照，通知 FSM
	if r.LastIncludedIndex > 0 && r.ApplyCh != nil {
		snapshotData, _, _, err := wal.LoadLatestSnapshot()
		if err == nil && snapshotData != nil {
			select {
			case r.ApplyCh <- LogEntry{
				Index:      int(r.LastIncludedIndex),
				Term:       int(r.LastIncludedTerm),
				Command:    snapshotData,
				IsSnapshot: true,
			}:
			default:
				slog.Warn("ApplyCh full during init, snapshot skipped")
			}
		}
	}

	r.commitCond = sync.NewCond(&r.mu)
	r.timer = time.NewTimer(r.electionTimeout)

	go r.electionLoop()

	return r
}

// persistStateLocked 仅持久化 Term 和 votedFor（增量持久化，O(1)）
func (r *Raft) Start() {
	if r.state == Leader {
		r.startHeartbeatLoop()
	}
}

func (r *Raft) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}

// resetElectionTimer 复用单一 timer: 先 Stop→drain 残留信号, 再用新随机超时 Reset
func (r *Raft) relIndex(absIndex int) int {
	base := -1
	if r.LastIncludedIndex > 0 {
		base = int(r.LastIncludedIndex)
	}
	return absIndex - base - 1
}

// getTermAt 获取指定绝对索引处的日志 term（考虑快照偏移）
func (r *Raft) getTermAt(absIndex int) int {
	if absIndex < 0 {
		return 0
	}
	if absIndex == int(r.LastIncludedIndex) && r.LastIncludedIndex > 0 {
		return int(r.LastIncludedTerm)
	}
	relativeIndex := r.relIndex(absIndex)
	if relativeIndex >= 0 && relativeIndex < len(r.log) {
		return r.log[relativeIndex].Term
	}
	return 0
}

// getLastLogIndex 获取最后一条日志的绝对索引
func (r *Raft) getLastLogIndex() int {
	if len(r.log) > 0 {
		return r.log[len(r.log)-1].Index
	}
	if r.LastIncludedIndex > 0 {
		return int(r.LastIncludedIndex)
	}
	return -1
}

func (r *Raft) State() (State, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.Term
}

func (r *Raft) Log() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	logCopy := make([]LogEntry, len(r.log))
	copy(logCopy, r.log)
	return logCopy
}

// CommitIndex 获取当前提交索引
func (r *Raft) CommitIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}
