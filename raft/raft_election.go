// 本文件是 Raft 的选举：选举计时、发起投票、统计选票与成为 leader。
package raft

import (
	"log/slog"
	"math/rand"
	"time"
)

func (r *Raft) electionLoop() {
	for {
		select {
		case <-r.stopCh:
			r.timer.Stop()
			return
		case <-r.timer.C:
			r.startElection()
			r.resetElectionTimer()
		case <-r.heartbeatCh:
			r.resetElectionTimer()
		case <-r.electionCh:
			r.resetElectionTimer()
		}
	}
}

// LeaderHint 返回当前已知 leader 的地址供路由重定向：本节点即 leader 时返回自身地址；
// 否则返回从 AppendEntries 学到的 leader 地址；均未知时 ok=false。
func (r *Raft) LeaderHint() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Leader {
		return r.addrMap[r.me], true
	}
	if r.currentLeader >= 0 {
		if addr, ok := r.addrMap[r.currentLeader]; ok {
			return addr, true
		}
	}
	return "", false
}

// Stop 停止 Raft 的选举与心跳循环并释放定时器。幂等（可重复调用）。
// 用途：测试清理（避免泄漏的选举 goroutine 跨用例互扰），以及 Multi-Raft 的组启停。
func (r *Raft) resetElectionTimer() {
	if !r.timer.Stop() {
		select {
		case <-r.timer.C:
		default:
		}
	}
	r.electionTimeout = MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))
	r.timer.Reset(r.electionTimeout)
}

func (r *Raft) startElection() {
	r.mu.Lock()

	if r.state == Leader {
		r.mu.Unlock()
		return
	}

	slog.Info("starting election", "state", r.state, "term", r.Term)

	r.state = Candidate
	r.currentLeader = -1 // 进入选举，leader 暂未知
	r.Term++
	r.votedFor = r.me
	r.persistStateLocked()

	lastLogIndex := -1
	lastLogTerm := 0
	if len(r.log) > 0 {
		lastLogIndex = r.log[len(r.log)-1].Index
		lastLogTerm = r.log[len(r.log)-1].Term
	} else if r.LastIncludedIndex > 0 {
		lastLogIndex = int(r.LastIncludedIndex)
		lastLogTerm = int(r.LastIncludedTerm)
	}

	args := &RequestVoteArgs{
		GroupID:      r.groupID,
		Term:         r.Term,
		CandidateID:  r.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	peerCount := len(r.peers) - 1
	voteCh := make(chan bool, peerCount+1)

	for i := range r.peers {
		if i == r.me {
			continue
		}

		go func(peerID int) {
			reply, err := r.SendRequestVote(r.addrMap[peerID], args)
			if err != nil {
				voteCh <- false
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.Term {
				r.Term = reply.Term
				r.state = Follower
				r.votedFor = -1
				voteCh <- false
				return
			}

			if reply.Term == r.Term && reply.VoteGranted {
				voteCh <- true
			} else {
				voteCh <- false
			}
		}(i)
	}

	electionTerm := r.Term
	r.mu.Unlock()

	// 异步收票：不阻塞 electionLoop。否则候选人在收票的最多 500ms 里无法处理现任 leader
	// 的心跳（AppendEntries 本会把它退回 Follower 并重置计时器），会持续无谓改选、令 leader 抖动。
	go r.awaitVotes(voteCh, peerCount, electionTerm)
}

// awaitVotes 收集本轮（electionTerm）选票并决定当选/退选。仅当仍是本轮任期的 Candidate 时
// 才动作，避免作用于已被更高任期/心跳终结的过期选举。
func (r *Raft) awaitVotes(voteCh chan bool, peerCount int, electionTerm int) {
	wonAsCandidate := func() {
		r.mu.Lock()
		if r.state == Candidate && r.Term == electionTerm {
			r.becomeLeader()
		}
		r.mu.Unlock()
	}

	if peerCount == 0 {
		wonAsCandidate() // 单节点：直接当选
		return
	}

	votes := 1
	timeout := time.After(500 * time.Millisecond)
	for j := 0; j < peerCount; j++ {
		select {
		case voteGranted := <-voteCh:
			if voteGranted {
				votes++
				if votes > len(r.peers)/2 {
					wonAsCandidate()
					return
				}
			}
		case <-timeout:
			r.mu.Lock()
			if r.state == Candidate && r.Term == electionTerm {
				r.state = Follower
				r.votedFor = -1
			}
			r.mu.Unlock()
			return
		}
	}
}

func (r *Raft) becomeLeader() {
	slog.Info("becoming leader", "term", r.Term)
	r.state = Leader
	r.currentLeader = r.me

	// 计算下一个日志的绝对索引（考虑快照偏移）
	nextLogIndex := 0
	if len(r.log) > 0 {
		nextLogIndex = r.log[len(r.log)-1].Index + 1
	} else if r.LastIncludedIndex > 0 {
		nextLogIndex = int(r.LastIncludedIndex) + 1
	}

	for i := range r.peers {
		r.nextIndex[i] = nextLogIndex
		r.matchIndex[i] = int(r.LastIncludedIndex)
	}

	slog.Debug("heartbeat loop started")
	r.startHeartbeatLoop()
}
