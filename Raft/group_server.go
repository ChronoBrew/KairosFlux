package Raft

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

// RaftGroupServer 是 Multi-Raft 的传输多路复用器：同一节点上的多个 Raft 组共享一个 RPC
// 端点，按入站请求的 GroupID 分发到对应组的 *Raft。
//
// 注册名固定为 "RaftRPC"，故与现有客户端 Call("RaftRPC.RequestVote", ...) 完全兼容——
// 单组即 groupID 0。这样"一个节点多个组共享一个端口"无需改动发送端调用点，只靠 args.GroupID。
type RaftGroupServer struct {
	mu     sync.RWMutex
	groups map[int]*Raft
}

// NewRaftGroupServer 创建一个空的多组分发器。
func NewRaftGroupServer() *RaftGroupServer {
	return &RaftGroupServer{groups: make(map[int]*Raft)}
}

// AddGroup 注册（或替换）一个组。
func (s *RaftGroupServer) AddGroup(groupID int, r *Raft) {
	s.mu.Lock()
	s.groups[groupID] = r
	s.mu.Unlock()
}

// RemoveGroup 注销一个组（组停机后调用）。
func (s *RaftGroupServer) RemoveGroup(groupID int) {
	s.mu.Lock()
	delete(s.groups, groupID)
	s.mu.Unlock()
}

func (s *RaftGroupServer) group(id int) *Raft {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groups[id]
}

// RegisterRPC 以固定名 "RaftRPC" 注册到 net/rpc 服务器，令按组分发对现有客户端透明。
func (s *RaftGroupServer) RegisterRPC(server *rpc.Server) error {
	return server.RegisterName("RaftRPC", s)
}

// RequestVote 按 GroupID 分发到对应组的投票处理（复用单组 RaftRPC 的处理逻辑）。
func (s *RaftGroupServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r := s.group(args.GroupID)
	if r == nil {
		return fmt.Errorf("raft: no group %d", args.GroupID)
	}
	return (&RaftRPC{raft: r}).RequestVote(args, reply)
}

// AppendEntries 按 GroupID 分发。
func (s *RaftGroupServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r := s.group(args.GroupID)
	if r == nil {
		return fmt.Errorf("raft: no group %d", args.GroupID)
	}
	return (&RaftRPC{raft: r}).AppendEntries(args, reply)
}

// InstallSnapshot 按 GroupID 分发。
func (s *RaftGroupServer) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	r := s.group(args.GroupID)
	if r == nil {
		return fmt.Errorf("raft: no group %d", args.GroupID)
	}
	return (&RaftRPC{raft: r}).InstallSnapshot(args, reply)
}

// ProposeArgs 是客户端向某组提交一条命令的请求（leader-aware 路由入口）。
type ProposeArgs struct {
	GroupID int
	Command []byte
}

// ProposeReply 回复提交结果：非 leader 时给出 LeaderHint 供客户端重定向。
type ProposeReply struct {
	Success    bool
	NotLeader  bool
	LeaderHint string // 已知 leader 地址；空表示未知（正在选主）
}

// Propose 在本节点的目标组上提交命令：本节点是该组 leader 则 AppendEntry 并等待提交，
// 否则回 NotLeader + LeaderHint 供客户端重定向到 leader。
func (s *RaftGroupServer) Propose(args *ProposeArgs, reply *ProposeReply) error {
	r := s.group(args.GroupID)
	if r == nil {
		return fmt.Errorf("raft: no group %d", args.GroupID)
	}
	index, err := r.AppendEntry(args.Command)
	if err != nil { // 非 leader
		reply.NotLeader = true
		if addr, ok := r.LeaderHint(); ok {
			reply.LeaderHint = addr
		}
		return nil
	}
	r.WaitCommitIndex(index)
	reply.Success = true
	return nil
}

// ProposeToGroup 向 groupID 组提交一条命令，自动跟随 leader 重定向：依次尝试各节点，命中
// 非 leader 则立刻按其 LeaderHint 重定向到 leader；直至提交成功或 totalTimeout 到期。
// 采用「按截止时间反复重试整轮」，以容忍选主/换届的短暂无主窗口。perCallTimeout 限制单次
// RPC（避免 leader 侧等待提交时挂住调用方）。
func ProposeToGroup(addrs []string, groupID int, command []byte, perCallTimeout, totalTimeout time.Duration) error {
	args := &ProposeArgs{GroupID: groupID, Command: command}
	deadline := time.Now().Add(totalTimeout)

	tryOne := func(target string) (done bool, hint string) {
		reply, err := callPropose(target, args, perCallTimeout)
		if err != nil {
			return false, ""
		}
		if reply.Success {
			return true, ""
		}
		if reply.NotLeader {
			return false, reply.LeaderHint
		}
		return false, ""
	}

	for time.Now().Before(deadline) {
		for _, target := range addrs {
			if time.Now().After(deadline) {
				break
			}
			done, hint := tryOne(target)
			if done {
				return nil
			}
			// 立刻跟随 leader 重定向（若给了提示且非本节点）。
			if hint != "" && hint != target {
				if d, _ := tryOne(hint); d {
					return nil
				}
			}
		}
		time.Sleep(50 * time.Millisecond) // 等选主/换届后重试整轮
	}
	return fmt.Errorf("propose: timed out finding leader for group %d", groupID)
}

// callPropose 对 addr 发起一次带超时的 Propose RPC。
func callPropose(addr string, args *ProposeArgs, timeout time.Duration) (*ProposeReply, error) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	var reply ProposeReply
	call := client.Go("RaftRPC.Propose", args, &reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			return nil, call.Error
		}
		return &reply, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("propose call timeout to %s", addr)
	}
}
