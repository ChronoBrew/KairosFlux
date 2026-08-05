package Raft

import (
	"fmt"
	"net/rpc"
	"sync"
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
