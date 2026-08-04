package cluster

import (
	"context"
	"log/slog"
)

// IsLocal 判定 key 的属主是否为 self（本节点）。用于网关侧决定「本地处理」
// 还是「转发到属主节点」。
func (p *Placement) IsLocal(key []byte, self string) bool {
	return p.OwnerOf(key) == self
}

// ForwardFunc 抽象「把一次写入转发到指定节点」的行为。
//
// 之所以做成函数类型：真实转发的实现（编解码、连接复用、重试）与控制面解耦，
// 由上层在传输层就绪后注入，控制面只负责决定「转发给谁」。
type ForwardFunc func(ctx context.Context, node string, key, value []byte) error

// Forward 是跨节点写转发桩（stretch）。
//
// 控制面已能算出属主（OwnerOf），但真正把数据发往属主节点需要一条跨节点数据
// 通道——当前 Raft 使用 net/rpc 静态传输，无法承载分片间的数据转发，属传输层
// 重写范围（见架构文档）。此处保留接口与调用点、记录目标属主，当前返回
// errNotImplemented，待传输层就绪后接入具体 ForwardFunc。
func (p *Placement) Forward(ctx context.Context, key, value []byte) error {
	owner := p.OwnerOf(key)
	slog.Warn("[cluster] forward: TODO, cross-node data transport not implemented (stretch)", "owner", owner)
	return errNotImplemented
}
