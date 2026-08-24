package transport

import (
	"sync"
	"time"

	"github.com/ChronoBrew/KairosFlux/kairnet/handler"
)

// ConnRegistry 是连接注册表的契约——原在根包 interfaces.go，随重构第五步
// 迁入 transport（迁移映射表标注的直接移动项之一）。
type ConnRegistry interface {
	Add(conn handler.Conn)
	Remove(conn handler.Conn)
	Get(connID uint32) handler.Conn
	Len() int
	ClearConn()

	// BeginClosingAll/Wait 是重构第四步（修复 bug①）新增的两个方法，
	// 支撑 Server 优雅关闭的"广播 -> 等待 -> 强制"三段式：
	// BeginClosingAll 给所有连接广播"决定关闭"（不物理清理），Wait
	// 阻塞到所有连接都已完成物理关闭或超时。ClearConn 保留作为超时后
	// 的强制兜底，语义不变。
	BeginClosingAll()
	Wait(timeout time.Duration) bool
}

type ConnManager struct {
	mu          sync.RWMutex
	connections map[uint32]handler.Conn

	// wg 追踪"当前有多少个已注册的连接尚未完成物理关闭"——Add 时 +1，
	// 每个连接的 Stop() 保证只会真正执行一次物理清理，Remove 就在那次
	// 清理里被调用一次，对应 -1。Wait 供 Server 优雅关闭时等待所有连接
	// 都走完各自的收尾（含 StartWriter 排空），见根包 server.go 的 Stop。
	wg sync.WaitGroup
}

var _ ConnRegistry = &ConnManager{}

func NewConnManager() *ConnManager {
	return &ConnManager{
		connections: make(map[uint32]handler.Conn),
	}
}

func (cm *ConnManager) Add(conn handler.Conn) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.connections[conn.ID()] = conn
	cm.wg.Add(1)
}

func (cm *ConnManager) Remove(conn handler.Conn) {
	cm.mu.Lock()
	_, existed := cm.connections[conn.ID()]
	delete(cm.connections, conn.ID())
	cm.mu.Unlock()
	if existed {
		cm.wg.Done()
	}
}

func (cm *ConnManager) Get(connId uint32) handler.Conn {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if conn, ok := cm.connections[connId]; ok {
		return conn
	}
	return nil
}

// Len 返回当前连接数。
//
// 必须持锁：Add/Remove 在锁内写这张 map，而 acceptLoop 每接受一个连接都调用 Len 做
// MaxConn 准入判断，二者天然并发。无锁读 map 撞上并发写 map 时，Go 运行时会直接抛出
// "concurrent map read and map write" 使进程崩溃，而非仅报告竞态。
func (cm *ConnManager) Len() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.connections)
}

func (cm *ConnManager) ClearConn() {
	// 先在锁内快照并清空连接表，再在锁外逐个 Stop——Stop 内部会回调 Remove 再次加锁，
	// 若在持锁时调用会自死锁（sync.Mutex 不可重入）。
	cm.mu.Lock()
	conns := make([]handler.Conn, 0, len(cm.connections))
	for _, conn := range cm.connections {
		conns = append(conns, conn)
	}
	cm.connections = make(map[uint32]handler.Conn)
	cm.mu.Unlock()

	for _, conn := range conns {
		conn.Stop()
	}
}

// BeginClosingAll 给当前所有已注册连接广播"决定关闭"（Closing）——只标记
// 状态、不做物理清理，写路径仍然打开。是 Server 优雅关闭（修复 bug①）的
// 第一阶段：让每个连接的读循环在完成当前在途请求后主动退出，而不是被
// ClearConn 强行打断。
func (cm *ConnManager) BeginClosingAll() {
	cm.mu.RLock()
	conns := make([]handler.Conn, 0, len(cm.connections))
	for _, conn := range cm.connections {
		conns = append(conns, conn)
	}
	cm.mu.RUnlock()

	for _, conn := range conns {
		conn.BeginClosing()
	}
}

// Wait 阻塞直到所有已注册连接都完成物理关闭（Remove 被调用），或超时。
// 返回 true 表示在超时前全部完成。是 Server 优雅关闭的第二阶段：先
// BeginClosingAll 广播意图，再等各连接自己收尾——只有等到这里返回，
// Server.Stop 才能确定"不会再有任何连接产出新响应了"。
func (cm *ConnManager) Wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		cm.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
