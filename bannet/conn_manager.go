package bannet

import (
	"sync"
)

type ConnManager struct {
	mu          sync.RWMutex
	connections map[uint32]IConnect
}

var _ IConnManager = &ConnManager{}

func NewConnManager() *ConnManager {
	return &ConnManager{
		connections: make(map[uint32]IConnect),
	}
}

func (cm *ConnManager) Add(conn IConnect) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.connections[conn.GetConnID()] = conn
}

func (cm *ConnManager) Remove(conn IConnect) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.connections, conn.GetConnID())
}

func (cm *ConnManager) Get(connId uint32) IConnect {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if conn, ok := cm.connections[connId]; ok {
		return conn
	}
	return nil
}

func (cm *ConnManager) Len() int {
	return len(cm.connections)
}

func (cm *ConnManager) ClearConn() {
	// 先在锁内快照并清空连接表，再在锁外逐个 Stop——Stop 内部会回调 Remove 再次加锁，
	// 若在持锁时调用会自死锁（sync.Mutex 不可重入）。
	cm.mu.Lock()
	conns := make([]IConnect, 0, len(cm.connections))
	for _, conn := range cm.connections {
		conns = append(conns, conn)
	}
	cm.connections = make(map[uint32]IConnect)
	cm.mu.Unlock()

	for _, conn := range conns {
		conn.Stop()
	}
}
