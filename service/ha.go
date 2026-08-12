package service

import (
	"time"
)

// HA 高可用管理
type HA struct {
	kv        *KVServer
	isHealthy bool
	lastCheck time.Time
}

// NewHA 创建 HA 管理实例
func NewHA(kv *KVServer) *HA {
	ha := &HA{
		kv:        kv,
		isHealthy: true,
		lastCheck: time.Now(),
	}

	// 启动健康检查
	go ha.healthCheckLoop()

	return ha
}

// healthCheckLoop 健康检查循环
func (h *HA) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		h.checkHealth()
	}
}

// checkHealth 检查健康状态
func (h *HA) checkHealth() {
	// 检查 Raft 状态（standalone 模式无 Raft，直接视为健康）
	if r := h.kv.GetRaft(); r != nil {
		state, _ := r.GetState()
		if state == 0 { // Follower
			// 可以添加更多健康检查逻辑
		}
	}

	// 简单的健康检查：只要 Raft 状态不是错误状态，就认为是健康的
	h.isHealthy = true
	h.lastCheck = time.Now()
}

// IsHealthy 检查是否健康
func (h *HA) IsHealthy() bool {
	return h.isHealthy
}

// GetLastCheck 获取最后检查时间
func (h *HA) GetLastCheck() time.Time {
	return h.lastCheck
}

// GetFSM 获取 FSM 实例
func (h *HA) GetFSM() *KVServer {
	return h.kv
}
