// Package delivery 是数仓写入前置缓冲的「下游投递层」骨架：把本地缓冲的数据
// 按批投递到一个或多个下游 sink（ClickHouse / Doris / 湖仓 / 文件）。
//
// 借鉴 dubbo-go 的治理模型，但落在数据面而非服务网格：多个 sink 就是要被
// 「治理」的一组后端——健康感知路由、熔断、重试、背压反传（见 governance 子包），
// 投递进度用强一致 offset 记录（见 offset 子包）。全程零依赖。
package delivery

import "context"

// Record 是一条待投递的缓冲记录。
type Record struct {
	Key   []byte
	Value []byte
}

// SinkHealth 是某个 sink 的当前健康状态，供治理层做路由与熔断决策。
type SinkHealth struct {
	Healthy bool
	Reason  string // 不健康时的原因，便于观测；健康时为空
}

// Sink 是一个下游投递目标。实现须保证 Send 对同一批的重复投递不会破坏
// 下游正确性（幂等），或由上层 offset 语义约束——见 offset 子包的说明。
type Sink interface {
	// Name 返回 sink 的唯一名字，用于路由、offset key 与指标标签。
	Name() string
	// Send 投递一批记录。返回 nil 表示整批已被下游接收（ack）。
	Send(ctx context.Context, batch []Record) error
	// Health 返回当前健康状态，供治理层实时读取。
	Health() SinkHealth
}
