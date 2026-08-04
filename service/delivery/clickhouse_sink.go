package delivery

import (
	"context"
	"errors"
)

// errSinkNotImplemented 表示该 sink 尚未接入真实下游。
var errSinkNotImplemented = errors.New("delivery: sink not implemented")

// ClickHouseSink 是对接 ClickHouse 的桩：占位实现 Sink 接口，标注为不健康，
// 使治理层永远不会把流量路由到它，直到真正接入 HTTP / stream load。
type ClickHouseSink struct {
	name string
}

// NewClickHouseSink 创建一个名为 name 的 ClickHouse 桩 sink。
func NewClickHouseSink(name string) *ClickHouseSink {
	return &ClickHouseSink{name: name}
}

func (s *ClickHouseSink) Name() string { return s.name }

func (s *ClickHouseSink) Send(ctx context.Context, batch []Record) error {
	return errSinkNotImplemented
}

func (s *ClickHouseSink) Health() SinkHealth {
	return SinkHealth{Healthy: false, Reason: "not implemented"}
}
