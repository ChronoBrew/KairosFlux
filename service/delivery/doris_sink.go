package delivery

import "context"

// DorisSink 是对接 Doris 的桩：占位实现 Sink 接口，标注为不健康，
// 使治理层永远不会把流量路由到它，直到真正接入 stream load。
type DorisSink struct {
	name string
}

// NewDorisSink 创建一个名为 name 的 Doris 桩 sink。
func NewDorisSink(name string) *DorisSink {
	return &DorisSink{name: name}
}

func (s *DorisSink) Name() string { return s.name }

func (s *DorisSink) Send(ctx context.Context, batch []Record) error {
	return errSinkNotImplemented
}

func (s *DorisSink) Health() SinkHealth {
	return SinkHealth{Healthy: false, Reason: "not implemented"}
}
