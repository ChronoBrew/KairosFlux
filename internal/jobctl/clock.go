package jobctl

import "time"

// Clock 把"现在几点"变成一个可注入的依赖，而不是散落各处的 time.Now()。
// 两个用途都被任务书明确允许墙钟：调度触发（判断该不该跑）与事件时间戳
// （记录跑了什么时候）；但 reconcile 的幂等决策本身不能直接依赖
// clock.Now() 的具体纳秒值——见 Slot 的文档。测试用固定 Clock 驱动一万次
// 重跑，不靠真实 sleep。
type Clock interface {
	Now() time.Time
}

// SystemClock 是生产环境用的 Clock：直接转发 time.Now()。
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// Slot 把墙钟时间量化成一个"逻辑时间片"整数：nowNanos 整除
// intervalSeconds*1e9。同一时间片内，不管调用多少次、调用时刻的纳秒值
// 具体是多少，Slot 的返回值不变——这是幂等键（见 reconciler.go 的
// idempotencyKey）不直接依赖墙钟纳秒值、只依赖 slot 这个整数的原因：
// 墙钟仍然参与"该不该触发"这个决策（slot 从 Clock 算出来），但不参与
// "同一个 slot 内重复调用是否产生同一个幂等键"这个不变量。
func Slot(now time.Time, intervalSeconds int64) int64 {
	if intervalSeconds <= 0 {
		intervalSeconds = 1
	}
	return now.UnixNano() / (intervalSeconds * int64(time.Second))
}
