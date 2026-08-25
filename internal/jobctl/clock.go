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
//
// 本函数是"Unix 纪元锚定"的默认变体：slot 边界落在 Unix 纪元起每隔
// interval 处（daily job 即 UTC 零点切分，这是 M3 遗留修正点——任务书
// 要求时槽锚点可配置，见 AnchoredSlot 与 JobSpec.Schedule）。
func Slot(now time.Time, intervalSeconds int64) int64 {
	if intervalSeconds <= 0 {
		intervalSeconds = 1
	}
	return now.UnixNano() / (intervalSeconds * int64(time.Second))
}

// AnchoredSlot 是 Slot 的可配置锚点变体：slot 边界精确落在指定时区里
// 每天的指定本地墙钟时刻（如 daily@16:10 Asia/Shanghai 的每天 16:10），
// 而不是 Unix 纪元零点。计算是 now 的纯函数（只读 now 与时区声明，不读
// 任何其它隐式状态，与本仓库"确定性来源"的一贯要求一致）——同一时间片
// 内重复调用返回值不变，幂等键语义与 Slot 完全相同，只是边界位置不同。
//
// 实现是"当日锚点 + 参考锚点"两段式：slot = floor((now-anchorToday)/I) +
// floor((anchorToday-refAnchor)/I)，其中 anchorToday 是 now 所在自然日
// 本地墙钟 hour:min 的锚点时刻，refAnchor 是 1970-01-01 同一墙钟时刻的
// 锚点（该时区固定的纪元参考，与 now 无关）。只减当日偏移的朴素公式对
// 固定偏移时区会退化成"边界在本地零点"（整体平移不改变格子切分），
// 两段式才保证边界精确落在锚点时刻。floorDiv 保证锚点前 1ns 的正确
// 落槽（Go 整数除法对负值向零截断，直接除会把 -1ns 截成 0）。固定偏移
// 时区（如 Asia/Shanghai，无 DST）下每天恰好推进 1 个 slot；有 DST 的
// 时区在过渡日前后 slot 的实际时长相应伸缩（当日锚点间距 23h/25h），
// 这是墙钟语义的应有之义。
func AnchoredSlot(now time.Time, intervalSeconds int64, loc *time.Location, hour, min int) int64 {
	if intervalSeconds <= 0 {
		intervalSeconds = 1
	}
	intervalNanos := intervalSeconds * int64(time.Second)
	t := now.In(loc)
	anchor := time.Date(t.Year(), t.Month(), t.Day(), hour, min, 0, 0, loc)
	ref := time.Date(1970, 1, 1, hour, min, 0, 0, loc)
	return floorDiv(now.UnixNano()-anchor.UnixNano(), intervalNanos) +
		floorDiv(anchor.UnixNano()-ref.UnixNano(), intervalNanos)
}

// floorDiv 是向下取整的整数除法（Go 的 / 对负值向零截断，这里需要的是
// 数学意义的 floor）：商向负无穷取整。
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
