package schema

import (
	"encoding/json"
	"fmt"

	"github.com/NeverENG/BanDB/internal/metrics"
)

// quotePrefix 是行情快照记录的 key 前缀约定：quote:<YYYY-MM-DD>:<代码>。
// 日期在前是刻意选择——同一天的全市场快照在 key 空间连续，投递按位点批量拉取、
// retention 按已投递位点回收都天然按「日」成批，不需要为行情数据改投递/回收逻辑。
const quotePrefix = "quote:"

// maxPctChange 是涨跌幅的物理极限（非交易规则的涨跌停线，是数据合理性上限）：
// 用它拦截明显损坏/错位的数据，不是复刻交易所的涨跌停规则。
//
// 取 21% 而非更直觉的 20%：创业板/科创板涨跌停线是 ±20%，但涨停价按当日基准价
// 四舍五入到分后，折算涨跌幅可能略超 20.00%——真实数据实测命中过
// 300069（+20.01%）与 301106（+20.02%），若阈值卡在 20% 会把合规的涨停日
// 误判为数据异常拒收。21% 与 QuantBrew 数据质检的 max_daily_move=0.21 口径
// 对齐（两套系统对同一份行情数据的合理性判断必须一致，否则一边收一边拒会
// 制造出"同一天数据在两个系统里不一致"的诡异状态）。见
// docs/iteration-2026-08-20-quantscout-realdata-fixes.md 的 D1 记录。
const maxPctChange = 0.21

func init() {
	Register(quotePrefix, QuoteSnapshot{})
}

// QuoteSnapshot 是全市场股票日线快照的校验规则：
//   - 必填字段：code/date/open/high/low/close/volume；
//   - 价格字段（open/high/low/close）必须 > 0；
//   - volume 必须 >= 0，量纲契约见下方 quoteRecord.Volume 字段注释；
//   - OHLC 逻辑一致：low <= open <= high 且 low <= close <= high；
//   - 涨跌幅物理极限 ±21%：用可选字段 prev_close 与 close 比对（阈值取 21% 而非
//     20% 的理由见 maxPctChange 注释）；缺 prev_close（或其为 0，无法算比例）
//     时跳过该项检查并计数，不视为失败——首个交易日、停牌后复牌等场景本就没有
//     可比的昨收。
type QuoteSnapshot struct{}

// quoteRecord 用指针字段区分「缺失」与「零值」，只用于必填性判断；
// PrevClose 额外可选，不在必填清单内。
type quoteRecord struct {
	Code  string   `json:"code"`
	Date  string   `json:"date"`
	Open  *float64 `json:"open"`
	High  *float64 `json:"high"`
	Low   *float64 `json:"low"`
	Close *float64 `json:"close"`

	// Volume 量纲契约：手（A 股惯例，1 手 = 100 股）。QuantScout 按此单位写入，
	// 下游 ClickHouse 表（docs/clickhouse-schema.md）同步标注同一单位。
	//
	// 量纲必须在协议/schema 层显式写死，不能靠"约定俗成"心照不宣——一旦写入方
	// 与消费方对量纲的假设不一致（如某处误按"股"而非"手"消费），产生的是
	// 系统性的 100 倍误差，且不会报任何错，只会在下游账务/回测里悄悄算错，
	// 比拒绝一条明显非法的记录危险得多。
	Volume *float64 `json:"volume"`

	PrevClose *float64 `json:"prev_close"`
}

func (QuoteSnapshot) Validate(value []byte) error {
	var r quoteRecord
	if err := json.Unmarshal(value, &r); err != nil {
		return fmt.Errorf("quote: invalid json: %w", err)
	}

	if r.Code == "" {
		return fmt.Errorf("quote: missing required field %q", "code")
	}
	if r.Date == "" {
		return fmt.Errorf("quote: missing required field %q", "date")
	}
	for _, p := range []struct {
		name string
		v    *float64
	}{
		{"open", r.Open}, {"high", r.High}, {"low", r.Low}, {"close", r.Close}, {"volume", r.Volume},
	} {
		if p.v == nil {
			return fmt.Errorf("quote: missing required field %q", p.name)
		}
	}

	for _, p := range []struct {
		name string
		v    float64
	}{
		{"open", *r.Open}, {"high", *r.High}, {"low", *r.Low}, {"close", *r.Close},
	} {
		if p.v <= 0 {
			return fmt.Errorf("quote: non-positive price: %s=%v", p.name, p.v)
		}
	}
	if *r.Volume < 0 {
		return fmt.Errorf("quote: negative volume: %v", *r.Volume)
	}

	low, open, high, cls := *r.Low, *r.Open, *r.High, *r.Close
	if low > open || open > high {
		return fmt.Errorf("quote: OHLC inconsistent: low=%v open=%v high=%v", low, open, high)
	}
	if low > cls || cls > high {
		return fmt.Errorf("quote: OHLC inconsistent: low=%v close=%v high=%v", low, cls, high)
	}

	if r.PrevClose == nil || *r.PrevClose == 0 {
		metrics.SchemaChecksSkipped.Add(1)
		return nil
	}
	pct := (cls - *r.PrevClose) / *r.PrevClose
	if pct > maxPctChange || pct < -maxPctChange {
		return fmt.Errorf("quote: pct change %.4f exceeds physical limit ±%.0f%%", pct, maxPctChange*100)
	}
	return nil
}
