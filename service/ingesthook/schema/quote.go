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
// 现实市场即使涨跌停也很少超过 ±20%，用它拦截明显损坏/错位的数据。
const maxPctChange = 0.20

func init() {
	Register(quotePrefix, QuoteSnapshot{})
}

// QuoteSnapshot 是全市场股票日线快照的校验规则：
//   - 必填字段：code/date/open/high/low/close/volume；
//   - 价格字段（open/high/low/close）必须 > 0；
//   - volume 必须 >= 0；
//   - OHLC 逻辑一致：low <= open <= high 且 low <= close <= high；
//   - 涨跌幅物理极限 ±20%：用可选字段 prev_close 与 close 比对；
//     缺 prev_close（或其为 0，无法算比例）时跳过该项检查并计数，不视为失败——
//     首个交易日、停牌后复牌等场景本就没有可比的昨收。
type QuoteSnapshot struct{}

// quoteRecord 用指针字段区分「缺失」与「零值」，只用于必填性判断；
// PrevClose 额外可选，不在必填清单内。
type quoteRecord struct {
	Code      string   `json:"code"`
	Date      string   `json:"date"`
	Open      *float64 `json:"open"`
	High      *float64 `json:"high"`
	Low       *float64 `json:"low"`
	Close     *float64 `json:"close"`
	Volume    *float64 `json:"volume"`
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
