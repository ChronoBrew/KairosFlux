package schema

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChronoBrew/KairosFlux/internal/metrics"
)

// validQuote 返回一份通过全部校验的基线记录，测试用例只改动其中需要触发失败的字段。
func validQuote() map[string]any {
	return map[string]any{
		"code":       "600000",
		"date":       "2026-08-17",
		"open":       10.0,
		"high":       10.5,
		"low":        9.8,
		"close":      10.2,
		"volume":     1_000_000.0,
		"prev_close": 10.0, // (10.2-10.0)/10.0 = 2%，未超 ±20%
	}
}

func mustJSON(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal 测试记录失败: %v", err)
	}
	return b
}

func TestQuoteSnapshot_ValidRecordPasses(t *testing.T) {
	if err := (QuoteSnapshot{}).Validate(mustJSON(t, validQuote())); err != nil {
		t.Fatalf("合法记录应通过校验，得到错误: %v", err)
	}
}

func TestQuoteSnapshot_InvalidJSONRejected(t *testing.T) {
	if err := (QuoteSnapshot{}).Validate([]byte("not json")); err == nil {
		t.Fatal("非法 JSON 应被拒绝")
	}
}

// 必填字段：code/date/open/high/low/close/volume 缺一即拒绝。
func TestQuoteSnapshot_MissingRequiredFieldsRejected(t *testing.T) {
	for _, field := range []string{"code", "date", "open", "high", "low", "close", "volume"} {
		t.Run(field, func(t *testing.T) {
			rec := validQuote()
			delete(rec, field)
			err := (QuoteSnapshot{}).Validate(mustJSON(t, rec))
			if err == nil {
				t.Fatalf("缺失字段 %q 应被拒绝", field)
			}
			if !strings.Contains(err.Error(), field) {
				t.Fatalf("错误信息应指出缺失字段 %q，得到: %v", field, err)
			}
		})
	}
}

// 价格字段（open/high/low/close）必须 > 0；0 与负数均拒绝（D5：非正价格必须显式拒绝）。
func TestQuoteSnapshot_NonPositivePriceRejected(t *testing.T) {
	for _, field := range []string{"open", "high", "low", "close"} {
		for _, bad := range []float64{0, -1.5} {
			t.Run(field, func(t *testing.T) {
				rec := validQuote()
				rec[field] = bad
				// 避免同时触发 OHLC 不一致掩盖了价格校验的断言意图：
				// 价格校验在 OHLC 一致性校验之前执行，故此处仍应命中「非正价格」错误。
				err := (QuoteSnapshot{}).Validate(mustJSON(t, rec))
				if err == nil {
					t.Fatalf("%s=%v 应被拒绝", field, bad)
				}
				if !strings.Contains(err.Error(), "non-positive price") {
					t.Fatalf("错误应为非正价格拒绝，得到: %v", err)
				}
			})
		}
	}
}

func TestQuoteSnapshot_NegativeVolumeRejected(t *testing.T) {
	rec := validQuote()
	rec["volume"] = -1.0
	err := (QuoteSnapshot{}).Validate(mustJSON(t, rec))
	if err == nil || !strings.Contains(err.Error(), "negative volume") {
		t.Fatalf("负成交量应被拒绝，得到: %v", err)
	}
}

// volume=0 是合法值（如停牌当日），不应被当作缺失或负数拒绝。
func TestQuoteSnapshot_ZeroVolumeAllowed(t *testing.T) {
	rec := validQuote()
	rec["volume"] = 0.0
	if err := (QuoteSnapshot{}).Validate(mustJSON(t, rec)); err != nil {
		t.Fatalf("volume=0 应放行，得到错误: %v", err)
	}
}

// OHLC 逻辑一致：low <= open <= high 且 low <= close <= high。
func TestQuoteSnapshot_OHLCInconsistentRejected(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(rec map[string]any)
	}{
		{"low>open", func(r map[string]any) { r["low"] = 10.5 }},                    // low(10.5) > open(10.0)
		{"open>high", func(r map[string]any) { r["open"] = 20.0 }},                  // open(20) > high(10.5)
		{"low>close", func(r map[string]any) { r["low"] = 10.3; r["open"] = 10.4 }}, // low(10.3) > close(10.2)，同时保 low<=open 避免误撞前一分支
		{"close>high", func(r map[string]any) { r["close"] = 20.0 }},                // close(20) > high(10.5)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := validQuote()
			c.mutate(rec)
			err := (QuoteSnapshot{}).Validate(mustJSON(t, rec))
			if err == nil || !strings.Contains(err.Error(), "OHLC inconsistent") {
				t.Fatalf("OHLC 不一致场景 %s 应被拒绝，得到: %v", c.name, err)
			}
		})
	}
}

// 涨跌幅物理极限 ±21%：用 prev_close 与 close 比对，超限拒绝。
func TestQuoteSnapshot_PctChangeExceedsLimitRejected(t *testing.T) {
	cases := []struct {
		name  string
		close float64
		prev  float64
	}{
		{"暴涨超限", 13.0, 10.0},     // +30%
		{"暴跌超限", 7.0, 10.0},      // -30%
		{"刚过21%上限", 12.11, 10.0}, // +21.1%，恰好越过新阈值
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hi := c.close
			if c.prev > hi {
				hi = c.prev
			}
			lo := c.close
			if c.prev < lo {
				lo = c.prev
			}
			rec := validQuote()
			rec["close"] = c.close
			rec["prev_close"] = c.prev
			rec["open"] = c.prev
			rec["high"] = hi + 1 // 保证不会先被 OHLC 校验拦下
			rec["low"] = lo - 1
			err := (QuoteSnapshot{}).Validate(mustJSON(t, rec))
			if err == nil || !strings.Contains(err.Error(), "physical limit") {
				t.Fatalf("涨跌幅超 ±21%% 应被拒绝，得到: %v", err)
			}
		})
	}
}

// 恰好 ±21% 是边界合法值，不应被拒绝。
func TestQuoteSnapshot_PctChangeAtBoundaryPasses(t *testing.T) {
	rec := validQuote()
	rec["prev_close"] = 10.0
	rec["close"] = 12.1 // 恰好 +21%
	rec["high"] = 12.5
	rec["low"] = 9.5
	rec["open"] = 10.0
	if err := (QuoteSnapshot{}).Validate(mustJSON(t, rec)); err != nil {
		t.Fatalf("恰好 ±21%% 边界应放行，得到错误: %v", err)
	}
}

// TestQuoteSnapshot_RealCreationBoardLimitUpNotFalselyRejected 复现 QuantScout
// 全量实测（5241 行真实行情）里被 ±20% 阈值误伤的两个真实创业板涨停日：
// 300069（+20.01%）与 301106（+20.02%）——涨停价四舍五入到分后折算涨跌幅
// 略超 20.00%，这两个真实样本正是把阈值从 20% 调到 21% 的直接原因（见
// docs/iteration-2026-08-20-quantscout-realdata-fixes.md 的 D1 记录）。
func TestQuoteSnapshot_RealCreationBoardLimitUpNotFalselyRejected(t *testing.T) {
	cases := []struct {
		name  string
		code  string
		prev  float64
		close float64
	}{
		// 20.01%：prev_close=10.00，涨停价按 ±20% 四舍五入到分得 12.00，
		// 但真实行情源里出现的是折算后 12.001 量级的浮点误差被保留到了两位小数
		// 之外的口径不一致场景——直接用能产生 20.01% 的一组价格复现该现象。
		{"300069_20.01pct", "300069", 12.499, 15.0},
		{"301106_20.02pct", "301106", 24.995, 30.0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pct := (c.close - c.prev) / c.prev
			if pct < 0.2000 || pct > 0.2100 {
				t.Fatalf("测试用例构造错误：pct=%.4f 应落在 (20%%,21%%] 区间内才有意义", pct)
			}
			rec := map[string]any{
				"code":       c.code,
				"date":       "2026-08-18",
				"open":       c.prev,
				"high":       c.close,
				"low":        c.prev,
				"close":      c.close,
				"volume":     1_000_000.0,
				"prev_close": c.prev,
			}
			if err := (QuoteSnapshot{}).Validate(mustJSON(t, rec)); err != nil {
				t.Fatalf("真实涨停日 %s（pct=%.4f%%）不应被误判为异常数据拒收，得到错误: %v", c.code, pct*100, err)
			}
		})
	}
}

// 无昨收（缺字段或为 0）时跳过涨跌幅校验并计数，不视为失败——首个交易日/复牌首日无可比昨收。
func TestQuoteSnapshot_MissingPrevCloseSkipsCheckAndCounts(t *testing.T) {
	before := metrics.Take().SchemaChecksSkipped

	rec := validQuote()
	delete(rec, "prev_close")
	if err := (QuoteSnapshot{}).Validate(mustJSON(t, rec)); err != nil {
		t.Fatalf("缺昨收应跳过涨跌幅校验并放行，得到错误: %v", err)
	}

	rec2 := validQuote()
	rec2["prev_close"] = 0.0
	if err := (QuoteSnapshot{}).Validate(mustJSON(t, rec2)); err != nil {
		t.Fatalf("昨收为 0 应跳过涨跌幅校验并放行，得到错误: %v", err)
	}

	after := metrics.Take().SchemaChecksSkipped
	if d := after - before; d != 2 {
		t.Fatalf("SchemaChecksSkipped 增量应为 2（两次跳过），得到 %d", d)
	}
}
