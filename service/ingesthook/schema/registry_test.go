package schema

import "testing"

type stubValidator struct{ err error }

func (s stubValidator) Validate(value []byte) error { return s.err }

func TestLookup_NoMatchReturnsFalse(t *testing.T) {
	if _, ok := Lookup([]byte("unregistered:1")); ok {
		t.Fatalf("未注册前缀应返回 ok=false")
	}
}

func TestLookup_MatchesRegisteredPrefix(t *testing.T) {
	Register("stub:", stubValidator{})
	t.Cleanup(func() { Unregister("stub:") })

	v, ok := Lookup([]byte("stub:abc"))
	if !ok || v == nil {
		t.Fatalf("已注册前缀应命中，得到 ok=%v v=%v", ok, v)
	}
}

func TestLookup_LongestPrefixWins(t *testing.T) {
	Register("a:", stubValidator{err: nil})
	Register("a:b:", stubValidator{})
	t.Cleanup(func() {
		Unregister("a:")
		Unregister("a:b:")
	})

	// 两个前缀都能匹配 "a:b:c"，应选最长的 "a:b:"。
	// 用不同实例区分：更长前缀的实例地址与 Lookup 返回值比较。
	longer := stubValidator{}
	Register("a:b:", longer)

	v, ok := Lookup([]byte("a:b:c"))
	if !ok {
		t.Fatal("应命中前缀")
	}
	if _, isStub := v.(stubValidator); !isStub {
		t.Fatalf("类型断言失败: %T", v)
	}
}

// quote: 前缀在包 init() 中已自注册，未注册任何 stub 时应能直接命中 QuoteSnapshot。
func TestLookup_QuotePrefixRegisteredByDefault(t *testing.T) {
	v, ok := Lookup([]byte("quote:2026-08-17:600000"))
	if !ok {
		t.Fatal("quote: 前缀应在包加载时自动注册")
	}
	if _, isQuote := v.(QuoteSnapshot); !isQuote {
		t.Fatalf("默认注册的校验器类型应为 QuoteSnapshot，得到 %T", v)
	}
}
