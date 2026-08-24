package schema

// 机读错误码分类学的 0x3xxx 段（Kair-2 RFC §10.3）：schema 校验失败按具体原因
// 分配不同的数字码，客户端可以只读 Code 做程序化分支（"是价格问题"还是"是字段
// 缺失问题"），不需要解析 reason 字符串前缀——那种解析一旦措辞改动就会静默失效，
// 正是方案 §2.4 风险 1（"字符串匹配承载语义"）点名的反例之一。
//
// 当前只有 quote 类型分配了子码，对应 RFC §10.3 给出的四个值。ErrCodeMissingField
// 的取值刻意与 kairnet/codec.ErrCodeSchemaValidation 一致（都是 0x3001）：后者是
// v1.1 遗留下来的"所有 schema 校验失败共用一个码"的通用值，前者是 M1 按具体原因
// 拆分后"缺字段"这一具体原因的取值——两者数值相同不是巧合而是刻意选择，schema
// 校验器不返回 *ValidationError（如未来新类型尚未做码位细分）时，调用方按
// codec.ErrCodeSchemaValidation 兜底，效果与"归类为缺字段类"一致，不需要引入
// 第二个"未分类"哨兵值。ingesthook 包读取这个默认兜底码，schema 包本身不依赖
// kairnet/codec（避免内容校验层反向依赖协议层）。
const (
	ErrCodeMissingField      uint16 = 0x3001
	ErrCodeNonPositivePrice  uint16 = 0x3002
	ErrCodeOHLCInconsistent  uint16 = 0x3003
	ErrCodePctChangeExceeded uint16 = 0x3004
)

// ValidationError 是 Validator.Validate 失败时应返回的错误类型：Code 是上面的
// 机读码，Reason 是给人看的描述（字面文本与 M1 之前 fmt.Errorf 拼出的字符串保持
// 逐字节一致，quote_test.go 里对错误文案的 strings.Contains 断言零改动）。
type ValidationError struct {
	Code   uint16
	Reason string
	// Wrapped 可选：包一层被拒绝的底层错误（如 JSON 解析失败），支持 errors.Is/As。
	Wrapped error
}

func (e *ValidationError) Error() string { return e.Reason }

func (e *ValidationError) Unwrap() error { return e.Wrapped }

// CodeOf 从一个 Validator.Validate 返回的 error 里取出机读码：是 *ValidationError
// 则返回其 Code；否则返回 fallback（调用方通常传 kairnet/codec.ErrCodeSchemaValidation
// 作为"未分类，归入 schema 校验失败大类"的兜底值）。nil err 的行为未定义，调用方
// 不应对 nil err 调用本函数。
func CodeOf(err error, fallback uint16) uint16 {
	if verr, ok := err.(*ValidationError); ok {
		return verr.Code
	}
	return fallback
}
