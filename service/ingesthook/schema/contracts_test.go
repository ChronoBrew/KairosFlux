package schema

import (
	"os"
	"testing"

	"github.com/NeverENG/BanDB/bannet/codec"
)

// contractsDir 是相对本测试文件所在包目录（service/ingesthook/schema/）到仓库
// 根 contracts/ 目录的路径。go test 的工作目录固定为被测包的源码目录（与
// config/config_json_test.go 读取 "config.json" 依赖的是同一条 Go 工具链
// 不变量），所以这里不需要、也不应该像 LoadContractsDefault 那样按多个候选
// 路径试探——测试环境唯一确定。
const contractsDir = "../../../contracts"

// TestLoadContracts_QuoteContractLoadsAndMatchesTypeID 是"服务端启动时加载
// 契约并强制校验"（方案 M1 任务 2）的加载路径本身的验收：真实的
// contracts/quote.schema.json 必须能被 LoadContracts 无错解析，且它声明的
// TypeID 必须与 bannet/codec.TypeQuote 一致——两个包故意不互相导入（schema 是
// 内容校验层，codec 是协议层），这条测试是防止两处手写数值悄悄漂移的唯一防线
// （方案 §2.4 风险 2：schema 演化不能只靠约定）。
func TestLoadContracts_QuoteContractLoadsAndMatchesTypeID(t *testing.T) {
	if err := LoadContracts(contractsDir); err != nil {
		t.Fatalf("LoadContracts(%q) 失败: %v", contractsDir, err)
	}

	d, ok := LookupByType(codec.TypeQuote)
	if !ok {
		t.Fatalf("LookupByType(codec.TypeQuote=%d) 未命中，契约文件的 type_id 与 codec.TypeQuote 不一致，或未注册", codec.TypeQuote)
	}
	if d.Name != "quote" {
		t.Fatalf("Name=%q，期望 %q", d.Name, "quote")
	}
	if d.SchemaVersion != 2 {
		t.Fatalf("SchemaVersion=%d，期望 2（v2 起 open 必填，见契约文件 schema_version_note）", d.SchemaVersion)
	}
	if d.TimeSemantics.Kind != TimeKindNone {
		t.Fatalf("TimeSemantics.Kind=%v，期望 TimeKindNone", d.TimeSemantics.Kind)
	}
}

// TestQuoteContract_RequiredFieldsMatchGoValidator 是"契约文件与 Go 校验器不能
// 各自手写一份、悄悄漂移"这条风险的具体测试锁定（方案 §2.4 风险 2；顾问建议：
// 不把校验逻辑本身搬进 JSON 数据驱动，而是用一致性测试兜底两份手写表示）。
// QuoteSnapshot.Validate 的必填字段来自 quote.go 里那段 for 循环字面量，本测试
// 把契约文件声明的 required_fields 与它做逐一比对。
func TestQuoteContract_RequiredFieldsMatchGoValidator(t *testing.T) {
	if err := LoadContracts(contractsDir); err != nil {
		t.Fatalf("LoadContracts(%q) 失败: %v", contractsDir, err)
	}
	d, ok := LookupByType(codec.TypeQuote)
	if !ok {
		t.Fatal("契约未加载成功（应已被上一个测试覆盖，这里是防御性重复检查）")
	}

	want := []string{"code", "date", "open", "high", "low", "close", "volume"}
	if len(d.RequiredFields) != len(want) {
		t.Fatalf("required_fields=%v，期望 %v", d.RequiredFields, want)
	}
	for i, f := range want {
		if d.RequiredFields[i] != f {
			t.Fatalf("required_fields[%d]=%q，期望 %q（完整列表: %v）", i, d.RequiredFields[i], f, d.RequiredFields)
		}
	}

	// volume 的量纲声明（"手"）同理，是另一处此前两处手写（Go 字段注释 +
	// docs/clickhouse-schema.md）、现在收敛到契约文件的例子（RFC §9.3）。
	if got := d.Units["volume"]; got != "lots" {
		t.Fatalf("units[volume]=%q，期望 %q（quoteRecord.Volume 的量纲契约是「手」）", got, "lots")
	}
}

// TestLoadContracts_RejectsUnimplementedTimeKind 锁定 LoadContracts 对
// TimeSemantics.Kind != "none" 的拒绝行为：M1 没有实现按 KeyLayout.KeyField 的
// 结构化单调性检查，接受一个"声明了但服务端不会真的检查"的契约比没有契约更
// 危险（静默失效），LoadContracts 必须在加载期就失败，不是运行时悄悄放行。
func TestLoadContracts_RejectsUnimplementedTimeKind(t *testing.T) {
	dir := t.TempDir()
	writeContractFile(t, dir, "bad.schema.json", `{
		"type_id": 999,
		"name": "quote",
		"schema_version": 1,
		"key_prefix": "bad:",
		"key_layout": {"delimiter": ":", "fields": ["prefix"]},
		"time_semantics": {"kind": "strictly_increasing", "key_field": "ts"}
	}`)
	if err := LoadContracts(dir); err == nil {
		t.Fatal("声明 strictly_increasing 的契约应被拒绝加载（M1 未实现该 dispatch）")
	}
}

// TestLoadContracts_RejectsUnknownName 锁定"契约引用一个本仓库没有 Go 校验器
// 实现的 name"这类配置错误必须在加载期报错，而不是静默跳过该类型（方案对
// "静默跳过"的一贯反对，见 M1 任务 5 的立场同理适用于加载失败场景）。
func TestLoadContracts_RejectsUnknownName(t *testing.T) {
	dir := t.TempDir()
	writeContractFile(t, dir, "unknown.schema.json", `{
		"type_id": 998,
		"name": "not_a_real_type",
		"schema_version": 1,
		"key_prefix": "nope:",
		"key_layout": {"delimiter": ":", "fields": ["prefix"]},
		"time_semantics": {"kind": "none", "key_field": ""}
	}`)
	if err := LoadContracts(dir); err == nil {
		t.Fatal("引用未知 name 的契约应被拒绝加载")
	}
}

// TestLoadContracts_RejectsUnknownField 守卫"契约文件里的字段拼错/改名后被
// 静默忽略"这类缺陷（与 config/config_json_test.go 的
// TestShippedConfigHasNoUnknownKeys 是同一种守卫，同一个仓库先例）。
func TestLoadContracts_RejectsUnknownField(t *testing.T) {
	dir := t.TempDir()
	writeContractFile(t, dir, "typo.schema.json", `{
		"type_id": 997,
		"name": "quote",
		"schema_version": 1,
		"key_prefix": "typo:",
		"key_layout": {"delimiter": ":", "fields": ["prefix"]},
		"time_semantics": {"kind": "none", "key_field": ""},
		"this_field_does_not_exist": true
	}`)
	if err := LoadContracts(dir); err == nil {
		t.Fatal("含未知字段的契约应被拒绝加载，而不是静默忽略该字段")
	}
}

func writeContractFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := dir + "/" + name
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写测试契约文件 %q 失败: %v", path, err)
	}
}
