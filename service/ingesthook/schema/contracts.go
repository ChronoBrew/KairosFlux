package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 本文件实现方案「BanDB——时态内核与 AI 数据平面」M1 的"契约文件"任务：把
// schema.Register 在 Go 代码里手写的类型声明，升级为可从磁盘加载、机器可读、
// 版本化的 contracts/*.schema.json 文件（字段/单位/必填/幂等键/PIT 语义/生产者/
// 消费者/schema 版本，见方案 §6 数据契约清单）。
//
// 刻意的范围边界（M1，不是遗漏）：
//   - 只有 quote（行情快照）在本仓库有真实的 Go 校验器与生产写入路径，因此只有
//     它的契约文件被 LoadContracts 加载并强制生效；方案 §6 列出的另外十种工件
//     （universe/fundamentals/screens/...）本仓库既不读也不写，为它们写契约文件
//     会是"文档穿着实现的外衣"——没有校验器、没有加载器去启用它们，写了也不会
//     被任何代码路径触达。这十种工件的契约留给它们各自的生产者/消费者仓库
//     （QuantScout=Python、QuantBrew=Rust）在各自接入 BanDB 契约时补齐，本次不
//     抢跑。
//   - 字段级别的校验规则（必填/OHLC一致性/涨跌幅上限）仍然是 Go 代码
//     （QuoteSnapshot.Validate），不是从 JSON 数据驱动出来的通用规则引擎——RFC
//     §9.4 明确这是渐进式迁移，BanDB CLAUDE.md 的"外科手术式修改"也不支持为了
//     "更干净"重写一段已经过测试、没人要求改动的校验逻辑。契约文件与 Go 代码
//     之间可能出现的漂移（如 units/必填字段列表两处手写不一致），由
//     TestQuoteContract_RequiredFieldsMatchGoValidator（contracts_test.go）
//     这条一致性测试兜底，而不是消灭其中一份手写。
//   - TimeSemantics.Kind 目前只接受 "none"：declaring "strictly_increasing"/
//     "non_decreasing" 需要按 KeyLayout.KeyField 做结构化单调性检查，这部分
//     dispatch 逻辑本 M1 未实现（见 ingesthook/filter.go 的对应分支），LoadContracts
//     对非 none 的取值直接拒绝加载——不接受一个"声明了但服务端不会真的检查"的
//     契约，那是比没有契约更危险的静默失效。
type contractFile struct {
	TypeID            uint16            `json:"type_id"`
	Name              string            `json:"name"`
	SchemaVersion     int               `json:"schema_version"`
	SchemaVersionNote string            `json:"schema_version_note"`
	KeyPrefix         string            `json:"key_prefix"`
	KeyLayout         contractKeyLayout `json:"key_layout"`
	TimeSemantics     contractTimeSem   `json:"time_semantics"`
	Producer          string            `json:"producer"`
	Consumers         []string          `json:"consumers"`
	PITSemantics      string            `json:"pit_semantics"`
	IdempotencyKey    []string          `json:"idempotency_key"`
	RequiredFields    []string          `json:"required_fields"`
	Units             map[string]string `json:"units"`
	ValidationRules   []json.RawMessage `json:"validation_rules"`
	ErrorCodes        map[string]string `json:"error_codes"`
}

type contractKeyLayout struct {
	Delimiter string   `json:"delimiter"`
	Fields    []string `json:"fields"`
}

type contractTimeSem struct {
	Kind     string `json:"kind"`
	KeyField string `json:"key_field"`
}

// builtinValidators 把契约文件里的 name 映射到本仓库实际的 Go 校验器实现。契约
// 文件声明"这个类型该怎么被校验"的元数据（必填字段、量纲……），但校验逻辑本身
// 仍是 Go 代码——这张表是两者之间唯一的接线点。契约文件引用一个这里没有的 name
// 是配置错误，LoadContracts 会报错而不是静默跳过该类型。
var builtinValidators = map[string]Validator{
	"quote": QuoteSnapshot{},
}

// LoadContracts 从 dir 目录加载全部 *.schema.json 契约文件并注册进
// RegisterDescriptor。任何一个文件解析失败、引用未知 name、或声明了本 M1 未实现
// 的 TimeSemantics.Kind，整体加载失败并返回 error——契约加载是"服务端启动时
// 强制校验"的一部分（方案 M1 任务 2），失败必须让调用方（cmd/ban-server 的
// main）拒绝启动，而不是打个日志退回旧行为静默降级。
func LoadContracts(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("schema: read contracts dir %q: %w", dir, err)
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".schema.json") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("schema: read contract %q: %w", path, err)
		}

		var c contractFile
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			return fmt.Errorf("schema: parse contract %q: %w", path, err)
		}

		validator, ok := builtinValidators[c.Name]
		if !ok {
			return fmt.Errorf("schema: contract %q declares name %q with no registered Go validator (see builtinValidators)", path, c.Name)
		}

		kind, err := parseTimeKind(c.TimeSemantics.Kind)
		if err != nil {
			return fmt.Errorf("schema: contract %q: %w", path, err)
		}

		RegisterDescriptor(Descriptor{
			TypeID:         c.TypeID,
			Name:           c.Name,
			SchemaVersion:  c.SchemaVersion,
			KeyPrefix:      c.KeyPrefix,
			KeyLayout:      KeyLayout{Delimiter: c.KeyLayout.Delimiter, Fields: c.KeyLayout.Fields},
			TimeSemantics:  TimeSemantics{Kind: kind, KeyField: c.TimeSemantics.KeyField},
			RequiredFields: c.RequiredFields,
			Units:          c.Units,
			IdempotencyKey: c.IdempotencyKey,
			PITSemantics:   c.PITSemantics,
			Producer:       c.Producer,
			Consumers:      c.Consumers,
			Validation:     validator,
		})
	}
	return nil
}

// defaultContractDirs 镜像 config/global.go 的 defaultConfigPaths 取值思路：同一个
// 二进制可能从项目根、cmd 子目录或其它工作目录启动，候选路径覆盖这几种常见情形。
// 与 config 的"都不存在则保持默认"不同，契约目录一个都找不到时 LoadContractsDefault
// 直接返回 error——配置有安全默认值，契约没有：找不到契约就悄悄不做类型校验，
// 正是方案 §2.4 要消灭的"静默跳过"。
var defaultContractDirs = []string{
	"contracts",
	"../contracts",
	"../../contracts",
}

// LoadContractsDefault 依次尝试 defaultContractDirs，加载第一个存在的目录。
func LoadContractsDefault() error {
	for _, dir := range defaultContractDirs {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			return LoadContracts(dir)
		}
	}
	return fmt.Errorf("schema: no contracts directory found in any of %v", defaultContractDirs)
}

func parseTimeKind(kind string) (TimeKind, error) {
	switch kind {
	case "none":
		return TimeKindNone, nil
	case "strictly_increasing", "non_decreasing":
		return TimeKindNone, fmt.Errorf("time_semantics.kind %q declared but M1 does not implement its dispatch yet (see ingesthook/filter.go); refusing to load a contract the server cannot actually enforce", kind)
	default:
		return TimeKindNone, fmt.Errorf("unknown time_semantics.kind %q", kind)
	}
}
