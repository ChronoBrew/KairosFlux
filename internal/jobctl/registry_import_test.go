package jobctl

import (
	"strings"
	"testing"
)

func TestRegistryImporter_ImportsNewRecordsAndSkipsUnchanged(t *testing.T) {
	store := newFakeStore()
	imp := &RegistryImporter{Store: store}

	line1 := `{"verdict":{"fingerprint":"aaa111","kind":"factor_gate"}}`
	line2 := `{"verdict":{"fingerprint":"bbb222","kind":"factor_gate"}}`
	input := line1 + "\n" + line2 + "\n"

	result, err := imp.ImportReader(strings.NewReader(input))
	if err != nil {
		t.Fatalf("导入出错: %v", err)
	}
	if result.TotalLines != 2 || result.Imported != 2 || result.Unchanged != 0 || len(result.Errors) != 0 {
		t.Fatalf("首次导入结果不符: %+v", result)
	}
	if got := store.versionCount(registryFingerprintKey("aaa111")); got != 1 {
		t.Fatalf("aaa111 应有 1 条版本，实际 %d", got)
	}

	// 重复导入同一份内容：应全部标记为 unchanged，不产生新版本。
	result2, err := imp.ImportReader(strings.NewReader(input))
	if err != nil {
		t.Fatalf("第二次导入出错: %v", err)
	}
	if result2.Imported != 0 || result2.Unchanged != 2 {
		t.Fatalf("重复导入应全部 unchanged，实际 %+v", result2)
	}
	if got := store.versionCount(registryFingerprintKey("aaa111")); got != 1 {
		t.Fatalf("重复导入后 aaa111 仍应只有 1 条版本，实际 %d", got)
	}

	// 内容变化后再导入：应产生新版本（fingerprint 相同、内容不同）。
	line1Updated := `{"verdict":{"fingerprint":"aaa111","kind":"factor_gate","level":"pass_raw"}}`
	result3, err := imp.ImportReader(strings.NewReader(line1Updated + "\n"))
	if err != nil {
		t.Fatalf("第三次导入出错: %v", err)
	}
	if result3.Imported != 1 {
		t.Fatalf("内容变化应触发重新导入，实际 %+v", result3)
	}
	if got := store.versionCount(registryFingerprintKey("aaa111")); got != 2 {
		t.Fatalf("aaa111 内容变化后应累计 2 条版本，实际 %d", got)
	}
}

func TestRegistryImporter_RecordsErrorsWithoutStoppingOtherLines(t *testing.T) {
	store := newFakeStore()
	imp := &RegistryImporter{Store: store}

	input := `not json` + "\n" +
		`{"verdict":{}}` + "\n" + // 缺 fingerprint
		`{"verdict":{"fingerprint":"ccc333"}}` + "\n"

	result, err := imp.ImportReader(strings.NewReader(input))
	if err != nil {
		t.Fatalf("导入出错: %v", err)
	}
	if len(result.Errors) != 2 {
		t.Fatalf("应有 2 条错误行，实际 %d: %+v", len(result.Errors), result.Errors)
	}
	if result.Imported != 1 {
		t.Fatalf("剩下的合法行仍应被导入，实际 imported=%d", result.Imported)
	}
	if result.Errors[0].Line != 1 || result.Errors[1].Line != 2 {
		t.Fatalf("错误行号不符: %+v", result.Errors)
	}
}

func TestRegistryImporter_SkipsBlankLines(t *testing.T) {
	store := newFakeStore()
	imp := &RegistryImporter{Store: store}

	input := "\n\n" + `{"verdict":{"fingerprint":"ddd444"}}` + "\n\n"
	result, err := imp.ImportReader(strings.NewReader(input))
	if err != nil {
		t.Fatalf("导入出错: %v", err)
	}
	if result.TotalLines != 1 || result.Imported != 1 || len(result.Errors) != 0 {
		t.Fatalf("空行应被跳过，结果不符: %+v", result)
	}
}
