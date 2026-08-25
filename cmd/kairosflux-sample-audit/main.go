// 命令 kairosflux-sample-audit 是"audit 审计导出"能力样例（发布批次阶段 A）：
// embedded 引擎写入多来源数据后，用 LIST_WRITES 导出 append-only JSONL
// 审计文件（每行一条写入信封：logical_key/seq/write_ts/source/schema_ver/
// payload_hash/payload_b64/hash_ok），末尾追加清单行（导出条数 +
// export_fingerprint——导出内容的确定性摘要，见 exportManifestRecord 的
// 文档：它是"本次导出文件"的完整性校验值，不是数据集状态指纹，二者不可
// 互相比较）。
//
// 用法（README"一条命令跑通"）：
//
//	go run ./cmd/kairosflux-sample-audit -data-dir /tmp/kf-demo-audit -out /tmp/kf-audit.jsonl
//
// 退出码：0=导出完成且全部信封 hash 自检通过；1=任一步失败或发现漂移。
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux"
	"github.com/ChronoBrew/KairosFlux/internal/temporal"
)

// exportWriteRecord 是 JSONL 每行记录（与 kairosflux-cli 的 export-writes
// 同形状——同一份导出契约的嵌入式复刻，字段顺序/类型在编译期锁定）。
type exportWriteRecord struct {
	LogicalKey  string `json:"logical_key"`
	Seq         uint64 `json:"seq"`
	WriteTS     int64  `json:"write_ts"`
	Source      string `json:"source"`
	SchemaVer   uint32 `json:"schema_ver"`
	PayloadHash string `json:"payload_hash"`
	PayloadB64  string `json:"payload_b64"`
	HashOK      bool   `json:"hash_ok"`
}

// exportManifestRecord 是末尾清单行。
type exportManifestRecord struct {
	Manifest          bool   `json:"_manifest"`
	Count             int    `json:"count"`
	ExportFingerprint string `json:"export_fingerprint"`
}

func main() {
	dataDir := flag.String("data-dir", "/tmp/kairosflux-sample-audit", "数据目录（不存在会自动创建）")
	outPath := flag.String("out", "/tmp/kairosflux-audit.jsonl", "JSONL 导出路径（追加写入）")
	flag.Parse()

	e, err := kairosflux.NewEmbedded(kairosflux.Options{DataDir: *dataDir})
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-audit] 启动失败:", err)
		os.Exit(1)
	}
	defer e.Close()

	// 多来源、多键的合成审计素材：两个来源各写若干版本。写入顺序用显式
	// 切片而非 map——seq 按写入顺序分配，map 迭代序会破坏导出文件的
	// 逐字节确定性（同一份账本两次导出必须一致）。
	now := time.Now().UnixNano()
	type write struct{ source, code string }
	plan := []write{
		{"quantscout-crawler", "000001"},
		{"quantscout-crawler", "000002"},
		{"quantscout-crawler", "000003"},
		{"jobctl-reconcile", "000001"},
		{"jobctl-reconcile", "000002"},
	}
	for i, w := range plan {
		payload := fmt.Sprintf(`{"code":%q,"date":"2026-08-17","close":%d.%.2d}`, w.code, i+1, i*7)
		if _, err := e.PutVersioned("quote:2026-08-17:"+w.code, []byte(payload), now+int64(i), w.source, 2); err != nil {
			fmt.Fprintln(os.Stderr, "[sample-audit] PUT_VERSIONED 失败:", err)
			os.Exit(1)
		}
	}

	writes, err := e.ListWrites("quote:", 0, 0, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-audit] LIST_WRITES 失败:", err)
		os.Exit(1)
	}
	fmt.Printf("[sample-audit] LIST_WRITES → %d 条写入，按来源:", len(writes.Entries))
	for _, s := range writes.BySource {
		fmt.Printf(" %s x%d", s.Source, s.Count)
	}
	fmt.Println()

	// 导出一致性检查：每条信封 hash 自检必须通过（写入后未发生漂移）。
	for _, we := range writes.Entries {
		if !we.HashOK {
			fmt.Fprintf(os.Stderr, "[sample-audit] 数据漂移: %s seq=%d hash_ok=false\n", we.LogicalKey, we.Seq)
			os.Exit(1)
		}
	}

	f, err := os.OpenFile(*outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-audit] 打开导出文件失败:", err)
		os.Exit(1)
	}
	defer f.Close()

	// export_fingerprint：对 (LogicalKey, Seq, Payload) 三元组集合的确定性
	// 摘要——与 kairosflux-cli 的 export-writes 同一计算（internal/temporal.
	// Fingerprint），保证同一份账本两次导出得到逐字节相同的 JSONL 与指纹。
	exportFP := computeExportFingerprint(writes.Entries)

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, we := range writes.Entries {
		rec := exportWriteRecord{
			LogicalKey:  we.LogicalKey,
			Seq:         we.Seq,
			WriteTS:     we.WriteNanos,
			Source:      we.Source,
			SchemaVer:   we.SchemaVer,
			PayloadHash: we.PersistedHash,
			PayloadB64:  base64.StdEncoding.EncodeToString(we.Payload),
			HashOK:      we.HashOK,
		}
		if err := enc.Encode(rec); err != nil {
			fmt.Fprintln(os.Stderr, "[sample-audit] 写导出行失败:", err)
			os.Exit(1)
		}
	}
	if err := enc.Encode(exportManifestRecord{Manifest: true, Count: len(writes.Entries), ExportFingerprint: exportFP}); err != nil {
		fmt.Fprintln(os.Stderr, "[sample-audit] 写清单行失败:", err)
		os.Exit(1)
	}

	fmt.Printf("[sample-audit] 导出完成: %d 条 → %s（export_fingerprint=%s，全部信封 hash 自检通过）\n",
		len(writes.Entries), *outPath, exportFP)
}

// computeExportFingerprint 计算导出内容的确定性指纹：与 kairosflux-cli 的
// export-writes 同一算法（按 (LogicalKey, Seq) 排序后的
// internal/temporal.Fingerprint），保证两条导出路径产出同值，可互相校验。
func computeExportFingerprint(entries []kairosflux.WriteEnvelope) string {
	sorted := make([]kairosflux.WriteEnvelope, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].LogicalKey != sorted[j].LogicalKey {
			return sorted[i].LogicalKey < sorted[j].LogicalKey
		}
		return sorted[i].Seq < sorted[j].Seq
	})
	fp := make([]temporal.Entry, 0, len(sorted))
	for _, e := range sorted {
		fp = append(fp, temporal.Entry{LogicalKey: e.LogicalKey, Seq: e.Seq, Payload: e.Payload})
	}
	return temporal.Fingerprint(fp)
}
