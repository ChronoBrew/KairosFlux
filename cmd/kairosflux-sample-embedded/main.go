// 命令 kairosflux-sample-embedded 是"embedded 模式进程内全流程"能力样例
// （发布批次阶段 A）：把 KairosFlux 当库嵌进进程，跑通
// 合成数据 → PUT_VERSIONED 版本化写入 → GET_AS_OF 定点读取 → LIST_VERSIONS
// 版本清单 → REPLAY_FINGERPRINT 重放指纹（:current 对账）→ LIST_WRITES 审计
// 全链路，全部走 service/kairosflux.go 的可导入 API，不开任何网络监听。
//
// 用法（README"一条命令跑通"）：
//
//	go run ./cmd/kairosflux-sample-embedded -data-dir /tmp/kf-demo-data
//
// 退出码：0=全链路通过（指纹对账零不一致）；1=任一步失败或对账不一致。
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ChronoBrew/KairosFlux/service"
)

func main() {
	dataDir := flag.String("data-dir", "/tmp/kairosflux-sample-embedded", "数据目录（不存在会自动创建）")
	flag.Parse()

	e, err := service.NewEmbedded(service.Options{DataDir: *dataDir})
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-embedded] 启动失败:", err)
		os.Exit(1)
	}
	defer e.Close()

	// 三个交易日快照，write_ts 用显式可控时间戳——as-of 定点语义的演示前提。
	base := time.Date(2026, 8, 17, 9, 30, 0, 0, time.UTC).UnixNano()
	const key = "quote:2026-08-17:510300"
	payloads := []string{
		`{"code":"510300","date":"2026-08-17","open":3.90,"high":3.95,"low":3.88,"close":3.92,"volume":1200000}`,
		`{"code":"510300","date":"2026-08-17","open":3.92,"high":3.98,"low":3.90,"close":3.96,"volume":1500000}`,
		`{"code":"510300","date":"2026-08-17","open":3.96,"high":4.00,"low":3.93,"close":3.98,"volume":1100000}`,
	}
	for i, p := range payloads {
		seq, err := e.PutVersioned(key, []byte(p), base+int64(i)*int64(time.Second), "sample-demo", 2)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[sample-embedded] PUT_VERSIONED 失败:", err)
			os.Exit(1)
		}
		fmt.Printf("写入版本 seq=%d: %s\n", seq, p)
	}

	// as-of：9:30:00.5 时刻只能看到第 1 个版本（写入发生在 9:30:00/01/02，
	// 0.5 秒处只已写入第一个——绝不返回未来写入）。
	v, found, err := e.GetAsOf(key, base+int64(500*time.Millisecond))
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-embedded] GET_AS_OF 失败:", err)
		os.Exit(1)
	}
	if !found {
		fmt.Fprintln(os.Stderr, "[sample-embedded] as-of 时刻无可见版本（不应发生）")
		os.Exit(1)
	}
	if v.Seq != 1 {
		fmt.Fprintf(os.Stderr, "[sample-embedded] as-of(9:30:00.5) 应返回 seq=1，实际 seq=%d（as-of 语义违反）\n", v.Seq)
		os.Exit(1)
	}
	fmt.Printf("GET_AS_OF(9:30:00.5) → seq=%d payload=%s\n", v.Seq, v.Payload)

	versions, err := e.ListVersions(key)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-embedded] LIST_VERSIONS 失败:", err)
		os.Exit(1)
	}
	fmt.Printf("LIST_VERSIONS → %d 个版本（seq 升序）\n", len(versions))

	r, err := e.ReplayFingerprint("quote:", 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-embedded] REPLAY_FINGERPRINT 失败:", err)
		os.Exit(1)
	}
	fmt.Printf("REPLAY_FINGERPRINT → 逻辑键=%d 指纹=%s 对账不一致=%d\n", r.KeyCount, r.Fingerprint, len(r.Mismatches))
	if len(r.Mismatches) > 0 {
		fmt.Fprintln(os.Stderr, "[sample-embedded] 指纹对账不一致（:current 与重放结果不符）:", r.Mismatches)
		os.Exit(1)
	}

	writes, err := e.ListWrites("quote:", 0, 0, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "[sample-embedded] LIST_WRITES 失败:", err)
		os.Exit(1)
	}
	fmt.Printf("LIST_WRITES → %d 条写入，按来源:", len(writes.Entries))
	for _, s := range writes.BySource {
		fmt.Printf(" %s x%d", s.Source, s.Count)
	}
	fmt.Println()

	fmt.Println("[sample-embedded] 全链路通过（指纹对账零不一致）")
}
