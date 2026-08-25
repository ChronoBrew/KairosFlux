// 命令 kairosflux-crashsim 是找茬面的崩溃模拟器（阶段 B）：独立进程打开
// embedded 引擎连续写入 N 条版本化记录，每 50 条用原子 rename 更新一次
// progress 文件（记录"已拿到 fsync 确认的最大下标"），供外部观察者在任意
// 时刻 SIGKILL（或 rlimit 触发信号）后验证崩溃一致性：
//
//	恢复后可恢复条数 ≥ progress 记录值（已确认持久化的写一条不能少）
//	REPLAY_FINGERPRINT 对账不一致 = 0（:current 与版本账本重放一致）
//
// progress 文件本身是 best-effort（崩溃点可能滞后于实际落盘数），断言口径
// 是"已确认 ≥ progress"，不是"已确认 == progress"。
//
// 用法：kairosflux-crashsim -data-dir <dir> [-total 3000] [-progress <dir>/progress]
// 参数与主仓 cmd/kairosflux-bench 的 adversarial 子命令配套。
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux"
)

func main() {
	dataDir := flag.String("data-dir", "/tmp/kf-crashsim", "数据目录")
	total := flag.Int("total", 3000, "总写入条数")
	progressPath := flag.String("progress", "", "progress 文件路径（默认 <data-dir>/progress）")
	flag.Parse()

	if *progressPath == "" {
		*progressPath = filepath.Join(*dataDir, "progress")
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir:", err)
		os.Exit(1)
	}

	e, err := kairosflux.NewEmbedded(kairosflux.Options{DataDir: *dataDir})
	if err != nil {
		fmt.Fprintln(os.Stderr, "NewEmbedded:", err)
		os.Exit(1)
	}
	defer e.Close()

	base := time.Now().UnixNano() - int64(*total)
	for i := 1; i <= *total; i++ {
		key := fmt.Sprintf("quote:2026-08-17:%05d", i%100000)
		payload := []byte(fmt.Sprintf(`{"code":"%05d","date":"2026-08-17","open":%.2f,"high":%.2f,"low":%.2f,"close":%.2f,"volume":%d}`,
			i%100000, 1+float64(i%100)/100, 1+float64(i%100)/100, 1+float64(i%100)/100, 1+float64(i%100)/100, i*7))
		if _, err := e.PutVersioned(key, payload, base+int64(i), "crashsim", 2); err != nil {
			fmt.Fprintln(os.Stderr, "写入失败(下标", i, "):", err)
			writeProgress(*progressPath, i-1) // 最后确认的写到 i-1
			os.Exit(3)
		}
		if i%50 == 0 {
			writeProgress(*progressPath, i)
		}
	}
	writeProgress(*progressPath, *total)
	fmt.Println("crashsim: 全部写入完成 total=", *total)
}

// writeProgress 原子写 progress 文件（临时文件 + rename），崩溃点最多丢失
// 最近一次进度更新，不产生半写状态。
func writeProgress(path string, n int) {
	tmp := path + ".tmp"
	_ = os.WriteFile(tmp, []byte(fmt.Sprintf("%d\n", n)), 0o644)
	_ = os.Rename(tmp, path)
}
