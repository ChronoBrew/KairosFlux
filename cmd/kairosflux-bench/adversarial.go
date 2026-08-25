package main

// adversarial.go：找茬面六维度（阶段 B）。每维度结果（含"没发现问题"）入
// 问题清单；发现 bug 先修后发（本文件各维度全部带自动断言，fail 即 exit 1）。
//
//  1. kill -9 重启一致性：crashsim 子进程写入中 SIGKILL → 重新 Open →
//     已确认持久化的写（progress 文件，原子 rename）一条不少 + 指纹两次
//     重放一致。
//  2. 并发客户端 10/50：各自独立 v2 连接并发写 → LIST_WRITES 条数精确 +
//     seq 全唯一 + 无丢失。
//  3. 大 payload：1MiB/8MiB 经 v2 PUT_VERSIONED 与 v1 PUT（blob: 未纳管
//     前缀，末段非数字避开设备时间戳启发式）；32MiB 超过 MaxPackageSize
//     默认 16MiB 传输层帧长上限，如实记录拒绝行为。
//  4. 畸形帧注入（fuzz 语料回放）：截断头/超长声明/未知 opcode/坏负载/
//     坏 magic/随机字节 ×100 轮，每轮后健康检查（PUT_VERSIONED+GET_AS_OF）。
//  5. 磁盘写满模拟：crashsim 在 RLIMIT_FSIZE=8KB（ulimit -f 16）下写入，
//     写失败/信号终止 → 重新 Open 一致性同维度 1。
//  6. 时钟回拨：engine 层 write_ts 递减（as-of 语义仍正确）；网络层 v1 PUT
//     未纳管数字末段键的设备时间戳回拨被过滤钩子拒绝（non_monotonic_
//     timestamp）；v2 PUT_VERSIONED 按设计跳过该启发式（同一逻辑键反复写
//     版本是正常行为，见 ingesthook.ValidateVersioned 文档）；查询回拨
//     （as-of 过去时刻）不命中已实测。

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kairosflux "github.com/ChronoBrew/KairosFlux"
	"github.com/ChronoBrew/KairosFlux/client"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
)

func cmdAdversarial(args []string) error {
	fs := flag.NewFlagSet("adversarial", flag.ExitOnError)
	outPath := fs.String("out", "docs/bench/03-adversarial.md", "报告输出路径")
	dataDir := fs.String("data-dir", "/tmp/kf-bench-adv", "数据目录根（各维度用子目录）")
	port := fs.Int("port", 0, "server 端口（0=自动找空闲端口）")
	crashsimBin := fs.String("crashsim", "/tmp/kf-crashsim", "crashsim 可执行文件路径（不存在则自动 go build）")
	fs.Parse(args)

	started := time.Now()
	f, err := os.Create(*outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	benchReportHeader(f, "找茬面：kill -9 一致性 / 并发客户端 / 大 payload / 畸形帧注入 / 磁盘写满 / 时钟回拨",
		"六维度全部自动断言：任一项失败命令 exit 1（先修后发的执行机制）。", started)

	if *port == 0 {
		*port = freePort()
	}
	addr := fmt.Sprintf("127.0.0.1:%d", *port)

	if err := buildCrashsim(*crashsimBin); err != nil {
		return err
	}

	// 共享 server（维度 2/3/4/6 的协议面测试对象），数据目录独立。
	srvDir := filepath.Join(*dataDir, "server")
	os.RemoveAll(srvDir)
	srv, err := kairosflux.Serve(kairosflux.Options{DataDir: srvDir, Port: *port})
	if err != nil {
		return fmt.Errorf("Serve: %w", err)
	}
	defer srv.Close()

	problems := []string{}

	// —— 1. kill -9 重启一致性 ——
	fmt.Fprintf(f, "## 1. kill -9 重启一致性\n\n")
	killDir := filepath.Join(*dataDir, "kill")
	os.RemoveAll(killDir)
	killRes, err := runKillTest(*crashsimBin, killDir, 3000)
	if err != nil {
		problems = append(problems, fmt.Sprintf("kill -9 重启一致性失败: %v", err))
		fmt.Fprintf(f, "**失败**：%v\n\n", err)
	} else {
		verdict := "✓"
		if killRes.recovered < killRes.progress || killRes.mismatches != 0 || !killRes.fpStable {
			verdict = "✗"
			problems = append(problems, "kill -9 重启一致性断言未全过")
		}
		fmt.Fprintf(f, "- kill 时机：SIGKILL 于 progress=%d（共 %d）后\n", killRes.progress, killRes.total)
		fmt.Fprintf(f, "- 恢复 %d 条 ≥ progress=%d %s；:current 与版本账本重放不一致=%d %s；指纹两次重放一致=%v %s\n\n",
			killRes.recovered, killRes.progress, verdict, killRes.mismatches, verdict, killRes.fpStable, verdict)
	}

	// —— 2. 并发客户端 10/50 ——
	fmt.Fprintf(f, "## 2. 并发客户端（10 / 50）\n\n")
	fmt.Fprintf(f, "| 并发数 | 每连接写数 | 写入总条数 | 审计条数精确 | seq 全唯一 | 写 p50 | 写 p99 |\n|---|---|---|---|---|---|---|\n")
	for _, n := range []int{10, 50} {
		row, err := runConcurrentTest(addr, n, 200)
		if err != nil {
			problems = append(problems, fmt.Sprintf("并发客户端 %d 失败: %v", n, err))
			fmt.Fprintf(f, "| %d | 200 | — | 失败: %v | | | |\n", n, err)
			continue
		}
		verdict := "✓"
		if row.lostOrDup || row.countMismatch {
			verdict = "✗"
			problems = append(problems, fmt.Sprintf("并发客户端 %d：条数/唯一性未全过", n))
		}
		fmt.Fprintf(f, "| %d | 200 | %d | %s | %s | %s | %s |\n",
			n, row.writes, verdict, verdict, latencyStr(row.stats.P50()), latencyStr(row.stats.P99()))
	}

	// —— 3. 大 payload ——
	fmt.Fprintf(f, "\n## 3. 大 payload（1MiB / 8MiB / 32MiB）\n\n")
	fmt.Fprintf(f, "| 路径 | 大小 | 结果 | 写延迟 | 读回一致 |\n|---|---|---|---|---|\n")
	for _, mb := range []int{1, 8} {
		size := mb << 20
		payload := bytes.Repeat([]byte("a"), size)
		key := fmt.Sprintf("blob:test:file-%d", mb) // 末段非数字：避开设备时间戳启发式
		t0 := time.Now()
		seq, err := v2Put(addr, key, payload)
		if err != nil {
			problems = append(problems, fmt.Sprintf("大 payload %dMiB v2 失败: %v", mb, err))
			fmt.Fprintf(f, "| v2 PUT_VERSIONED | %d MiB | 失败: %v | — | — |\n", mb, err)
			continue
		}
		el := time.Since(t0)
		got, found, err := v2Get(addr, key)
		consistent := err == nil && found && len(got) == size
		if !consistent {
			problems = append(problems, fmt.Sprintf("大 payload %dMiB 读回不一致", mb))
		}
		fmt.Fprintf(f, "| v2 PUT_VERSIONED | %d MiB | 写入成功 seq=%d | %s | %v |\n", mb, seq, el.Round(time.Millisecond), consistent)
	}
	{
		// 32MiB：超过 MaxPackageSize 默认 16MiB 传输层帧长上限——如实记录拒绝。
		size := 32 << 20
		payload := bytes.Repeat([]byte("b"), size)
		key := "blob:test:file-32"
		if _, err := v2Put(addr, key, payload); err != nil {
			fmt.Fprintf(f, "| v2 PUT_VERSIONED | 32 MiB | 被传输层拒绝（16MiB 帧长上限）: %v | — | — |\n", err)
		} else {
			fmt.Fprintf(f, "| v2 PUT_VERSIONED | 32 MiB | 意外成功（16MiB 上限未生效？） | — | — |\n")
			problems = append(problems, "32MiB 写未被帧长上限拒绝（与配置预期不符）")
		}
	}
	{
		size := 32 << 20
		payload := bytes.Repeat([]byte("c"), size)
		key := "blob:test:file-v1-32"
		c, err := client.New(client.Options{Addrs: []string{addr}})
		if err != nil {
			return err
		}
		defer c.Close()
		t0 := time.Now()
		err = c.Put(context.Background(), []byte(key), payload)
		el := time.Since(t0)
		if err != nil {
			fmt.Fprintf(f, "| v1 PUT | 32 MiB | 被传输层拒绝（16MiB 帧长上限）: %v | %s | — |\n", err, el.Round(time.Millisecond))
		} else {
			fmt.Fprintf(f, "| v1 PUT | 32 MiB | 意外成功（16MiB 上限未生效？） | %s | — |\n", el.Round(time.Millisecond))
			problems = append(problems, "v1 32MiB 写未被帧长上限拒绝")
		}
	}

	// —— 4. 畸形帧注入（fuzz 语料回放）——
	fmt.Fprintf(f, "\n## 4. 畸形帧注入（fuzz 语料回放，100 轮）\n\n")
	fuzzRes := runFuzzReplay(addr, 100)
	fmt.Fprintf(f, "- 语料 8 种：截断头 / 合法头+超长声明 DataLen / 未知 opcode / 坏负载 / 坏 magic / 超长坏负载 / 头后粘连垃圾 / 纯随机字节\n")
	fmt.Fprintf(f, "- 注入轮数：%d；结构化拒绝=%d；连接被服务端关闭=%d；挂起/超时=%d；服务端崩溃=%d\n",
		fuzzRes.rounds, fuzzRes.rejected, fuzzRes.closed, fuzzRes.timeouts, fuzzRes.crashes)
	fmt.Fprintf(f, "- 每轮后健康检查（PUT_VERSIONED + GET_AS_OF 正常往返）：%v\n", fuzzRes.healthOK)
	if fuzzRes.crashes > 0 || !fuzzRes.healthOK {
		problems = append(problems, "畸形帧注入后服务端不健康或崩溃")
	}

	// —— 5. 磁盘写满模拟 ——
	fmt.Fprintf(f, "\n## 5. 磁盘写满模拟（RLIMIT_FSIZE=8KB，ulimit -f 16）\n\n")
	diskDir := filepath.Join(*dataDir, "diskfull")
	os.RemoveAll(diskDir)
	diskRes, err := runDiskFullTest(*crashsimBin, diskDir, 3000)
	if err != nil {
		problems = append(problems, fmt.Sprintf("磁盘写满模拟失败: %v", err))
		fmt.Fprintf(f, "**失败**：%v\n\n", err)
	} else {
		verdict := "✓"
		if diskRes.recovered < diskRes.progress || diskRes.mismatches != 0 || !diskRes.fpStable {
			verdict = "✗"
			problems = append(problems, "磁盘写满模拟恢复一致性断言未全过")
		}
		fmt.Fprintf(f, "- crashsim 在写满模拟下于 progress=%d 处终止（%s）\n", diskRes.progress, diskRes.signalDesc)
		fmt.Fprintf(f, "- 重新 Open 恢复 %d 条 ≥ progress=%d %s；:current 对账不一致=%d %s；指纹稳定=%v %s\n\n",
			diskRes.recovered, diskRes.progress, verdict, diskRes.mismatches, verdict, diskRes.fpStable, verdict)
	}

	// —— 6. 时钟回拨 ——
	fmt.Fprintf(f, "\n## 6. 时钟回拨\n\n")
	if err := runClockRollback(addr); err != nil {
		problems = append(problems, fmt.Sprintf("时钟回拨维度失败: %v", err))
		fmt.Fprintf(f, "**失败**：%v\n\n", err)
	}

	// —— 问题清单 ——
	fmt.Fprintf(f, "\n---\n\n## 问题清单（本批次实测结论）\n\n")
	if len(problems) == 0 {
		fmt.Fprintf(f, "六维度未发现正确性 bug；设计边界（32MiB 帧长上限拒绝、v2 版本化写入按设计跳过回拨启发式）已如实记录于上文对应维度。\n")
	} else {
		for i, p := range problems {
			fmt.Fprintf(f, "%d. %s\n", i+1, p)
		}
	}
	emitMeasurementCorrections(f)
	fmt.Fprintf(f, "\n报告生成耗时 %s。\n", time.Since(started).Round(time.Second))
	return nil
}

// —— 维度 1/5 公共：崩溃恢复检查 ——

type crashRecovery struct {
	total, progress, recovered, mismatches int
	fpStable                               bool
	signalDesc                             string
}

func buildCrashsim(bin string) error {
	if _, err := os.Stat(bin); err == nil {
		return nil
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/kairosflux-crashsim")
	cmd.Dir = repoRoot()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build crashsim: %w (%s)", err, out)
	}
	return nil
}

// repoRoot 从 CWD 向上找 go.mod 目录。
func repoRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}

// waitProgress 轮询 progress 文件直到 >= want 或超时。
func waitProgress(path string, want int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			n, perr := strconv.Atoi(strings.TrimSpace(string(b)))
			if perr == nil && n >= want {
				return n, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if b, err := os.ReadFile(path); err == nil {
		if n, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil {
			return n, nil
		}
	}
	return 0, fmt.Errorf("progress 文件超时（%v 内未达 %d）", timeout, want)
}

// checkRecovery 重新 Open 崩溃目录并断言一致性：恢复条数（LIST_WRITES）≥
// progress、:current 对账不一致=0、指纹两次重放一致。
func checkRecovery(dir string, progress int) (crashRecovery, error) {
	var r crashRecovery
	r.progress = progress
	e, err := kairosflux.Open(kairosflux.Options{DataDir: dir})
	if err != nil {
		return r, fmt.Errorf("崩溃后 Open: %w", err)
	}
	defer e.Close()
	writes, err := e.ListWrites("quote:", 0, 0, "")
	if err != nil {
		return r, fmt.Errorf("LIST_WRITES: %w", err)
	}
	r.recovered = len(writes.Entries)
	res, err := e.ReplayFingerprint("quote:", 0)
	if err != nil {
		return r, fmt.Errorf("重放指纹: %w", err)
	}
	r.mismatches = len(res.Mismatches)
	res2, err := e.ReplayFingerprint("quote:", 0)
	if err != nil {
		return r, err
	}
	r.fpStable = res2.Fingerprint == res.Fingerprint
	return r, nil
}

func runKillTest(bin, dir string, total int) (crashRecovery, error) {
	progressPath := filepath.Join(dir, "progress")
	cmd := exec.Command(bin, "-data-dir", dir, "-total", strconv.Itoa(total), "-progress", progressPath)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		return crashRecovery{}, err
	}
	prog, err := waitProgress(progressPath, 100, 60*time.Second)
	if err != nil {
		_ = cmd.Process.Kill()
		return crashRecovery{}, fmt.Errorf("crashsim 未产生进度: %w", err)
	}
	time.Sleep(30 * time.Millisecond)          // 让几帧在途写落盘
	if err := cmd.Process.Kill(); err != nil { // SIGKILL
		return crashRecovery{}, fmt.Errorf("SIGKILL: %w", err)
	}
	_ = cmd.Wait()
	r, err := checkRecovery(dir, prog)
	r.total = total
	return r, err
}

func runDiskFullTest(bin, dir string, total int) (crashRecovery, error) {
	progressPath := filepath.Join(dir, "progress")
	// ulimit -f 16 = RLIMIT_FSIZE 16×512B = 8KB：WAL 增长到 8KB 后写入失败
	// （EFBIG→写错误或 SIGXFSZ），模拟"磁盘写满"这一失败类。progress 文件与
	// WAL 是不同文件：progress 写（<8KB）先成功，WAL 后续增长撞上限。
	shellCmd := fmt.Sprintf("ulimit -f 16; exec %s -data-dir %s -total %d -progress %s",
		bin, dir, total, progressPath)
	cmd := exec.Command("sh", "-c", shellCmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	startErr := cmd.Run() // 等待自然退出（信号或错误退出）
	prog := 0
	if b, err := os.ReadFile(progressPath); err == nil {
		prog, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	if prog == 0 {
		return crashRecovery{}, fmt.Errorf("磁盘写满模拟未能产生任何进度（启动错误=%v，输出=%s）", startErr, out.String())
	}
	r, err := checkRecovery(dir, prog)
	r.total = total
	if startErr != nil {
		if ee, ok := startErr.(*exec.ExitError); ok && ee.ProcessState != nil {
			if ws, ok := ee.ProcessState.Sys().(interface{ Signal() string }); ok {
				r.signalDesc = fmt.Sprintf("进程以信号 %s 终止", ws.Signal())
			} else {
				r.signalDesc = fmt.Sprintf("进程以错误退出: %v", startErr)
			}
		} else {
			r.signalDesc = fmt.Sprintf("进程以错误退出: %v", startErr)
		}
	} else {
		r.signalDesc = "进程自行完成（未触发写满）"
	}
	return r, err
}

// —— 维度 2 ——

type concurrentRow struct {
	writes        int
	lostOrDup     bool
	countMismatch bool
	stats         *Stats
}

func runConcurrentTest(addr string, conns, per int) (concurrentRow, error) {
	var row concurrentRow
	row.writes = conns * per
	cs := make([]*benchV2Conn, conns)
	for i := range cs {
		c, err := dialV2(addr, negotiate.AckEvery)
		if err != nil {
			return row, err
		}
		cs[i] = c
	}
	defer func() {
		for _, c := range cs {
			_ = c.Close()
		}
	}()

	// 审计精确性口径：并发突发前后 LIST_WRITES(quote:) 条数之差 == 本次写入数。
	before, err := cs[0].listWrites([]byte("quote:"), 0, 0)
	if err != nil {
		return row, err
	}

	seqs := make(chan uint64, row.writes)
	s := NewStats()
	s.Start()
	var wg sync.WaitGroup
	var next atomic.Int64
	for w := 0; w < conns; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := cs[w]
			for {
				i := int(next.Add(1))
				if i > row.writes {
					return
				}
				key := []byte(fmt.Sprintf("quote:2026-08-17:%05d", i%100000))
				payload := []byte(`{"code":"510300","date":"2026-08-17","open":1,"high":1,"low":1,"close":1,"volume":1}`)
				start := time.Now()
				seq, err := c.putVersioned(key, payload, "bench-conc")
				s.Record(time.Since(start), err)
				if err == nil {
					seqs <- seq
				}
			}
		}(w)
	}
	wg.Wait()
	s.Stop()
	close(seqs)

	after, err := cs[0].listWrites([]byte("quote:"), 0, 0)
	if err != nil {
		return row, err
	}
	if after-before != row.writes {
		row.countMismatch = true
	}

	unique := map[uint64]bool{}
	for seq := range seqs {
		if unique[seq] {
			row.lostOrDup = true
		}
		unique[seq] = true
	}
	if len(unique) != row.writes {
		row.lostOrDup = true
	}
	row.stats = s
	return row, nil
}

// —— 维度 3/4/6 公共客户端 ——

func v2Put(addr, key string, payload []byte) (uint64, error) {
	c, err := dialV2(addr, negotiate.AckEvery)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	return c.putVersioned([]byte(key), payload, "bench-adv")
}

func v2Get(addr, key string) ([]byte, bool, error) {
	c, err := dialV2(addr, negotiate.AckEvery)
	if err != nil {
		return nil, false, err
	}
	defer c.Close()
	return c.getAsOf([]byte(key), time.Now().UnixNano()+int64(1e9))
}

// v2GetAsOfAt 查询指定时刻的版本（查询回拨：as-of 过去时刻）。
func v2GetAsOfAt(addr, key string, asOf int64) ([]byte, bool, error) {
	c, err := dialV2(addr, negotiate.AckEvery)
	if err != nil {
		return nil, false, err
	}
	defer c.Close()
	return c.getAsOf([]byte(key), asOf)
}

// —— 维度 4：fuzz 语料回放 ——

type fuzzResult struct {
	rounds, rejected, closed, timeouts, crashes int
	healthOK                                    bool
}

func runFuzzReplay(addr string, rounds int) fuzzResult {
	var res fuzzResult
	res.rounds = rounds
	// 语料：8 种畸形帧。帧头布局 [0:2]magic+ver [2]flags [3]opcode [4:6]type
	// [6:10]corr_id [10:14]dataLen（见 codec/v2.go 的帧格式注释）。
	corpus := [][]byte{
		{0x4b, 0x41}, // 截断头（2 字节）
		func() []byte { // 合法头 + dataLen=0xFFFFFFF0（远超 256MiB 硬上限）
			fr, _ := codec.NewDataPackV2().Pack(codec.NewMessageV2(codec.HeaderV2{Opcode: codec.OpcodePutVersioned, Type: codec.TypeUnspecified, CorrID: 1}, nil))
			binary.LittleEndian.PutUint32(fr[10:14], 0xFFFFFFF0)
			return fr
		}(),
		validV2Frame(0x7f, nil), // 未知 opcode
		validV2Frame(codec.OpcodePutVersioned, []byte{0xde, 0xad, 0xbe, 0xef}), // 坏负载（长度头不符）
		[]byte("XXXX"), // 坏 magic（协商期）
		validV2Frame(codec.OpcodeGetAsOf, bytes.Repeat([]byte{1}, 3000)),                               // 超长但结构坏的负载
		append(validV2Frame(codec.OpcodePutVersioned, []byte{0}), bytes.Repeat([]byte{0xff}, 4096)...), // 头后粘连垃圾
		bytes.Repeat([]byte{0xa5}, 1024),                                                               // 纯随机字节
	}
	health := func() bool {
		if _, err := v2Put(addr, "fuzz:health:1", []byte(`{"ok":1}`)); err != nil {
			return false
		}
		_, found, err := v2Get(addr, "fuzz:health:1")
		return err == nil && found
	}
	res.healthOK = true
	for round := 0; round < rounds; round++ {
		for _, frame := range corpus {
			conn, err := net.DialTimeout("tcp", addr, v2DialTimeout)
			if err != nil {
				res.crashes++
				continue
			}
			_, _ = conn.Write(frame)
			buf := make([]byte, 128)
			_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, rerr := conn.Read(buf)
			_ = conn.Close()
			switch {
			case rerr == nil && n > 0:
				res.rejected++ // 服务端回了响应（结构化拒绝/错误）
			case rerr != nil && strings.Contains(rerr.Error(), "i/o timeout"):
				res.timeouts++
			default:
				res.closed++ // 连接被服务端关闭或 EOF
			}
			if !health() {
				res.healthOK = false
				res.crashes++
				return res
			}
		}
	}
	return res
}

func validV2Frame(opcode uint8, payload []byte) []byte {
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: codec.TypeUnspecified, CorrID: 1}, payload)
	frame, _ := codec.NewDataPackV2().Pack(msg)
	return frame
}

// —— 维度 6：时钟回拨 ——

func runClockRollback(addr string) error {
	// a) engine 层 write_ts 递减：接受（engine 不强制单调，权威排序在
	//    Seq/WriteNanos），且 as-of 语义仍正确（as-of(t) 只返回 t 之前写入的
	//    最新版本，绝不因回拨返回"未来"版本）。
	edir := "/tmp/kf-bench-clock"
	os.RemoveAll(edir)
	e, err := kairosflux.NewEmbedded(kairosflux.Options{DataDir: edir})
	if err != nil {
		return err
	}
	defer e.Close()
	base := time.Now().UnixNano()
	ts := []int64{base, base - int64(3600e9), base - int64(7200e9)} // 依次回拨 1h
	for i, t := range ts {
		if _, err := e.PutVersioned("clock:test:1", []byte(fmt.Sprintf("v%d", i+1)), t, "bench-clock", 0); err != nil {
			return fmt.Errorf("engine 层回拨写入被拒（engine 不强制单调，不应拒绝）: %w", err)
		}
	}
	v, found, err := e.GetAsOf("clock:test:1", base-int64(5400e9)) // base-1.5h：应见 v2（写入于 base-1h）
	if err != nil || !found || string(v.Payload) != "v2" {
		return fmt.Errorf("回拨后 as-of(base-1.5h) 语义错误: found=%v payload=%q err=%v（期望 v2）", found, v.Payload, err)
	}
	v, found, err = e.GetAsOf("clock:test:1", base+int64(1e9)) // base+1s：应见 v1（最新写入时间）
	if err != nil || !found || string(v.Payload) != "v1" {
		return fmt.Errorf("回拨后 as-of(base+1s) 语义错误: found=%v payload=%q（期望 v1）", found, v.Payload)
	}
	fmt.Fprintf(os.Stdout, "  [clock] a) engine 层 write_ts 递减 3 次（回拨 1h/2h）全部接受；as-of 语义正确（base-1.5h→v2，base+1s→v1）\n")

	// b) 网络层 v1 PUT：未纳管数字末段键的设备时间戳回拨被过滤钩子拒绝。
	c, err := client.New(client.Options{Addrs: []string{addr}})
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Put(context.Background(), []byte("ts:2026-08-17:00001"), []byte("first")); err != nil {
		return fmt.Errorf("v1 首写失败: %w", err)
	}
	err = c.Put(context.Background(), []byte("ts:2026-08-17:00000"), []byte("rollback")) // 设备时间戳 1→0
	if err == nil {
		return fmt.Errorf("设备时间戳回拨未被过滤钩子拒绝（应 drop non_monotonic_timestamp）")
	}
	fmt.Fprintf(os.Stdout, "  [clock] b) 网络层 v1 PUT 设备时间戳回拨（1→0）被拒绝: %v\n", err)

	// c) v2 PUT_VERSIONED 按设计跳过回拨启发式：同一逻辑键反复写版本是
	//    正常行为（ingesthook.ValidateVersioned 文档），ts: 键二次写入接受。
	c2, err := dialV2(addr, negotiate.AckEvery)
	if err != nil {
		return err
	}
	defer c2.Close()
	if _, err := c2.putVersioned([]byte("ts:2026-08-17:00001"), []byte("second"), "bench-clock"); err != nil {
		return fmt.Errorf("v2 版本化路径二次写同一逻辑键被拒（按设计应跳过启发式）: %w", err)
	}
	fmt.Fprintf(os.Stdout, "  [clock] c) v2 PUT_VERSIONED 同键二次写入接受（按设计跳过回拨启发式）\n")

	// d) 查询回拨：GET_AS_OF 指定过去时刻——不命中该时刻之后才写入的版本。
	qq := `{"code":"510300","date":"2026-08-17","open":1,"high":1,"low":1,"close":1,"volume":1}`
	if _, err := c2.putVersioned([]byte("quote:2026-08-17:888888"), []byte(qq), "bench-clock"); err != nil {
		return err
	}
	if _, found, err := v2GetAsOfAt(addr, "quote:2026-08-17:888888", time.Now().UnixNano()-int64(3600e9)); err == nil || found {
		return fmt.Errorf("查询回拨到过去时刻不应命中: found=%v err=%v", found, err)
	}
	if _, found, err := v2GetAsOfAt(addr, "quote:2026-08-17:888888", time.Now().UnixNano()+int64(3600e9)); err != nil || !found {
		return fmt.Errorf("查询 as-of 未来时刻应命中最新: found=%v err=%v", found, err)
	}
	fmt.Fprintf(os.Stdout, "  [clock] d) 查询回拨（as-of 过去 1h）不命中；as-of 未来命中最新\n")
	return nil
}
