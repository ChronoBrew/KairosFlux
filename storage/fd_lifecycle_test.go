package storage

// fd_lifecycle_test.go：读路径句柄生命周期的回归守护（任务 #52）。
//
// 背景：100w 档（2963 张 SSTable）8 并发 SCAN 时引擎文件句柄耗尽（EMFILE，
// "scan: skip unreadable sstable ... open: too many open files"，扫描跳过全部表）；
// 10w 档（约 300 张表）同路径正常。根因不是「句柄只增不减」的积累，而是瞬态峰值
// 失控：每次扫描为每张表各 open 一个新句柄并持有到归并结束，峰值 = 表数 × (1+并发)。
// 2963 × 9 ≈ 2.7 万，超过 macOS 内核每进程真实上限 kern.maxfilesperproc=10240
// （ulimit -n 报 1048576 是假象，内核上限才是硬约束）；300 × 9 ≈ 2700 安全。
//
// 修复：迭代器不再拥有文件——经 openRef 复用 fdCache 常驻共享句柄（ReadAt 驱动，
// 不依赖文件内偏移，任意并发读者可同读一个句柄），Close 时 closeRef 释放；删除
// 路径（closeFile）标记 removed，待最后一个引用释放才真正 close。进程句柄峰值
// 由表数界定，与并发扫描数无关。

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
)

// testFDCount 返回进程当前打开的文件描述符数。经 /dev/fd 计数（macOS 与 Linux
// 均有；Linux 下是 /proc/self/fd 的符号链接）。取不到（如无 /dev/fd 的环境）返回 -1，
// 调用方按「不可测」跳过而非失败。
func testFDCount() int {
	f, err := os.Open("/dev/fd")
	if err != nil {
		return -1
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return -1
	}
	return len(names)
}

// testMaxTables 根据进程句柄上限选取表数：目标是「并发 8 扫描时峰值超出上限」
// （修复前必现 EMFILE），同时常驻句柄本身不超上限。上限取不到时用 3000
// （100w 档同规模）。容差逻辑：本机 macOS 上 Getrlimit 报 1048576，但内核真实
// 上限 10240——表数取 min(3000, limit/8) 在这台机器上仍是 3000，峰值 2.7 万 > 10240，
// 回归可测；Linux CI（默认 1024）取 512，峰值 4608 > 1024，同样可测。
func testMaxTables(t *testing.T) int {
	const want = 3000
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err == nil && lim.Cur > 0 {
		if int(lim.Cur) < want*2 { // 环境上限比 100w 档规模小：按上限的一半取
			n := int(lim.Cur) / 2
			if n < 256 {
				n = 256
			}
			t.Logf("进程句柄上限 %d 较小，表数降为 %d", lim.Cur, n)
			return n
		}
	}
	return want
}

// TestScanRange_FDStableManyTables 是 #52 的回归测试：多表规模 + 8 并发全量扫描，
// 断言 (1) 每次扫描结果完整（修复前 EMFILE 后跳过全部表、结果为 0）；(2) 扫描
// 期间句柄峰值不抬升（修复后并发扫描不再新增句柄）；(3) 扫描结束后句柄数回落。
//
// 句柄计数容差 32：允许测试框架自身（网络/日志/temp 目录）的零星波动，不允许
// 量级增长——修复前并发扫描阶段句柄数会冲破上限并 EMFILE。
func TestScanRange_FDStableManyTables(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过（写盘 3000 张表较慢）")
	}
	base := testFDCount()
	if base < 0 {
		t.Skip("无 /dev/fd，句柄计数不可用")
	}

	dir := t.TempDir()
	e := NewEngine(Options{
		Dir:               dir,
		MaxMemTableSize:   1 << 30, // 全量直接落表，不经过内存表
		MaxCompactionSize: 1 << 30, // 关闭 compaction，隔离本测试关注的路径
	})
	defer e.Close()

	// 直写 SSTable（不经 WAL/memtable），快速构造多表规模。
	tables := testMaxTables(t)
	const perTable = 64
	for b := 0; b < tables; b++ {
		entries := make([]LogEntry, 0, perTable)
		for i := 0; i < perTable; i++ {
			k := fmt.Sprintf("quote:2026-08-17:%07d", b*perTable+i)
			entries = append(entries, LogEntry{Key: []byte(k), Value: []byte(`{"code":"x","open":1,"high":2,"low":1,"close":1.5,"volume":1}`)})
		}
		if err := e.sst.WriteToSSTable(entries); err != nil {
			t.Fatal(err)
		}
	}
	metas := e.sst.Metas()
	if len(metas) != tables {
		t.Fatalf("want %d sstables, got %d", tables, len(metas))
	}
	// 预热块索引/布隆并让常驻句柄进缓存（模拟 GET_AS_OF 之后的驻留态）。
	for i := 0; i < len(metas); i++ {
		e.getFromSSTables([]byte(fmt.Sprintf("quote:2026-08-17:%07d", i*perTable)))
	}
	baseline := testFDCount()
	if baseline > base+32 { // 常驻句柄数=表数+少量，量级校验
		t.Logf("baseline fd=%d（表数 %d，进程基底 %d）", baseline, tables, base)
	}
	if baseline >= 0 && baseline-base > tables+64 {
		t.Fatalf("常驻句柄异常: base=%d baseline=%d（表数 %d）", base, baseline, tables)
	}

	// 并发全量扫描：修复前此阶段 EMFILE、扫描全跳过；修复后零新增句柄。
	const workers = 8
	const rounds = 10
	expected := tables * perTable
	var hits [workers * rounds]int
	var wg sync.WaitGroup
	// 扫描期间的句柄峰值抽样：修复前并发扫描持有 表数×并发 个瞬态句柄，峰值超限；
	// 修复后与 baseline 持平。
	var peak int32
	stopSampling := make(chan struct{})
	var sampleWg sync.WaitGroup
	sampleWg.Add(1)
	go func() {
		defer sampleWg.Done()
		for {
			select {
			case <-stopSampling:
				return
			default:
				if n := testFDCount(); int32(n) > peak {
					peak = int32(n)
				}
			}
		}
	}()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				n := 0
				e.ScanRange(nil, nil, func(k, v []byte) bool {
					n++
					return true
				})
				hits[w*rounds+r] = n
			}
		}(w)
	}
	wg.Wait()
	close(stopSampling)
	sampleWg.Wait()

	for i, n := range hits {
		if n != expected {
			t.Errorf("scan %d 命中 %d 条, want %d（修复前 EMFILE 时此值为 0）", i, n, expected)
		}
	}
	if got := int(peak); got > baseline+64 {
		t.Errorf("扫描期间句柄峰值 %d 超出基线 %d 过多（修复前 ≈ 表数×(1+并发)，修复后应持平）", got, baseline)
	}
	after := testFDCount()
	if after > baseline+32 {
		t.Errorf("扫描后句柄数 %d 未回落（基线 %d）", after, baseline)
	}
	t.Logf("tables=%d scans=%d×%d peak=%d baseline=%d after=%d", tables, workers, rounds, peak, baseline, after)
}

// TestScanRange_FDErrorPathNoLeak 强制打开失败的错误路径：删除一张表再扫描，
// openRef 失败 → 该表被跳过（其余表正常），句柄数仍回落。守护「错误路径同关闭」。
func TestScanRange_FDErrorPathNoLeak(t *testing.T) {
	base := testFDCount()
	if base < 0 {
		t.Skip("无 /dev/fd，句柄计数不可用")
	}
	dir := t.TempDir()
	e := NewEngine(Options{
		Dir:               dir,
		MaxMemTableSize:   1 << 30,
		MaxCompactionSize: 1 << 30,
	})
	defer e.Close()

	const tables = 64 // 错误路径不依赖规模，保持轻量
	const perTable = 32
	for b := 0; b < tables; b++ {
		entries := make([]LogEntry, 0, perTable)
		for i := 0; i < perTable; i++ {
			k := fmt.Sprintf("quote:2026-08-17:%05d", b*perTable+i)
			entries = append(entries, LogEntry{Key: []byte(k), Value: []byte("v")})
		}
		if err := e.sst.WriteToSSTable(entries); err != nil {
			t.Fatal(err)
		}
	}
	metas := e.sst.Metas()

	// 预热 + 删除一张表（不更新 metas，模拟「快照里还有、磁盘已删」的窗口）。
	for i := 0; i < len(metas); i++ {
		e.getFromSSTables([]byte(fmt.Sprintf("quote:2026-08-17:%05d", i*perTable)))
	}
	gone := metas[0].Filepath
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	// 完整模拟 DeleteSSTable 的删除序列（os.Remove + closeFile 剔缓存），但不更新
	// metas 快照——保持「快照里还有、磁盘已删」的窗口，正是 openRef 失败的错误路径。
	e.sst.closeFile(gone)
	baseline := testFDCount()

	for r := 0; r < 5; r++ {
		n := 0
		e.ScanRange(nil, nil, func(k, v []byte) bool {
			n++
			return true
		})
		// 每轮扫描都走一遍「一张表打开失败 → 跳过」的错误路径。
		if want := tables*perTable - perTable; n != want {
			t.Errorf("round %d 命中 %d 条, want %d（被删表应跳过、其余完整）", r, n, want)
		}
	}
	after := testFDCount()
	if after > baseline+32 {
		t.Errorf("错误路径扫描后句柄数 %d 未回落（基线 %d）", after, baseline)
	}
}

// TestFDCacheRefLifecycle 直接守护 fdCache 引用计数的核心不变量（#52）：
// 删除路径在迭代器引用释放前不得真正关闭句柄（否则并发扫描读到一半被掐断）；
// 引用归零后句柄关闭、缓存剔除，路径不可再开。
func TestFDCacheRefLifecycle(t *testing.T) {
	base := testFDCount()
	if base < 0 {
		t.Skip("无 /dev/fd，句柄计数不可用")
	}
	dir := t.TempDir()
	ss := NewSSTable(Options{Dir: dir})
	if err := ss.WriteToSSTable([]LogEntry{entry("k1", "v1")}); err != nil {
		t.Fatal(err)
	}
	path := ss.Metas()[0].Filepath

	// 迭代器路径取引用：句柄进缓存。
	f, err := ss.openRef(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := testFDCount(); got != base+1 {
		t.Fatalf("openRef 后句柄数 = %d, want %d（+1）", got, base+1)
	}

	// 删除序列（与 DeleteSSTable 一致）：先 unlink，再 closeFile。此时引用未释放，
	// 句柄不得真正关闭——并发扫描仍可经它读完 unlink 后 inode 上的数据。
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ss.closeFile(path)
	if got := testFDCount(); got != base+1 {
		t.Fatalf("closeFile 后句柄数 = %d, want %d（引用未释放不得关闭）", got, base+1)
	}
	if _, err := f.ReadAt([]byte{0}, 0); err != nil {
		t.Fatalf("未释放的共享句柄应仍可读: %v", err)
	}

	// 引用释放：句柄关闭、缓存剔除。
	ss.closeRef(path)
	if got := testFDCount(); got != base {
		t.Fatalf("closeRef 后句柄数 = %d, want %d（应已关闭）", got, base)
	}

	// 已删除路径不可再开（等价 os.Open ENOENT）。
	if _, err := ss.openRef(path); err == nil {
		t.Fatal("已删除文件的 openRef 应失败")
	}
	if got := testFDCount(); got != base {
		t.Fatalf("openRef 失败后句柄数 = %d, want %d（错误路径不得新增句柄）", got, base)
	}
}
