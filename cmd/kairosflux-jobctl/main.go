// kairosflux-jobctl 是 M3 声明式 Job 控制面的独立命令行入口（docs/方案-
// BanDB-时态内核与AI数据平面.md §M3）。独立 cmd 入口、不改任何既有路由
// 注册——本二进制只是 internal/jobctl 的一个瘦壳，对一个已经在跑的
// KairosFlux v2 服务端发起 PUT_VERSIONED/GET_AS_OF 请求。
//
// 子命令：
//
//	apply <spec.json>              把一份 JSON job spec 写入 job:spec:{name}
//	tick <name...>                 对给定的 job 名先跑启动恢复扫描、再跑一次
//	                                reconcile（读 spec、执行/判断、写
//	                                status+events）
//	run <name...>                  常驻 reconcile loop，按 -tick-period 周期跑 tick
//	import-registry <jsonl路径>    一次性把 QuantBrew 的 registry.jsonl
//	                                导入 strategy:index 对象索引（M31）
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ChronoBrew/KairosFlux/internal/jobctl"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage()
		return 2
	}

	fs := flag.NewFlagSet("kairosflux-jobctl", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9000", "KairosFlux v2 服务端地址")
	timeout := fs.Duration("timeout", 5*time.Second, "单次请求超时")
	tickPeriod := fs.Duration("tick-period", 60*time.Second, "run 子命令的 reconcile 周期")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	rest := fs.Args()

	switch args[0] {
	case "apply":
		return cmdApply(*addr, *timeout, rest)
	case "tick":
		return cmdTick(*addr, *timeout, rest)
	case "run":
		return cmdRun(*addr, *timeout, *tickPeriod, rest)
	case "import-registry":
		return cmdImportRegistry(*addr, *timeout, rest)
	default:
		printUsage()
		return 2
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "用法: kairosflux-jobctl [-addr host:port] [-timeout d] <apply|tick|run|import-registry> ...")
}

func newStore(addr string, timeout time.Duration) *jobctl.V2Store {
	return jobctl.NewV2Store(addr, timeout)
}

func cmdApply(addr string, timeout time.Duration, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "用法: kairosflux-jobctl apply <spec.json>")
		return 2
	}
	raw, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取 spec 文件失败: %v\n", err)
		return 1
	}
	spec, err := jobctl.ParseJobSpec(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "spec 校验失败: %v\n", err)
		return 1
	}
	store := newStore(addr, timeout)
	defer store.Close()
	seq, err := jobctl.Apply(store, spec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "apply 失败: %v\n", err)
		return 1
	}
	fmt.Printf("已 apply job=%s，job:spec:%s 新版本 seq=%d\n", spec.Name, spec.Name, seq)
	return 0
}

func cmdTick(addr string, timeout time.Duration, names []string) int {
	if len(names) == 0 {
		fmt.Fprintln(os.Stderr, "用法: kairosflux-jobctl tick <job名...>")
		return 2
	}
	store := newStore(addr, timeout)
	defer store.Close()
	loop := jobctl.NewLoop(store, jobctl.NewReconciler(store), names, 0)
	// 先跑启动恢复扫描（崩溃遗留的状态修复，只写 status），再正常 tick——
	// 与 run 子命令的 Loop.Run 行为一致（Run 内部也是先 Recover 再进循环）。
	errs := loop.Recover()
	errs = append(errs, loop.Tick()...)
	for _, e := range errs {
		fmt.Fprintf(os.Stderr, "错误: %v\n", e)
	}
	if len(errs) > 0 {
		return 1
	}
	return 0
}

func cmdRun(addr string, timeout, tickPeriod time.Duration, names []string) int {
	if len(names) == 0 {
		fmt.Fprintln(os.Stderr, "用法: kairosflux-jobctl run <job名...>")
		return 2
	}
	store := newStore(addr, timeout)
	defer store.Close()
	loop := jobctl.NewLoop(store, jobctl.NewReconciler(store), names, tickPeriod)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	loop.Run(ctx, func(errs []jobctl.TickError) {
		for _, e := range errs {
			log.Printf("jobctl tick 错误: %v", e)
		}
	})
	return 0
}

func cmdImportRegistry(addr string, timeout time.Duration, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "用法: kairosflux-jobctl import-registry <registry.jsonl路径>")
		return 2
	}
	f, err := os.Open(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开 registry 文件失败: %v\n", err)
		return 1
	}
	defer f.Close()

	store := newStore(addr, timeout)
	defer store.Close()
	importer := &jobctl.RegistryImporter{Store: store}
	result, err := importer.ImportReader(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "导入失败: %v\n", err)
		return 1
	}
	fmt.Printf("总行数=%d 新导入=%d 未变化=%d 错误=%d\n",
		result.TotalLines, result.Imported, result.Unchanged, len(result.Errors))
	for _, e := range result.Errors {
		fmt.Fprintf(os.Stderr, "  第 %d 行: %s\n", e.Line, e.Reason)
	}
	if len(result.Errors) > 0 {
		return 1
	}
	return 0
}
