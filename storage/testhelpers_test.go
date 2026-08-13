package storage

import "testing"

// newBareMemTable 构造一个不启动后台 goroutine 的引擎（供确定性测试直接驱动
// FlushToSSTable/CompactSSTable），参数取自传入的 opts，与生产 NewEngine 一致。
func newBareMemTable(sst *SSTable, opts Options) *Engine {
	opts = opts.withDefaults()
	return &Engine{
		active:        newSkipList(opts.SkipListMaxLevel, opts.SkipListP),
		sst:           sst,
		maxSize:       opts.MaxMemTableSize,
		maxCompaction: opts.MaxCompactionSize,
		opts:          opts,
	}
}

// testOptions 返回一份指向独立临时目录的引擎参数。
//
// 测试一律用它构造引擎，不再改 config.G：全局配置被并发运行的用例与它们各自的后台协程
// 共享，此前正因如此出现过跨用例干扰与偶发失败。参数各自独立后，用例之间不再有隐式耦合。
func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{Dir: t.TempDir()}
}
