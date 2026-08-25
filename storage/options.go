package storage

import "github.com/ChronoBrew/KairosFlux/config"

// Options 是存储引擎的全部可调参数，构造时一次性传入。
//
// 之所以显式传参而非在包内读全局 config.G：全局配置让同一进程内无法并存两套不同配置的
// 引擎（多节点集成测试正需要），也让测试之间经由全局变量互相影响——本包此前正因如此
// 出现过偶发失败。它还迫使生产代码写防御性代码：构造时把配置「快照」一份，以避开与测试
// 中并发改配置形成的数据竞争。参数一旦由调用方传入，这些问题都不复存在。
type Options struct {
	// Dir 是 SSTable 文件目录。
	Dir string

	// MaxMemTableSize 是 active 表的条目数阈值，超过即触发 flush。
	MaxMemTableSize int

	// MaxCompactionSize 是单层文件数阈值，达到即触发该层 compaction。
	MaxCompactionSize int

	// MaxInflightBytes 是未 flush 数据（active + 正在 flush 的 dirty）的字节预算，
	// 超出即阻塞写入等待 flush 归还信用。<=0 关闭背压。
	MaxInflightBytes int64

	// BlockCacheBytes 是 SSTable 数据块缓存的字节预算。<=0 关闭缓存。
	BlockCacheBytes int64

	// SkipListMaxLevel 与 SkipListP 是跳表的最大层高与升层概率。
	SkipListMaxLevel int
	SkipListP        float64
}

// DefaultOptions 从全局配置取一份参数。
//
// 这是全局配置进入存储层的唯一入口：调用方在构造时读一次，此后引擎只认自己那份参数，
// 不再受 config.G 后续变动影响。
func DefaultOptions() Options {
	return OptionsFromConfig(config.G)
}

// OptionsFromConfig 从一份显式配置构造 Options——与 DefaultOptions 的唯一区别
// 是数据来源（显式 cfg 而不是包级 config.G）。kairosflux 引擎
// （service/kairosflux.go）为每个实例使用独立数据目录构造存储时走这里，
// 与 DefaultOptions 保持同一份字段映射，不另抄一遍。
func OptionsFromConfig(cfg *config.GlobalConfig) Options {
	return Options{
		Dir:               cfg.SSTablePath,
		MaxMemTableSize:   cfg.MaxMemTableSize,
		MaxCompactionSize: cfg.MaxCompactionSize,
		MaxInflightBytes:  cfg.MemTableMaxInflightBytes,
		BlockCacheBytes:   cfg.BlockCacheBytes,
		SkipListMaxLevel:  cfg.MaxMemTableLevel,
		SkipListP:         cfg.MaxMemTableP,
	}
}

// withDefaults 补齐零值字段，使 Options{Dir: dir} 这样的部分构造也可用。
func (o Options) withDefaults() Options {
	if o.MaxMemTableSize <= 0 {
		o.MaxMemTableSize = 1024
	}
	if o.MaxCompactionSize <= 0 {
		o.MaxCompactionSize = 4
	}
	if o.SkipListMaxLevel <= 0 {
		o.SkipListMaxLevel = 32
	}
	if o.SkipListP <= 0 || o.SkipListP >= 1 {
		o.SkipListP = 0.5
	}
	return o
}
