package config

import (
	"encoding/json"
	"log/slog"
	"os"
)

// Option 是函数式配置选项：在默认配置上原地施加一处改动。用 New(opts...) 组合。
//
// 相比手撸 flag/直接改全局，函数式选项是 Go 惯用法：可读、可组合、易测——
//
//	cfg := config.New(config.WithSSTablePath(tmp), config.WithShardCount(4))
//
// 「读配置文件」「解析命令行/环境变量」本身也是选项（FromJSONFile / FromEnvAndFlags），
// 与字段选项一样可自由组合，从而把加载来源与加载顺序显式化。
type Option func(*GlobalConfig)

// New 在纯代码默认值上依次施加 opts，返回配置。opts 为空即返回默认配置。
func New(opts ...Option) *GlobalConfig {
	c := defaultGlobalConfig()
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FromJSONFile 依次尝试 paths，命中第一个存在的文件并反序列化覆盖到配置；都不存在则保持默认。
func FromJSONFile(paths ...string) Option {
	return func(c *GlobalConfig) {
		var data []byte
		var err error
		for _, p := range paths {
			if data, err = os.ReadFile(p); err == nil {
				slog.Info("config file found", "path", p)
				break
			}
		}
		if err != nil {
			slog.Warn("no config file found, using defaults")
			return
		}
		if err := json.Unmarshal(data, c); err != nil {
			slog.Error("failed to parse config file", "error", err)
			return
		}
		slog.Info("config file loaded")
	}
}

// FromEnvAndFlags 应用命令行(-me)/环境变量(RAFT_ME)覆盖，归一化运行模式并校验（非法则 panic）。
// 置于文件选项之后，实现「默认 → 文件 → 命令行/环境」的覆盖优先级。
func FromEnvAndFlags() Option {
	return func(c *GlobalConfig) { c.ParseFlags() }
}

// —— 字段选项：常用可调项的显式 setter，供测试与程序化构造使用。 ——

func WithName(name string) Option     { return func(c *GlobalConfig) { c.Name = name } }
func WithHost(host string) Option     { return func(c *GlobalConfig) { c.Host = host } }
func WithPort(port int) Option        { return func(c *GlobalConfig) { c.Port = port } }
func WithMode(mode string) Option     { return func(c *GlobalConfig) { c.Mode = mode } }
func WithMe(me int) Option            { return func(c *GlobalConfig) { c.Me = me } }
func WithPeers(peers []string) Option { return func(c *GlobalConfig) { c.Peers = peers } }

func WithWALPath(p string) Option     { return func(c *GlobalConfig) { c.WALPath = p } }
func WithSSTablePath(p string) Option { return func(c *GlobalConfig) { c.SSTablePath = p } }
func WithMaxMemTableSize(n int) Option {
	return func(c *GlobalConfig) { c.MaxMemTableSize = n }
}
func WithMaxCompactionSize(n int) Option {
	return func(c *GlobalConfig) { c.MaxCompactionSize = n }
}

func WithShardCount(n int) Option { return func(c *GlobalConfig) { c.ShardCount = n } }
func WithVNodes(n int) Option     { return func(c *GlobalConfig) { c.VNodes = n } }
