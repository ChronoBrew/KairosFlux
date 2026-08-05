package config

import (
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/NeverENG/BanDB/network/banIface"
)

// 运行模式取值
const (
	ModeStandalone = "standalone" // 单机：写经存储层 WAL，不启动 Raft
	ModeRaft       = "raft"       // 集群：写经 Raft 日志
)

type GlobalConfig struct {
	Name    string
	Port    int
	Host    string
	Version string

	WALPath           string
	SSTablePath       string
	MaxMemTableSize   int
	MaxCompactionSize int

	// MemTableMaxInflightBytes 未刷盘数据（active + 正在刷的 dirty）的字节预算。
	// 字节级令牌桶背压：超出预算时写入阻塞，等 flush 归还信用。<=0 关闭背压。
	MemTableMaxInflightBytes int64

	TcpServer      banIface.IServer
	MaxConn        int
	MaxPackageSize uint32

	WorkerPoolSize   uint32
	MaxWorkerTaskLen uint32
	MaxMsgChanLen    uint32

	MaxMemTableP     float64
	MaxMemTableLevel int

	// Mode 运行模式："standalone"（单机 WAL，不启动 Raft）或 "raft"（集群，写经 Raft 日志）。
	// 留空时按 len(Peers) 推断：1→standalone，>1→raft。
	Mode string

	// Raft 集群配置
	Peers []string // 集群中所有节点的地址
	Me    int      // 当前节点在 Peers 中的索引（0-based）
	// Raft 快照配置
	RaftSnapshotThreshold   int // 触发快照的日志数量阈值
	RaftSnapshotKeepEntries int // 快照后保留的日志条目数

	// 下游投递（delivery）配置：默认关闭，不影响既有写/读路径。
	DeliveryEnabled     bool   // 是否启动下游投递循环
	DeliveryFilePath    string // FileSink 的 JSONL 输出路径
	DeliveryBatchSize   int    // 每批投递的记录数
	DeliveryIntervalMs  int    // 投递轮询间隔（毫秒）
	DeliveryExactlyOnce bool   // 用幂等 sink（按 key HWM 去重）达 effectively-once；关则 at-least-once

	// 分片集群（cluster）配置：骨架期供路由/放置控制面使用，默认单分片。
	ShardCount int // 分片数（ShardOf 取模基数）
	VNodes     int // 一致性哈希每节点的虚拟节点数

	// ShardRoutingEnabled 开启分片路由：不属本节点的 key 经 BanNet 转发到 owner。
	// 默认关闭（单机行为不变）。开启时 Peers 视为各节点的 BanNet 地址，Peers[Me] 须为本节点
	// 监听地址（Host:Port）。
	ShardRoutingEnabled bool
}

func (g *GlobalConfig) Init() {
	// 尝试多个可能的路径
	paths := []string{
		"config/config.json",       // 从项目根目录运行
		"../config/config.json",    // 从 cmd/Server 或 cmd/client 运行
		"../../config/config.json", // 从更深层目录运行
		"config.json",              // 当前目录
	}

	var data []byte
	var err error

	for _, path := range paths {
		data, err = os.ReadFile(path)
		if err == nil {
			slog.Info("config file found", "path", path)
			break
		}
	}

	if err != nil {
		slog.Error("failed to read config", "error", err)
		slog.Warn("falling back to default config")
		return // 使用默认配置，不退出
	}

	err = json.Unmarshal(data, g)
	if err != nil {
		slog.Error("failed to parse config", "error", err)
		return
	}

	slog.Info("config initialized")
}

// defaultGlobalConfig 返回纯代码默认值，不读取配置文件、不解析命令行。
// NewGlobalConfig 在其上叠加 Init()(文件覆盖) 与 ParseFlags()(命令行覆盖)。
func defaultGlobalConfig() *GlobalConfig {
	logDir := defaultLogDir()
	return &GlobalConfig{

		Name:                     "Raft",
		Port:                     8080,
		Host:                     "localhost",
		Version:                  "1.0.0",
		MaxConn:                  1000,
		MaxPackageSize:           16 << 20, // 16MiB：容纳多模态大值(相机帧)与多条 SCAN 响应
		WorkerPoolSize:           10,
		MaxWorkerTaskLen:         10000,
		MaxMsgChanLen:            100,
		TcpServer:                nil,
		MaxMemTableP:             0.5,
		MaxMemTableLevel:         32,
		MaxMemTableSize:          1024,
		MemTableMaxInflightBytes: 64 << 20, // 64MiB 未刷盘字节预算
		WALPath:                  filepath.Join(logDir, "wal.log"),
		SSTablePath:              logDir,
		Peers:                    []string{"localhost:8080"}, // 默认单节点
		Me:                       0,                          // 默认节点ID
		RaftSnapshotThreshold:    1000,                       // 默认快照阈值
		RaftSnapshotKeepEntries:  100,                        // 默认保留条目数
		DeliveryEnabled:          false, // 默认不启动下游投递
		DeliveryFilePath:         filepath.Join(logDir, "delivery.jsonl"),
		DeliveryBatchSize:        100,
		DeliveryIntervalMs:       1000,
		DeliveryExactlyOnce:      true, // 启用投递时默认走幂等 sink

		ShardCount:               1,   // 默认单分片
		VNodes:                   128, // 一致性哈希默认虚拟节点数
		ShardRoutingEnabled:      false,
	}
}

func NewGlobalConfig() *GlobalConfig {
	global := defaultGlobalConfig()
	global.Init()
	global.ParseFlags()
	return global
}

func defaultLogDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return "log"
	}

	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, "log")
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}

	return "log"
}

// ParseFlags 解析命令行参数
func (g *GlobalConfig) ParseFlags() {
	// 创建一个新的 FlagSet，避免与全局的 CommandLine 冲突
	fs := flag.NewFlagSet("bandb", flag.ContinueOnError)
	fs.Usage = func() {}

	// 定义命令行参数
	meFlag := fs.Int("me", -1, "Current node index in peers list")

	// 解析命令行参数，忽略未定义的参数
	err := fs.Parse(meFlagArgs(os.Args[1:]))
	if err != nil {
		// 忽略错误，继续执行
	}

	// 处理命令行参数
	if *meFlag >= 0 {
		g.Me = *meFlag
		slog.Info("me set via flag", "me", g.Me)
	}

	// 处理环境变量（优先级低于命令行参数）
	if g.Me < 0 {
		if meEnv := os.Getenv("RAFT_ME"); meEnv != "" {
			if meInt, err := strconv.Atoi(meEnv); err == nil {
				g.Me = meInt
				slog.Info("me set via env", "me", g.Me)
			}
		}
	}

	g.resolveMode()

	// standalone 不启动 Raft，无需 Me/Peers 校验
	if g.Mode == ModeStandalone {
		slog.Info("config finalized", "mode", g.Mode)
		return
	}

	// 验证配置
	if g.Me < 0 || g.Me >= len(g.Peers) {
		slog.Error("invalid me value", "me", g.Me, "peers_len", len(g.Peers))
		panic("invalid me value")
	}

	slog.Info("config finalized", "mode", g.Mode, "peers", g.Peers, "me", g.Me)
}

// resolveMode 归一化运行模式：显式取值优先，留空时按 Peers 数量推断。
func (g *GlobalConfig) resolveMode() {
	switch g.Mode {
	case ModeStandalone, ModeRaft:
		// 显式指定，尊重之
	case "":
		if len(g.Peers) > 1 {
			g.Mode = ModeRaft
		} else {
			g.Mode = ModeStandalone
		}
	default:
		slog.Error("invalid mode", "mode", g.Mode)
		panic("invalid mode: " + g.Mode)
	}
}

func meFlagArgs(args []string) []string {
	filtered := make([]string, 0, 2)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-me" {
			filtered = append(filtered, arg)
			if i+1 < len(args) {
				filtered = append(filtered, args[i+1])
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "-me=") {
			filtered = append(filtered, arg)
		}
	}
	return filtered
}

var G = NewGlobalConfig()
