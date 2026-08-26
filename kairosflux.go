// Package kairosflux 是 KairosFlux 的可导入双模式引擎 API（顶层包，发布批次
// 阶段 A）：embedded（纯进程内）与 server（同一套 API 的网络壳）共用同一个
// Engine，时态内核五操作 + 审计 + Context/Proposal 访问口在两种模式下逐字节
// 同语义。跨仓调用方（ChronoBrew/ChronoBrew 的 sample 与 E2E 测试）用
// `import kairosflux "github.com/ChronoBrew/KairosFlux"` 即可获得全部能力，
// 不需要 import 任何 internal 包。
//
// 三种构造形态：
//
//   - NewEmbedded：embedded 模式。DataDir 不存在会创建，不开网络监听，
//     适合"把 KairosFlux 当库嵌进另一个进程"（E2E 测试、样例、CI 装置）。
//   - Open：打开既有数据目录（embedded/server 由 Options.Port 决定，Port<=0
//     为 embedded）。DataDir 必须已存在——重启恢复（kill -9 后一致性验证、
//     长期数据复用）用这个形态。
//   - Serve：server 模式。DataDir 不存在会创建，构造即绑定监听（Port 必填），
//     Addr() 返回监听地址，Close() 优雅关停。server 模式的网络壳与生产装配
//     Node（service/node.go）是同一个核心接线（v1 Router + v2 RouterV2 +
//     采集过滤钩子），差别是 Node 额外挂分片路由/网关准入/下游投递/周期指标
//     这些按配置门控的生产子系统（Engine 的 server 模式不挂，接口面以"真的
//     会换实现"为准，不造插件注册表）。
//
// 与 Node 的关系：Node 仍是生产服务器装配（cmd/kairosflux-server 的薄壳），
// 读进程级 config.G；Engine 是给"把 KairosFlux 当库/当嵌入式服务"的调用方
// 用的实例级 API，每个 Engine 持有自己的数据目录与监听配置，互不干扰。
// Engine 只支持 standalone 模式（WAL 持久化）；Raft 集群是另一个部署形态，
// 不在本 API 的范围内。
//
// 已知边界（诚实标注，不是遗漏）：
//   - Engine 的进程内写路径（PutVersioned）不经过协议层角色强制
//     （service/router_v2.go 的 handlePutVersioned 对 source 的 agent 身份
//     校验只覆盖 PUT_VERSIONED 线协议帧）——进程内调用方本身就是可信代码，
//     角色强制是"线协议上防越权"的机制，不适用于同进程直调。server 模式下
//     经网络的写入仍受完整协议层强制。
//   - kairnet.Server 的连接数上限（config.G.MaxConn）仍读进程级全局配置
//     （kairnet/server.go 的 acceptLoop，改动会牵动既有行为，未在本批次触碰）：
//     Engine 的 server 模式沿用进程默认值 1000。
//   - Engine 不加载 contracts/*.schema.json（service 的 schema.LoadContractsDefault
//     是生产契约校验的前提；Engine 的采集过滤钩子对未注册前缀走默认放行
//     回退路径——见 service/ingesthook/filter.go 的 validate 回退），嵌入式
//     场景不需要仓库内契约文件即可运行。
//   - 存储引擎无独立 Close（flush/compaction 工作协程随进程退出）；Engine.Close
//     负责排空并关闭 WAL 文件句柄与（server 模式下）网络壳，保证同进程内
//     Open 复用同一数据目录安全。
package kairosflux

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/internal/aiplane"
	"github.com/ChronoBrew/KairosFlux/internal/temporal"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
)

// Options 是 Engine 的唯一构造参数（NewEmbedded/Open/Serve 三形态共用）。
type Options struct {
	// DataDir 是数据目录（WAL 与 SSTable 的落盘处）。必填。
	DataDir string

	// Host 是 server 模式的监听主机（默认 "127.0.0.1"）。embedded 模式不使用。
	Host string

	// Port 是 server 模式的监听端口。<=0 构造 embedded 模式（不开网络监听）；
	// Serve 要求 >0。
	Port int

	// WindowSafetyValveN 透传给 RouterV2 的 ack=window 安全阀 N
	// （service/router_v2.go 的 windowSafetyValveN 文档）；<=0 用生产默认值
	// 1000。embedded 模式下无网络连接，不参与 ack 协商，本字段无意义。
	WindowSafetyValveN uint32

	// MaxRSSMb 是内存护栏的进程 RSS 上限（MiB，见 service/guardrail.go）；
	// <=0 关闭护栏（默认）。只对 server 模式有意义——embedded 模式进程内
	// 直调由宿主进程自己管理内存预算。
	MaxRSSMb int64
}

// Engine 是 KairosFlux 的双模式引擎：embedded 与 server 共用同一个时态内核
// （PUT_VERSIONED/GET_AS_OF/LIST_VERSIONS/REPLAY_FINGERPRINT/LIST_WRITES），
// server 模式额外持有网络壳（kairnet.Server，v1+v2 双协议）。Engine 的方法
// 在两种模式下逐字节同语义——这是"server 模式 = 同一 API 的网络壳"的落点。
type Engine struct {
	cfg      *config.GlobalConfig
	kv       *service.KVServer
	temporal *service.TemporalStore
	server   *kairnet.Server // server 模式非 nil；embedded 模式 nil
	rw       engineReadWriter
	// guardrailCancel 是内存护栏采样循环的取消函数（仅 server 模式且
	// MaxRSSMb>0 时非 nil），Close 时调用，保证护栏 goroutine 随引擎退出。
	guardrailCancel context.CancelFunc
}

// 公开类型别名：跨仓调用方（ChronoBrew/ChronoBrew 的 sample 与 E2E 测试）
// 直接用这些名字构造/读取，不需要 import internal 包。类型身份与
// internal/aiplane、internal/temporal 里的定义完全一致（Go 类型别名），
// 不存在"公共副本与内部定义悄悄分叉"的空间。
type (
	// Version 是 PUT_VERSIONED 写入的一条不可变版本记录（LogicalKey/Seq/
	// WriteNanos/Source/SchemaVer/PersistedHash/Payload，见
	// internal/temporal.Version）。
	Version = temporal.Version

	// Proposal 是 agent 唯一能写入的对象（字段契约见
	// contracts/aiplane/proposal.schema.json）。
	Proposal = aiplane.Proposal

	// ProposalKind 枚举 agent 能提交的提议种类（factor/hypothesis/
	// experiment/recommendation/review）。
	ProposalKind = aiplane.ProposalKind

	// ContextRequest 是 BuildContext 的唯一输入参数（as-of 语义）。
	ContextRequest = aiplane.ContextRequest

	// ContextBundle 是 BuildContext 的输出（确定性上下文包，字段契约见
	// contracts/aiplane/context.schema.json）。
	ContextBundle = aiplane.ContextBundle

	// WriteEnvelope 是 LIST_WRITES 返回的单条审计信封
	// （LogicalKey/Seq/WriteNanos/Source/SchemaVer/PersistedHash/Payload/
	// HashOK，见 service.WriteEnvelope）。
	WriteEnvelope = service.WriteEnvelope

	// ReplayResult 是 REPLAY_FINGERPRINT 的结果（KeyCount/Fingerprint/
	// Mismatches/Bounded，见 service.ReplayResult）。
	ReplayResult = service.ReplayResult

	// ListWritesResult 是 LIST_WRITES 的结果（Entries + BySource 聚合，
	// 见 service.ListWritesResult）。
	ListWritesResult = service.ListWritesResult
)

// ProposalKind 常量（值即 aiplane 的枚举值，跨仓调用方不需要 import
// internal 包即可引用）。
const (
	ProposalFactor         = aiplane.ProposalFactor
	ProposalHypothesis     = aiplane.ProposalHypothesis
	ProposalExperiment     = aiplane.ProposalExperiment
	ProposalRecommendation = aiplane.ProposalRecommendation
	ProposalReview         = aiplane.ProposalReview
)

// NewEmbedded 构造 embedded 模式引擎：DataDir 不存在则创建，不开网络监听。
// 失败以 error 返回（不 panic、不静默降级），调用方据此决定如何处理。
func NewEmbedded(opts Options) (*Engine, error) {
	return newEngine(opts, false)
}

// Open 打开既有数据目录构造引擎（embedded/server 由 Options.Port 决定）：
// DataDir 必须已存在，否则报错。这是"重启恢复"形态——kill -9 之后用同一
// DataDir 重新 Open，WAL 重放 + SSTable 加载使数据回到崩溃前状态
// （崩溃安全顺序见 internal/temporal 包文档：先落版本键、再落 :current
// 指针）。
func Open(opts Options) (*Engine, error) {
	return newEngine(opts, true)
}

// Serve 构造 server 模式引擎：DataDir 不存在则创建，Port 必须 >0；构造完成
// 即已绑定监听（kairnet.Server.Start 同步绑定，绑定失败以 error 返回），
// Addr() 可读，调用方后续只需 Close() 优雅关停。网络壳与 Node 共用同一套
// 核心接线（v1 Router + v2 RouterV2 + 采集过滤钩子），Node 的分片路由/准入/
// 投递/指标等按配置门控的生产子系统不挂载（见本文件顶部文档）。
func Serve(opts Options) (*Engine, error) {
	if opts.Port <= 0 {
		return nil, errors.New("kairosflux: Serve 要求 Options.Port > 0（server 模式）")
	}
	return newEngine(opts, false)
}

// newEngine 是三种构造形态的公共装配。requireDir=true 表示 DataDir 必须
// 已存在（Open 的语义）；否则不存在时创建（NewEmbedded/Serve 的语义）。
// Port>0 时挂网络壳（server 模式），否则纯 embedded。
func newEngine(opts Options, requireDir bool) (*Engine, error) {
	if opts.DataDir == "" {
		return nil, errors.New("kairosflux: Options.DataDir 不能为空")
	}
	if requireDir {
		fi, err := os.Stat(opts.DataDir)
		if err != nil || !fi.IsDir() {
			return nil, fmt.Errorf("kairosflux: Open 要求数据目录已存在: %s", opts.DataDir)
		}
	} else if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("kairosflux: 创建数据目录失败: %w", err)
	}

	host := opts.Host
	if host == "" {
		host = "127.0.0.1"
	}

	// 每个 Engine 一份独立配置：数据目录（WAL+SSTable）、监听地址、standalone
	// 模式。不触碰进程级 config.G（config.New 是纯代码默认值 + 显式选项，
	// 不读 config.json、不解析命令行）。
	cfg := config.New(
		config.WithMode(config.ModeStandalone),
		config.WithHost(host),
		config.WithPort(opts.Port),
		config.WithWALPath(filepath.Join(opts.DataDir, "wal.log")),
		config.WithSSTablePath(opts.DataDir),
		config.WithMaxRSSMb(opts.MaxRSSMb),
	)

	kv, err := service.NewKVServerWithConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kairosflux: 打开存储失败: %w", err)
	}
	kv.Run() // standalone 模式无 apply 循环，直接返回

	t := service.NewTemporalStore(kv)
	e := &Engine{
		cfg:      cfg,
		kv:       kv,
		temporal: t,
		rw:       engineReadWriter{t: t},
	}

	if opts.Port > 0 {
		if err := e.attachNetworkShell(opts); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// attachNetworkShell 装配 server 模式的网络壳：v1 Router（MsgPut/MsgGet/
// MsgDelete/MsgScan）+ v2 RouterV2（PUT_VERSIONED/GET_AS_OF/LIST_VERSIONS/
// REPLAY_FINGERPRINT/LIST_WRITES + ack 三档/窗口/STAT/BYE）+ 采集过滤钩子，
// 与 Node（service/node.go）的 handler 接线同构。
func (e *Engine) attachNetworkShell(opts Options) error {
	filter := ingesthook.NewFilter([]string{"gps", "user_id"}, 0, true)
	router := service.NewRouter(e.kv)
	router.SetPreHandle(filter.Handle)
	routerV2 := service.NewRouterV2(e.kv, filter, opts.WindowSafetyValveN)

	// 内存护栏启动自检 + 装配（与 service/node.go 同构）：max_rss_mb>0 时
	// v1/v2 Router 共享同一个护栏实例，超限拒写（v1 overloaded / v2
	// ErrCodeMemoryLimit）。护栏随 Engine 生命周期运行（Close 取消），
	// embedded 模式（Port<=0）不挂网络壳，不进本函数。GOGC 用
	// SetGCPercent(-1) 只读查询（负数为"不改，返回当前值"）。
	slog.Info("内存护栏启动自检", "GOGC", debug.SetGCPercent(-1), "max_rss_mb", e.cfg.MaxRSSMb)
	if guardrail := service.NewMemoryGuardrail(e.cfg.MaxRSSMb, slog.Default()); guardrail != nil {
		router.SetMemoryGuardrail(guardrail)
		routerV2.SetMemoryGuardrail(guardrail)
		gctx, gcancel := context.WithCancel(context.Background())
		e.guardrailCancel = gcancel
		guardrail.Start(gctx)
	}

	srv := kairnet.NewServerWithConfig(e.cfg)
	srv.AddHandler(proto.MsgPut, router)
	srv.AddHandler(proto.MsgGet, router)
	srv.AddHandler(proto.MsgDelete, router)
	srv.AddHandler(proto.MsgScan, router)
	srv.SetV2Handler(routerV2)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()

	// Start 绑定失败时只记日志不返回 error（kairnet.Server.Start 的既有语义），
	// 这里用一次真实拨号确认监听已就绪：拨通即绑定成功（该探测连接立即关闭，
	// 服务端 accept 后按 EOF 正常清理）；拨不通则报错返回，不退回"构造成功但
	// 端口没开"的静默状态。
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", srv.IP, srv.Port), 200*time.Millisecond)
	if err != nil {
		return fmt.Errorf("kairosflux: 网络壳绑定监听失败（端口不可用或被占用）: %w", err)
	}
	conn.Close()

	e.server = srv
	return nil
}

// PutVersioned 写入一个不可变版本，返回分配到的 seq。与网络协议 PUT_VERSIONED
// 同一语义（source/schemaVersion 落进操作元数据信封，供 LIST_WRITES 审计
// 按来源/契约版本过滤）；进程内直调不经过协议层 agent 角色强制（见本文件
// 顶部"已知边界"）。writeNanos 是写入时刻（unix 纳秒），调用方控制——E2E
// 测试用可控时间戳构造 as-of 定点语义；生产通常传 time.Now().UnixNano()。
func (e *Engine) PutVersioned(logical string, payload []byte, writeNanos int64, source string, schemaVersion uint32) (uint64, error) {
	return e.temporal.PutVersioned(logical, payload, writeNanos, source, schemaVersion)
}

// GetAsOf 返回 logical 在 asOfNanos 时刻可见的最新版本（as-of 语义：绝不
// 返回 asOfNanos 之后写入的版本）。该时刻无可见版本时 found=false。
func (e *Engine) GetAsOf(logical string, asOfNanos int64) (Version, bool, error) {
	return e.temporal.GetAsOf(logical, asOfNanos)
}

// ListVersions 返回 logical 的全部版本，按 seq 升序（从未写过返回空切片）。
func (e *Engine) ListVersions(logical string) ([]Version, error) {
	return e.temporal.ListVersions(logical)
}

// ReplayFingerprint 对 prefix（数据集）重放出每个逻辑键的最新状态并计算
// 确定性指纹（sha256 over (LogicalKey, Seq, Payload) 集合）。asOfNanos<=0
// 无时间上界并核对 :current 指针（Mismatches 是真实对账结果）；asOfNanos>0
// 按此刻重放（Bounded=true，不做 :current 对账，见 ReplayResult.Bounded 的
// 文档——调用方必须区分"核对通过"与"没核对"）。
func (e *Engine) ReplayFingerprint(prefix string, asOfNanos int64) (ReplayResult, error) {
	return e.temporal.ReplayFingerprint(prefix, asOfNanos)
}

// ListWrites 是审计查询：扫描 prefix 下的全部版本键，按 [tFromNanos,
// tToNanos]（<=0 表示对应方向无界）与 sourceFilter（""=不过滤）筛出每次
// 历史写入，返回信封列表（按 LogicalKey,Seq 升序，确定性输出）与按来源
// 聚合计数。temporal.Store 的幂等/崩溃安全保证使 HashOK 可作"数据在写入
// 之后是否发生过漂移"的逐条自检。
func (e *Engine) ListWrites(prefix string, tFromNanos, tToNanos int64, sourceFilter string) (ListWritesResult, error) {
	return e.temporal.ListWrites(prefix, tFromNanos, tToNanos, sourceFilter)
}

// SubmitProposal 是 agent 写提议的唯一入口（Proposal 访问口）：字段校验 +
// 角色强制（只接受 Proposal 对象，写落 proposal: 键空间）委托
// internal/aiplane.SubmitProposal，返回提议指纹与本次写入分配到的 seq。
// 与 jobctl.V2Store（网络瘦客户端）是同一个 ReadWriter 语义的不同实现——
// 这里是进程内直连 TemporalStore。
func (e *Engine) SubmitProposal(p Proposal) (fingerprint string, seq uint64, err error) {
	return aiplane.SubmitProposal(e.rw, p)
}

// BuildContext 是 Context 访问口：为研究员 agent 组装确定性上下文包
// （数据集契约 + 摘要、测过哪些因子、策略状态、风控红线 + 各自摘要），
// 同一请求（同一 as_of + 同一底层账本/契约/红线文件）两次调用逐字节相同。
// contractsDir 通常是 contracts/ 目录，redlinesPath 通常是
// riskredlines/redlines.json（委托 internal/aiplane.BuildContext）。
func (e *Engine) BuildContext(req ContextRequest, contractsDir, redlinesPath string) (ContextBundle, error) {
	return aiplane.BuildContext(e.rw, req, contractsDir, redlinesPath)
}

// Addr 返回 server 模式的监听地址（Host:Port）。embedded 模式返回空串。
func (e *Engine) Addr() string {
	if e.server == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", e.server.IP, e.server.Port)
}

// Close 优雅关停引擎（幂等）：server 模式先按 kairnet 三段式排空在途请求并
// 关闭监听，随后排空并关闭 WAL 文件句柄（service.KVServer.Close 负责）——
// 此后同进程内用同一 DataDir 重新 Open 是安全的（WAL 文件唯一写者不变量
// 恢复）。存储引擎的 flush/compaction 工作协程随进程退出，无需也不存在单独
// 的关闭入口（见 storage.Engine 文档）。
func (e *Engine) Close() error {
	if e.guardrailCancel != nil {
		e.guardrailCancel()
	}
	if e.server != nil {
		e.server.Stop()
	}
	return e.kv.Close()
}

// engineReadWriter 是 Engine 面向 aiplane 的 ReadWriter 适配器：以进程内
// TemporalStore 实现 Writer/AsOfReader/PrefixLister 三接口，与生产实现
// jobctl.V2Store（经 v2 网络协议访问服务端）同语义、同键空间、同折叠前
// 输出形状（ListPrefix 返回全部历史版本，折叠是 aiplane.LatestPerLogicalKey
// 的职责），只是不经网络。
type engineReadWriter struct {
	t *service.TemporalStore
}

// PutVersioned 实现 aiplane.Writer：write_ts 用当前时间，schema_ver=0
// （进程内直调不承载协议层 schema 版本协商，与 V2Store 的请求帧同缺省）。
func (w engineReadWriter) PutVersioned(logicalKey string, payload []byte, source string) (uint64, error) {
	return w.t.PutVersioned(logicalKey, payload, time.Now().UnixNano(), source, 0)
}

// GetAsOf 实现 aiplane.AsOfReader：返回 logical 在 asOfNanos 时刻可见版本
// 的 payload。
func (w engineReadWriter) GetAsOf(logicalKey string, asOfNanos int64) ([]byte, bool, error) {
	v, found, err := w.t.GetAsOf(logicalKey, asOfNanos)
	if err != nil || !found {
		return nil, found, err
	}
	return v.Payload, true, nil
}

// ListPrefix 实现 aiplane.PrefixLister：与 jobctl.V2Store.ListPrefix 完全
// 同语义——asOfNanos 作为写入时间上界走 LIST_WRITES 的 tToNanos（<=0 无
// 上界），返回该前缀下每个逻辑键的每一条历史版本。
func (w engineReadWriter) ListPrefix(prefix string, asOfNanos int64) ([]proto.WriteEnvelopeView, error) {
	res, err := w.t.ListWrites(prefix, 0, asOfNanos, "")
	if err != nil {
		return nil, err
	}
	out := make([]proto.WriteEnvelopeView, 0, len(res.Entries))
	for _, e := range res.Entries {
		out = append(out, proto.WriteEnvelopeView{
			LogicalKey:  e.LogicalKey,
			Seq:         e.Seq,
			WriteNanos:  e.WriteNanos,
			Source:      e.Source,
			SchemaVer:   e.SchemaVer,
			PayloadHash: e.PersistedHash,
			Payload:     e.Payload,
			HashOK:      e.HashOK,
		})
	}
	return out, nil
}
