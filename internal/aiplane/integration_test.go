package aiplane

// M4 端到端验收测试："agent 能回答'amihud 测过几次、判决是什么、进了哪个
// 策略、模拟盘如何'——用真实迁入的 M31 注册表数据做端到端测试"（任务书
// 附加纪律）。起一个真实的 KairosFlux v2 服务端（与 cmd/kairosflux-server
// 同样的接线，服务端起法复制自 internal/jobctl/v2store_integration_test.go
// 的 startTestKairosFluxServer——本包在 service 包之外，不能直接调用那个
// 测试专用的非导出函数，自己拼一遍），用 jobctl.RegistryImporter 真实导入
// QuantBrew 的 experiments/registry.jsonl，再用 aiplane 的 Context/证据图谱
// 查询能力对真实导入的数据作答。
//
// 真实文件路径通过环境变量 QUANTBREW_REGISTRY_PATH 覆盖，默认值是本机已知
// 的绝对路径；文件不存在时跳过（不是失败）——保持测试可移植：换一台机器/
// CI 环境没有这个兄弟仓库时，不应该让 go test ./... 整体变红，只是这一个
// "真实数据"验收点无法运行。

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChronoBrew/KairosFlux/config"
	"github.com/ChronoBrew/KairosFlux/internal/jobctl"
	"github.com/ChronoBrew/KairosFlux/kairnet"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service"
	"github.com/ChronoBrew/KairosFlux/service/ingesthook"
)

const defaultQuantBrewRegistryPath = "/Users/4ge0/Desktop/code/Project/QuantBrew/experiments/registry.jsonl"

func quantBrewRegistryPath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("QUANTBREW_REGISTRY_PATH"); p != "" {
		return p
	}
	return defaultQuantBrewRegistryPath
}

// startTestKairosFluxServer 复制自 internal/jobctl/v2store_integration_test.go
// 的同名函数（该函数未导出，本包在 jobctl 包之外，无法直接复用，只能按
// 同样的接线自己拼一遍——两处重复的是"如何起一个测试用真实服务端"这段装配
// 代码，不是业务逻辑，保持与既有先例一致）。
func startTestKairosFluxServer(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	oldWAL, oldSST, oldMode := config.G.WALPath, config.G.SSTablePath, config.G.Mode
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.Mode = config.ModeStandalone
	t.Cleanup(func() {
		config.G.WALPath, config.G.SSTablePath, config.G.Mode = oldWAL, oldSST, oldMode
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("取空闲端口失败: %v", err)
	}
	addr := l.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	l.Close()

	kv := service.NewKVServer()
	router := service.NewRouter(kv)
	filter := ingesthook.NewFilter(nil, 0, false)
	router.SetPreHandle(filter.Handle)
	routerV2 := service.NewRouterV2(kv, filter, service.DefaultV2WindowSafetyValveN)

	srv := kairnet.NewServer()
	srv.IP = host
	srv.Port = port
	srv.AddRouter(proto.MsgPut, router)
	srv.AddRouter(proto.MsgGet, router)
	srv.AddRouter(proto.MsgDelete, router)
	srv.AddRouter(proto.MsgScan, router)
	srv.AddRouterV2(routerV2)
	srv.SetConnStartFunc(router.OnConnStart)
	srv.SetConnStopFunc(router.OnConnStop)
	srv.Start()
	t.Cleanup(func() { srv.Stop(); kv.Close() })

	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("服务端在 2s 内未就绪: %s", addr)
	return ""
}

// TestEvidenceChain_EndToEnd_RealRegistryData_AnswersAmihudQuestions 是任务书
// 附加纪律要求的端到端验收：真实导入 QuantBrew 的 registry.jsonl 后，用
// aiplane 的查询能力回答"amihud 测过几次、判决是什么、进了哪个策略、模拟盘
// 如何"，如实报告（含如实降级），不编造。
func TestEvidenceChain_EndToEnd_RealRegistryData_AnswersAmihudQuestions(t *testing.T) {
	registryPath := quantBrewRegistryPath(t)
	if _, err := os.Stat(registryPath); err != nil {
		t.Skipf("QuantBrew registry.jsonl 不存在（%s），跳过端到端真实数据验收: %v", registryPath, err)
	}

	addr := startTestKairosFluxServer(t)
	store := jobctl.NewV2Store(addr, 5*time.Second)
	defer store.Close()

	// 第一步：真实导入（不是构造 fixture）。
	f, err := os.Open(registryPath)
	if err != nil {
		t.Fatalf("打开 registry.jsonl 失败: %v", err)
	}
	defer f.Close()
	importer := &jobctl.RegistryImporter{Store: store}
	result, err := importer.ImportReader(f)
	if err != nil {
		t.Fatalf("导入失败: %v", err)
	}
	t.Logf("真实导入结果: 总行数=%d 新导入=%d 未变化=%d 错误=%d", result.TotalLines, result.Imported, result.Unchanged, len(result.Errors))
	if result.TotalLines == 0 {
		t.Fatal("registry.jsonl 为空，无法验证端到端查询")
	}

	// as_of 取一个足够大的时间点覆盖刚才的导入写入（真实服务端的 write_ts
	// 是 time.Now().UnixNano()，用"当前时刻+安全余量"作为 as_of 上界——
	// 这里的 as_of 值本身不追求"确定性"，它只是端到端验收测试的查询参数，
	// 与 BuildContext 单元测试里验证的"同一个 as_of 两次调用逐字节相同"是
	//两回事：那条契约要求同一个 as_of 值重复调用结果不变，不要求 as_of
	// 值本身如何选取）。
	asOf := time.Now().Add(time.Second).UnixNano()

	// 问题 1+2："amihud 测过几次、判决是什么"——用 SearchExperimentsByMention
	// 检索（明确标注为文本检索，见 experiment.go 文档），不是从 registry
	// 的结构化字段精确解出"因子名"（该字段在 QuantBrew 侧尚不存在）。
	matches, err := SearchExperimentsByMention(store, asOf, "amihud")
	if err != nil {
		t.Fatalf("检索 amihud 相关实验失败: %v", err)
	}
	t.Logf("amihud 相关实验命中数: %d", len(matches))
	for _, m := range matches {
		t.Logf("  fingerprint=%s test_index=%d level=%s as_of=%s hypothesis=%q",
			m.Fingerprint, m.TestIndex, m.Level, m.AsOf, m.HypothesisSummary)
	}
	if len(matches) == 0 {
		t.Skip("真实 registry.jsonl 当前不含 amihud 相关记录，端到端问答无法继续（如实报告，不编造）")
	}

	// 问题 3+4："进了哪个策略、模拟盘如何"——通过证据图谱查询。真实数据里
	// factor->experiment 这条边此前从未被 AdmitFactorEvidence 建立过（M4
	// 是第一次接入这条能力），所以这里先以 amihud_illiq 为结构化 factor
	// 名（来自 matches[0] 的 hypothesis_summary 人工确认，不是程序化解析）
	// 显式把它接入证据图谱，再查询——这一步模拟"引擎已经把这条实验计入了
	// amihud_illiq 因子"的裁决动作。
	const factorName = "amihud_illiq"
	for _, m := range matches {
		if err := AdmitFactorEvidence(store, asOf, factorName, m.Fingerprint); err != nil {
			t.Fatalf("AdmitFactorEvidence 失败(fp=%s): %v", m.Fingerprint, err)
		}
	}

	chain, err := QueryEvidenceChain(store, factorName, asOf)
	if err != nil {
		t.Fatalf("QueryEvidenceChain 失败: %v", err)
	}

	t.Logf("=== 端到端证据链查询实测输出（factor=%s） ===", factorName)
	t.Logf("查到关联实验数: %d", len(chain.Experiments))
	for _, exp := range chain.Experiments {
		t.Logf("  experiment_fingerprint=%s", exp.ExperimentFingerprint)
		if exp.Experiment != nil {
			t.Logf("    判决(level)=%s p_raw=%v p_global=%v test_index=%d as_of=%s",
				exp.Experiment.Level, exp.Experiment.PRaw, exp.Experiment.PGlobal, exp.Experiment.TestIndex, exp.Experiment.AsOf)
		}
		if len(exp.Strategies) == 0 {
			t.Logf("    进了哪个策略: 未查到关联的 Strategy 对象（尚未登记/尚未进入策略候选）")
		}
		for _, strat := range exp.Strategies {
			t.Logf("    进了策略: %s", strat.Name)
			if len(strat.PaperAccounts) == 0 {
				t.Logf("      模拟盘如何: 未查到关联的 PaperAccount 对象（尚未接入模拟盘）")
			}
			for _, paper := range strat.PaperAccounts {
				t.Logf("      模拟盘: %s status=%v", paper.Name, paper.Object)
			}
		}
	}
	if len(chain.Degraded) > 0 {
		t.Logf("如实降级说明（%d 条，不代表查询失败，代表这些环节确实还没有登记）:", len(chain.Degraded))
		for _, d := range chain.Degraded {
			t.Logf("  - %s", d)
		}
	}

	// 结构化断言（不只是打日志）：真实数据目前不应该出现"编造出"策略/模拟盘
	// 关联——所有关联实验都应该在 Degraded 里出现"尚未关联任何 Strategy 对象"
	// 这一类说明，除非之前的测试运行已经在这个全新的临时服务端里注册过
	// （不可能，每次测试都是全新 t.TempDir()+全新 v2 服务端）。
	for _, exp := range chain.Experiments {
		if len(exp.Strategies) != 0 {
			t.Fatalf("真实数据当前不应有已登记的 Strategy 关联（这是全新的临时账本），实际: %+v", exp.Strategies)
		}
	}
	foundDegradeNote := false
	for _, d := range chain.Degraded {
		if strings.Contains(d, "尚未关联任何 Strategy 对象") {
			foundDegradeNote = true
		}
	}
	if !foundDegradeNote {
		t.Fatal("应至少有一条'尚未关联任何 Strategy 对象'的如实降级说明")
	}
}
