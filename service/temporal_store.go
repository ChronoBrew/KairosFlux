package service

import (
	"sort"
	"sync"

	"github.com/ChronoBrew/KairosFlux/internal/temporal"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// TemporalRawStore 是 TemporalStore 依赖的存储能力：在 KVStore（Write/Get，
// 业务写入/字面量读取）之上，额外要求一个不经内部键过滤的原始范围扫描
// （ScanRaw），供读取版本键/current 指针使用。*KVServer 满足之（见
// service/fsm.go 的 ScanRaw 与 Scan 的区分注释）。
//
// 独立于 KVStore 定义，而不是把 ScanRaw 加进 KVStore 本身：KVStore 是 v1
// Router/v2 RouterV2 的通用业务存储抽象，分片路由测试等场景会用简化的假
// store 注入它；ScanRaw 是时态内核私有的读取需求，不应该成为所有 KVStore
// 实现都必须满足的义务。
type TemporalRawStore interface {
	KVStore
	ScanRaw(start, end []byte) []proto.ScanEntry
}

// TemporalStore 是版本化写入/as-of 读取/指纹重放对账的业务实现（时态内核
// M0 接线，docs/rfc/时态内核-M0-版本化与as-of.md）。internal/temporal 只提供
// 纯语义（键布局、AsOf、Fingerprint），本类型负责把它们接到真实存储：
// PUT_VERSIONED 落两条 KV 写（先版本键、再 :current 指针，顺序即崩溃安全
// 保证——见 temporal 包文档），GET_AS_OF/LIST_VERSIONS 走 ScanRaw 重放版本
// 集合，ReplayFingerprint 做全量重放对账。
type TemporalStore struct {
	store TemporalRawStore

	// seqLocks 按逻辑键分别加锁，保护"读当前 seq → 写版本键 → 写指针键"这段
	// 临界区不被同一逻辑键的并发写打断（跨逻辑键之间不互相阻塞）。
	seqLocks sync.Map // logical string -> *sync.Mutex

	// seqCache 记录每个逻辑键进程内已知的最新 seq，命中时免去"写前先读
	// :current"这一步。这不只是优化：Raft 模式下 Write() 在日志提交后返回，
	// 而 storage.Put 是在 KVServer.Run 的 apply 循环里异步落地的（见
	// service/fsm.go Apply），提交与落盘之间存在窗口——若每次都靠读回
	// :current 算下一个 seq，这个窗口期内的并发写会读到同一个旧 seq、
	// 相互覆盖同一个版本键。进程内维护的单调计数器不依赖那次异步落盘，
	// 从而在单 leader 模型下仍然正确；冷启动（进程重启/leader 切换后第一次
	// 触碰某个逻辑键）缓存未命中，退回读 :current——这依赖 Raft 现有的
	// "已提交的日志在成为可服务的 leader 之前已经 apply 完"这条不变量
	// （KVServer.WaitUntilReady 等既有机制维持），不是本次改动新引入的假设。
	seqCache sync.Map // logical string -> uint64
}

// NewTemporalStore 构造一个 TemporalStore。
func NewTemporalStore(store TemporalRawStore) *TemporalStore {
	return &TemporalStore{store: store}
}

func (s *TemporalStore) lockFor(logical string) *sync.Mutex {
	v, _ := s.seqLocks.LoadOrStore(logical, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// currentPointer 读取 logical 的 :current 指针；不存在返回 found=false。
func (s *TemporalStore) currentPointer(logical string) (temporal.CurrentValue, bool, error) {
	raw, err := s.store.Get([]byte(temporal.CurrentStorageKey(logical)))
	if err != nil {
		// KVStore.Get 对"键不存在"与真实故障都可能到这里；区分需要
		// storage.ErrKeyNotFound，但 TemporalStore 只依赖 KVStore 接口、不
		// 依赖 storage 包本身（避免多一条依赖边）。约定：Get 返回非 nil error
		// 且找不到底层类型断言时，统一当作"未找到"——与 KVServer.Get 对外
		// 的既有约定一致（调用方以 errors.Is(err, storage.ErrKeyNotFound)
		// 判别，这里的 store.Get 就是那同一个方法）。
		return temporal.CurrentValue{}, false, nil
	}
	cur, ok := temporal.DecodeCurrentValue(raw)
	if !ok {
		return temporal.CurrentValue{}, false, nil
	}
	return cur, true, nil
}

// nextSeq 返回 logical 的下一个待分配 seq。调用方必须已持有 lockFor(logical)。
func (s *TemporalStore) nextSeq(logical string) (uint64, error) {
	if v, ok := s.seqCache.Load(logical); ok {
		return v.(uint64) + 1, nil
	}
	cur, found, err := s.currentPointer(logical)
	if err != nil {
		return 0, err
	}
	if !found {
		return 1, nil // 第一次写：seq 从 1 起（与 RFC 示例 v...003 对应"第三次写"一致）
	}
	return cur.Seq + 1, nil
}

// PutVersioned 为 logical 写入一条新版本，返回分配到的 seq。writeNanos 由
// 调用方给定（服务端 time.Now().UnixNano()，见 RouterV2.handlePutVersioned；
// KairosFlux 不像 QuantBrew 回测内核那样要求确定性时钟，这里用真实时钟是本仓库
// 既有惯例，见 storage/sstable_write.go 等处的 time.Now() 用法)。
//
// source/schemaVersion 是 M2 操作元数据信封新增的两个字段（写入方标识、写入
// 时刻的 schema 契约版本），随本次写入一起落盘进版本键的 value（见
// temporal.EncodeVersionRecord），供 LIST_WRITES 审计查询按来源/契约版本过滤、
// 以及独立于 :current 对账的第二条数据完整性自检（payload_hash，见
// WriteEnvelope.HashOK）。source="" 表示调用方未声明来源，不是错误——M0 时期
// 的旧客户端调用 PUT_VERSIONED 时协议里根本没有这个字段，语义上等价于
// "未声明"，不是"声明了空字符串"。
func (s *TemporalStore) PutVersioned(logical string, payload []byte, writeNanos int64, source string, schemaVersion uint32) (uint64, error) {
	lock := s.lockFor(logical)
	lock.Lock()
	defer lock.Unlock()

	seq, err := s.nextSeq(logical)
	if err != nil {
		return 0, err
	}

	v := temporal.Version{
		LogicalKey:    logical,
		Seq:           seq,
		WriteNanos:    writeNanos,
		Source:        source,
		SchemaVer:     schemaVersion,
		PersistedHash: temporal.HashPayload(payload),
		Payload:       payload,
	}

	// 崩溃安全顺序（temporal 包文档）：先落版本键，再落 :current 指针——
	// 指针永远指向已落盘的版本；反过来则可能出现指针指向尚未落盘的版本。
	versionKey := temporal.VersionStorageKey(logical, seq)
	if err := s.store.Write(Command{
		Type:  CommandPut,
		Key:   []byte(versionKey),
		Value: temporal.EncodeVersionRecord(v),
	}); err != nil {
		return 0, err
	}

	currentKey := temporal.CurrentStorageKey(logical)
	currentVal := temporal.EncodeCurrentValue(temporal.CurrentValue{Seq: seq, PayloadHash: v.PayloadHash()})
	if err := s.store.Write(Command{
		Type:  CommandPut,
		Key:   []byte(currentKey),
		Value: currentVal,
	}); err != nil {
		return 0, err
	}

	s.seqCache.Store(logical, seq)
	return seq, nil
}

// ListVersions 返回 logical 的全部版本，按 seq 升序。没有任何版本时返回空
// 切片、无错误（"从未写过"不是错误）。
func (s *TemporalStore) ListVersions(logical string) ([]temporal.Version, error) {
	lower := []byte(temporal.VersionStorageKeyLowerBound(logical))
	upper := []byte(temporal.VersionStorageKeyUpperBound(logical))
	raw := s.store.ScanRaw(lower, upper)

	out := make([]temporal.Version, 0, len(raw))
	for _, e := range raw {
		gotLogical, seq, ok := temporal.ParseVersionStorageKey(string(e.Key))
		if !ok || gotLogical != logical {
			// 区间扫描理论上只会命中 logical 自己的版本键（见
			// VersionStorageKeyLowerBound/UpperBound 的字典序论证），这里的
			// exact-match 校验是纵深防御，不依赖区间边界分析本身零失误。
			continue
		}
		rec, ok := temporal.DecodeVersionRecord(e.Value)
		if !ok {
			continue
		}
		rec.LogicalKey = logical
		rec.Seq = seq
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// GetAsOf 返回 logical 在 asOfNanos 时刻可见的版本（见 temporal.AsOf 语义：
// 绝不返回未来写入）。找不到返回 found=false。
func (s *TemporalStore) GetAsOf(logical string, asOfNanos int64) (temporal.Version, bool, error) {
	versions, err := s.ListVersions(logical)
	if err != nil {
		return temporal.Version{}, false, err
	}
	v, ok := temporal.AsOf(versions, asOfNanos)
	return v, ok, nil
}

// ReplayResult 是 ReplayFingerprint 的结果：keyCount 是前缀内发现的逻辑键数，
// mismatches 是"重放出的最新版本"与 :current 指针不一致的逻辑键清单
// （seq 或 payload 指纹任一不符即算不一致），fingerprint 是对"每个逻辑键的
// 最新版本"这一重放状态集合算出的确定性摘要（用于跨进程/重复运行对比，见
// internal/temporal.Fingerprint 文档），不是逐版本全历史的指纹——历史版本
// 本身不参与"当前状态是否正确"的判定，只参与"重放出最新版本"这一步骤。
//
// Bounded 为 true 表示这次调用带了 asOfNanos 上界（M2 服务化升级：REPLAY_
// FINGERPRINT 支持"按数据集/时间范围重放"，见任务书 M2 第 1 项），此时
// Mismatches 恒为空——不是"核对后发现零不一致"，是"这次调用没有做 :current
// 对账"（:current 永远指向全局最新版本，与某个历史时间点的重放结果比较没有
// 意义，语义上不可比）。调用方（CLI/上游脚本）必须看这个字段来判断
// Mismatches==0 到底是"核对通过"还是"没核对"，不能只看 Mismatches 长度。
type ReplayResult struct {
	KeyCount    uint32
	Mismatches  []string
	Fingerprint string
	Bounded     bool
}

// ReplayFingerprint 对 prefix（数据集）下的每个逻辑键重放出状态，并对"每个
// 逻辑键的重放结果"这一集合算出联合指纹（M2 任务书第 1 项："按数据集/时间
// 范围扫描版本集合 → 重放最新状态 → 指纹与 :current 比对"）。
//
// asOfNanos<=0 表示无时间上界（默认/兼容 M0 行为）：重放出全局最新版本
// （temporal.Latest），并与该逻辑键的 :current 指针对账——这是 M0 起就有的
// 验收标准（"注入单字节漂移→指纹不一致报警""重启后重放结果与崩溃前一致"）
// 覆盖的路径，字节行为不变。
//
// asOfNanos>0 表示按此刻重放（temporal.AsOf 语义："当时系统知道什么"）——
// 典型用途是 paper ledger 对账：QuantBrew 想核对"某个交易日结束时刻，这个
// 数据集的状态指纹是多少"，与自己算出的 data_fingerprint 交叉验证（M2 任务书
// 第 3 项）。这条路径不与 :current 对账（见 ReplayResult.Bounded 的文档），
// Mismatches 恒为空、Bounded=true。
//
// 前缀内逻辑键的发现方式：扫描 [prefix, prefix+0xFF] 找 :current 指针键（不
// 是版本键——每个逻辑键只有一个 :current，版本键数量随写入次数增长，用
// :current 枚举逻辑键避免同一逻辑键被数出多次）。这是 ScanRaw（不过滤内部
// 键）存在的原因：Scan（业务 SCAN，过滤内部键）看不到这些键。
//
// 时间二级索引：故意不做（M2 任务书明确"先用版本键前缀扫描+benchmark 实测，
// 扫描慢于阈值再立项索引"）——asOfNanos 只改变"用哪个版本参与指纹"这一步的
// 选择逻辑，扫描路径（对每个候选逻辑键的全部版本发起一次 ScanRaw）与无上界
// 路径完全相同，性能特征不因加了这个参数而变化，见 service/temporal_soak_bench_test.go
// 的实测数字。
func (s *TemporalStore) ReplayFingerprint(prefix string, asOfNanos int64) (ReplayResult, error) {
	bounded := asOfNanos > 0

	upper := append([]byte(prefix), 0xFF)
	raw := s.store.ScanRaw([]byte(prefix), upper)

	logicalKeys := make([]string, 0)
	for _, e := range raw {
		key := string(e.Key)
		if !temporal.IsCurrentStorageKey(key) {
			continue
		}
		logicalKeys = append(logicalKeys, key[:len(key)-len(":current")])
	}
	sort.Strings(logicalKeys)

	var entries []temporal.Entry
	var mismatches []string
	for _, logical := range logicalKeys {
		versions, err := s.ListVersions(logical)
		if err != nil {
			return ReplayResult{}, err
		}

		if bounded {
			v, ok := temporal.AsOf(versions, asOfNanos)
			if !ok {
				continue // 该逻辑键在 asOfNanos 之前还不存在，不参与本次重放
			}
			entries = append(entries, temporal.Entry{LogicalKey: logical, Seq: v.Seq, Payload: v.Payload})
			continue
		}

		latest, ok := temporal.Latest(versions)
		if !ok {
			mismatches = append(mismatches, logical) // :current 存在但重放不出任何版本：孤儿指针
			continue
		}
		entries = append(entries, temporal.Entry{LogicalKey: logical, Seq: latest.Seq, Payload: latest.Payload})

		cur, found, err := s.currentPointer(logical)
		if err != nil {
			return ReplayResult{}, err
		}
		if !found || cur.Seq != latest.Seq || cur.PayloadHash != latest.PayloadHash() {
			mismatches = append(mismatches, logical)
		}
	}

	return ReplayResult{
		KeyCount:    uint32(len(logicalKeys)),
		Mismatches:  mismatches,
		Fingerprint: temporal.Fingerprint(entries),
		Bounded:     bounded,
	}, nil
}

// WriteEnvelope 是 LIST_WRITES 一条命中记录的完整视图：在 temporal.Version
// 的基础上加上 HashOK——PersistedHash（写入时刻记的）与现算 sha256(Payload)
// 是否一致的自检结果。PersistedHash==""（M0 存量记录，从未被信封化）时
// HashOK 恒为 true（没有历史记录可比对，不是"检查通过"，只是"无可检查项"，
// 见字段文档）。
type WriteEnvelope struct {
	LogicalKey    string
	Seq           uint64
	WriteNanos    int64
	Source        string
	SchemaVer     uint32
	PersistedHash string
	Payload       []byte
	HashOK        bool
}

// SourceCount 是 LIST_WRITES 附带的按来源聚合计数（"某键某时段被写了几次、
// 来自谁"，M2 任务书第 2 项的 COUNT 聚合）。
type SourceCount struct {
	Source string
	Count  uint32
}

// ListWritesResult 是 ListWrites 的结果。
type ListWritesResult struct {
	Entries  []WriteEnvelope
	BySource []SourceCount
}

// ListWrites 是审计查询的最小完备形态（as_of(t) 定点取值 + LIST_WRITES(key,
// t1..t2) 定点审计，M2 任务书第 2 项）：扫描 prefix 下的全部版本键（不是
// REPLAY_FINGERPRINT 那样只看 :current 枚举出的"每键一条"，这里要看到每一
// 次历史写入），按 [tFromNanos, tToNanos] 过滤 write_ts、按 source 过滤来源，
// 返回命中的信封列表（按 LogicalKey,Seq 升序，确定性输出）与按来源的聚合
// 计数（按 Source 升序）。
//
// tFromNanos<=0 表示无下界，tToNanos<=0 表示无上界——真实的 write_ts
// （time.Now().UnixNano()）恒为正数，0 或负数不是任何真实写入会取到的值，
// 用它们做"无界"哨兵不会与真实数据混淆（同 envelopeMarkerBit 的判据依据，
// 见 internal/temporal 包文档）。sourceFilter==""表示不按来源过滤。
//
// 时间二级索引：同 ReplayFingerprint，故意不做——但两者的扫描成本模型不同，
// 不能混为一谈：本函数只对 [prefix, prefix+0xFF] 发起一次 ScanRaw，扫描成本
// 是 O(该前缀下的版本总数)，时间/来源过滤是扫描之后的内存筛选，不改变这一次
// range scan 本身的成本；而 ReplayFingerprint 是先枚举 :current 指针键、再对
// 每个候选逻辑键各发起一次独立的 ScanRaw（O(逻辑键数)次扫描），是完全不同的
// 成本模型——这正是 soak 测试（service/temporal_soak_bench_test.go）量出
// ListWrites 显著快于 ReplayFingerprint（14.35 us/key vs 51.23 us/key，
// 10万键×10版本）的原因，"是否要上时间二级索引"这个决策应该主要看
// ReplayFingerprint 那个数字，不是这里。
func (s *TemporalStore) ListWrites(prefix string, tFromNanos, tToNanos int64, sourceFilter string) (ListWritesResult, error) {
	upper := append([]byte(prefix), 0xFF)
	raw := s.store.ScanRaw([]byte(prefix), upper)

	var entries []WriteEnvelope
	bySource := make(map[string]uint32)
	for _, e := range raw {
		key := string(e.Key)
		if temporal.IsCurrentStorageKey(key) {
			continue // 只看版本键本身，:current 指针不是一次"写入"记录
		}
		logical, seq, ok := temporal.ParseVersionStorageKey(key)
		if !ok {
			continue
		}
		rec, ok := temporal.DecodeVersionRecord(e.Value)
		if !ok {
			continue
		}
		if tFromNanos > 0 && rec.WriteNanos < tFromNanos {
			continue
		}
		if tToNanos > 0 && rec.WriteNanos > tToNanos {
			continue
		}
		if sourceFilter != "" && rec.Source != sourceFilter {
			continue
		}

		hashOK := rec.PersistedHash == "" || rec.PersistedHash == temporal.HashPayload(rec.Payload)
		entries = append(entries, WriteEnvelope{
			LogicalKey:    logical,
			Seq:           seq,
			WriteNanos:    rec.WriteNanos,
			Source:        rec.Source,
			SchemaVer:     rec.SchemaVer,
			PersistedHash: rec.PersistedHash,
			Payload:       rec.Payload,
			HashOK:        hashOK,
		})
		bySource[rec.Source]++
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].LogicalKey != entries[j].LogicalKey {
			return entries[i].LogicalKey < entries[j].LogicalKey
		}
		return entries[i].Seq < entries[j].Seq
	})

	sourceNames := make([]string, 0, len(bySource))
	for src := range bySource {
		sourceNames = append(sourceNames, src)
	}
	sort.Strings(sourceNames) // 禁止依赖 map 迭代序：显式排序后再输出
	counts := make([]SourceCount, 0, len(sourceNames))
	for _, src := range sourceNames {
		counts = append(counts, SourceCount{Source: src, Count: bySource[src]})
	}

	return ListWritesResult{Entries: entries, BySource: counts}, nil
}
