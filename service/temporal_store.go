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
func (s *TemporalStore) PutVersioned(logical string, payload []byte, writeNanos int64) (uint64, error) {
	lock := s.lockFor(logical)
	lock.Lock()
	defer lock.Unlock()

	seq, err := s.nextSeq(logical)
	if err != nil {
		return 0, err
	}

	v := temporal.Version{LogicalKey: logical, Seq: seq, WriteNanos: writeNanos, Payload: payload}

	// 崩溃安全顺序（temporal 包文档）：先落版本键，再落 :current 指针——
	// 指针永远指向已落盘的版本；反过来则可能出现指针指向尚未落盘的版本。
	versionKey := temporal.VersionStorageKey(logical, seq)
	if err := s.store.Write(Command{
		Type:  CommandPut,
		Key:   []byte(versionKey),
		Value: temporal.EncodeVersionValue(writeNanos, payload),
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
		writeNanos, payload, ok := temporal.DecodeVersionValue(e.Value)
		if !ok {
			continue
		}
		out = append(out, temporal.Version{
			LogicalKey: logical,
			Seq:        seq,
			WriteNanos: writeNanos,
			Payload:    payload,
		})
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
type ReplayResult struct {
	KeyCount    uint32
	Mismatches  []string
	Fingerprint string
}

// ReplayFingerprint 对 prefix 下的每个逻辑键：从其全部版本重放出最新版本，
// 与该逻辑键的 :current 指针对账（seq 与 payload 指纹是否一致），并对"每个
// 逻辑键的最新版本"这一集合算出联合指纹。
//
// 前缀内逻辑键的发现方式：扫描 [prefix, prefix+0xFF] 找 :current 指针键（不
// 是版本键——每个逻辑键只有一个 :current，版本键数量随写入次数增长，用
// :current 枚举逻辑键避免同一逻辑键被数出多次）。这是 ScanRaw（不过滤内部
// 键）存在的原因：Scan（业务 SCAN，过滤内部键）看不到这些键。
func (s *TemporalStore) ReplayFingerprint(prefix string) (ReplayResult, error) {
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
	}, nil
}
