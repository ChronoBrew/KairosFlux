package delivery

import (
	"bytes"

	"github.com/ChronoBrew/KairosFlux/predicate"
	"github.com/ChronoBrew/KairosFlux/proto"
	"github.com/ChronoBrew/KairosFlux/service/delivery/offset"
)

// Source 是投递的数据来源：给定游标，返回下一批记录与推进后的游标。
// 游标是「已投递位置」的抽象；KV 实现下即为下一个待读 key。
type Source interface {
	// Fetch 返回从 cursor（含）起、至多 limit 条记录，以及推进后的下一游标。
	// 无更多数据时返回空批，next 等于传入 cursor。
	Fetch(cursor []byte, limit int) (batch []Record, next []byte, err error)
}

// 【语义边界，重要】KVSource 按 key 升序推进游标，因此只保证「key 单调递增地到达」时不漏投。
// 若写入是乱序的——例如多个 writer 并发写入分散在整个键空间的 key——投递游标可能已经越过
// 某个位置，而更小的 key 此后才落地：那些记录永远排在游标之前，不会再被投递。
//
// 实测：2000 条记录若在投递启动前写完，10 轮取满 2000 条；若与 20 个并发 writer 同时进行，
// 游标会冲到很后面，只投出约 310 条。
//
// 这不是本类型能单独解决的：要覆盖乱序到达，游标需改为按「写入序」而非「key 序」推进
// （例如为每条写入分配单调序号并按其建立索引），属独立设计。当前实现适用于时间序 key
// （如 imu:dev0:<ts>）这类天然单调的摄入场景。
//
// KVScanner 抽象出 deliverer 依赖的存储读能力（由 service.KVServer 满足），
// 定义在此以避免 delivery 反向依赖 service，防止 import 环。
type KVScanner interface {
	Scan(start, end []byte, pred predicate.Predicate, limit int) []proto.ScanEntry
}

// KVSource 基于 KV 范围扫描把缓冲数据作为有序投递源：按 key 升序，
// 从 cursor 起扫描 [cursor, end]，取前 limit 条，游标推进到最后一条 key 之后。
type KVSource struct {
	kv  KVScanner
	end []byte // 扫描上界（含），nil 表示不限
}

// NewKVSource 创建以 kv 为读端、扫描上界为 end 的投递源。
func NewKVSource(kv KVScanner, end []byte) *KVSource {
	return &KVSource{kv: kv, end: end}
}

func (s *KVSource) Fetch(cursor []byte, limit int) ([]Record, []byte, error) {
	// 把 limit 传下去：本轮只需 limit 条，扫描不必物化整个剩余区间。多要一条余量，
	// 以免恰好被跳过的保留 key（游标自身）占掉配额导致本批空转。
	entries := s.kv.Scan(cursor, s.end, predicate.Predicate{Op: predicate.OpNone}, limit+1)
	reserved := []byte(offset.ReservedPrefix)
	batch := make([]Record, 0, limit)
	var lastScanned []byte
	for _, e := range entries {
		lastScanned = e.Key
		// 跳过 offset 等保留 key，避免把投递游标自身当业务数据投递到下游。
		if bytes.HasPrefix(e.Key, reserved) {
			continue
		}
		batch = append(batch, Record{Key: e.Key, Value: e.Value})
		if len(batch) >= limit {
			break
		}
	}
	if lastScanned == nil {
		return batch, cursor, nil
	}
	// 游标推进到本次已消费的最后一条 key 之后（含被跳过的保留 key），保证前进：
	// append 0x00 得到字节序严格更大的下一 key，使下一轮扫描不再重复读到它。
	next := append(append([]byte(nil), lastScanned...), 0x00)
	return batch, next, nil
}
