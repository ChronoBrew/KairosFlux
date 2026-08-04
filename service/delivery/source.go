package delivery

import (
	"github.com/NeverENG/BanDB/pkg/predicate"
	"github.com/NeverENG/BanDB/pkg/proto"
)

// Source 是投递的数据来源：给定游标，返回下一批记录与推进后的游标。
// 游标是「已投递位置」的抽象；KV 实现下即为下一个待读 key。
type Source interface {
	// Fetch 返回从 cursor（含）起、至多 limit 条记录，以及推进后的下一游标。
	// 无更多数据时返回空批，next 等于传入 cursor。
	Fetch(cursor []byte, limit int) (batch []Record, next []byte, err error)
}

// KVScanner 抽象出 deliverer 依赖的存储读能力（由 service.KVServer 满足），
// 定义在此以避免 delivery 反向依赖 service，防止 import 环。
type KVScanner interface {
	Scan(start, end []byte, pred predicate.Predicate) []proto.ScanEntry
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
	entries := s.kv.Scan(cursor, s.end, predicate.Predicate{Op: predicate.OpNone})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	batch := make([]Record, len(entries))
	for i, e := range entries {
		batch[i] = Record{Key: e.Key, Value: e.Value}
	}
	if len(batch) == 0 {
		return batch, cursor, nil
	}
	// 游标推进到最后一条 key 之后：append 0x00 得到字节序严格更大的下一 key，
	// 使下一轮扫描不再重复读到本批最后一条。
	last := batch[len(batch)-1].Key
	next := append(append([]byte(nil), last...), 0x00)
	return batch, next, nil
}
