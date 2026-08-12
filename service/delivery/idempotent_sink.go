package delivery

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sync"
)

// IdempotentFileSink 是幂等版 FileSink：把「已投递的最大 key」(HWM，high-water-mark)
// 用文件自身编码——投递按 key 升序进行，文件即按升序追加，故文件最后一条记录的 key
// 就是 HWM。Send 时先滤掉 key<=HWM 的记录（它们已在文件中），把剩余追加并 fsync，
// 再把 HWM 推进到本批最大 key。
//
// 关键：记录写入与 HWM 前进共享同一次 fsync——因为 HWM 就是文件里的数据本身，没有
// 独立的 HWM 存储，也就没有「数据已落、HWM 未落」的原子性缺口。崩溃只有两种结局：
// fsync 前（整批不持久，干净重投）或 fsync 后（整批 + HWM 同时持久，重投被跳过）。
// 配合强一致 offset 保证零丢失，二者合起来在「有序、按 key 幂等」前提下达成
// effectively-once（详见 docs 迭代报告的语义边界）。
type IdempotentFileSink struct {
	name string
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	hwm  []byte // 已投递并 fsync 的最大 key；nil 表示尚未投递过
}

// NewIdempotentFileSink 打开（或创建）path，并从已有内容恢复 HWM（最后一条记录的 key）。
func NewIdempotentFileSink(name, path string) (*IdempotentFileSink, error) {
	hwm, err := recoverHWM(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &IdempotentFileSink{name: name, f: f, w: bufio.NewWriter(f), hwm: hwm}, nil
}

// recoverHWM 扫描已存在的 JSONL，返回最后一条记录的 key 作为 HWM（空/不存在返回 nil）。
// 因投递按 key 升序追加，最后一条即最大 key。O(file) 扫描对骨架足够；后续可优化为从
// 文件尾反向读一行。
func recoverHWM(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var last []byte
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for sc.Scan() {
		var r fileRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue
		}
		last = r.Key
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return last, nil
}

func (s *IdempotentFileSink) Name() string { return s.name }

// Send 滤掉 key<=HWM 的已投递记录，把剩余追加并单次 fsync，再推进内存 HWM。
// 若整批都已投递过（重投场景），直接返回 nil（幂等跳过），不产生重复。
func (s *IdempotentFileSink) Send(ctx context.Context, batch []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	enc := json.NewEncoder(s.w)
	var maxKey []byte
	wrote := false
	for _, r := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.hwm != nil && bytes.Compare(r.Key, s.hwm) <= 0 {
			continue // key<=HWM：已投递过，幂等跳过
		}
		if err := enc.Encode(fileRecord{Key: r.Key, Value: r.Value}); err != nil {
			return err
		}
		wrote = true
		if maxKey == nil || bytes.Compare(r.Key, maxKey) > 0 {
			maxKey = r.Key
		}
	}
	if !wrote {
		return nil // 全部已投递，无需写、无需推进 HWM
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	// fsync 成功后才推进内存 HWM：与文件里的持久数据保持一致。
	s.hwm = append([]byte(nil), maxKey...)
	return nil
}

// Health 只要文件句柄仍打开即视为健康。
func (s *IdempotentFileSink) Health() SinkHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return SinkHealth{Healthy: false, Reason: "file closed"}
	}
	return SinkHealth{Healthy: true}
}

// Close 先 flush 缓冲再关闭底层文件。
func (s *IdempotentFileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	_ = s.w.Flush()
	err := s.f.Close()
	s.f = nil
	return err
}
