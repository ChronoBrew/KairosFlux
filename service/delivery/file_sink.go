package delivery

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"
)

// FileSink 把每条记录以一行 JSON（JSONL）追加写入本地文件，是投递层的
// 「脊柱」实现：真实可跑、可肉眼校验，作为对接真正数仓 connector 前的落地目标。
// Send 结束时 fsync，保证 ack 语义是「已持久化」。
type FileSink struct {
	name string
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
}

// fileRecord 是 JSONL 的一行结构；[]byte 经 json 编码为 base64 字符串，
// 从而对二进制 value 也无损。
type fileRecord struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// NewFileSink 以追加模式打开（或创建）path，返回一个名为 name 的 FileSink。
func NewFileSink(name, path string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{name: name, f: f, w: bufio.NewWriter(f)}, nil
}

func (s *FileSink) Name() string { return s.name }

// Send 把整批记录逐行写入并 fsync。任一步失败即返回错误，视为整批未 ack。
func (s *FileSink) Send(ctx context.Context, batch []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	enc := json.NewEncoder(s.w)
	for _, r := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := enc.Encode(fileRecord{Key: r.Key, Value: r.Value}); err != nil {
			return err
		}
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// Health 只要文件句柄仍打开即视为健康。
func (s *FileSink) Health() SinkHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return SinkHealth{Healthy: false, Reason: "file closed"}
	}
	return SinkHealth{Healthy: true}
}

// Close 刷盘并关闭底层文件。
func (s *FileSink) Close() error {
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
