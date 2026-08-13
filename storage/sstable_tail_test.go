package storage

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// errDiskFull 模拟写入过程中的 I/O 故障。
var errDiskFull = errors.New("disk full")

// failingWriter 在累计写出 limit 字节后开始报错。
type failingWriter struct {
	written int
	limit   int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.written >= w.limit {
		return 0, errDiskFull
	}
	n := len(p)
	if w.written+n > w.limit {
		n = w.limit - w.written
		w.written += n
		return n, errDiskFull
	}
	w.written += n
	return n, nil
}

// TestWriteTailSurfacesWriteErrors 固定写尾路径的错误契约：任何一段写失败都必须被上报。
//
// 这条契约不是可选的。SSTable 尾部（块索引 + 布隆 + Footer）残缺会让 footer magic 校验
// 失败，重启时 EnsureMeta 在新格式文件上算出错误的 MaxKey，[MinKey,MaxKey] 过滤随即跳过
// 整个文件；而若错误被吞掉，调用方会以为落盘成功——Flush 会丢弃内存中的唯一副本，
// compaction 会删除源文件。也就是说，吞掉这里的错误等于静默丢数据。
func TestWriteTailSurfacesWriteErrors(t *testing.T) {
	blocks := []blockMeta{
		{lastKey: []byte("aaaa"), blockOffset: 0},
		{lastKey: []byte("bbbb"), blockOffset: 128},
		{lastKey: []byte("cccc"), blockOffset: 256},
	}
	keys := [][]byte{[]byte("aaaa"), []byte("bbbb"), []byte("cccc")}

	// 先量出完整尾部长度，据此在每个字节位置上注入故障。
	var full bytes.Buffer
	if _, err := writeTail(&full, blocks, keys, 4096); err != nil {
		t.Fatalf("正常写出不应失败: %v", err)
	}
	total := full.Len()
	if total == 0 {
		t.Fatal("尾部长度为 0，测试无意义")
	}

	for limit := 0; limit < total; limit++ {
		w := &failingWriter{limit: limit}
		_, err := writeTail(w, blocks, keys, 4096)
		if err == nil {
			t.Fatalf("在第 %d/%d 字节处注入故障，writeTail 仍返回 nil——错误被吞掉了", limit, total)
		}
		if !errors.Is(err, errDiskFull) {
			t.Fatalf("limit=%d: 错误应包裹底层故障, 实际: %v", limit, err)
		}
		// 错误信息应指出是哪一段失败，便于定位。
		if !strings.Contains(err.Error(), "block index") &&
			!strings.Contains(err.Error(), "bloom") &&
			!strings.Contains(err.Error(), "footer") {
			t.Fatalf("limit=%d: 错误未标明失败段落: %v", limit, err)
		}
	}
}

// TestWriteTailRoundTrip 校验 writeTail 写出的字节能被读路径的解析逻辑接受，
// 确保两个调用方共用同一份写尾实现后布局未变。
func TestWriteTailRoundTrip(t *testing.T) {
	blocks := []blockMeta{{lastKey: []byte("k1"), blockOffset: 0}, {lastKey: []byte("k9"), blockOffset: 64}}
	keys := [][]byte{[]byte("k1"), []byte("k9")}
	const indexStart = 1024

	var buf bytes.Buffer
	pb, err := writeTail(&buf, blocks, keys, indexStart)
	if err != nil {
		t.Fatalf("writeTail: %v", err)
	}
	if pb == nil {
		t.Fatal("writeTail 应返回构建好的布隆过滤器")
	}
	for _, k := range keys {
		if !pb.MayContain(k) {
			t.Fatalf("布隆过滤器应包含 %q", k)
		}
	}

	// Footer 位于末尾 16 字节：BlockCount(4) + IndexOffset(8) + Magic(4)。
	b := buf.Bytes()
	if len(b) < int(indexFooterSize) {
		t.Fatalf("尾部过短: %d", len(b))
	}
	footer := b[len(b)-int(indexFooterSize):]
	gotCount := uint32(footer[0])<<24 | uint32(footer[1])<<16 | uint32(footer[2])<<8 | uint32(footer[3])
	if gotCount != uint32(len(blocks)) {
		t.Fatalf("footer 块数 = %d, want %d", gotCount, len(blocks))
	}
	var gotMagic uint32
	for _, c := range footer[12:16] {
		gotMagic = gotMagic<<8 | uint32(c)
	}
	if gotMagic != indexFooterMagic {
		t.Fatalf("footer magic = %#x, want %#x", gotMagic, indexFooterMagic)
	}
}
