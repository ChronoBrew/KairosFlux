package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL 操作码
const (
	WALOpPut    uint8 = 1
	WALOpDelete uint8 = 2
)

// 文件系统权限：目录 0o755、文件 0o644（标准约定，抽成命名常量避免散落的魔法数）。
const (
	dirPerm  = 0o755
	filePerm = 0o644
)

// walMaxBatch 单次 group commit 最多攒多少条记录再 fsync，防止极端突发下批次无界增长。
const walMaxBatch = 256

// walReq 一条待落盘的写请求；done 用于回传该请求所在批次的 fsync 结果。
type walReq struct {
	op    uint8
	key   []byte
	value []byte
	done  chan error
}

// WALRecord 一条完整 WAL 记录，供 Rewrite 批量重建 WAL 内容时使用。
type WALRecord struct {
	Op    uint8
	Key   []byte
	Value []byte
}

// walRewrite 一次 WAL 重写请求：把 WAL 内容整体替换为 records，done 回传结果。
type walRewrite struct {
	records []WALRecord
	done    chan error
}

// WAL 存储层预写日志：standalone 模式下，写先 append + fsync 到此处再进 memtable，
// 提供单机崩溃恢复。记录格式 [op u8][klen u32][vlen u32][key][value]（BigEndian）。
// 与 Raft/raft_wal.go 一致：重放读到残缺尾部记录时直接停止（撕裂的尾写按 EOF 处理），
// 不使用 CRC。重放是幂等盲写（Put/Delete），未截断的 WAL 反复重放也安全。
//
// 写入走 group commit：所有 Append 把请求投递到 reqCh，由唯一的 flushLoop 攒批——
// 把当前排队的并发写一次性写入后只 fsync 一次，再唤醒整批等待者。这既把 N 次 fsync
// 摊销为 1 次（并发越高摊销越充分），又让 flushLoop 成为文件的唯一写者，消除了此前
// 无锁并发 Write+Sync 的隐患。持久化契约不变：Append 返回即代表该记录已 fsync 落盘。
type WAL struct {
	file      *os.File
	path      string
	reqCh     chan *walReq
	rewriteCh chan *walRewrite
	closeCh   chan struct{}
	done      chan struct{} // flushLoop 退出后关闭
	once      sync.Once
}

// NewWAL 打开（或创建）WAL 文件，以追加模式准备写入，并启动 group commit flushLoop。
func NewWAL(path string) (*WAL, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		file:      f,
		path:      path,
		reqCh:     make(chan *walReq),
		rewriteCh: make(chan *walRewrite),
		closeCh:   make(chan struct{}),
		done:      make(chan struct{}),
	}
	go w.flushLoop()
	return w, nil
}

// Close 停止 flushLoop（其会先排空已投递的请求）并关闭底层文件。
func (w *WAL) Close() error {
	w.once.Do(func() { close(w.closeCh) })
	<-w.done
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Append 追加一条记录，返回时该记录已随所在批次一起 fsync 落盘。
// 删除墓碑用 op=WALOpDelete、value 传 nil。
func (w *WAL) Append(op uint8, key, value []byte) error {
	req := &walReq{op: op, key: key, value: value, done: make(chan error, 1)}
	w.reqCh <- req
	return <-req.done
}

// Rewrite 把 WAL 内容整体替换为 records（截断后重写并 fsync），用于 checkpoint 自清洁。
// 调用方必须保证调用期间没有并发 Append（否则会丢写）：本项目由 KVServer.cpMu 独占锁
// 让所有写静默后再调用。重写经 flushLoop 执行以维持"文件唯一写者"不变式。
func (w *WAL) Rewrite(records []WALRecord) error {
	rw := &walRewrite{records: records, done: make(chan error, 1)}
	w.rewriteCh <- rw
	return <-rw.done
}

// flushLoop 是文件的唯一写者：阻塞等到第一个请求后，非阻塞排空当前队列凑成一批，
// 全部写入后只 fsync 一次，再把结果回传给整批。收到关闭信号时排空剩余请求后退出。
func (w *WAL) flushLoop() {
	defer close(w.done)
	batch := make([]*walReq, 0, walMaxBatch)
	for {
		select {
		case first := <-w.reqCh:
			batch = append(batch[:0], first)
			w.drainInto(&batch)
			w.commit(batch)
		case rw := <-w.rewriteCh:
			rw.done <- w.doRewrite(rw.records)
		case <-w.closeCh:
			// 排空关闭前已投递的请求，避免其永久阻塞在 <-done。
			batch = batch[:0]
			w.drainInto(&batch)
			if len(batch) > 0 {
				w.commit(batch)
			}
			return
		}
	}
}

// drainInto 非阻塞地把当前排队的请求追加进 batch，直到队列空或达到批次上限。
func (w *WAL) drainInto(batch *[]*walReq) {
	for len(*batch) < walMaxBatch {
		select {
		case r := <-w.reqCh:
			*batch = append(*batch, r)
		default:
			return
		}
	}
}

// commit 顺序写入整批记录后只 fsync 一次，并把同一结果回传给批内每个等待者。
func (w *WAL) commit(batch []*walReq) {
	var err error
	for _, r := range batch {
		if err == nil {
			err = w.writeRecord(r.op, r.key, r.value)
		}
	}
	if err == nil {
		err = w.file.Sync()
	}
	for _, r := range batch {
		r.done <- err
	}
}

// doRewrite 通过"临时文件 + 原子 rename"把 WAL 内容整体替换为 records。
// 不能就地 Truncate(0)+重写：那三步非原子，若在 Truncate 之后、重写 fsync 之前崩溃，
// WAL 会残缺/清空——而快照里的 active+dirty 尚无 SSTable 副本，会丢已 ack 的写。
// 先把 records 写满并 fsync 到 tmp，再 rename 覆盖（POSIX 原子），最后 fsync 父目录
// 让 rename 本身持久。恢复时只会看到完整的旧 WAL 或完整的新 WAL，绝不残缺。
// 只由 flushLoop 调用，保持文件唯一写者不变式。
func (w *WAL) doRewrite(records []WALRecord) error {
	tmpPath := w.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	for _, r := range records {
		if _, err := tmp.Write(encodeRecord(r.Op, r.Key, r.Value)); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		return err
	}
	// 重新打开为新文件（旧 fd 指向已被覆盖的旧 inode）。flushLoop 是唯一写者，无并发。
	_ = w.file.Close()
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return err
	}
	w.file = f
	return fsyncDir(filepath.Dir(w.path))
}

// encodeRecord 编码一条 WAL 记录：[op u8][klen u32][vlen u32][key][value]（BigEndian）。
func encodeRecord(op uint8, key, value []byte) []byte {
	buf := make([]byte, 9, 9+len(key)+len(value))
	buf[0] = op
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(value)))
	buf = append(buf, key...)
	buf = append(buf, value...)
	return buf
}

// writeRecord 把单条记录字节写入文件（不 fsync）。O_APPEND 保证单次 Write 原子追加。
func (w *WAL) writeRecord(op uint8, key, value []byte) error {
	_, err := w.file.Write(encodeRecord(op, key, value))
	return err
}

// fsyncDir fsync 目录项，使其中文件的 rename 本身持久（崩溃后 rename 不丢失）。
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Replay 从头读取全部记录，对每条调用 fn。读到残缺记录（撕裂尾写）即停止重放，
// 返回 nil；底层 IO 错误或 fn 返回错误则向上抛出。
func (w *WAL) Replay(fn func(op uint8, key, value []byte) error) error {
	f, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var hdr [9]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // EOF 或残缺头部：正常结束
		}
		op := hdr[0]
		klen := binary.BigEndian.Uint32(hdr[1:5])
		vlen := binary.BigEndian.Uint32(hdr[5:9])

		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			break // 残缺尾写
		}
		var value []byte
		if vlen > 0 {
			value = make([]byte, vlen)
			if _, err := io.ReadFull(r, value); err != nil {
				break // 残缺尾写
			}
		}

		if err := fn(op, key, value); err != nil {
			return err
		}
	}
	return nil
}
