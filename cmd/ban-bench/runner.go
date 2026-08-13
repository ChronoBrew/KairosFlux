package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/proto"
)

type Config struct {
	Addr      string
	Workers   int
	Duration  time.Duration
	KeySize   int
	ValueSize int
	KeyCount  int
	ReadRatio float64
	Mode      string
	Warmup    time.Duration
}

type Benchmark struct {
	cfg   Config
	stats *Stats
}

func NewBenchmark(cfg Config) *Benchmark {
	return &Benchmark{
		cfg:   cfg,
		stats: NewStats(),
	}
}

func (b *Benchmark) Run() error {
	if b.cfg.Mode == "get" || b.cfg.Mode == "mixed" || b.cfg.Mode == "delete" {
		fmt.Println("[Phase 1] Pre-populating data...")
		if err := b.prePopulate(); err != nil {
			return fmt.Errorf("pre-populate failed: %w", err)
		}
	}

	if b.cfg.Warmup > 0 {
		fmt.Printf("[Phase 2] Warming up (%s)...\n", b.cfg.Warmup)
		b.runPhase(b.cfg.Warmup, nil)
	}

	fmt.Printf("[Phase 3] Running benchmark (%s)...\n", b.cfg.Duration)
	b.stats.Start()
	b.runPhase(b.cfg.Duration, b.stats)
	b.stats.Stop()

	PrintReport(b.cfg, b.stats)

	// 收紧校验：压测必须真正跑起来且基本无错，否则视为失败（让 CI 能拦住
	// 服务端不可达 / 协议回归导致的“假绿”）。
	ops := b.stats.TotalOps()
	if ops == 0 {
		return fmt.Errorf("benchmark executed 0 operations — server unreachable or not serving")
	}
	if errs := b.stats.TotalErrs(); errs > 0 {
		rate := float64(errs) / float64(ops)
		if rate > maxErrRate {
			return fmt.Errorf("benchmark error rate %.2f%% (%d/%d) exceeds %.0f%% threshold",
				rate*100, errs, ops, maxErrRate*100)
		}
	}
	return nil
}

// maxErrRate 是压测可容忍的最大错误率，超过则判定失败。
const maxErrRate = 0.01

func (b *Benchmark) prePopulate() error {
	stats := NewStats()
	stats.Start()

	var wg sync.WaitGroup
	keyCh := make(chan int, b.cfg.Workers*2)

	go func() {
		for i := 0; i < b.cfg.KeyCount; i++ {
			keyCh <- i
		}
		close(keyCh)
	}()

	for w := 0; w < b.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := dial(b.cfg.Addr)
			if err != nil {
				stats.Record(0, err) // 记为错误，避免连接失败被静默忽略导致 CI 假绿
				return
			}
			defer c.Close()

			value := make([]byte, b.cfg.ValueSize)
			for i := range keyCh {
				key := makeKey(i, b.cfg.KeySize)
				start := time.Now()
				err := put(c, key, value)
				stats.Record(time.Since(start), err)
			}
		}()
	}

	wg.Wait()
	stats.Stop()

	if stats.TotalErrs() > 0 {
		return fmt.Errorf("%d errors during pre-population", stats.TotalErrs())
	}

	fmt.Printf("  Pre-populated %d keys in %s (%.0f qps)\n",
		stats.TotalOps(), stats.Duration().Round(time.Millisecond), stats.QPS())
	return nil
}

func (b *Benchmark) runPhase(dur time.Duration, stats *Stats) {
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	if stats != nil {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					ops := stats.TotalOps()
					elapsed := time.Since(stats.startTime)
					qps := float64(ops) / elapsed.Seconds()
					fmt.Printf("\r  Running... %d ops | %.0f qps | %s elapsed",
						ops, qps, elapsed.Round(time.Second))
				case <-stopCh:
					return
				}
			}
		}()
	}

	for w := 0; w < b.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.workerLoop(dur, stopCh, stats)
		}()
	}

	time.Sleep(dur)
	close(stopCh)
	wg.Wait()

	if stats != nil {
		fmt.Print("\r") // clear progress line
	}
}

func (b *Benchmark) workerLoop(dur time.Duration, stopCh chan struct{}, stats *Stats) {
	c, err := dial(b.cfg.Addr)
	if err != nil {
		if stats != nil {
			stats.Record(0, err) // 记为错误，避免连接失败被静默忽略
		}
		return
	}
	defer c.Close()

	value := make([]byte, b.cfg.ValueSize)
	deadline := time.Now().Add(dur)

	for time.Now().Before(deadline) {
		select {
		case <-stopCh:
			return
		default:
		}

		keyIdx, _ := rand.Int(rand.Reader, big.NewInt(int64(b.cfg.KeyCount)))
		key := makeKey(int(keyIdx.Int64()), b.cfg.KeySize)

		op := b.chooseOp()

		var opErr error
		start := time.Now()

		switch op {
		case "put":
			opErr = put(c, key, value)
		case "get":
			_, opErr = get(c, key)
		case "delete":
			opErr = del(c, key)
		}

		lat := time.Since(start)
		if stats != nil {
			stats.Record(lat, opErr)
		}
	}
}

func (b *Benchmark) chooseOp() string {
	switch b.cfg.Mode {
	case "put":
		return "put"
	case "get":
		return "get"
	case "delete":
		return "delete"
	case "mixed":
		r, _ := rand.Int(rand.Reader, big.NewInt(100))
		if r.Int64() < int64(b.cfg.ReadRatio*100) {
			return "get"
		}
		return "put"
	default:
		return "put"
	}
}

// --- protocol helpers ---

func dial(addr string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp4", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func send(conn *net.TCPConn, msgID string, data []byte) error {
	msg := bannet.NewMessage(msgID, data)
	dp := bannet.NewDataPack()
	packed, err := dp.Pack(msg)
	if err != nil {
		return err
	}
	_, err = conn.Write(packed)
	return err
}

func recv(conn *net.TCPConn) ([]byte, error) {
	dp := bannet.NewDataPack()
	headLen := dp.HeadLen()

	header := make([]byte, headLen)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	tempMsg, err := dp.UnPack(header)
	if err != nil {
		return nil, err
	}

	if tempMsg.IDLen > 0 {
		idBuf := make([]byte, tempMsg.IDLen)
		if _, err := io.ReadFull(conn, idBuf); err != nil {
			return nil, err
		}
	}

	dataLen := tempMsg.MsgLen()
	if dataLen > 0 {
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			return nil, err
		}
		return data, nil
	}
	return []byte{}, nil
}

func readFull(conn *net.TCPConn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// parseStatus 拆解响应负载 [statusLen u8][status bytes], 返回 status 字符串与剩余字节
func parseStatus(payload []byte) (string, []byte, error) {
	if len(payload) < 1 {
		return "", nil, fmt.Errorf("empty payload")
	}
	statusLen := int(payload[0])
	if len(payload) < 1+statusLen {
		return "", nil, fmt.Errorf("truncated status")
	}
	return string(payload[1 : 1+statusLen]), payload[1+statusLen:], nil
}

func put(conn *net.TCPConn, key, value []byte) error {
	keyLen := make([]byte, 4)
	valLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLen, uint32(len(key)))
	binary.LittleEndian.PutUint32(valLen, uint32(len(value)))

	data := concatBytes(keyLen, valLen, key, value)
	if err := send(conn, proto.MsgPut, data); err != nil {
		return err
	}
	resp, err := recv(conn)
	if err != nil {
		return err
	}
	status, _, err := parseStatus(resp)
	if err != nil || status != proto.StatusOK {
		return fmt.Errorf("server error")
	}
	return nil
}

func get(conn *net.TCPConn, key []byte) ([]byte, error) {
	keyLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLen, uint32(len(key)))
	data := concatBytes(keyLen, key)

	if err := send(conn, proto.MsgGet, data); err != nil {
		return nil, err
	}
	resp, err := recv(conn)
	if err != nil {
		return nil, err
	}
	status, rest, err := parseStatus(resp)
	if err != nil || status != proto.StatusOK {
		return nil, fmt.Errorf("key not found")
	}
	if len(rest) < 4 {
		return nil, fmt.Errorf("short response")
	}
	valueLen := binary.LittleEndian.Uint32(rest[:4])
	if len(rest) < 4+int(valueLen) {
		return nil, fmt.Errorf("incomplete value")
	}
	return rest[4 : 4+valueLen], nil
}

func del(conn *net.TCPConn, key []byte) error {
	keyLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLen, uint32(len(key)))
	data := concatBytes(keyLen, key)

	if err := send(conn, proto.MsgDelete, data); err != nil {
		return err
	}
	resp, err := recv(conn)
	if err != nil {
		return err
	}
	status, _, err := parseStatus(resp)
	if err != nil || status != proto.StatusOK {
		return fmt.Errorf("server error")
	}
	return nil
}

func makeKey(idx int, keySize int) []byte {
	s := fmt.Sprintf("%0*x", keySize, idx)
	return []byte(s)
}

// concatBytes 按总长度一次分配后拼接多个字节切片。
func concatBytes(parts ...[]byte) []byte {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
