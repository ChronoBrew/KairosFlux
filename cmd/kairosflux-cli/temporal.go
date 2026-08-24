package main

// kairosflux-cli 对时态内核 M0 新增四个 v2 opcode（PUT_VERSIONED/GET_AS_OF/
// LIST_VERSIONS/REPLAY_FINGERPRINT，docs/rfc/时态内核-M0-版本化与as-of.md）的
// 命令行入口。v1 client SDK（client 包）只覆盖 PUT/GET/DELETE/SCAN 这四个 v1
// opcode，没有 v2 能力；这里不去扩建一整套 v2 SDK，只用 kairnet/negotiate 与
// kairnet/codec 已经导出的协商/拼帧函数拼出"连一次、发一帧、收一帧"的最小
// 客户端——与 service/router_v2_integration_test.go 的测试用 v2Client 是同一
// 思路，区别只是这里是生产可执行文件而不是测试代码。
//
// REPLAY_FINGERPRINT 之所以做成服务端 opcode 而不是脱离网络直接开库的离线
// 工具：本仓库已有 cmd/kairosflux-ingest 那种直接 storage.NewEngine 的离线模式先例，
// 但那要求没有其它进程同时持有同一份 LSM 数据目录——REPLAY_FINGERPRINT 的
// 典型使用场景恰恰是"服务端正在跑，我想现在核对一下账本"，两个进程各自打开
// 同一组 SSTable/WAL 文件是不安全的（见 storage.Engine 的 fileMu 系列注释）。
// 让服务端自己算、CLI 只是瘦客户端，避免了这个问题。

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/ChronoBrew/KairosFlux/internal/temporal"
	"github.com/ChronoBrew/KairosFlux/kairnet/codec"
	"github.com/ChronoBrew/KairosFlux/kairnet/negotiate"
	"github.com/ChronoBrew/KairosFlux/proto"
)

// runTemporalCommand 处理 put-versioned/get-as-of/list-versions/fingerprint
// 四条命令，返回进程退出码。与 runCommand（v1 命令）分开，因为这四条走独立的
// v2 瘦客户端，不共享 v1 client SDK 的连接/错误类型。
func runTemporalCommand(addr string, args []string) int {
	switch args[0] {
	case "put-versioned":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli put-versioned <key> <val> [source]")
			return 2
		}
		source := ""
		if len(args) >= 4 {
			source = args[3]
		}
		seq, err := putVersioned(addr, args[1], args[2], source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "写入失败: %v\n", err)
			return 1
		}
		fmt.Printf("已写入版本 seq=%d: %s = %s\n", seq, args[1], args[2])

	case "get-as-of":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli get-as-of <key> <as_of_nanos>")
			return 2
		}
		asOfNanos, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "as_of_nanos 不是合法整数: %v\n", err)
			return 2
		}
		v, found, err := getAsOf(addr, args[1], asOfNanos)
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取失败: %v\n", err)
			return 1
		}
		if !found {
			fmt.Fprintf(os.Stderr, "该时刻无可见版本: %s @ %d\n", args[1], asOfNanos)
			return 3
		}
		fmt.Printf("seq=%d write_nanos=%d payload=%s\n", v.Seq, v.WriteNanos, v.Payload)

	case "list-versions":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli list-versions <key>")
			return 2
		}
		versions, err := listVersions(addr, args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取失败: %v\n", err)
			return 1
		}
		if len(versions) == 0 {
			fmt.Println("（无版本）")
			return 0
		}
		for _, v := range versions {
			fmt.Printf("seq=%d write_nanos=%d payload=%s\n", v.Seq, v.WriteNanos, v.Payload)
		}

	case "fingerprint":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli fingerprint <prefix> [as_of_nanos]")
			return 2
		}
		var asOfNanos int64
		if len(args) >= 3 {
			v, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "as_of_nanos 不是合法整数: %v\n", err)
				return 2
			}
			asOfNanos = v
		}
		result, err := fingerprintReplay(addr, args[1], asOfNanos)
		if err != nil {
			fmt.Fprintf(os.Stderr, "重放失败: %v\n", err)
			return 1
		}
		if result.Bounded {
			// 区间查询没有对 :current 做对账（见 service.ReplayResult.Bounded 的
			// 文档）——不能打印"不一致数=0"，那会被误读成"核对通过"。
			fmt.Printf("逻辑键数=%d 指纹=%s（区间查询：未做 :current 对账）\n", result.KeyCount, result.Fingerprint)
			return 0
		}
		fmt.Printf("逻辑键数=%d 不一致数=%d 指纹=%s\n", result.KeyCount, result.MismatchCount, result.Fingerprint)
		for _, k := range result.MismatchKeys {
			fmt.Printf("  不一致: %s\n", k)
		}
		if result.MismatchCount > 0 {
			return 4 // 独立退出码：对账不一致，区别于 1(故障)/2(用法)/3(未找到)
		}

	case "list-writes":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli list-writes <prefix> [from] [to] [source]")
			return 2
		}
		tFrom, tTo, source, err := parseListWritesArgs(args[1:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 2
		}
		entries, counts, err := listWrites(addr, args[1], tFrom, tTo, source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "查询失败: %v\n", err)
			return 1
		}
		fmt.Printf("命中 %d 条写入\n", len(entries))
		for _, e := range entries {
			// 三态而不是两态：hash_ok(=true 且确有持久化哈希可比对) 才是真的
			// "核对通过"；PayloadHash=="" 是 M0 存量记录（从未被信封化，没有
			// 历史哈希可比对），HashOK 恒为 true 但这不代表"核对通过"，只是
			// "没有可核对的东西"——与 REPLAY_FINGERPRINT 区间查询"不一致数=0
			// 不代表核对通过"是同一类问题，不能把"未核对"印成看起来像"核对
			// 通过"的同一个词。
			hashNote := "hash_ok"
			switch {
			case e.PayloadHash == "":
				hashNote = "hash_unverifiable" // M0 存量记录，无持久化哈希可比对
			case !e.HashOK:
				hashNote = "HASH_MISMATCH" // 数据在写入之后发生过静默漂移
			}
			fmt.Printf("  %s seq=%d write_ts=%d source=%q schema_ver=%d %s payload=%s\n",
				e.LogicalKey, e.Seq, e.WriteNanos, e.Source, e.SchemaVer, hashNote, e.Payload)
		}
		for _, c := range counts {
			fmt.Printf("  来源计数: %q x%d\n", c.Source, c.Count)
		}

	case "export-writes":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "用法: kairosflux-cli export-writes <prefix> <outfile> [from] [to] [source]")
			return 2
		}
		tFrom, tTo, source, err := parseListWritesArgs(append([]string{args[1]}, args[3:]...))
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 2
		}
		n, exportFingerprint, err := exportWrites(addr, args[1], args[2], tFrom, tTo, source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "导出失败: %v\n", err)
			return 1
		}
		fmt.Printf("已导出 %d 条到 %s（export_fingerprint=%s，不是数据集状态指纹，仅本次导出文件的完整性校验值）\n",
			n, args[2], exportFingerprint)
	}
	return 0
}

// parseListWritesArgs 解析 list-writes/export-writes 共用的 [from] [to]
// [source] 位置参数（args 不含 prefix 本身）。省略的参数取默认值（0/""）。
func parseListWritesArgs(args []string) (tFrom, tTo int64, source string, err error) {
	if len(args) >= 2 {
		tFrom, err = strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return 0, 0, "", fmt.Errorf("from 不是合法整数: %w", err)
		}
	}
	if len(args) >= 3 {
		tTo, err = strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return 0, 0, "", fmt.Errorf("to 不是合法整数: %w", err)
		}
	}
	if len(args) >= 4 {
		source = args[3]
	}
	return tFrom, tTo, source, nil
}

const v2DialTimeout = 5 * time.Second

// v2Conn 是一条已完成 v2 协商（ack=every）的连接，只支持"发一帧、收一帧"的
// 请求-响应，够用于这四个控制类 opcode——它们本身就不参与 ack 三档窗口/
// 累计记账（见 kairnet/codec.OpcodePutVersioned 的文档），用 every 语义天然
// 匹配。
type v2Conn struct {
	conn net.Conn
}

func dialV2(addr string) (*v2Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, v2DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("拨号失败: %w", err)
	}
	version, ack, err := negotiate.ClientNegotiateWithAck(conn, v2DialTimeout, negotiate.AckEvery)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("v2 协商失败: %w", err)
	}
	if version != negotiate.VersionV2 {
		conn.Close()
		return nil, fmt.Errorf("服务端不支持 v2 协议（协商结果=%v）", version)
	}
	if ack != negotiate.AckEvery {
		conn.Close()
		return nil, fmt.Errorf("服务端确认 ack 档位=%v，期望 every", ack)
	}
	return &v2Conn{conn: conn}, nil
}

func (c *v2Conn) Close() error { return c.conn.Close() }

// roundTrip 发一帧、等一帧响应；corr_id 固定为 1——这条连接只做单次请求-响应
// 就断开，不需要用 corr_id 区分并发的多个请求。
func (c *v2Conn) roundTrip(opcode uint8, payload []byte) (*codec.MessageV2, error) {
	msg := codec.NewMessageV2(codec.HeaderV2{Opcode: opcode, Type: codec.TypeUnspecified, CorrID: 1}, payload)
	frame, err := codec.NewDataPackV2().Pack(msg)
	if err != nil {
		return nil, fmt.Errorf("编帧失败: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(v2DialTimeout)); err != nil {
		return nil, fmt.Errorf("设置写超时失败: %w", err)
	}
	if _, err := c.conn.Write(frame); err != nil {
		return nil, fmt.Errorf("写帧失败: %w", err)
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(v2DialTimeout)); err != nil {
		return nil, fmt.Errorf("设置读超时失败: %w", err)
	}
	resp, err := codec.NewDataPackV2().Decode(c.conn, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("读帧失败: %w", err)
	}
	return resp, nil
}

// errFromErrResp 把一个已确认是 ERR 的响应转成 Go error。
func errFromErrResp(resp *codec.MessageV2) error {
	code, reason, ok := proto.DecodeV2ErrPayload(resp.Payload)
	if !ok {
		return fmt.Errorf("服务端返回 ERR，但负载无法解析")
	}
	return fmt.Errorf("服务端拒绝: code=%#x reason=%s", code, reason)
}

// putVersioned 对应 PUT_VERSIONED：返回本次写入分配到的 seq。source 为空
// 表示不声明来源（M0 老调用等价形态，见 proto.EncodePutVersionedFrame）。
func putVersioned(addr, key, value, source string) (uint64, error) {
	c, err := dialV2(addr)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	resp, err := c.roundTrip(codec.OpcodePutVersioned, proto.EncodePutVersionedFrame([]byte(key), []byte(value), source))
	if err != nil {
		return 0, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return 0, errFromErrResp(resp)
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("OK 响应负载长度=%d，期望 8（seq）", len(resp.Payload))
	}
	// RouterV2.handlePutVersioned 的 OK 响应负载就是裸 [seq u64 LE] 8 字节
	// （不是 EncodeVersionEntry——那是 GET_AS_OF/LIST_VERSIONS 的格式，PUT_VERSIONED
	// 的调用方已经知道自己刚写的 payload 是什么，只需要服务端告诉它分配到的 seq）。
	return binary.LittleEndian.Uint64(resp.Payload), nil
}

// getAsOf 对应 GET_AS_OF。
func getAsOf(addr, key string, asOfNanos int64) (proto.VersionEntryView, bool, error) {
	c, err := dialV2(addr)
	if err != nil {
		return proto.VersionEntryView{}, false, err
	}
	defer c.Close()

	resp, err := c.roundTrip(codec.OpcodeGetAsOf, proto.EncodeAsOfFrame([]byte(key), asOfNanos))
	if err != nil {
		return proto.VersionEntryView{}, false, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		if _, reason, ok := proto.DecodeV2ErrPayload(resp.Payload); ok && reason == "notfound" {
			return proto.VersionEntryView{}, false, nil
		}
		return proto.VersionEntryView{}, false, errFromErrResp(resp)
	}
	seq, writeNanos, payload, _, ok := proto.DecodeVersionEntry(resp.Payload)
	if !ok {
		return proto.VersionEntryView{}, false, fmt.Errorf("响应负载无法解析")
	}
	return proto.VersionEntryView{Seq: seq, WriteNanos: writeNanos, Payload: payload}, true, nil
}

// listVersions 对应 LIST_VERSIONS。
func listVersions(addr, key string) ([]proto.VersionEntryView, error) {
	c, err := dialV2(addr)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	resp, err := c.roundTrip(codec.OpcodeListVersions, proto.EncodeKeyOnlyFrame([]byte(key)))
	if err != nil {
		return nil, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return nil, errFromErrResp(resp)
	}
	versions, ok := proto.DecodeListVersionsResponse(resp.Payload)
	if !ok {
		return nil, fmt.Errorf("响应负载无法解析")
	}
	return versions, nil
}

// fingerprintReplay 对应 REPLAY_FINGERPRINT。asOfNanos<=0 为无界（M0 兼容
// 行为，与 :current 对账）；asOfNanos>0 为区间/定点查询（M2，不与 :current
// 对账，见 result.Bounded 的文档）。
func fingerprintReplay(addr, prefix string, asOfNanos int64) (proto.ReplayFingerprintView, error) {
	c, err := dialV2(addr)
	if err != nil {
		return proto.ReplayFingerprintView{}, err
	}
	defer c.Close()

	resp, err := c.roundTrip(codec.OpcodeReplayFingerprint, proto.EncodeReplayFingerprintRequest([]byte(prefix), asOfNanos))
	if err != nil {
		return proto.ReplayFingerprintView{}, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return proto.ReplayFingerprintView{}, errFromErrResp(resp)
	}
	result, ok := proto.DecodeReplayFingerprintResponse(resp.Payload)
	if !ok {
		return proto.ReplayFingerprintView{}, fmt.Errorf("响应负载无法解析")
	}
	return result, nil
}

// listWrites 对应 LIST_WRITES（时态内核 M2 审计查询）。tFromNanos/tToNanos<=0
// 表示对应方向无界，sourceFilter==""表示不按来源过滤。
func listWrites(addr, prefix string, tFromNanos, tToNanos int64, sourceFilter string) ([]proto.WriteEnvelopeView, []proto.SourceCountView, error) {
	c, err := dialV2(addr)
	if err != nil {
		return nil, nil, err
	}
	defer c.Close()

	req := proto.EncodeListWritesRequest([]byte(prefix), tFromNanos, tToNanos, []byte(sourceFilter))
	resp, err := c.roundTrip(codec.OpcodeListWrites, req)
	if err != nil {
		return nil, nil, err
	}
	if resp.Header.Opcode != codec.OpcodeOK {
		return nil, nil, errFromErrResp(resp)
	}
	entries, counts, ok := proto.DecodeListWritesResponse(resp.Payload)
	if !ok {
		return nil, nil, fmt.Errorf("响应负载无法解析")
	}
	return entries, counts, nil
}

// exportWriteRecord 是 export-writes 导出的 JSONL 每行记录——用具名结构体而
// 不是 map[string]interface{}，字段顺序/类型在编译期锁定，不会因为 map 迭代
// 序或后续误改而漂移（gofmt/govet 管不到 map 字段顺序，但管得到结构体）。
type exportWriteRecord struct {
	LogicalKey  string `json:"logical_key"`
	Seq         uint64 `json:"seq"`
	WriteTS     int64  `json:"write_ts"`
	Source      string `json:"source"`
	SchemaVer   uint32 `json:"schema_ver"`
	PayloadHash string `json:"payload_hash"`
	PayloadB64  string `json:"payload_b64"`
	HashOK      bool   `json:"hash_ok"`
}

// exportManifestRecord 是 JSONL 文件末尾追加的一行清单记录。ExportFingerprint
// 是本次导出内容（LogicalKey/Seq/Payload 三元组集合）的确定性摘要——刻意
// 不叫 fingerprint/state_fingerprint：它与 REPLAY_FINGERPRINT 的"数据集最新
// 状态指纹"输入集合不同（导出的是窗口内的全部写入历史，REPLAY_FINGERPRINT
// 只看每个逻辑键的最新版本），两者不应该被拿来互相比较，命名上必须区分
// 开，否则下游（QuantBrew 侧接线）会写出一个注定失败的比较。
type exportManifestRecord struct {
	Manifest          bool   `json:"_manifest"`
	Count             int    `json:"count"`
	ExportFingerprint string `json:"export_fingerprint"`
}

// exportWrites 调用 LIST_WRITES 并把结果写成 append-only JSONL：每行一条
// exportWriteRecord（按 logical_key,seq 升序，客户端侧再排一次，不依赖服务端
// 已经排好序这一点——防御性，即使未来某个实现细节变化也不影响导出文件的
// 确定性），文件末尾追加一行 exportManifestRecord。返回导出的记录数与
// export_fingerprint。
func exportWrites(addr, prefix, outPath string, tFromNanos, tToNanos int64, sourceFilter string) (int, string, error) {
	entries, _, err := listWrites(addr, prefix, tFromNanos, tToNanos, sourceFilter)
	if err != nil {
		return 0, "", err
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].LogicalKey != entries[j].LogicalKey {
			return entries[i].LogicalKey < entries[j].LogicalKey
		}
		return entries[i].Seq < entries[j].Seq
	})

	f, err := os.Create(outPath)
	if err != nil {
		return 0, "", fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer f.Close()

	fpEntries := make([]temporal.Entry, 0, len(entries))
	enc := json.NewEncoder(f)
	for _, e := range entries {
		rec := exportWriteRecord{
			LogicalKey:  e.LogicalKey,
			Seq:         e.Seq,
			WriteTS:     e.WriteNanos,
			Source:      e.Source,
			SchemaVer:   e.SchemaVer,
			PayloadHash: e.PayloadHash,
			PayloadB64:  base64.StdEncoding.EncodeToString(e.Payload),
			HashOK:      e.HashOK,
		}
		if err := enc.Encode(rec); err != nil {
			return 0, "", fmt.Errorf("写入第 %d 条失败: %w", len(fpEntries)+1, err)
		}
		fpEntries = append(fpEntries, temporal.Entry{LogicalKey: e.LogicalKey, Seq: e.Seq, Payload: e.Payload})
	}

	exportFingerprint := temporal.Fingerprint(fpEntries)
	if err := enc.Encode(exportManifestRecord{Manifest: true, Count: len(entries), ExportFingerprint: exportFingerprint}); err != nil {
		return 0, "", fmt.Errorf("写入清单行失败: %w", err)
	}
	return len(entries), exportFingerprint, nil
}
