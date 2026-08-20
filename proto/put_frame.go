package proto

import "encoding/binary"

// PUT 请求负载布局（小端）：[keyLen u32][valueLen u32][key][value]。
// GET/DELETE 请求负载布局（小端）：[keyLen u32][key]。
//
// 这两种布局此前在 service.Router（handlePut/handleGet/handleDelete）与
// service/ingesthook.Filter（parsePut，供 gRPC 入口复用同一套清洗规则）里
// 各自独立实现过一遍——两处对同一段二进制解析各写一份，改一处容易漏改
// 另一处。这里定义唯一实现，两个调用方都改为调用它。
//
// 放在 proto 包而非 service 或 ingesthook：router 与 ingesthook 是通过
// cmd/ban-server/server.go 在装配时并列接线的兄弟包，彼此不互相 import
// （router.go 未 import ingesthook），把编解码放进其中任何一个都会新增一条
// 此前不存在的包依赖边。proto 是二者都已依赖的公共下游（负责 wire format），
// SCAN 负载的编解码（DecodeScanRequest/EncodeScanResponse，见 scan.go）已经
// 是同样的先例——PUT/GET/DELETE 负载编解码归入这里是同一个决策的延续。

// DecodePutFrame 解析 PUT 负载 keyLen(u32 LE)+valueLen(u32 LE)+key+value。
// 长度字段与实际数据不符时返回 ok=false，调用方按各自策略处理畸形帧
// （记录日志、计入 metrics 等），本函数只负责解析、不做那些决策。
func DecodePutFrame(data []byte) (key, value []byte, ok bool) {
	if len(data) < 8 {
		return nil, nil, false
	}
	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))
	valueLen := int(binary.LittleEndian.Uint32(data[4:8]))
	if keyLen < 0 || valueLen < 0 || 8+keyLen+valueLen > len(data) {
		return nil, nil, false
	}
	return data[8 : 8+keyLen], data[8+keyLen : 8+keyLen+valueLen], true
}

// EncodePutFrame 按 PUT 负载格式编帧：keyLen(u32 LE)+valueLen(u32 LE)+key+value。
// 与 DecodePutFrame 互为逆操作。
func EncodePutFrame(key, value []byte) []byte {
	buf := make([]byte, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(value)))
	copy(buf[8:], key)
	copy(buf[8+len(key):], value)
	return buf
}

// DecodeKeyFrame 解析 GET/DELETE 负载 keyLen(u32 LE)+key。长度字段与实际数据
// 不符时返回 ok=false。
func DecodeKeyFrame(data []byte) (key []byte, ok bool) {
	if len(data) < 4 {
		return nil, false
	}
	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if keyLen < 0 || 4+keyLen > len(data) {
		return nil, false
	}
	return data[4 : 4+keyLen], true
}
