package bannet_test

import (
	"fmt"
	"testing"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/pkg/proto"
)

// TestScanResponseSurvivesWire 复现并守护 SCAN 的网络缝：一个多条命中的响应
// 必须能完整通过「服务端 Pack → 客户端 UnPack（受 MaxPackageSize 限制）→ 解码」，
// 不再被旧的 1024B 上限拒绝。该路径正是单元测试绕过、却最易出错的地方。
func TestScanResponseSurvivesWire(t *testing.T) {
	entries := make([]proto.ScanEntry, 0, 200)
	for i := 0; i < 200; i++ {
		entries = append(entries, proto.ScanEntry{
			Key:   []byte(fmt.Sprintf("imu:dev0:%06d", i)),
			Value: []byte(`{"az":9.95,"ax":0.01,"ay":9.8}`),
		})
	}
	payload := proto.EncodeScanResponse(proto.StatusOK, entries)
	if len(payload) <= 1024 {
		t.Fatalf("测试前提失效：响应应远超 1024B，实际 %d", len(payload))
	}

	dp := bannet.NewDataPack()

	// 服务端 SendBuffMsg 的打包路径。
	packet, err := dp.Pack(bannet.NewMessage(proto.MsgRespOK, payload))
	if err != nil {
		t.Fatalf("Pack 失败: %v", err)
	}

	// 客户端 readResponse 的读取路径：先解头部（此处触发 MaxPackageSize 检查）。
	headLen := int(dp.HeadLen())
	if len(packet) < headLen {
		t.Fatalf("packet 过短")
	}
	tempMsg, err := dp.UnPack(packet[:headLen])
	if err != nil {
		t.Fatalf("UnPack 拒绝了大响应（MaxPackageSize 太小？）: %v", err)
	}
	m := tempMsg

	off := headLen + int(m.IDLen)
	data := packet[off : off+int(tempMsg.MsgLen())]

	status, got, err := proto.DecodeScanResponse(data)
	if err != nil {
		t.Fatalf("DecodeScanResponse 失败: %v", err)
	}
	if status != proto.StatusOK || len(got) != len(entries) {
		t.Fatalf("跨网络往返不一致: status=%q n=%d 期望 %d", status, len(got), len(entries))
	}
}
