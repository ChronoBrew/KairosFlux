package banNet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

// 报文格式:
//   [dataLen u32 LE][msgIDLen u16 LE][msgID bytes][data bytes]
// 头部固定 6 字节; msgID 与 data 是变长 trailing 部分。

type DataPack struct{}

var _ banIface.IDataPack = &DataPack{}

func NewDataPack() *DataPack { return &DataPack{} }

func (dp *DataPack) GetHeadLen() uint32 {
	return 6 // dataLen u32 + msgIDLen u16
}

func (dp *DataPack) Pack(msg banIface.IMessage) ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetMsgLen()); err != nil {
		return nil, err
	}
	id := msg.GetMsgID()
	if len(id) > 0xFFFF {
		return nil, fmt.Errorf("msgID too long: %d", len(id))
	}
	if err := binary.Write(dataBuff, binary.LittleEndian, uint16(len(id))); err != nil {
		return nil, err
	}
	if _, err := dataBuff.WriteString(id); err != nil {
		return nil, err
	}
	if _, err := dataBuff.Write(msg.GetData()); err != nil {
		return nil, err
	}
	return dataBuff.Bytes(), nil
}

// UnPack 只解析定长头部 (6 字节), 返回带 DataLen 与 IDLen 的占位 Message;
// 调用方拿到 IDLen 后, 还需要从连接读取 IDLen+DataLen 字节填充 Id 与 Data。
func (dp *DataPack) UnPack(data []byte) (banIface.IMessage, error) {
	dataBuff := bytes.NewReader(data)

	msg := &Message{}
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.DataLen); err != nil {
		return nil, err
	}
	var idLen uint16
	if err := binary.Read(dataBuff, binary.LittleEndian, &idLen); err != nil {
		return nil, err
	}

	if config.G.MaxPackageSize > 0 && msg.DataLen > config.G.MaxPackageSize {
		slog.Warn("banNet frame exceeds max package size", "dataLen", msg.DataLen, "max", config.G.MaxPackageSize)
		return nil, errors.New("data too large")
	}
	// 借用 Id 暂存 IDLen 信息: 调用方先从 GetMsgID() 拿不到东西, 通过头部之后另读 IDLen 字节填回。
	// 这里用 SetMsgLen 仅保留 DataLen 不冲突, IDLen 通过返回的 Message.IDLen 提供。
	msg.IDLen = idLen
	return msg, nil
}
