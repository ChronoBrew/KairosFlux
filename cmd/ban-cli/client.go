package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/NeverENG/BanDB/bannet"
	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/pkg/utils"
)

// Client KV 存储客户端
type Client struct {
	addr string
	conn net.Conn
}

// NewClient 创建新客户端
func NewClient(addr string) *Client {
	return &Client{
		addr: addr,
	}
}

// Connect 连接到服务端
func (c *Client) Connect() error {
	var err error
	c.conn, err = net.Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", c.addr, err)
	}

	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	return nil
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SendPut 发送 PUT 请求
func (c *Client) SendPut(key []byte, value []byte) error {
	msg := utils.NewMessage(proto.MsgPut, key, value)

	if err := c.send(msg); err != nil {
		return fmt.Errorf("failed to send PUT request: %v", err)
	}

	_, payload, err := c.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	status, _, err := parseStatus(payload)
	if err != nil {
		return err
	}
	if status != proto.StatusOK {
		return fmt.Errorf("server error")
	}
	return nil
}

// SendGet 发送 GET 请求
func (c *Client) SendGet(key []byte) ([]byte, error) {
	keyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(key)))

	data := utils.ByteBuilder(keyLenBytes, key)
	msg := utils.NewMessage2(proto.MsgGet, data)

	if err := c.send(msg); err != nil {
		return nil, fmt.Errorf("failed to send GET request: %v", err)
	}

	_, payload, err := c.readResponse()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	status, rest, err := parseStatus(payload)
	if err != nil {
		return nil, err
	}
	if status != proto.StatusOK {
		return nil, fmt.Errorf("key not found or server error")
	}

	if len(rest) < 4 {
		return nil, fmt.Errorf("invalid response format")
	}
	valueLen := binary.LittleEndian.Uint32(rest[:4])
	if len(rest) < 4+int(valueLen) {
		return nil, fmt.Errorf("incomplete response")
	}
	return rest[4 : 4+valueLen], nil
}

// SendDelete 发送 DELETE 请求
func (c *Client) SendDelete(key []byte) error {
	keyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(key)))

	data := utils.ByteBuilder(keyLenBytes, key)
	msg := utils.NewMessage2(proto.MsgDelete, data)

	if err := c.send(msg); err != nil {
		return fmt.Errorf("failed to send DELETE request: %v", err)
	}

	_, payload, err := c.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	status, _, err := parseStatus(payload)
	if err != nil {
		return err
	}
	if status != proto.StatusOK {
		return fmt.Errorf("server error")
	}
	return nil
}

// SendScan 发送 SCAN 范围查询，返回命中条目。
func (c *Client) SendScan(req proto.ScanRequest) ([]proto.ScanEntry, error) {
	msg := utils.NewMessage2(proto.MsgScan, proto.EncodeScanRequest(req))

	if err := c.send(msg); err != nil {
		return nil, fmt.Errorf("failed to send SCAN request: %v", err)
	}

	_, payload, err := c.readResponse()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	status, entries, err := proto.DecodeScanResponse(payload)
	if err != nil {
		return nil, err
	}
	if status != proto.StatusOK {
		return nil, fmt.Errorf("server error")
	}
	return entries, nil
}

// send 打包并写出一条消息
func (c *Client) send(msg *utils.Message) error {
	dp := bannet.NewDataPack()
	// utils.Message 实现了 bannet.Frame 接口（同样的方法集）
	packet, err := dp.Pack(msg)
	if err != nil {
		return fmt.Errorf("failed to pack message: %v", err)
	}
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write(packet); err != nil {
		return err
	}
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	return nil
}

// readResponse 读取响应: 返回 msgID 与 data payload
func (c *Client) readResponse() (string, []byte, error) {
	dp := bannet.NewDataPack()
	headLen := dp.GetHeadLen()

	header := make([]byte, headLen)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return "", nil, fmt.Errorf("failed to read response header: %v", err)
	}

	tempMsg, err := dp.UnPack(header)
	if err != nil {
		return "", nil, fmt.Errorf("failed to unpack header: %v", err)
	}

	mImpl, ok := tempMsg.(*bannet.Message)
	if !ok {
		return "", nil, fmt.Errorf("unexpected message type")
	}

	var msgID string
	if mImpl.IDLen > 0 {
		idBuf := make([]byte, mImpl.IDLen)
		if _, err := io.ReadFull(c.conn, idBuf); err != nil {
			return "", nil, fmt.Errorf("failed to read msgID: %v", err)
		}
		msgID = string(idBuf)
	}

	dataLen := tempMsg.GetMsgLen()
	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(c.conn, data); err != nil {
			return "", nil, fmt.Errorf("failed to read response data: %v", err)
		}
	}
	return msgID, data, nil
}

// parseStatus 从负载头部解析 [statusLen u8][status bytes], 返回 status 字符串与剩余字节
func parseStatus(payload []byte) (string, []byte, error) {
	if len(payload) < 1 {
		return "", nil, fmt.Errorf("invalid response: empty payload")
	}
	statusLen := int(payload[0])
	if len(payload) < 1+statusLen {
		return "", nil, fmt.Errorf("invalid response: truncated status")
	}
	return string(payload[1 : 1+statusLen]), payload[1+statusLen:], nil
}
