package bannet

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/pkg/utils"
)

// Client 是可复用的 BanNet TCP 客户端：按二进制 TLV 协议对一个 BanNet 服务端发起
// PUT/GET/DELETE。命令行交互客户端在 cmd `client/`（package main，不可被库导入），
// 故这里在 banNet 包内提供一份库版本，供跨节点转发（分片集群）等复用。
//
// 协议（见 pkg/proto/codes.go）：请求/响应帧为 [dataLen u32 LE][msgIDLen u16 LE][msgID][data]；
// PUT/DEL 响应 data = [statusLen u8][status]；GET 响应追加 [valueLen u32 LE][value]。
type Client struct {
	addr    string
	conn    net.Conn
	timeout time.Duration
}

// NewClient 创建一个连往 addr 的客户端（尚未拨号）。timeout<=0 取 5s。
func NewClient(addr string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{addr: addr, timeout: timeout}
}

// Connect 建立 TCP 连接。
func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return fmt.Errorf("banNet client dial %s: %w", c.addr, err)
	}
	c.conn = conn
	return nil
}

// Close 关闭连接。
func (c *Client) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// Put 发送 PUT 并等待成功响应。
func (c *Client) Put(key, value []byte) error {
	msg := utils.NewMessage(proto.MsgPut, key, value)
	payload, err := c.roundTrip(msg)
	if err != nil {
		return err
	}
	status, _, err := parseStatus(payload)
	if err != nil {
		return err
	}
	if status != proto.StatusOK {
		return fmt.Errorf("banNet client PUT: server status %q", status)
	}
	return nil
}

// Get 发送 GET，返回 value 与是否命中。未命中返回 (nil, false, nil)。
func (c *Client) Get(key []byte) ([]byte, bool, error) {
	keyLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLen, uint32(len(key)))
	msg := utils.NewMessage2(proto.MsgGet, utils.ByteBuilder(keyLen, key))
	payload, err := c.roundTrip(msg)
	if err != nil {
		return nil, false, err
	}
	status, rest, err := parseStatus(payload)
	if err != nil {
		return nil, false, err
	}
	if status != proto.StatusOK {
		return nil, false, nil // 未命中/远端错误：按未命中处理
	}
	if len(rest) < 4 {
		return nil, false, fmt.Errorf("banNet client GET: truncated response")
	}
	valueLen := binary.LittleEndian.Uint32(rest[:4])
	if len(rest) < 4+int(valueLen) {
		return nil, false, fmt.Errorf("banNet client GET: incomplete value")
	}
	return rest[4 : 4+valueLen], true, nil
}

// Delete 发送 DELETE 并等待成功响应。
func (c *Client) Delete(key []byte) error {
	keyLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLen, uint32(len(key)))
	msg := utils.NewMessage2(proto.MsgDelete, utils.ByteBuilder(keyLen, key))
	payload, err := c.roundTrip(msg)
	if err != nil {
		return err
	}
	status, _, err := parseStatus(payload)
	if err != nil {
		return err
	}
	if status != proto.StatusOK {
		return fmt.Errorf("banNet client DELETE: server status %q", status)
	}
	return nil
}

// roundTrip 打包发送一条消息并读回一条响应的 data 负载。
func (c *Client) roundTrip(msg *utils.Message) ([]byte, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("banNet client: not connected")
	}
	dp := NewDataPack()
	packet, err := dp.Pack(msg)
	if err != nil {
		return nil, fmt.Errorf("banNet client pack: %w", err)
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	if _, err := c.conn.Write(packet); err != nil {
		return nil, err
	}
	c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	return c.readResponse()
}

// readResponse 读取一条响应帧，返回其 data 负载。
func (c *Client) readResponse() ([]byte, error) {
	dp := NewDataPack()
	header := make([]byte, dp.GetHeadLen())
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, fmt.Errorf("banNet client read header: %w", err)
	}
	tempMsg, err := dp.UnPack(header)
	if err != nil {
		return nil, fmt.Errorf("banNet client unpack: %w", err)
	}
	mImpl, ok := tempMsg.(*Message)
	if !ok {
		return nil, fmt.Errorf("banNet client: unexpected message type")
	}
	if mImpl.IDLen > 0 {
		idBuf := make([]byte, mImpl.IDLen)
		if _, err := io.ReadFull(c.conn, idBuf); err != nil {
			return nil, fmt.Errorf("banNet client read msgID: %w", err)
		}
	}
	dataLen := tempMsg.GetMsgLen()
	if dataLen == 0 {
		return nil, nil
	}
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(c.conn, data); err != nil {
		return nil, fmt.Errorf("banNet client read data: %w", err)
	}
	return data, nil
}

// parseStatus 从响应 data 头部解析 [statusLen u8][status]，返回 status 与剩余字节。
func parseStatus(payload []byte) (string, []byte, error) {
	if len(payload) < 1 {
		return "", nil, fmt.Errorf("banNet client: empty payload")
	}
	statusLen := int(payload[0])
	if len(payload) < 1+statusLen {
		return "", nil, fmt.Errorf("banNet client: truncated status")
	}
	return string(payload[1 : 1+statusLen]), payload[1+statusLen:], nil
}
