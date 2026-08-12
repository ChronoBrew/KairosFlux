package client

import "errors"

// SDK 对外暴露的哨兵错误。调用方一律用 errors.Is 判别，不得比较错误文本。
var (
	// ErrKeyNotFound 表示 key 不存在，或其最新版本是删除墓碑。
	// 这是正常的查询结果而非故障：SDK 不会对其重试，调用方通常也不应记为错误。
	ErrKeyNotFound = errors.New("bandb: key not found")

	// ErrOverloaded 表示服务端准入过载并主动拒绝了本次请求。
	// SDK 在重试预算内会自动退避重试；耗尽后才把它返回给调用方。
	ErrOverloaded = errors.New("bandb: server overloaded")

	// ErrDropped 表示请求被服务端的落盘前钩子按策略丢弃（如畸形帧、超限 value）。
	// 这是确定性的拒绝，重试无意义，故 SDK 不重试。
	ErrDropped = errors.New("bandb: request dropped by server policy")

	// ErrServer 表示服务端返回了内部错误。可重试。
	ErrServer = errors.New("bandb: server error")

	// ErrClosed 表示客户端已关闭。
	ErrClosed = errors.New("bandb: client closed")

	// ErrProtocol 表示收到无法解析的响应，通常意味着版本不匹配或连接串话。
	// SDK 遇到它会丢弃该连接而非放回池中，避免污染后续请求。
	ErrProtocol = errors.New("bandb: protocol error")
)
