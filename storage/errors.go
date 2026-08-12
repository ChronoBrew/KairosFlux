package storage

import "errors"

// 存储层的哨兵错误。调用方一律用 errors.Is 判别，不得比较错误文本。
//
// 引入哨兵的目的不只是风格：此前「key 不存在」在四处各自 errors.New 一个新对象，
// 与「读盘失败」等真实故障在调用方看来完全无法区分——两者都只是「某个 error」，
// 于是上层只能一视同仁地当作失败处理。哨兵使二者可判别。
//
// 错误文本遵循 Go 的约定：小写开头、无尾随标点，并以包名前缀标明来源
// （对照 Pebble 的 base.ErrNotFound、Badger 的 ErrKeyNotFound）。
var (
	// ErrKeyNotFound 表示 key 不存在，或其最新版本是一个删除墓碑。
	// 它是正常的查询结果而非故障，调用方通常应据此返回「无此键」而不是「内部错误」。
	ErrKeyNotFound = errors.New("storage: key not found")

	// ErrMemTableUnavailable 表示内存表尚未初始化或已关闭，此时读写均无法进行。
	// 与 ErrKeyNotFound 不同，它指示的是引擎状态异常。
	ErrMemTableUnavailable = errors.New("storage: memtable unavailable")

	// ErrNoEntries 表示待写入 SSTable 的条目集为空，本次落盘无需进行。
	ErrNoEntries = errors.New("storage: no entries to write")
)
