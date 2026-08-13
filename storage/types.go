package storage

import ()

type LogEntry struct {
	Key   []byte
	Value []byte
}

// SSTableMeta 是一个 SSTable 文件的内存元信息。
//
// MaxKeyKnown 表示 MaxKey 是否可信：仅当它取自文件尾部的块索引（新格式）或写入时直接
// 填入才为 true。不可信时读路径不施加上界过滤——猜一个 MaxKey 比不过滤危险得多：猜低了
// 会把命中 key 整段跳过，表现为数据「消失」，而不过滤只是多扫一个文件。
type SSTableMeta struct {
	Level    int
	Filepath string
	MinKey   []byte
	MaxKey   []byte
	Size     int64

	MaxKeyKnown bool
}
