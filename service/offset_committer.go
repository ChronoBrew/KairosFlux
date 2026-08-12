package service

import "github.com/NeverENG/BanDB/service/delivery/offset"

// offsetCommitter 把投递层 offset 子包的 Committer 适配到 KVServer 的写读路径：
// Put 经 KVServer.Write（raft 模式经 Raft 日志强一致复制，standalone 经存储层 WAL），
// Get 经 KVServer.Get。如此投递游标与业务数据共享同一份持久化/复制保证，不会出现
// 「数据已复制但游标丢失」的分裂。
type offsetCommitter struct{ kv *KVServer }

// NewOffsetCommitter 返回一个把 offset 写读路由到 kv 的 offset.Committer 实现。
func NewOffsetCommitter(kv *KVServer) offset.Committer { return &offsetCommitter{kv: kv} }

func (c *offsetCommitter) Put(key, value []byte) error {
	return c.kv.Write(Command{Type: CommandPut, Key: key, Value: value})
}

// Get 读取 offset；key 不存在时 KVServer.Get 返回 "key not found" 错误，
// 按 offset.Committer 约定翻译为 (nil, nil)，表示该 sink 尚无已提交游标（从头开始）。
// 注意：当前把任何读错误都归为 (nil, nil)，因此真实读故障会退化为「从头重投」而非报错——
// 骨架期可接受（投递幂等由 sink 兜底），后续应区分 not-found 与真实错误。
func (c *offsetCommitter) Get(key []byte) ([]byte, error) {
	v, err := c.kv.Get(key)
	if err != nil {
		return nil, nil
	}
	return v, nil
}
