package kairnet

import "github.com/ChronoBrew/KairosFlux/kairnet/codec"

// 本文件是重构第一步（拆 codec 包，见 docs/rfc/bannet-重构.md C.7 步骤 1）的
// 门面：Message/DataPack/Codec 的实现已经搬进 kairnet/codec，这里用类型别名
// （非包装类型）把根包的公开标识符原样保留——`kairnet.Message`/`kairnet.DataPack`
// 与 `codec.Message`/`codec.DataPack` 是完全相同的类型（同一底层类型的两个
// 名字），已有调用方（cmd/kairosflux-bench、client/wire_compat_test.go 等）不需要
// 改一行代码或 import 路径。

// Message 是一帧的内存表示，参见 kairnet/codec.Message。
type Message = codec.Message

// DataPack 是 Kair 帧的编解码器，参见 kairnet/codec.DataPack。
type DataPack = codec.DataPack

// Codec 是帧编解码的抽象契约，参见 kairnet/codec.Codec。
type Codec = codec.Codec

// NewMessage 构造一个 Message，转发到 kairnet/codec.NewMessage。
func NewMessage(id string, data []byte) *Message {
	return codec.NewMessage(id, data)
}

// NewDataPack 构造一个 DataPack，转发到 kairnet/codec.NewDataPack。
func NewDataPack() *DataPack {
	return codec.NewDataPack()
}
