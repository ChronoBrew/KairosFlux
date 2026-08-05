package shardkv

import "encoding/json"

// command 是写进 Raft 日志、由每分片 FSM 应用的命令。
type command struct {
	Op    string `json:"op"` // "Put" | "Delete"
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func encodeCommand(c command) ([]byte, error) {
	return json.Marshal(c)
}

func decodeCommand(b []byte) (command, error) {
	var c command
	err := json.Unmarshal(b, &c)
	return c, err
}
