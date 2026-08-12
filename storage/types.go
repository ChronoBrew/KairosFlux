package storage

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type LogEntry struct {
	Key   []byte
	Value []byte
}

type SSTableMeta struct {
	Level    int
	Filepath string
	MinKey   []byte
	MaxKey   []byte
	Size     int64

	mu           sync.Once
	MaxKeyLoaded bool
}

func (meta *SSTableMeta) EnsureMeta() {
	meta.mu.Do(func() {
		if meta.MaxKeyLoaded {
			return
		}

		file, err := os.Open(meta.Filepath)
		if err != nil {
			return
		}
		defer file.Close()

		var maxKey []byte

		for {
			var keyLen uint32
			if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
				break
			}
			keyBytes := make([]byte, keyLen)
			if _, err := io.ReadFull(file, keyBytes); err != nil {
				break
			}

			var valueLen uint32
			if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
				break
			}
			if _, err := file.Seek(int64(valueLen), io.SeekCurrent); err != nil {
				break
			}

			maxKey = keyBytes
		}

		meta.MaxKey = maxKey
		meta.MaxKeyLoaded = true
	})
}
