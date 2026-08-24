package storage_test

import (
	"fmt"
	"github.com/ChronoBrew/KairosFlux/storage"
	"testing"
)

func benchmarkEnginePut(b *testing.B, valueSize int) {
	memTable := storage.NewEngine(storage.Options{Dir: b.TempDir(), MaxMemTableSize: 1000000}) // 阈值取大，基准期间不触发 flush

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("benchmark-key-%04d", i%10000))
		memTable.Put(key, value)
	}
}

func BenchmarkEngine_Put_64B(b *testing.B)  { benchmarkEnginePut(b, 64) }
func BenchmarkEngine_Put_256B(b *testing.B) { benchmarkEnginePut(b, 256) }
func BenchmarkEngine_Put_1KB(b *testing.B)  { benchmarkEnginePut(b, 1024) }
func BenchmarkEngine_Put_4KB(b *testing.B)  { benchmarkEnginePut(b, 4096) }

func BenchmarkEngine_Get(b *testing.B) {
	memTable := storage.NewEngine(storage.Options{Dir: b.TempDir(), MaxMemTableSize: 1000000}) // 阈值取大，基准期间不触发 flush

	value := make([]byte, 256)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("benchmark-key-%04d", i))
		memTable.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("benchmark-key-%04d", i%10000))
		memTable.Get(key)
	}
}

func BenchmarkEngine_Delete(b *testing.B) {
	memTable := storage.NewEngine(storage.Options{Dir: b.TempDir(), MaxMemTableSize: 1000000}) // 阈值取大，基准期间不触发 flush

	value := make([]byte, 256)
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("bench-key-%06d", i))
		memTable.Put(keys[i], value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memTable.Delete(keys[i])
	}
}

func BenchmarkMemTable_Put(b *testing.B) {
	mt := storage.NewEngine(storage.Options{Dir: b.TempDir(), MaxMemTableSize: 1000000}) // 阈值取大，基准期间不触发 flush
	value := []byte("benchmark-value-data-256-bytes-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		mt.Put(key, value)
	}
}

func BenchmarkMemTable_Get(b *testing.B) {
	mt := storage.NewEngine(storage.Options{Dir: b.TempDir(), MaxMemTableSize: 1000000}) // 阈值取大，基准期间不触发 flush
	value := []byte("benchmark-value")
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		mt.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i%100000))
		mt.Get(key)
	}
}
