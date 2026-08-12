package kvgrpc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/service"
)

// TestGRPCServer_PutGetStandalone 锁定回归：standalone 模式下 KVServer.raft 为 nil，
// gRPC 的 Put 走 kv.Write() 而非直接 AppendEntry，不再 nil 指针 panic。
func TestGRPCServer_PutGetStandalone(t *testing.T) {
	oldMode := config.G.Mode
	oldWAL := config.G.WALPath
	config.G.Mode = config.ModeStandalone
	config.G.WALPath = "test_grpc_wal_" + time.Now().Format("20060102150405.000000") + ".log"
	t.Cleanup(func() {
		os.Remove(config.G.WALPath)
		config.G.Mode = oldMode
		config.G.WALPath = oldWAL
	})

	srv := NewGRPCServer(service.NewKVServer())

	putResp, err := srv.Put(context.Background(), &PutRequest{Key: []byte("k1"), Value: []byte("v1")})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if !putResp.Success {
		t.Fatalf("Put failed in standalone mode")
	}

	getResp, err := srv.Get(context.Background(), &GetRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !getResp.Success || string(getResp.Value) != "v1" {
		t.Fatalf("Get returned success=%v value=%q, want true/v1", getResp.Success, getResp.Value)
	}
}
