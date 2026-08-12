// Package kvgrpc 是 BanDB 的 gRPC 传输实现，位于 internal/ 之下：它是内部传输，
// 不是对外契约。
//
// 对外契约只有两样：client 包（Go SDK）与 BanNet 协议规范（见 README）。把 .proto
// 交给使用方自行生成客户端，等于把内部传输实现当成公开 API——使用方要装 protoc 工具链、
// 要理解 protobuf，还要跟着我们的 proto 变更走。需要多语言接入时，应当在此之上提供各语言
// 的 SDK 或网关，而不是暴露 protobuf。
//
// Go 的 internal/ 由编译器强制：模块外的代码无法导入本包，故该边界不依赖口头约定。
package kvgrpc

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/NeverENG/BanDB/service"
)

type GRPCServer struct {
	UnimplementedKVServiceServer
	kv     *service.KVServer
	server *grpc.Server
}

func NewGRPCServer(kv *service.KVServer) *GRPCServer {
	return &GRPCServer{kv: kv}
}

func (s *GRPCServer) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	cmd := service.Command{Type: service.CommandPut, Key: req.Key, Value: req.Value}
	if err := s.kv.Write(cmd); err != nil {
		return &PutResponse{Success: false}, nil
	}
	return &PutResponse{Success: true}, nil
}

func (s *GRPCServer) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	value, err := s.kv.Get(req.Key)
	if err != nil {
		return &GetResponse{Success: false}, nil
	}
	return &GetResponse{Success: true, Value: value}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	cmd := service.Command{Type: service.CommandDelete, Key: req.Key}
	if err := s.kv.Write(cmd); err != nil {
		return &DeleteResponse{Success: false}, nil
	}
	return &DeleteResponse{Success: true}, nil
}

func (s *GRPCServer) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("gRPC listen failed: %w", err)
	}
	s.server = grpc.NewServer()
	RegisterKVServiceServer(s.server, s)
	fmt.Printf("[gRPC] Server listening on %s\n", addr)
	return s.server.Serve(lis)
}

func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}
