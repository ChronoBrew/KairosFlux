package test_grpc

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
