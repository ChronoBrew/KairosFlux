// Package kvgrpc 是 BanDB 的 gRPC 传输实现，位于 internal/ 之下：它是内部传输，
// 不是对外契约。
//
// 定位（务必读完再接线到生产）：gRPC 在本项目里是基准测试/协议对照用途，不是
// 生产摄入入口——生产摄入走 bannet 自研 TLV 协议（BANLV，见
// docs/BANLV-协议规范.md）。实测显示 BANLV 在不受 fsync 约束的读路径上吞吐约为
// gRPC 的 2.7 倍（写路径两者持平，受同一 fsync 瓶颈约束，与入口协议无关），这是
// 自研协议存在的理由、不是历史包袱，详见 README.md 的性能一节；cmd/ban-grpc-server 与
// cmd/ban-bench-grpc 的存在是为了给 BANLV 提供一个可对照的性能/协议基线，
// 不代表它是与 bannet 并列的生产候选。
//
// 对外契约只有两样：client 包（Go SDK）与 BANLV 协议规范（docs/BANLV-协议规范.md，
// 服务端实现见 bannet/）。把 .proto 交给使用方自行生成客户端，等于把内部传输
// 实现当成公开 API——使用方要装 protoc 工具链、要理解 protobuf，还要跟着我们的
// proto 变更走。需要多语言接入 BANLV 时，走各语言的最小 TLV 客户端（如
// client/python/），而不是暴露 protobuf。
//
// Go 的 internal/ 由编译器强制：模块外的代码无法导入本包，故该边界不依赖口头约定。
package kvgrpc

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/NeverENG/BanDB/service"
	"github.com/NeverENG/BanDB/service/ingesthook"
)

type GRPCServer struct {
	UnimplementedKVServiceServer
	kv     *service.KVServer
	filter *ingesthook.Filter
	server *grpc.Server
}

// NewGRPCServer 构造 gRPC 传输实现——用于基准测试/协议对照，不是生产摄入入口
// （见包注释）。filter 为 nil 时 Put 跳过清洗直接写入（历史行为，仅供不关心清洗
// 的测试/基准场景使用）；即便作为对照用途，也应传入非 nil filter 做纵深防御，
// 避免有人误把这条裸写路径当生产入口用时完全跳过畸形值/单调性/schema 校验，
// 见 cmd/ban-grpc-server/main.go 的构造处。
func NewGRPCServer(kv *service.KVServer, filter *ingesthook.Filter) *GRPCServer {
	return &GRPCServer{kv: kv, filter: filter}
}

// Put 写入前先经 Filter.Validate 清洗——与 bannet 入口（service/ingesthook.Filter.Handle）
// 复用同一套规则，只是跳过 bannet 特有的帧解析（gRPC 已经是结构化的 Key/Value，无需
// 从裸帧里解出）。Validate 判定 Drop 时返回 Success:false，不写入存储层，语义与写入失败
// 一致（PutResponse 无独立的错误字段）。
func (s *GRPCServer) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	key, value := req.Key, req.Value
	if s.filter != nil {
		newValue, changed, result, _ := s.filter.Validate(key, value)
		if result == ingesthook.ResultDrop {
			return &PutResponse{Success: false}, nil
		}
		if changed {
			value = newValue
		}
	}
	cmd := service.Command{Type: service.CommandPut, Key: key, Value: value}
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
