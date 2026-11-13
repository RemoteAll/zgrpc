// grpc-go 对照服务：**原样回显请求的 protobuf 字节**，与 zgrpc 的 `bench --serve-only`
// 行为完全一致（不做字段级编解码），这样比较的纯粹是 h2 + gRPC 框架开销。
//
// 不依赖 protoc：用透传 codec + UnknownServiceHandler，任何方法名都会被回显。
package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"
)

// rawCodec 把消息当成裸字节传递（等价于「不解码，直接回显」）。
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) { return *(v.(*[]byte)), nil }
func (rawCodec) Unmarshal(data []byte, v any) error {
	*(v.(*[]byte)) = data
	return nil
}
func (rawCodec) Name() string { return "proto" }

func rawEcho(_ any, stream grpc.ServerStream) error {
	var in []byte
	if err := stream.RecvMsg(&in); err != nil {
		return err
	}
	return stream.SendMsg(&in)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:18055", "监听地址")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(rawEcho),
	)
	log.Printf("grpc-go echo 已启动：%s", *addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
