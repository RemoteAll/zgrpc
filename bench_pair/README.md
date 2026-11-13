# 对拍工程（跨语言 gRPC 性能对比）

用**同一个压测器 ghz** 打不同语言的 gRPC 实现，服务端行为完全一致：**原样回显 protobuf 字节**。

## 目录

- `bench.proto`：`service Bench { rpc Echo(Payload) returns (Payload) }`
- `goserver/`：grpc-go 对照服务（透传 codec + UnknownServiceHandler，不做字段级编解码）
- `payload.json` / `goserver/goserver.exe`：**构建产物，不入库**（见 .gitignore）

## 跑法

```powershell
# 1) 启动 zgrpc 服务（端口 18054）
cd ..
.\zig-out\bin\bench.exe --serve-only        # 另开终端

# 2) 构建并启动 grpc-go 对照服务（端口 18055）
cd bench_pair\goserver
$env:GOPROXY='off'; $env:GOSUMDB='off'; $env:GOFLAGS='-mod=mod'
go build -o goserver.exe .
.\goserver.exe -addr 127.0.0.1:18055        # 另开终端

# 3) 生成 128 字节负载并按需压测
cd ..\bench_pair
$b = [byte[]](1..128 | ForEach-Object { 120 })
'{"data":"' + [Convert]::ToBase64String($b) + '"}' | Set-Content payload.json -NoNewline

# 单连接 64 路并发（ghz 的 --connections 默认就是 1）
ghz --insecure --proto bench.proto --call bench.Bench.Echo -D payload.json -c 64 -n 2000 127.0.0.1:18054
# 64 连接 64 并发
ghz --insecure --proto bench.proto --call bench.Bench.Echo -D payload.json -c 64 --connections 64 -n 4000 127.0.0.1:18054
```

## 注意

- **`ghz --connections` 默认 = 1**，所以 `-c N` 默认就是「一条连接上 N 路并发流」，
  正好用来验证服务端的多路复用能力。
- 必须在同一台机器、同一时段、杀软已排除的条件下对比，否则数字不可比。
- 依赖离线：本机访问 proxy.golang.org 不通，`goserver/go.mod` 已把版本钉在本地 module cache 里，
  用 `GOPROXY=off` 构建。换机器需按实际情况调整版本。
