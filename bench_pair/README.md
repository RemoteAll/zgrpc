# 目录

- `bench.proto`：`service Bench { rpc Echo(Payload) returns (Payload) }`
- `goserver/`：grpc-go 对照服务（透传 codec + UnknownServiceHandler，不做字段级编解码）
- `steady-matrix.ps1`：**稳态对照矩阵**（交替测两端、每档独立 N、3 次取中位数）
- `payload.json` / `goserver/goserver.exe`：构建产物，不入库（见 .gitignore）

# 两种压测器，用途不同

## 1) ghz（外部压测器，适合单连接多路复用）

```powershell
ghz --insecure --proto bench.proto --call bench.Bench.Echo -D payload.json -c 64 --connections 1 -n 100000 127.0.0.1:18054
```

注意 `--connections` **默认 1**，所以 `-c N` 默认就是「一条连接上 N 路并发流」。

**ghz 的硬限制（实测）**：固定 `-c 64` 只改连接数时，zgrpc 与 grpc-go 都呈同样的衰减
（52.4k → 56.9k → 49.8k → 42.8k → 39.3k → 26.5k），两端最后收敛到同一数值。
也就是说**连接数 > 4 之后 ghz 自己先到顶**，此时量的是压测器 + 操作系统，不是服务端。

## 2) 自带 Zig 客户端（**判优劣请用这个**）

```powershell
# 服务端（另开终端）
..\zig-out\bin\bench.exe --serve-only

# 客户端打本机 zgrpc
..\zig-out\bin\bench.exe --no-serve --client-pool --port 18054 --conns 16 --iterations 12000 --payload 128

# 客户端打 grpc-go（同一份客户端，公平）
..\zig-out\bin\bench.exe --no-serve --client-pool --port 18055 --conns 16 --iterations 12000 --payload 128
```

`--no-serve` 表示「不起本地服务端，只当客户端打 `--port`」。
bench 会额外打一行纯 ASCII 结果供脚本解析：

```
[bench-result] port=18054 conns=16 threads=16 ok=200000 failed=0 qps=62767.3 elapsed_ms=3081.21
```

这行走 **stderr**，因为人类可读报告走 zzig 日志组件，而控制台日志直写 `WriteConsoleW`
（中文 Windows 下才不乱码），**无法被重定向捕获**。

# 实测（本机 16 逻辑核，客户端与服务端同机）

同一 Zig 客户端扫连接数（每档 20 万请求）：

| conns | zgrpc | grpc-go | 差距 |
|---|---|---|---|
| 1 | 11322 | 7490 | +51% |
| 2 | 21360 | 12391 | +72% |
| 4 | 34035 | 19323 | +76% |
| 8 | 51765 | 24870 | +108% |
| 16 | 62767 | 31552 | +99% |
| 32 | 58393 | 34147 | +71% |
| 64 | 69448 | 35800 | +94% |

ghz 单连接多路复用（3 次取中位数）：`c=1` +100.6%、`c=8` +34.3%、`c=64` +28.3%。

# 注意

- 对比必须在**同一时段、同一台机器、杀软已排除**的条件下做，否则数字不可比；
- 本机访问 proxy.golang.org 不通，`goserver/go.mod` 已把版本钉在本地 module cache，
  用 `GOPROXY=off` 构建；换机器需按实际调整；
- `conns` 超过物理核数时两端都会退化（64 服务线程 + 64 压测 worker 抢 16 核，
  两端 CPU/req 都从 ~9us 涨到 ~110us），那类档位不适合下优劣结论。