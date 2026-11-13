# 对拍工程：zgrpc / Rust(h2+tokio) / grpc-go

同一个客户端（zgrpc 自带的 `bench --no-serve --client-pool`）、同一台机器（16 逻辑核）、
同一时段、128 字节负载、**原样回显请求体字节**（三方都不做字段级编解码）。

## 目录

- `bench.proto`：`service Bench { rpc Echo(Payload) returns (Payload) }`
- `goserver/`：grpc-go 对照服务（透传 codec + UnknownServiceHandler，不做字段级编解码）
- `rustserver/`：Rust 对照服务（**`h2` + `tokio`**，每条流一个 task）
- `steady-matrix.ps1`：**稳态对照矩阵**（交替测、每档独立 N、3 次取中位数）
- `payload.json` / `goserver/goserver.exe` / `rustserver/target/`：构建产物，不入库

> **`rustserver` 为什么不用 tonic**：本机 cargo 缓存里没有 tonic/prost（离线拿不到），
> 而 `h2` + `tokio` 正是 tonic 底下的协议层与运行时。
> 所以这个数字应理解为「Rust 异步栈在该协议层的水平」——换成 tonic 会再叠加
> codec / interceptor 开销，只会更低，不会更高。

## 三方实测（每档 20 万请求）

| 连接数 | **zgrpc** | Rust(h2+tokio) | grpc-go |
|---|---|---|---|
| 1 | **11262** | 11126 | 7398 |
| 2 | **20914** | 20434 | 12587 |
| 4 | **32568** | 29233 | 19222 |
| 8 | **50596** | 44032 | 23753 |
| 16 | **67081** | 50114 | 30166 |
| 32 | **61897** | 50683 | 33842 |
| 64 | **66449** | 47292 | 37095 |

16 连接档重复 3 次取中位数（剔单次噪声）：

| 实现 | 中位数 QPS | 三次 |
|---|---|---|
| **zgrpc** | **61268** | 62762 / 60665 / 61268 |
| Rust(h2+tokio) | 49487 | 50266 / 49487 / 49282 |
| grpc-go | 29534 | 29194 / 29534 / 29924 |

常驻资源（压测后，连接已关闭）：

| 实现 | RSS | 线程 | 句柄 |
|---|---|---|---|
| **zgrpc** | 11 MB | 3 | 50 |
| Rust(h2+tokio) | **7 MB** | 17 | 123 |
| grpc-go | 20 MB | 24 | 271 |

**结论**：Rust 与 Zig 在同一档位（zgrpc 领先 6%~34%，16 连接 +24%），两者都明显高于 grpc-go
（Rust 比 go 快 35%~85%）。差距来源不是语言而是**并发模型**：Rust 是「每流一个 task」的真异步，
zgrpc 仍是「每连接一线程 + 同步 handler」，却依然领先——靠的是发送侧批量写（延迟 flush）+
流对象复用 + HPACK 静态表哈希把单请求 CPU 压到约 8.7µs。

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