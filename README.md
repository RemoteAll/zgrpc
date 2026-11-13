# zgrpc

纯 Zig、**零依赖**的 gRPC / HTTP/2 协议栈（Zig 0.17.1+）。

面向「需要 gRPC 的项目」独立成库，**不放进 zzig**：h2 + HPACK + gRPC 是一整套协议栈，
不该让所有用 zzig 的项目都背上这份编译与维护成本；本库也不依赖 zzig，两边互不绑死。

## 现状

| 模块 | 能力 | 说明 |
|---|---|---|
| `hpack` | 头部压缩（RFC 7541） | **解码完整**（静态表 + 动态表 + Huffman）；**编码**：静态命中走索引，字符串在「明文 / Huffman」之间**自动取更短**（典型头部省 25.8%） |
| `h2` | HTTP/2 明文 h2c（prior-knowledge）服务端 | SETTINGS/ACK、HEADERS/CONTINUATION（含 PADDED/PRIORITY）、DATA（含 PADDED + 流控回补）、WINDOW_UPDATE（连接级 + 每流）、PING、RST_STREAM、GOAWAY；**多路复用**（多流并存、发送阻塞时帧入 pending 队列） |
| `grpc` | gRPC 语义层 | 消息分帧、状态码、路由、unary / 服务端流式 / **客户端流与 bidi**（`server.stream_routes` + `streamHooks`）、**gRPC-Web**、**gzip 消息压缩**、**`grpc-timeout` 截止时间**、`grpc-message` 的 percent-encoding、非 POST→**405** / content-type 不符→**415**、**Custom-Metadata（含 `-bin` base64）**、**gRPC-Web 文本模式（`grpc-web-text`，base64）** |
| `client` | gRPC 客户端（h2c） | 握手、`unary`、**`callMany`（同一连接并发多条流）**、`clientStream`（客户端流/bidi），按 stream_id 归位、回补窗口；发 `grpc-timeout` / `user-agent` / 自定义 metadata，按规范**合成** Status 并解 percent-encoding |
| `health` | 标准 `grpc.health.v1.Health` | `Check`（unary）/ `Watch`（流式，同步模型下默认推一次）；最简 protobuf 手写编解码，零依赖 |
| `proto` / `descriptor` | proto3 子集语法树 → `google.protobuf.FileDescriptorProto` | 代码生成与服务反射**共用一份解析器**；descriptor 编码严格按官方字段号，enum/message 字段类型可区分 |
| `reflection` | 标准 `grpc.reflection.v1`（并注册 `v1alpha` 兼容） | grpcurl / ghz **无需 `.proto`** 即可 `list` / `describe` / 直接调用；数据用内部 arena，`deinit` 一次释放 |
| `pb` | 最小 protobuf 运行时 | varint/zigzag/tag/length-delimited/fixed/packed 编解码；**不做 .proto 代码生成**，手写 message 约 10 行 |
| `gzip` / `clock` | 压缩与单调时钟 | `gzip` 走 `std.compress.flate`；`clock` 直问 OS（`QueryPerformanceCounter` / `CLOCK_MONOTONIC`），服务 `grpc-timeout` |

**未做**（明确列出，避免误解为完整实现）：TLS/ALPN、服务端推送、优先级调度、
handler 并行执行（业务仍同步串行，串行只是执行顺序，**不会因此拒绝并发流**）、
客户端流/bidi 背压（gRPC-Web 规范本身也不支持 bidi）、`grpc-timeout` 的**强制中断**（本层只判超时并回 `DEADLINE_EXCEEDED`，
不会打断已阻塞在业务里的调用）、
well-known types / `Any`（`.proto` 子集代码生成与**服务反射**已支持，见下）。

### keepalive（服务端主动 PING）

长连接穿过 NAT / 四层 LB 时会被当成「空闲」掐掉（常见阈值 5~60 分钟）。启用方式：

```zig
var server = zgrpc.grpc.Server{
    .routes = &routes,
    .stream_routes = &stream_routes,
    .outer_ctx = &dummy,
    .keepalive_interval_ms = 30_000,   // 30s 一次
};
const ka = try server.spawnKeepalive(allocator);   // 一个线程服务所有连接
defer { server.stopKeepalive(); ka.join(); }
```

只配 `keepalive_interval_ms` **不会自动发** —— 还需要 `spawnKeepalive()`，或由调用方周期调 `pingAll()`
（适合已有事件循环的场景）。不主动断开无响应的连接：`h2.Connection.pingAcks()` 暴露计数，策略留给调用方。

### 对照官方规范的符合性

以 `doc/PROTOCOL-HTTP2.md` 与 `doc/PROTOCOL-WEB.md` 逐条比对（2026-11-12）：

| 规范条款 | 状态 |
|---|---|
| `:method POST`、`POST /{service}/{method}` | ✅ 非 POST 回 **HTTP 405**（不伪装成 `:status 200`） |
| `content-type: application/grpc[+proto]` | ✅ 不符回 **HTTP 415**（规范 SHOULD） |
| `te: trailers` | ✅ 客户端必发；服务端缺失时宽容处理并**留痕日志** |
| `grpc-timeout`（≤8 位 + H/M/S/m/u/n） | ✅ 解析为单调时钟截止时刻，超时回 `DEADLINE_EXCEEDED(4)` |
| `grpc-encoding` / `grpc-accept-encoding` | ✅ gzip；未声明不压缩，响应头按 ABNF 顺序 |
| `[压缩位:1][长度:4 大端]` 分帧 | ✅ 压缩标志未声明时恒为 0 |
| `:status 200` + `grpc-status` 在 trailers | ✅ 即使 OK 也发 trailers；支持 Trailers-Only 形态 |
| `grpc-message` percent-encoding | ✅ 发送侧编码、客户端解码；非法 `%xx` 原样保留（规范 MUST NOT error） |
| `grpc-status` 十进制无前导零 | ✅ |
| 客户端遇非 200 / 缺 status 必须**合成** | ✅ 合成 `UNAVAILABLE(14)` / `UNKNOWN(2)` 并附可读 message |
| gRPC-Web：trailers 走 body 尾帧（MSB=1、键名小写） | ✅ 尾帧 `0x80` + 4B 长度 + `grpc-status: …` |
| gRPC-Web：缺省格式按 `+proto` | ✅ 响应 `application/grpc-web+proto` |
| gRPC-Web：文本流（`application/grpc-web-text`，默认 base64，不得假设 padding 只在帧边界） | ✅ 请求整体解码、响应逐帧 base64（带 padding，解码端忽略 `=`） |
| 建议 `user-agent` 用 `grpc-<lang>/<ver>` | ✅ 默认 `grpc-zgrpc/0.2.1` |
| Custom-Metadata：`-bin` 用 base64（MUST 接受带/不带 padding，应发无 padding）；重复名可逗号合并 | ✅ `requestBinaryMetadata(All)` / `setBinaryHeader` / `setBinaryTrailer`；`grpc-status-details-bin` 可直接用 `setBinaryTrailer` |
| `grpc.health.v1.Health` | ✅ `Check` / `Watch`（`zgrpc.health`） |

## 快速开始

```zig
const zgrpc = @import("zgrpc");

fn say(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    try call.sendMessage(msg.payload); // 回显
}

// 路由表 → h2 连接
var routes = [_]zgrpc.grpc.Route{.{ .path = "/echo.Echo/Say", .handler = say }};
var server = zgrpc.grpc.Server{ .routes = &routes, .outer_ctx = &some_ctx };

var conn = zgrpc.h2.Connection.init(allocator, reader, writer, .{});
try conn.serve(server.context(), server.handler());
```

完整可运行例子见 `examples/grpc_echo_server.zig` / `examples/grpc_echo_client.zig`。

## 代码生成（可选）

`tools/proto_gen.zig` 把 proto3 的**子集**编译成 Zig 结构体 + `encode`/`decode` + 路由常量，
生成结果直接跑在 `zgrpc.pb` 上（零依赖）：

```bash
zig build codegen        # 从 examples/echo.proto 生成 examples/echo.pb.zig
# 或直接指定输入输出：
zig run tools/proto_gen.zig -- my.proto my.pb.zig
```

支持：`syntax = "proto3"`、`package`、`message`（标量 / `string` / `bytes` / 枚举 / 嵌套 message 字段 /
**`repeated` 任意元素**：标量、`string`、`bytes`、enum、message / **`map<K,V>`**：key 为整数或 `string` /
**`oneof`**：生成 Zig tagged union / **`optional`**：proto3 显式 presence /
**嵌套 `message`/`enum` 定义**：生成嵌套声明，引用写 `Outer.Inner`）、
`enum`、`service`（含 `stream` 标记，生成路由常量）。

`map` 生成的形式是 `[]const XxxEntry`（`XxxEntry = struct { key: K, value: V }`）+ `get<Field>(k)`
线性查找辅助 —— **零依赖、不引入 hash 容器**，小 map 足够快；条目类型名与描述符里的嵌套类型同名，便于双向对照。

`import` 也能生成（**protoc 风格**）：每个 `.proto` 产出自己的 `.pb.zig`，跨文件引用走
`const common_types = @import("common/types.pb.zig");` + `common_types.Money`。
跑一次 `proto_gen` 会**顺带生成全部依赖**，不用逐个执行：

```bash
zig run tools/proto_gen.zig -- examples/proto_import/shop.proto examples/proto_import/shop.pb.zig
# → examples/proto_import/shop.pb.zig（root）
# → examples/proto_import/common/types.pb.zig（依赖）
```

约定：**输出文件放在 root `.proto` 同一目录**（`@import` 的相对路径按此推导），
import 路径按 **root 所在目录**解析（等价 protoc 的单一 `-I`）。

**不支持**（遇到会报错并给行号，不静默忽略）：`import public` / `import weak`、`reserved`、`extend`、`option`。

## 服务反射（`grpcurl` 无需 `.proto`）

实现标准 `grpc.reflection.v1.ServerReflection`（同时注册 `v1alpha` 兼容老工具），
让 `grpcurl` / `ghz` / Postman 等能**自省**服务定义并直接发请求：
```zig
// 1. 起服务时登记 .proto 源码（嵌入式，运行时不需要 .proto 文件）
var refl = try zgrpc.reflection.Service.init(allocator, &.{
    .{ .file_name = "echo.proto", .source = @embedFile("echo.proto") },
});
defer refl.deinit();

// 2. 两条路由（v1 + v1alpha）；ctx 走 per-route，与业务路由共存互不干扰
var stream_routes = [_]zgrpc.grpc.StreamRoute{
    .{ .path = "/echo.Echo/Chat", .on_message = chatHandler },
    refl.streamRoute(),
    refl.streamRouteV1Alpha(),
};
```

```bash
grpcurl -plaintext 127.0.0.1:18051 list                  # echo.Echo
grpcurl -plaintext 127.0.0.1:18051 describe echo.Echo     # 服务 + 方法（含 stream 标记）
grpcurl -plaintext 127.0.0.1:18051 describe echo.Summary  # 消息（含 enum / 嵌套 / repeated）
grpcurl -plaintext -d '{"value":3,"tag":"hi"}' 127.0.0.1:18051 echo.Echo/Say
```

要点：
- **数据一次解析、长期持有**：初始化时把每条 `.proto` 解析成 `FileDescriptorProto` 并存下
  `file_name` / 符号索引 / 服务名，请求期只做查表，不重复解析。
- **`v1` 与 `v1alpha` 双注册**：新版工具走 `v1`，老版 `grpcurl`/`ghz` 走 `v1alpha`，协议载荷相同。
- **enum 字段不能当 message 编码**：`FieldDescriptorProto.type` 必须与 `type_name` 指向的定义一致
  （enum→14、message→11），写错会让 `protoreflect` 报 `resolved "X", but it is not an message`。

## 客户端本地读超时（内核层，含 Windows）

`Client.Options.timeout_ms` 是**整次调用**的本地超时：等响应头、逐帧读响应体都受它约束。

```zig
var client = zgrpc.client.Client.init(gpa, &sr.interface, &sw.interface, .{
    .authority = "127.0.0.1:50051",
    .timeout_ms = 200,               // 200ms 内没读完就返回 error.Timeout
});
defer client.deinit();
client.enableTimeout(stream, io);     // 启用本地读超时
```

**实现（零线程、零额外内存）**

| 平台 | 方式 |
|---|---|
| Windows | 在 AFD 句柄上自己发起 overlapped `IOCTL_AFD_RECEIVE`，再用 `NtWaitForSingleObject(句柄)` 带时限等待；到点 `NtCancelIoFileEx` 取消并等 IRP 收尾 |
| POSIX | `setsockopt(SO_RCVTIMEO)`，到点 `read` 返回 `EAGAIN` |

实测：服务端接受连接但永不应答时，`timeout_ms = 200` 的调用在 **201~211ms** 返回 `error.Timeout`（`zig build afd-probe` / `timeout-probe`）。

**为什么不是「std 没提供就只能自己起线程」** —— 读 Zig 0.17.1 源码可以看到：`netReadWindows` 发起读时**已经是 AFD overlapped 模式**（`.AfdFlags = .{ .NO_FAST_IO = true, .OVERLAPPED = true }`），只是它把 **event 传成 `null`**、改用 APC 例程等待，所以在 std 内部无法给这次读设时限；而它的状态映射里早就写了 `.IO_TIMEOUT => error.ConnectionTimedOut`。把同一件事换成「自己发起 + 等句柄 signaling」就能拿到原生超时。

顺带记录两个源码事实，避免再走弯路：
- `IpAddress.ConnectOptions.timeout` 字段存在，但 Windows 实现是 `@panic("TODO implement netConnectIpWindows with timeout")`；
- AFD 层**不认** `SO_RCVTIMEO`（`IOCTL_AFD_SOCKOPT` 返回 `STATUS_NOT_SUPPORTED`）—— 那是 Winsock 层的概念，而 Zig 用的是 AFD 句柄。

**使用提示**：超时后连接**不要复用**（协议状态已不完整），重新建连即可；现在 socket 可以安全 `close`（取消后 IRP 已收尾）。
## 性能（实测，Windows / ReleaseFast）

```bash
zig build bench -Doptimize=ReleaseFast          # 编译
.\zig-out\bin\bench.exe --iterations 5000       # 单连接复用、请求 128 字节回显、预热 200 次
```

| 指标 | 数值 |
|---|---|
| QPS | **4762** |
| 延迟 p50 | **168.5 us** |
| 延迟 p95 | **382.3 us** |
| 延迟 p99 | **559.3 us** |
| 慢调用（>5ms） | 7 / 5000（0.14%） |

### 必须关闭 Nagle（`TCP_NODELAY`）

gRPC 的一次调用是 HEADERS + DATA + TRAILERS 多个小包。Nagle 会让后面的小包等前一个的 ACK，与对端的「延迟 ACK」叠加后，会出现**随机 10~200ms 的延迟尖峰**。同一台机器、同一份代码的对比：

| 指标 | 开 Nagle（系统默认） | 关 Nagle |
|---|---|---|
| QPS | 427 | **4762**（11 倍） |
| p50 | 328 us | **168 us** |
| p95 | 14708 us | **382 us**（38 倍） |
| 慢调用占比 | 10.7% | 0.14% |

- **客户端**：`client.enableTimeout(stream, io)` 会自动关闭 Nagle；只需要这一项就调 `client.tuneSocket(stream, io)`；
- **服务端**：accept 之后调一次 `zgrpc.socket_timeout.setNoDelay(io, stream, true)`（见 `examples/grpc_echo_server.zig` 与 `tools/bench.zig`）。

### 富错误：`grpc-status-details-bin`

`grpc-status` 只有一个数字码。要告诉客户端「哪个字段错了 / 缺什么权限 / 多久后重试」，用 `zgrpc.status`：

```zig
try call.setStatusDetails(arena, .{
    .code = 3, // INVALID_ARGUMENT
    .message = "参数不合法",
    .details = &.{
        .{ .type_url = "type.googleapis.com/google.rpc.BadRequest", .value = bad_request_bytes },
        .{ .type_url = "type.googleapis.com/google.rpc.RetryInfo", .value = retry_bytes },
    },
});
call.setStatus(.invalid_argument, "参数不合法");
```

- 自动按 `-bin` 规则做 base64（无 padding）写入 trailers；
- 客户端侧用 `result.binaryMetadata(arena, "grpc-status-details-bin")` 取原始字节，再用 `zgrpc.status.decode` 解析；
- 未知字段跳过、截断数据返回错误而非 panic（protobuf 前向兼容）。
### 外部工具验证（`ghz`，业界标准 gRPC 压测器）

以上数字是自己进程内测的（客户端与服务端抢同一批 CPU）。用 Go 的 `ghz` 作**独立进程**压测同一份服务端：

```bash
zig build bench -Doptimize=ReleaseFast
.\zig-out\bin\bench.exe --serve-only          # 只起服务端（注册 /echo.Echo/Say）
ghz --insecure --proto examples/echo.proto --call echo.Echo/Say -c 20 -n 6000 127.0.0.1:18054
```

| 指标 | 数值 |
|---|---|
| Requests/sec | **10,452** |
| 平均延迟 | 1.76 ms |
| p50 / p95 / p99 | 2.00 / 3.00 / 3.51 ms |
| 成功 / 总数 | 6000 / 6000（100% OK） |

### 测试方法与局限（重要）

上面的数字分两类，**不能混为一谈**：

| 方式 | 典型数字 | 局限 |
|---|---|---|
| **自测**（客户端与服务端同进程） | 16 连接 **23,268 QPS**、p50 168us | 两端抢同一批 CPU，客户端开销被低估 → 只能当**乐观上界** |
| **ghz 独立进程**（业界标准） | 20 并发 **4,525 QPS**、p50 4.01 ms（两端各绑 6 核） | 客户端是 Go + gRPC 栈，**客户端自身可能就是瓶颈** |

**瓶颈不在服务端的实测证据**（Little's Law 自洽）：

| 并发 | 实测 QPS | p50 | 理论 = 并发 ÷ 延迟 |
|---|---|---|---|
| 20 | 4,525 | 4.01 ms | 20 ÷ 0.00436 = 4,587 |
| 200（不绑核） | 4,147 | 44.87 ms | 200 ÷ 0.04487 = 4,458 |
| 200（绑 6 核） | 2,561 | 61.63 ms | 200 ÷ 0.07572 = 2,641 |

三种配置**都撞在约 4,500 QPS**，且理论值与实测吻合 → 说明是同一个**非服务端**瓶颈（`ghz` 客户端 / loopback 协议栈）。
绑核后反而更慢（4,525 → 2,561），进一步说明限制来自客户端与核数，而非服务端处理能力。

**要拿到真实结果必须多机**（本仓库作者只有一台机器，以下为待执行清单）：

1. 两台机器（同交换机或直连），服务端独占一台、不绑核；
2. 压测端 `ghz -c 200 -z 60s`，绑到压测机全部核心；
3. **同时记录两端 CPU/内存**，确认压测机 CPU < 80%，否则客户端就是瓶颈；
4. 对比 Linux 与 Windows —— zio/`io_uring` 的优势只有在 Linux 才可能显现，本机（Windows）无法验证；
5. 若要对比 zio，用 `examples/with-zio/` 的同一份代码，只换 `Io` 实例，其余保持不变。
### 并发连接：没有人为上限

服务端模型是「**每连接一个线程**」（栈已压到 128 KiB），不存在硬编码的连接数上限。实测（本机 Windows、单进程、请求 128 字节回显）：

| 并发连接 | 16 | 64 | 128 | 256 | 512 |
|---|---|---|---|---|---|
| 总 QPS | **23,268** | 20,629 | 15,940 | 14,415 | 10,647 |
| 平均延迟 | 354 us | — | 376 us | 358 us | 372 us |

- **吞吐峰值在 16~64 连接**；再往上 QPS 缓慢下降，原因是线程变多后的调度开销，不是有连接数限制；
- 512 连接全部成功、零失败；
- 真正的天花板是**系统资源**：线程数 × 128 KiB 栈、进程句柄数、客户端侧 TCP 端口；
- 若单机要长期维持**数千**并发连接，才需要换「协程/事件驱动」模型（`examples/with-zio/` 已给出 zio 后端用法，Windows 走 IOCP）。
### 后端选择：std 还是 zio？

`zio` 是成熟的 Zig 异步运行时（Linux `io_uring` / Windows `IOCP` / macOS `kqueue`，MIT），它**完整实现了 `std.Io`**，所以 zgrpc 一行不用改就能跑在它上面（见 `examples/with-zio/`）。本机 Windows 实测对比：

| 并发连接 | std（每连接一线程） | zio（协程 + 事件循环） |
|---|---|---|
| 1 | **4762** QPS（p50 168us） | 2502 QPS（p50 238us） |
| 4 | **13221** QPS | 10401 QPS |
| 16 | **23268** QPS | 21761 QPS |
| 64 | **20629** QPS | 20467 QPS |

结论：**Windows 上 std 方案全场景不输 zio**（单连接快 1.9 倍，64 连接基本持平），所以默认保持零依赖的 std 实现；需要 Linux `io_uring` 或更大并发时可以换 zio 后端 —— 传 `rt.io()` 即可，库本身不依赖它。
### HPACK Huffman 收益

典型头部字符串样本 93 字节 → **69 字节（省 25.8%）**；实现上按「哪种更短用哪种」自动选择，因此短值（如 `POST`）不会被 Huffman 撑大。

> 与 HTTP/1.1 的 `wrk` 基准（如 lighting 的 710k QPS ping）**不可直接比**：gRPC 每个请求多一层 HPACK 编解码、长度前缀分帧与 trailers 往返。
## 构建与验证

```bash
zig build test           # 50 个单元/端到端测试（含 9 个 h2 健壮性测试）
zig build echo-server    # 监听 127.0.0.1:18051
zig build echo-client    # 并发双流 + bidi + 健康检查 + grpc-timeout 超时 + -bin metadata + grpc-web-text + pb 结构化往返
```

实测（2026-11-12，Windows，Zig 0.17.1）：

- `tools\regression.ps1` → 构建 PASS / tests 50（下限 50）/ 冒烟 PASS / **四向 Rust 对拍 PASS**，exit 0；
- 真实 socket：unary **并发双流**均 `grpc-status=0`；**bidi** 3 条请求 → 3 条响应（`chat:a/b/c`）；
  **健康检查** `grpc-status=0` + `ServingStatus=1`；**grpc-timeout 生效**：80ms 调睡 300ms 的方法 → `grpc-status=4`；
  **`-bin` metadata 往返**：`x-token-bin` → 响应头 `x-echo-bin` + trailer `x-note`。
- 单测含真实 h2 帧的 **405 / 415** 回归（`roundTripStatus` 走 preface → SETTINGS → HEADERS → DATA）。

## 为什么不直接复用现成 Zig 库

| 候选 | 星 | 最低 Zig | 依赖 | 结论 |
|---|---|---|---|---|
| `fanyang89/grpc-lite` | 7 | 0.17.0 | 13 个，含 nghttp2(C)、mbedtls、c-ares、zio、libxev | 独立项目可用，但非「零依赖 Zig 实现」 |
| `ziglana/gRPC-zig` | 146 | 未声明 | spice + zlib(C) | 可用性未实测 |
| `hendriknielaender/http2.zig` | 48 | 0.17.0 | boring(TLS)、turboapi-core | 只有 h2，无 gRPC |
| `nevindra/nilo` | 13 | 0.17.0 | zio、pg.zig、tls.zig、libdeflate、llhttp、zstd | 整套框架 |
| `zaxified/zig-libs` | 11 | 0.17.1 | 无 | 含 h2 协议但无 gRPC |

Zig 标准库 `std.http` 只有 HTTP/1.1，**没有 h2、没有 hpack**（`lib/std/http` 下仅
`Server.zig` / `Client.zig` / `HeadParser.zig` / `ChunkParser.zig`）。

## 部署

对外一律**反代终止 TLS**、后端 h2c；nginx / Caddy 样例、并发上限、超时语义、健康检查、上线自检清单见 [`docs/部署.md`](docs/部署.md)。

## 文档

- `docs/决策记录.md` —— 为什么独立成库、关键取舍与实测数据
