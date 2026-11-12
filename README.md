# zgrpc

纯 Zig、**零依赖**的 gRPC / HTTP/2 协议栈（Zig 0.17.1+）。

面向「需要 gRPC 的项目」独立成库，**不放进 zzig**：h2 + HPACK + gRPC 是一整套协议栈，
不该让所有用 zzig 的项目都背上这份编译与维护成本；本库也不依赖 zzig，两边互不绑死。

## 现状

| 模块 | 能力 | 说明 |
|---|---|---|
| `hpack` | 头部压缩（RFC 7541） | **解码完整**（静态表 + 动态表 + Huffman）；编码简化（静态命中走索引，其余明文 literal） |
| `h2` | HTTP/2 明文 h2c（prior-knowledge）服务端 | SETTINGS/ACK、HEADERS/CONTINUATION（含 PADDED/PRIORITY）、DATA（含 PADDED + 流控回补）、WINDOW_UPDATE（连接级 + 每流）、PING、RST_STREAM、GOAWAY；**多路复用**（多流并存、发送阻塞时帧入 pending 队列） |
| `grpc` | gRPC 语义层 | 消息分帧、状态码、路由、unary / 服务端流式 / **客户端流与 bidi**（`server.stream_routes` + `streamHooks`）、**gRPC-Web**、**gzip 消息压缩**、**`grpc-timeout` 截止时间**、`grpc-message` 的 percent-encoding、非 POST→**405** / content-type 不符→**415** |
| `client` | gRPC 客户端（h2c） | 握手、`unary`、**`callMany`（同一连接并发多条流）**、`clientStream`（客户端流/bidi），按 stream_id 归位、回补窗口；发 `grpc-timeout` / `user-agent`，按规范**合成** Status 并解 percent-encoding |
| `health` | 标准 `grpc.health.v1.Health` | `Check`（unary）/ `Watch`（流式，同步模型下默认推一次）；最简 protobuf 手写编解码，零依赖 |
| `gzip` / `clock` | 压缩与单调时钟 | `gzip` 走 `std.compress.flate`；`clock` 直问 OS（`QueryPerformanceCounter` / `CLOCK_MONOTONIC`），服务 `grpc-timeout` |

**未做**（明确列出，避免误解为完整实现）：TLS/ALPN、服务端推送、优先级调度、
handler 并行执行（业务仍同步串行，串行只是执行顺序，**不会因此拒绝并发流**）、
客户端流/bidi 背压、`grpc-timeout` 的**强制中断**（本层只判超时并回 `DEADLINE_EXCEEDED`，
不会打断已阻塞在业务里的调用）、`-bin` 二进制 metadata、`grpc-status-details-bin`、
`application/grpc-web-text`（base64 文本流）、protobuf 编解码（业务自带）。

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
| 建议 `user-agent` 用 `grpc-<lang>/<ver>` | ✅ 默认 `grpc-zgrpc/0.1.0` |
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

## 构建与验证

```bash
zig build test           # 31 个单元/端到端测试
zig build echo-server    # 监听 127.0.0.1:18051
zig build echo-client    # 并发双流 + bidi + 健康检查 + grpc-timeout 超时
```

实测（2026-11-12，Windows，Zig 0.17.1）：

- `tools\regression.ps1` → 构建 PASS / tests 31（下限 31）/ 冒烟 PASS / **三向 Rust 对拍 PASS**，exit 0；
- 真实 socket：unary **并发双流**均 `grpc-status=0`；**bidi** 3 条请求 → 3 条响应（`chat:a/b/c`）；
  **健康检查** `grpc-status=0` + `ServingStatus=1`；**grpc-timeout 生效**：80ms 调睡 300ms 的方法 → `grpc-status=4`。
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

## 文档

- `docs/决策记录.md` —— 为什么独立成库、关键取舍与实测数据
