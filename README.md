# zgrpc

纯 Zig、**零依赖**的 gRPC / HTTP/2 协议栈（Zig 0.17.1+）。

面向「需要 gRPC 的项目」独立成库，**不放进 zzig**：h2 + HPACK + gRPC 是一整套协议栈，
不该让所有用 zzig 的项目都背上这份编译与维护成本；本库也不依赖 zzig，两边互不绑死。

## 现状

| 模块 | 能力 | 说明 |
|---|---|---|
| `hpack` | 头部压缩（RFC 7541） | **解码完整**（静态表 + 动态表 + Huffman）；编码简化（静态命中走索引，其余明文 literal） |
| `h2` | HTTP/2 明文 h2c（prior-knowledge）服务端 | SETTINGS/ACK、HEADERS/CONTINUATION（含 PADDED）、DATA（含 PADDED + 流控回补）、WINDOW_UPDATE、PING、RST_STREAM、GOAWAY |
| `grpc` | gRPC 语义层 | 消息分帧、状态码、路由、unary 与**服务端流式**（`sendMessage` 可多次） |

**未做**（明确列出，避免误解为完整实现）：TLS/ALPN、服务端推送、优先级调度、
并发流（v1 串行，并发新流回 `RST_STREAM(REFUSED_STREAM)`，客户端可安全重试）、
压缩（`grpc-encoding`）、客户端流/bidi 背压、`grpc-timeout`、protobuf 编解码（业务自带）。

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
zig build test           # 13 个单元/端到端测试
zig build echo-server    # 监听 127.0.0.1:18051
zig build echo-client    # 跑一次真实 TCP unary 调用
```

实测（2026-11-12，Windows，Zig 0.17.1）：

- `zig build test --summary all` → **13/13 tests passed**，exit 0；
- 真实 socket：`echo-client` → `grpc-status=0 响应=hello-grpc`，exit 0；
  服务端日志 `收到 10 字节：hello-grpc`。

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
