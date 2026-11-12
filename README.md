# zgrpc

纯 Zig、**零依赖**的 gRPC / HTTP/2 协议栈（Zig 0.17.1+）。

面向「需要 gRPC 的项目」独立成库，**不放进 zzig**：h2 + HPACK + gRPC 是一整套协议栈，
不该让所有用 zzig 的项目都背上这份编译与维护成本；本库也不依赖 zzig，两边互不绑死。

## 现状

| 模块 | 能力 | 说明 |
|---|---|---|
| `hpack` | 头部压缩（RFC 7541） | **解码完整**（静态表 + 动态表 + Huffman）；编码简化（静态命中走索引，其余明文 literal） |
| `h2` | HTTP/2 明文 h2c（prior-knowledge）服务端 | SETTINGS/ACK、HEADERS/CONTINUATION（含 PADDED/PRIORITY）、DATA（含 PADDED + 流控回补）、WINDOW_UPDATE（连接级 + 每流）、PING、RST_STREAM、GOAWAY；**多路复用**（多流并存、发送阻塞时帧入 pending 队列） |
| `grpc` | gRPC 语义层 | 消息分帧、状态码、路由、unary / 服务端流式 / **客户端流与 bidi**（`server.stream_routes` + `streamHooks`） |
| `client` | gRPC 客户端（h2c） | 握手、`unary`、**`callMany`（同一连接并发多条流）**、`clientStream`（客户端流/bidi），按 stream_id 归位、回补窗口 |

**未做**（明确列出，避免误解为完整实现）：TLS/ALPN、服务端推送、优先级调度、
handler 并行执行（业务仍同步串行，串行只是执行顺序，**不会因此拒绝并发流**）、
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
zig build test           # 17 个单元/端到端测试
zig build echo-server    # 监听 127.0.0.1:18051
zig build echo-client    # 用 zgrpc.client 在同一 TCP 上并发两个 unary 调用
```

实测（2026-11-12，Windows，Zig 0.17.1）：

- `tools\regression.ps1` → 构建 PASS / tests 19（下限 19）/ 冒烟 PASS，exit 0；
- 真实 socket：unary **并发双流**均 `grpc-status=0`；**bidi** 3 条请求 → 3 条响应（`chat:a/b/c`），exit 0。

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
