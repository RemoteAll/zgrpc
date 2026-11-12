//! gRPC echo 服务端示例（明文 h2c，监听 127.0.0.1:18051）。
//!
//! 运行：`zig build echo-server`
//! 调用：`zig build echo-client`（同仓库客户端示例），或任意支持 h2c 的 gRPC 客户端。

const std = @import("std");
const zgrpc = @import("zgrpc");

const port: u16 = 18051;

/// 方法 `/echo.Echo/Say`：把请求消息原样回显。
fn sayHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    std.debug.print("[zgrpc] 收到 {d} 字节（压缩={}）：{s}\n", .{ msg.payload.len, msg.compressed, msg.payload });

    // 自定义二进制 metadata（`-bin` 结尾）：值已在协议层 base64 解码
    if (try call.requestBinaryMetadata(arena, "x-token-bin")) |token| {
        try call.setBinaryHeader("x-echo-bin", token); // 自动 base64（无 padding）
        try call.setTrailer("x-note", "metadata-ok");
    }
    try call.sendMessage(msg.payload);
}

/// 方法 `/echo.Echo/Chat`：**bidi** —— 每收到一条消息就即时回一条（带前缀）。
fn chatHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, msg: []const u8) anyerror!void {
    _ = ctx;
    var buf: [512]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "chat:{s}", .{msg});
    std.debug.print("[zgrpc] 流式收到 {d} 字节：{s}\n", .{ msg.len, msg });
    try call.sendMessage(out);
}

/// 方法 `/echo.Echo/Slow`：故意睡 300ms，用于验证 `grpc-timeout` → `DEADLINE_EXCEEDED`。
fn slowHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    std.debug.print("[zgrpc] /echo.Echo/Slow：睡 300ms（客户端 grpc-timeout 小于它就会超时）\n", .{});
    zgrpc.clock.sleepNs(300 * std.time.ns_per_ms);
    try call.sendMessage("slow done");
}

/// 标准 `grpc.health.v1.Health` 的业务判定：空服务名 / echo.Echo 视为健康。
fn healthCheck(ctx: ?*anyopaque, service: []const u8) zgrpc.health.ServingStatus {
    _ = ctx;
    if (service.len == 0 or std.mem.eql(u8, service, "echo.Echo")) return .serving;
    return .service_unknown;
}

/// 调用计数（演示 `pb` 模块的 uint64 回填）。
var bump_calls = std.atomic.Value(u64).init(0);

/// 方法 `/echo.Echo/Bump`：演示 **protobuf 结构化 message**（`zgrpc.pb`）。
///
/// ```proto
/// message BumpRequest  { int32  value = 1; string tag = 2; }
/// message BumpResponse { int32  value = 1; string tag = 2; uint64 calls = 3; }
/// ```
fn bumpHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "Bump：缺少请求消息");
        return;
    };

    var value: i32 = 0;
    var tag: []const u8 = "";
    var r = zgrpc.pb.Reader{ .data = msg.payload };
    while (r.next()) |f| switch (f.number) {
        1 => value = f.asInt32(),
        2 => tag = f.asString(),
        else => {},
    };

    var w = zgrpc.pb.Writer.init(arena);
    try w.int32(1, value + 1);
    try w.string(2, tag);
    try w.uint64(3, bump_calls.fetchAdd(1, .monotonic) + 1);
    try call.sendMessage(w.bytes());
}

pub fn main() !void {
    // 多线程服务端：Io 必须用线程安全分配器构造（page_allocator）
    const allocator = std.heap.page_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var health_checker = zgrpc.health.Checker{ .check = healthCheck };
    var health_svc = zgrpc.health.Service{ .checker = &health_checker };
    const health_routes = health_svc.routes();

    var routes = [_]zgrpc.grpc.Route{
        .{ .path = "/echo.Echo/Say", .handler = sayHandler },
        .{ .path = "/echo.Echo/Slow", .handler = slowHandler },
        .{ .path = "/echo.Echo/Bump", .handler = bumpHandler },
        health_routes[0],
        health_routes[1],
    };
    var stream_routes = [_]zgrpc.grpc.StreamRoute{
        .{ .path = "/echo.Echo/Chat", .on_message = chatHandler },
    };
    var dummy: u8 = 0;
    var server = zgrpc.grpc.Server{
        .routes = &routes,
        .stream_routes = &stream_routes,
        .outer_ctx = &dummy,
    };

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var listener = try addr.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);

    std.debug.print("[zgrpc] gRPC(h2c) 监听 127.0.0.1:{d}，方法 /echo.Echo/Say（unary）、/echo.Echo/Chat（bidi）、" ++
        "/echo.Echo/Slow（超时验证）、/grpc.health.v1.Health/{{Check,Watch}}\n", .{port});

    while (true) {
        const stream = listener.accept(io) catch |e| {
            std.debug.print("[zgrpc] accept 失败：{s}\n", .{@errorName(e)});
            continue;
        };
        const th = std.Thread.spawn(.{}, handleConn, .{ allocator, io, stream, &server }) catch |e| {
            std.debug.print("[zgrpc] 创建连接线程失败：{s}\n", .{@errorName(e)});
            stream.close(io);
            continue;
        };
        th.detach();
    }
}

fn handleConn(
    allocator: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,
    server: *zgrpc.grpc.Server,
) void {
    defer stream.close(io);
    var recv_buf: [16 * 1024]u8 = undefined;
    var send_buf: [16 * 1024]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);

    var conn = zgrpc.h2.Connection.init(allocator, &sr.interface, &sw.interface, .{});
    defer conn.deinit();

    // 流式模式：同时兼容整请求路由（unary）与流式路由（bidi/客户端流）
    conn.serveStreaming(server.streamHooks()) catch |e| {
        if (e != error.ConnectionClosed) {
            std.debug.print("[zgrpc] 连接异常结束：{s}\n", .{@errorName(e)});
        }
    };
}
