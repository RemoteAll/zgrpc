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
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    if (msg.compressed) {
        call.setStatus(.unimplemented, "本示例不支持压缩");
        return;
    }
    std.debug.print("[zgrpc] 收到 {d} 字节：{s}\n", .{ msg.payload.len, msg.payload });
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

pub fn main() !void {
    // 多线程服务端：Io 必须用线程安全分配器构造（page_allocator）
    const allocator = std.heap.page_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var routes = [_]zgrpc.grpc.Route{
        .{ .path = "/echo.Echo/Say", .handler = sayHandler },
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

    std.debug.print("[zgrpc] gRPC(h2c) 监听 127.0.0.1:{d}，方法 /echo.Echo/Say（unary）、/echo.Echo/Chat（bidi）\n", .{port});

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
