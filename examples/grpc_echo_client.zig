//! gRPC echo 客户端示例：用 `zgrpc.client` 在**同一条 TCP 连接**上并发发起两个 unary 调用。
//!
//! 运行（需先起 `zig build echo-server`）：`zig build echo-client`
//!
//! 之前这里是 200 行手写帧的样例；客户端库抽出来后只剩几十行 —— 手写帧的版本已由
//! `src/client.zig` 的单测覆盖（含流按 id 归位、非 0 状态码判定）。

const std = @import("std");
const zgrpc = @import("zgrpc");

const port: u16 = 18051;
const method = "/echo.Echo/Say";

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var stream = addr.connect(io, .{ .mode = .stream }) catch |e| {
        std.debug.print(
            "[zgrpc] 连接 127.0.0.1:{d} 失败（{s}）——请先运行 `zig build echo-server`\n",
            .{ port, @errorName(e) },
        );
        return e;
    };
    defer stream.close(io);

    var recv_buf: [16 * 1024]u8 = undefined;
    var send_buf: [16 * 1024]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);

    var client = zgrpc.client.Client.init(
        allocator,
        &sr.interface,
        &sw.interface,
        .{ .authority = "127.0.0.1" },
    );
    defer client.deinit();

    // 两条流并发发起（不同 stream_id，同一条连接）
    const results = try client.callMany(&.{
        .{ .method = method, .payload = "stream-one" },
        .{ .method = method, .payload = "stream-three" },
    }, a);

    var ok = true;
    for (results) |r| {
        const text = if (r.messages.len > 0) r.messages[0] else "<无消息>";
        std.debug.print("[zgrpc] 流 {d}：grpc-status={d} 响应={s}\n", .{ r.stream_id, r.status, text });
        if (!r.ok()) ok = false;
    }
    if (!ok) {
        std.debug.print("[zgrpc] 存在失败调用\n", .{});
        return error.GrpcStatusNotOk;
    }
    std.debug.print("[zgrpc] 两条并发流均成功（同一条 TCP 连接）\n", .{});

    // ---- 客户端流 / bidi：一次发 3 条消息，服务端每条即时回一条 ----
    const chat = try client.clientStream("/echo.Echo/Chat", &.{ "a", "b", "c" }, a);
    var n: usize = 0;
    for (chat.messages) |m| {
        std.debug.print("[zgrpc] bidi 响应[{d}]={s}\n", .{ n, m });
        n += 1;
    }
    if (!chat.ok() or chat.messages.len != 3) {
        std.debug.print("[zgrpc] bidi 调用失败：status={d} 消息数={d}\n", .{ chat.status, chat.messages.len });
        return error.GrpcStatusNotOk;
    }
    std.debug.print("[zgrpc] bidi 流成功：3 条请求 → 3 条响应\n", .{});
}
