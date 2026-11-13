//! gRPC echo 客户端示例：用 `zgrpc.client` 在**同一条 TCP 连接**上并发发起两个 unary 调用。
//!
//! 运行（需先起 `zig build echo-server`）：`zig build echo-client`
//!
//! 之前这里是 200 行手写帧的样例；客户端库抽出来后只剩几十行 —— 手写帧的版本已由
//! `src/client.zig` 的单测覆盖（含流按 id 归位、非 0 状态码判定）。

const std = @import("std");
const zgrpc = @import("zgrpc");

const default_port: u16 = 18051;
const method = "/echo.Echo/Say";
const chat_method = "/echo.Echo/Chat";

const Args = struct {
    port: u16 = default_port,
    /// 只跑跨实现共有的 echo 方法（对拍 Rust 服务端时用；它没有 health / Slow）
    basic: bool = false,
    /// >0 时额外发一条该字节数的消息并校验回显（验证 DATA 按 MAX_FRAME_SIZE 分片）
    big: usize = 0,
};

/// 端口取首个命令行参数（默认 18051）—— 便于连 Rust 对拍服务端（`zig-out/bin/echo-client 18061 --basic`）。
/// 支持 `--basic` 与 `--big <字节数>`。
fn parseArgs(init: std.process.Init) Args {
    var out: Args = .{};
    var it = std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa) catch return out;
    defer it.deinit();
    _ = it.skip(); // 程序名
    while (it.next()) |a| {
        if (std.mem.eql(u8, a, "--basic")) {
            out.basic = true;
        } else if (std.mem.eql(u8, a, "--big")) {
            if (it.next()) |n| {
                if (std.fmt.parseInt(usize, n, 10)) |v| {
                    out.big = v;
                } else |_| {}
            }
        } else if (std.fmt.parseInt(u16, a, 10)) |p| {
            out.port = p;
        } else |_| {}
    }
    return out;
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const args = parseArgs(init);
    const port = args.port;

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
    const chat = try client.clientStream(chat_method, &.{ "a", "b", "c" }, a);
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

    // ---- 大消息往返（--big N）：验证 DATA 按对端 MAX_FRAME_SIZE 分片 ----
    if (args.big > 0) {
        const payload = try a.alloc(u8, args.big);
        for (payload, 0..) |*b, i| b.* = @intCast(i % 251);
        const big_res = try client.callMany(&.{
            .{ .method = method, .payload = payload },
        }, a);
        const got_len = if (big_res[0].messages.len > 0) big_res[0].messages[0].len else 0;
        if (!big_res[0].ok() or big_res[0].messages.len != 1 or
            !std.mem.eql(u8, big_res[0].messages[0], payload))
        {
            std.debug.print("[zgrpc] 大消息回显失败：status={d} 收到 {d} 字节（期望 {d}）message={s}\n", .{
                big_res[0].status, got_len, args.big, big_res[0].message,
            });
            return error.GrpcStatusNotOk;
        }
        std.debug.print("[zgrpc] 大消息 {d} 字节完整往返（DATA 已按 MAX_FRAME_SIZE 分片）\n", .{args.big});
    }

    if (args.basic) {
        // 对拍外部服务端（如 Rust hyper）时只跑共有方法：health / Slow 是 zgrpc 自家扩展
        std.debug.print("[zgrpc] --basic：跳过 health 与 grpc-timeout 段\n", .{});
        return;
    }
    // ---- 标准健康检查 `grpc.health.v1.Health/Check`（空请求 = 整体健康）----
    const hc = try client.unary(zgrpc.health.check_path, "", a);
    if (!hc.ok() or hc.messages.len != 1 or
        !std.mem.eql(u8, hc.messages[0], &.{ 0x08, 0x01 }))
    {
        std.debug.print("[zgrpc] 健康检查失败：status={d} message={s} 响应字节数={d}\n", .{
            hc.status, hc.message, if (hc.messages.len > 0) hc.messages[0].len else 0,
        });
        return error.GrpcStatusNotOk;
    }
    std.debug.print("[zgrpc] 健康检查 SERVING（grpc-status=0，ServingStatus=1）\n", .{});

    // ---- `grpc-timeout`：80ms 调一个睡 300ms 的方法，期望 DEADLINE_EXCEEDED(4) ----
    const slow = try client.callMany(&.{
        .{ .method = "/echo.Echo/Slow", .timeout_ms = 80 },
    }, a);
    if (slow[0].status != @backingInt(zgrpc.grpc.Status.deadline_exceeded)) {
        std.debug.print("[zgrpc] 超时验证失败：期望 grpc-status=4，实际 {d}（message={s}）\n", .{
            slow[0].status, slow[0].message,
        });
        return error.GrpcStatusNotOk;
    }
    std.debug.print("[zgrpc] grpc-timeout 生效：grpc-status=4（{s}）\n", .{slow[0].message});

    // ---- 自定义 metadata（`-bin`）：发 base64，服务端解码后回显到响应头 + trailer ----
    const token = try zgrpc.grpc.encodeBase64NoPad(a, "hello-bin");
    const md = try client.callMany(&.{
        .{ .method = method, .payload = "with-metadata", .metadata = &.{
            .{ .name = "x-token-bin", .value = token },
        } },
    }, a);
    if (!md[0].ok()) {
        std.debug.print("[zgrpc] metadata 调用失败：status={d} message={s}\n", .{ md[0].status, md[0].message });
        return error.GrpcStatusNotOk;
    }
    const echoed = (try md[0].binaryMetadata(a, "x-echo-bin")) orelse {
        std.debug.print("[zgrpc] 响应缺少 x-echo-bin\n", .{});
        return error.MetadataMissing;
    };
    if (!std.mem.eql(u8, echoed, "hello-bin")) {
        std.debug.print("[zgrpc] x-echo-bin 回显不符：{s}\n", .{echoed});
        return error.MetadataMismatch;
    }
    const note = md[0].trailer("x-note") orelse "";
    if (!std.mem.eql(u8, note, "metadata-ok")) {
        std.debug.print("[zgrpc] trailers 里的 x-note 不符：{s}\n", .{note});
        return error.MetadataMismatch;
    }
    std.debug.print("[zgrpc] -bin metadata 往返成功：x-echo-bin={s} / x-note={s}\n", .{ echoed, note });

    // ---- gRPC-Web 文本模式（base64，浏览器 XHR 回退用）：另开一条连接 ----
    var stream2 = addr.connect(io, .{ .mode = .stream }) catch |e| {
        std.debug.print("[zgrpc] web-text 连接失败：{s}\n", .{@errorName(e)});
        return e;
    };
    defer stream2.close(io);
    var recv_buf2: [16 * 1024]u8 = undefined;
    var send_buf2: [16 * 1024]u8 = undefined;
    var sr2 = stream2.reader(io, &recv_buf2);
    var sw2 = stream2.writer(io, &send_buf2);
    var text_client = zgrpc.client.Client.init(allocator, &sr2.interface, &sw2.interface, .{
        .authority = "127.0.0.1",
        .content_type = .grpc_web_text,
    });
    defer text_client.deinit();

    const wt = try text_client.unary(method, "web-text-hello", a);
    if (!wt.ok() or wt.messages.len != 1 or !std.mem.eql(u8, wt.messages[0], "web-text-hello")) {
        std.debug.print("[zgrpc] grpc-web-text 往返失败：status={d} message={s} 消息数={d}\n", .{
            wt.status, wt.message, wt.messages.len,
        });
        return error.GrpcStatusNotOk;
    }
    std.debug.print("[zgrpc] grpc-web-text 往返成功：{s}（content-type={s}）\n", .{
        wt.messages[0], wt.header("content-type") orelse "?",
    });

    // ---- protobuf 结构化 message（`zgrpc.pb`）：手写 20 行编解码即可 ----
    var req_writer = zgrpc.pb.Writer.init(a);
    try req_writer.int32(1, 41);
    try req_writer.string(2, "pb-demo");
    const bump = try client.unary("/echo.Echo/Bump", req_writer.bytes(), a);
    if (!bump.ok() or bump.messages.len != 1) {
        std.debug.print("[zgrpc] Bump 调用失败：status={d} message={s} 消息数={d}\n", .{
            bump.status, bump.message, bump.messages.len,
        });
        return error.GrpcStatusNotOk;
    }
    var got_value: i32 = 0;
    var got_tag: []const u8 = "";
    var calls: u64 = 0;
    var rd = zgrpc.pb.Reader{ .data = bump.messages[0] };
    while (rd.next()) |f| switch (f.number) {
        1 => got_value = f.asInt32(),
        2 => got_tag = f.asString(),
        3 => calls = f.asUint64(),
        else => {},
    };
    if (got_value != 42 or !std.mem.eql(u8, got_tag, "pb-demo") or calls == 0) {
        std.debug.print("[zgrpc] Bump 响应不符：value={d} tag={s} calls={d}\n", .{ got_value, got_tag, calls });
        return error.MetadataMismatch;
    }
    std.debug.print("[zgrpc] pb 结构化往返成功：value 41→{d}，tag={s}，calls={d}\n", .{
        got_value, got_tag, calls,
    });
}
