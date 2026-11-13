//! 把 zgrpc 的 `std.Io` 换成 **zio 运行时**，验证「零依赖库 + 可选异步后端」这条路。
//!
//! 运行：`zig build run -- 3000`
//!
//! zio 实现了完整的 `std.Io` 接口（README 原话：*"Zig 0.17 code written against std.Io
//! runs on zio unmodified"*），因此 **zgrpc 一行都不用改**：
//!
//! - Linux：`io_uring`（不可用时自动退 `epoll`）
//! - Windows：`IOCP`
//! - macOS / BSD：`kqueue`
//!
//! 我们只是把 `init.io` 换成 `rt.io()`。对比数据请与 `tools/bench.zig`（std 后端）一起看。

const std = @import("std");
const zgrpc = @import("zgrpc");
const zio = @import("zio");

/// 让 `std.debug.print` 不阻塞事件循环（zio README 推荐，可选）。
pub const std_options_debug_io = zio.debug_io;

const port: u16 = 18055;
const method = "/bench.Bench/Echo";
const warmup: usize = 200;

fn sayHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    try call.sendMessage(msg.payload);
}

fn handleClient(
    io: std.Io,
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
    server: *zgrpc.grpc.Server,
) std.Io.Cancelable!void {
    zgrpc.grpc.Server.serveConn(server, allocator, io, stream);
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    // ---------- 换成 zio 运行时 ----------
    const rt = try zio.Runtime.init(gpa, .{ .executors = .auto });
    defer rt.deinit();
    const io = rt.io();

    var iterations: usize = 3000;
    var conns: usize = 1;
    var listen_port: u16 = port;
    // `serve` 模式：只起服务端（给外部压测工具用），与主 bench 的 `--serve-only` 等价。
    // 这样可以用**同一个客户端**打线程版与事件循环版，否则两边客户端不同、数字不可比。
    var serve_only = false;
    {
        var it = std.process.Args.Iterator.initAllocator(init.minimal.args, gpa) catch return;
        defer it.deinit();
        _ = it.skip();
        if (it.next()) |a| {
            if (std.mem.eql(u8, a, "serve")) {
                serve_only = true;
            } else {
                iterations = std.fmt.parseInt(usize, a, 10) catch iterations;
            }
        }
        if (it.next()) |a| {
            if (serve_only) {
                listen_port = std.fmt.parseInt(u16, a, 10) catch listen_port;
            } else {
                conns = std.fmt.parseInt(usize, a, 10) catch conns;
            }
        }
        if (it.next()) |a| listen_port = std.fmt.parseInt(u16, a, 10) catch listen_port;
    }
    if (conns == 0) conns = 1;
    if (conns > 64) conns = 64;

    // ---------- 服务端 ----------
    var dummy: u8 = 0;
    var routes = [_]zgrpc.grpc.Route{
        .{ .path = method, .handler = sayHandler },
    };
    var server = zgrpc.grpc.Server{ .routes = &routes, .outer_ctx = &dummy };

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", listen_port);
    var listener = try addr.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);

    // 服务端作为**运行时里的一个任务**跑（不能用裸 std.Thread：
    // zio 的 accept/读写在运行时上下文里才会挂起协程，跑到外部线程上会卡死）
    var srv_group: std.Io.Group = .init;
    srv_group.async(io, serveLoop, .{ io, gpa, &listener, &server });

    // ---------- 只起服务端：给外部压测工具用（与 bench --serve-only 等价） ----------
    if (serve_only) {
        std.debug.print("zio server listening on 127.0.0.1:{d}\n", .{listen_port});
        // 让主任务等 serveLoop 永不完结（await 后才不会在 main 返回时把运行时装掉）
        srv_group.await(io) catch {};
        return;
    }

    // ---------- 客户端：N 个**协程**各持一条连接（zio 下协程远轻于线程） ----------
    const payload = gpa.alloc(u8, 128) catch return;
    defer gpa.free(payload);
    @memset(payload, 'x');

    var stats: [64]WorkerStat = undefined;
    for (0..conns) |i| stats[i] = .{ .lat_ns = gpa.alloc(u64, iterations) catch return };
    defer for (stats[0..conns]) |st| gpa.free(st.lat_ns);

    var cgroup: std.Io.Group = .init;
    const t0 = zgrpc.clock.monotonicNs();
    for (0..conns) |i| {
        cgroup.concurrent(io, clientWorker, .{ io, gpa, addr, payload, iterations, &stats[i] }) catch {};
    }
    cgroup.await(io) catch {};
    const total: u64 = @intCast(zgrpc.clock.monotonicNs() - t0);

    var done: usize = 0;
    var sum: u64 = 0;
    for (stats[0..conns]) |st| {
        if (!st.ok) continue;
        done += iterations;
        for (st.lat_ns) |v| sum += v;
    }

    std.debug.print(
        \\
        \\=== zgrpc + zio 后端（{d} 连接 × {d} 次）===
        \\完成请求   : {d}
        \\墙钟总耗时 : {d:.2} ms
        \\总 QPS     : {d:.0}
        \\延迟 avg   : {d:.1} us
        \\
    , .{
        conns,
        iterations,
        done,
        @as(f64, @floatFromInt(total)) / 1e6,
        @as(f64, @floatFromInt(done)) * 1e9 / @as(f64, @floatFromInt(@max(total, 1))),
        @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(@max(done, 1))) / 1000.0,
    });
}

const WorkerStat = struct {
    ok: bool = false,
    lat_ns: []u64 = &.{},
};

fn clientWorker(
    io: std.Io,
    gpa: std.mem.Allocator,
    addr: std.Io.net.IpAddress,
    payload: []const u8,
    iterations: usize,
    stat: *WorkerStat,
) std.Io.Cancelable!void {
    var stream = addr.connect(io, .{ .mode = .stream }) catch return;
    defer stream.close(io);
    zgrpc.socket_timeout.setNoDelay(io, stream, true) catch {};

    var recv_buf: [64 * 1024]u8 = undefined;
    var send_buf: [64 * 1024]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);

    var client = zgrpc.client.Client.init(gpa, &sr.interface, &sw.interface, .{
        .authority = "127.0.0.1",
    });
    defer client.deinit();
    client.tuneSocket(stream, io);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    for (0..50) |_| {
        _ = arena_state.reset(.retain_capacity);
        _ = client.unary(method, payload, arena_state.allocator()) catch return;
    }
    for (0..iterations) |i| {
        _ = arena_state.reset(.retain_capacity);
        const a = zgrpc.clock.monotonicNs();
        const res = client.unary(method, payload, arena_state.allocator()) catch return;
        const b = zgrpc.clock.monotonicNs();
        if (res.messages.len != 1) return;
        stat.lat_ns[i] = @intCast(@max(b - a, 0));
    }
    stat.ok = true;
}
fn serveLoop(
    io: std.Io,
    allocator: std.mem.Allocator,
    listener: *std.Io.net.Server,
    server: *zgrpc.grpc.Server,
) std.Io.Cancelable!void {
    var group: std.Io.Group = .init;
    while (true) {
        const stream = listener.accept(io) catch return;
        // zio 下这里是**真并发**（协程 + 事件循环），不是线程池
        group.concurrent(io, handleClient, .{ io, allocator, stream, server }) catch {
            stream.close(io);
            continue;
        };
    }
}

fn us(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1000.0;
}

fn pct(len: usize, p: usize) usize {
    if (len == 0) return 0;
    const idx = (len * p + 99) / 100;
    return @min(idx, len) - 1;
}
