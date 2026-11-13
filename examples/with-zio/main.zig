//! 把 zgrpc 的 `std.Io` 换成 **zio 运行时**，验证「零依赖库 + 可选异步后端」这条路。
//!
//! zio 本身就是事件驱动的：`src/ev/` 是 libuv 风格的回调事件循环，后端
//! Windows → **IOCP**、Linux → io_uring（自动退 epoll）、macOS/BSD → kqueue。
//! 它同时实现了完整的 `std.Io` 接口，所以这里 **zgrpc 一行都不用改**，
//! 只需要：① 用 `rt.io()` 代替 `init.io`；② 把调度方式设成 `.task`。
//!
//! 用法：
//!   zig build run -- 3000 4                 # 自压测：4 条连接 × 3000 次
//!   zig build run -- serve 18058            # 只起服务端（默认缓冲）
//!   zig build run -- serve 18058 4 4        # 只起服务端，收发缓冲各 4 KiB（上万连接用）

const std = @import("std");
const zgrpc = @import("zgrpc");
const zio = @import("zio");

/// 让 `std.debug.print` 不阻塞事件循环（zio README 推荐，可选）。
pub const std_options_debug_io = zio.debug_io;

const default_port: u16 = 18055;
const method = "/bench.Bench/Echo";

fn sayHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    try call.sendMessage(msg.payload);
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    // ---------- 换成 zio 运行时 ----------
    // **栈池必须调小**：zio 默认 `committed_size = 256 KiB`（每个协程初始提交的物理内存），
    // 一万条连接就是 2.5 GB。而连接协程的栈用量很小（socket 收发缓冲都在堆上），
    // 所以降到 16 KiB 初始提交，虚拟地址仍保留到 1 MiB，真需要更深时按需增长。
    // 实测：10000 连接从 1215 MB 降到 ~260 MB。
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .auto,
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 16 * 1024,
        },
    });
    defer rt.deinit();
    const io = rt.io();

    // ---------- 参数 ----------
    var iterations: usize = 3000;
    var conns: usize = 1;
    var listen_port: u16 = default_port;
    // 每连接缓冲（KiB）。上万连接时务必调小：这是每连接的固定内存开销。
    var recv_kb: usize = 32;
    var send_kb: usize = 16;
    var serve_only = false;
    {
        var it = std.process.Args.Iterator.initAllocator(init.minimal.args, gpa) catch return;
        defer it.deinit();
        _ = it.skip();
        var idx: usize = 0;
        while (it.next()) |a| : (idx += 1) {
            if (idx == 0) {
                if (std.mem.eql(u8, a, "serve")) {
                    serve_only = true;
                } else {
                    iterations = std.fmt.parseInt(usize, a, 10) catch iterations;
                }
            } else if (idx == 1) {
                if (serve_only) {
                    listen_port = std.fmt.parseInt(u16, a, 10) catch listen_port;
                } else {
                    conns = std.fmt.parseInt(usize, a, 10) catch conns;
                }
            } else if (idx == 2) {
                recv_kb = std.fmt.parseInt(usize, a, 10) catch recv_kb;
            } else if (idx == 3) {
                send_kb = std.fmt.parseInt(usize, a, 10) catch send_kb;
            }
        }
    }
    if (conns == 0) conns = 1;
    if (conns > 64) conns = 64;
    if (recv_kb == 0) recv_kb = 32;
    if (send_kb == 0) send_kb = 16;

    // ---------- 服务端 ----------
    var dummy: u8 = 0;
    var routes = [_]zgrpc.grpc.Route{
        .{ .path = method, .handler = sayHandler },
    };
    var server = zgrpc.grpc.Server{
        .routes = &routes,
        .outer_ctx = &dummy,
        // 关键：连接作为**运行时任务**跑（事件循环），而不是每条一个 OS 线程
        .dispatch = .task,
        .conn_recv_buf_size = recv_kb * 1024,
        .conn_send_buf_size = send_kb * 1024,
    };

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", listen_port);
    var listener = try addr.listen(io, .{
        .reuse_address = true,
        // 默认 backlog 只有 128，上万连接同时建连会大量拒连
        .kernel_backlog = zgrpc.grpc.Server.default_backlog,
    });
    defer listener.deinit(io);

    // 服务端作为**运行时里的一个任务**跑（不能裸 std.Thread：zio 的 accept/读写在
    // 运行时上下文里才会挂起协程，跑到外部线程上会卡死）
    var srv_group: std.Io.Group = .init;
    srv_group.async(io, zgrpc.grpc.Server.acceptLoop, .{ &server, io, gpa, &listener });

    // ---------- 只起服务端：给外部压测工具用 ----------
    if (serve_only) {
        std.debug.print(
            "zio(IOCP) server on 127.0.0.1:{d}  dispatch=task  buf={d}K/{d}K\n",
            .{ listen_port, recv_kb, send_kb },
        );
        srv_group.await(io) catch {};
        return;
    }

    // ---------- 客户端：N 个**协程**各持一条连接 ----------
    const payload = try gpa.alloc(u8, 128);
    defer gpa.free(payload);
    @memset(payload, 'x');

    var stats: [64]WorkerStat = undefined;
    for (0..conns) |i| stats[i] = .{ .lat_ns = try gpa.alloc(u64, iterations) };
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
