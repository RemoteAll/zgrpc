//! zgrpc 基准：unary 吞吐 / 延迟分位 / HPACK Huffman 头部收缩率。
//!
//! 运行（建议 ReleaseFast，debug 下的绝对数字没有参考价值）：
//! `zig build bench -Doptimize=ReleaseFast -- --iterations 5000 --payload 128`
//!
//! 设计口径：
//! - **单连接复用**：全部迭代走同一条 TCP + 同一条 h2 连接（真实 gRPC 用法），
//!   不像多数玩具基准那样每次重建连接；
//! - **服务端与客户端同进程、不同线程**：排除网络栈以外的干扰，衡量协议层真实开销；
//! - 延迟取**每次 unary 往返**的墙钟差（单调时钟），排序后取 p50/p95/p99；
//! - 预热若干次后再计时，避开首调用的一次性开销（HPACK 表构建、缓冲扩容等）。

const std = @import("std");
const builtin = @import("builtin");
const zgrpc = @import("zgrpc");

const port: u16 = 18054;
const method = "/bench.Bench/Echo";

const default_iterations: usize = 3000;
const default_payload: usize = 128;
const warmup: usize = 200;

/// 回显 handler：把请求消息原样发回（衡量协议层开销，不含业务逻辑）。
fn sayHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    try call.sendMessage(msg.payload);
}

fn acceptLoop(
    allocator: std.mem.Allocator,
    io: std.Io,
    listener: *std.Io.net.Server,
    server: *zgrpc.grpc.Server,
) void {
    while (true) {
        const stream = listener.accept(io) catch return;
        // 用推荐栈（128 KiB）而非 std 默认 16 MiB：实测每连接创建成本 147.3 → 86.1 us
        const th = std.Thread.spawn(
            .{ .stack_size = zgrpc.grpc.Server.conn_stack_size },
            zgrpc.grpc.Server.serveConn,
            .{ server, allocator, io, stream },
        ) catch {
            stream.close(io);
            continue;
        };
        th.detach();
    }
}


const Args = struct {
    iterations: usize = default_iterations,
    payload: usize = default_payload,
    /// 短连接模式：每次调用新建连接、用完即关（衡量连接建立 + 服务端线程开销）
    short: bool = false,
    /// 只测线程创建/销毁成本（衡量「每连接一线程」模型的固定开销）
    thread_cost: bool = false,
    /// 并发连接数（每条连接一个独立客户端，各自跑 iterations 次）
    conns: usize = 1,
    /// 只起服务端不压测（供 ghz / wrk 等外部工具打）
    serve_only: bool = false,
};

fn parseArgs(init: std.process.Init) Args {
    var a = Args{};
    var it = std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa) catch return a;
    defer it.deinit();
    _ = it.skip(); // 程序名
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--iterations")) {
            if (it.next()) |v| a.iterations = std.fmt.parseInt(usize, v, 10) catch a.iterations;
        } else if (std.mem.eql(u8, arg, "--payload")) {
            if (it.next()) |v| a.payload = std.fmt.parseInt(usize, v, 10) catch a.payload;
        } else if (std.mem.eql(u8, arg, "--short")) {
            a.short = true;
        } else if (std.mem.eql(u8, arg, "--thread-cost")) {
            a.thread_cost = true;
        } else if (std.mem.eql(u8, arg, "--conns")) {
            if (it.next()) |v| a.conns = std.fmt.parseInt(usize, v, 10) catch a.conns;
        } else if (std.mem.eql(u8, arg, "--serve-only")) {
            a.serve_only = true;
        }
    }
    if (a.iterations == 0) a.iterations = 1;
    return a;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const cfg = parseArgs(init);

    // ---------- 服务端 ----------
    var dummy: u8 = 0;
    var routes = [_]zgrpc.grpc.Route{
        .{ .path = method, .handler = sayHandler },
        // 同时注册 echo.proto 里的方法名，便于用 ghz --proto examples/echo.proto 压测
        .{ .path = "/echo.Echo/Say", .handler = sayHandler },
    };
    var server = zgrpc.grpc.Server{ .routes = &routes, .outer_ctx = &dummy };

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var listener = addr.listen(io, .{ .reuse_address = true }) catch |e| {
        std.debug.print("[bench] 监听 {d} 失败：{s}\n", .{ port, @errorName(e) });
        return e;
    };
    defer listener.deinit(io);

    const srv_thread = try std.Thread.spawn(.{}, acceptLoop, .{ gpa, io, &listener, &server });
    srv_thread.detach();

    // ---------- 只起服务端：给 ghz 等外部压测工具用 ----------
    if (cfg.serve_only) {
        std.debug.print("[bench] 仅服务端模式，监听 127.0.0.1:{d}（ghz 可直连）\n", .{port});
        acceptLoop(gpa, io, &listener, &server);
        return;
    }

    // ---------- 线程成本微基准 ----------
    if (cfg.thread_cost) {
        try runThreadCost();
        return;
    }

    // ---------- 并发模式：N 条连接同时打 ----------
    if (cfg.conns > 1) {
        try runConcurrent(io, gpa, addr, cfg);
        return;
    }

    // ---------- 短连接模式：每次新建连接（衡量连接建立 + 服务端建线程的开销） ----------
    if (cfg.short) {
        try runShortConnections(io, gpa, addr, cfg);
        return;
    }

    // ---------- 客户端 ----------
    var stream = addr.connect(io, .{ .mode = .stream }) catch |e| {
        std.debug.print("[bench] 连接失败：{s}\n", .{@errorName(e)});
        return e;
    };
    defer stream.close(io);

    var recv_buf: [64 * 1024]u8 = undefined;
    var send_buf: [64 * 1024]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);

    // 关闭 Nagle：gRPC 是小包交互，Nagle + 延迟 ACK 会带来随机 10~200ms 尖峰
    zgrpc.socket_timeout.setNoDelay(io, stream, true) catch |e| {
        std.debug.print("[bench] 关闭 Nagle 失败（{s}），延迟可能偏高\n", .{@errorName(e)});
    };

    var client = zgrpc.client.Client.init(gpa, &sr.interface, &sw.interface, .{
        .authority = "127.0.0.1",
    });
    defer client.deinit();

    const payload = try gpa.alloc(u8, cfg.payload);
    defer gpa.free(payload);
    @memset(payload, 'x');

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    // ---------- 预热 ----------
    for (0..warmup) |_| {
        _ = arena_state.reset(.retain_capacity);
        const res = client.unary(method, payload, arena_state.allocator()) catch |e| {
            std.debug.print("[bench] 预热失败：{s}\n", .{@errorName(e)});
            return e;
        };
        if (res.messages.len != 1) {
            std.debug.print("[bench] 预热响应异常：messages={d}\n", .{res.messages.len});
            return error.Unexpected;
        }
    }

    // ---------- 计时 ----------
    const lat_ns = try gpa.alloc(u64, cfg.iterations);
    defer gpa.free(lat_ns);

    const t_all0 = zgrpc.clock.monotonicNs();
    for (0..cfg.iterations) |i| {
        _ = arena_state.reset(.retain_capacity);
        const t0 = zgrpc.clock.monotonicNs();
        const res = client.unary(method, payload, arena_state.allocator()) catch |e| {
            std.debug.print("[bench] 第 {d} 次调用失败：{s}\n", .{ i, @errorName(e) });
            return e;
        };
        const t1 = zgrpc.clock.monotonicNs();
        lat_ns[i] = @intCast(@max(t1 - t0, 0));
        if (res.messages.len != 1) return error.Unexpected;
    }
    const total_ns: u64 = @intCast(zgrpc.clock.monotonicNs() - t_all0);

    // 诊断：慢调用的位置分布（判断是周期性的还是随机的）
    {
        var slow_n: usize = 0;
        var shown: usize = 0;
        var prev_slow: ?usize = null;
        var min_gap: usize = std.math.maxInt(usize);
        var max_gap: usize = 0;
        for (lat_ns, 0..) |v, idx| {
            if (v <= 5_000_000) continue;
            slow_n += 1;
            if (prev_slow) |p_idx| {
                const gap = idx - p_idx;
                if (gap < min_gap) min_gap = gap;
                if (gap > max_gap) max_gap = gap;
            }
            prev_slow = idx;
            if (shown < 12) {
                std.debug.print("[bench] 慢调用 idx={d} 耗时={d:.1}us\n", .{ idx, nsToUs(v) });
                shown += 1;
            }
        }
        if (slow_n > 0) {
            std.debug.print(
                "[bench] 慢调用（>5ms）共 {d}/{d} 次，相邻下标间隔 min={d} max={d}\n",
                .{ slow_n, cfg.iterations, if (min_gap == std.math.maxInt(usize)) 0 else min_gap, max_gap },
            );
        }
    }

    // ---------- 统计 ----------
    var sum: u64 = 0;
    for (lat_ns) |v| sum += v;
    std.mem.sort(u64, lat_ns, {}, std.sort.asc(u64));

    const qps = @as(f64, @floatFromInt(cfg.iterations)) * 1e9 / @as(f64, @floatFromInt(@max(total_ns, 1)));
    const avg_us = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(cfg.iterations)) / 1000.0;

    std.debug.print(
        \\
        \\=== zgrpc unary 基准 ===
        \\构建模式     : {s}（ReleaseFast 之外的绝对数字仅供相对比较）
        \\迭代 / 预热  : {d} / {d}（单连接复用，请求 {d} 字节、响应同长）
        \\总耗时       : {d:.2} ms
        \\QPS          : {d:.0}
        \\延迟 avg     : {d:.1} us
        \\延迟 p50     : {d:.1} us
        \\延迟 p95     : {d:.1} us
        \\延迟 p99     : {d:.1} us
        \\延迟 max     : {d:.1} us
        \\
    , .{
        @tagName(builtin.mode),
        cfg.iterations,
        warmup,
        cfg.payload,
        @as(f64, @floatFromInt(total_ns)) / 1e6,
        qps,
        avg_us,
        nsToUs(lat_ns[percentileIndex(lat_ns.len, 50)]),
        nsToUs(lat_ns[percentileIndex(lat_ns.len, 95)]),
        nsToUs(lat_ns[percentileIndex(lat_ns.len, 99)]),
        nsToUs(lat_ns[lat_ns.len - 1]),
    });

    // ---------- HPACK Huffman 收益（典型 gRPC 响应头） ----------
    const sample = [_][]const u8{
        "grpc-zgrpc/0.2.1",
        "application/grpc",
        "application/grpc+proto",
        "/bench.Bench/Echo",
        "127.0.0.1",
        "identity,gzip",
    };
    var plain: usize = 0;
    var huffed: usize = 0;
    for (sample) |s| {
        plain += s.len;
        const h = zgrpc.hpack.huffmanEncodedLen(s);
        huffed += @min(h, s.len);
    }
    std.debug.print(
        "=== HPACK 头部字符串 ===\n样本字符总数 : {d} 字节\nHuffman 后   : {d} 字节（省 {d} 字节，{d:.1}%）\n\n",
        .{
            plain,
            huffed,
            plain - huffed,
            @as(f64, @floatFromInt(plain - huffed)) * 100.0 / @as(f64, @floatFromInt(plain)),
        },
    );
}

fn nsToUs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1000.0;
}

/// 排序后的分位下标（最近秩法）。
fn percentileIndex(len: usize, p: usize) usize {
    if (len == 0) return 0;
    const idx = (len * p + 99) / 100;
    return @min(idx, len) - 1;
}

/// 短连接模式：每次调用都新建连接、用完即关。
///
/// 它衡量的是「连接建立 + 服务端为连接创建线程」的代价：
/// 长连接模式下的数字只反映协议层开销，而真实服务往往要承受大量短命连接（探活、无连接池的客户端）。
fn runShortConnections(io: std.Io, gpa: std.mem.Allocator, addr: std.Io.net.IpAddress, cfg: Args) !void {
    const payload = try gpa.alloc(u8, cfg.payload);
    defer gpa.free(payload);
    @memset(payload, 'x');

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    const lat_ns = try gpa.alloc(u64, cfg.iterations);
    defer gpa.free(lat_ns);

    const t_all0 = zgrpc.clock.monotonicNs();
    for (0..cfg.iterations) |i| {
        const dt: u64 = blk: {
            var s = addr.connect(io, .{ .mode = .stream }) catch |e| {
                std.debug.print("[bench] 第 {d} 次连接失败：{s}\n", .{ i, @errorName(e) });
                return e;
            };
            defer s.close(io);
            zgrpc.socket_timeout.setNoDelay(io, s, true) catch {};
            var rbuf: [16 * 1024]u8 = undefined;
            var wbuf: [16 * 1024]u8 = undefined;
            var srr = s.reader(io, &rbuf);
            var sww = s.writer(io, &wbuf);
            var c = zgrpc.client.Client.init(gpa, &srr.interface, &sww.interface, .{
                .authority = "127.0.0.1",
            });
            defer c.deinit();
            _ = arena_state.reset(.retain_capacity);
            const t0 = zgrpc.clock.monotonicNs();
            const res = c.unary(method, payload, arena_state.allocator()) catch |e| {
                std.debug.print("[bench] 第 {d} 次调用失败：{s}\n", .{ i, @errorName(e) });
                return e;
            };
            const t1 = zgrpc.clock.monotonicNs();
            if (res.messages.len != 1) return error.Unexpected;
            break :blk @intCast(@max(t1 - t0, 0));
        };
        lat_ns[i] = dt;
    }
    const total_ns: u64 = @intCast(zgrpc.clock.monotonicNs() - t_all0);

    var sum: u64 = 0;
    for (lat_ns) |v| sum += v;
    std.mem.sort(u64, lat_ns, {}, std.sort.asc(u64));

    const qps = @as(f64, @floatFromInt(cfg.iterations)) * 1e9 / @as(f64, @floatFromInt(@max(total_ns, 1)));
    std.debug.print(
        \\
        \\=== zgrpc unary 基准（短连接：每次新建连接）===
        \\构建模式     : {s}
        \\迭代         : {d}（请求 {d} 字节）
        \\总耗时       : {d:.2} ms
        \\QPS          : {d:.0}
        \\延迟 avg     : {d:.1} us
        \\延迟 p50     : {d:.1} us
        \\延迟 p95     : {d:.1} us
        \\延迟 p99     : {d:.1} us
        \\
    , .{
        @tagName(builtin.mode),
        cfg.iterations,
        cfg.payload,
        @as(f64, @floatFromInt(total_ns)) / 1e6,
        qps,
        @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(cfg.iterations)) / 1000.0,
        nsToUs(lat_ns[percentileIndex(lat_ns.len, 50)]),
        nsToUs(lat_ns[percentileIndex(lat_ns.len, 95)]),
        nsToUs(lat_ns[percentileIndex(lat_ns.len, 99)]),
    });
}

fn nop() void {}

/// 线程创建/销毁成本：对比默认栈（16 MiB）与压小后的栈（128 KiB）。
///
/// 服务端目前是「每连接一个线程」，短连接场景下这个数字就是每连接的固定税。
fn runThreadCost() !void {
    const n = 500;
    var t0 = zgrpc.clock.monotonicNs();
    for (0..n) |_| {
        const th = try std.Thread.spawn(.{}, nop, .{});
        th.join();
    }
    const dt_default: u64 = @intCast(zgrpc.clock.monotonicNs() - t0);

    t0 = zgrpc.clock.monotonicNs();
    for (0..n) |_| {
        const th = try std.Thread.spawn(.{ .stack_size = 128 * 1024 }, nop, .{});
        th.join();
    }
    const dt_small: u64 = @intCast(zgrpc.clock.monotonicNs() - t0);

    const per_default = @as(f64, @floatFromInt(dt_default)) / @as(f64, @floatFromInt(n)) / 1000.0;
    const per_small = @as(f64, @floatFromInt(dt_small)) / @as(f64, @floatFromInt(n)) / 1000.0;
    std.debug.print(
        \\
        \\=== 线程创建+销毁成本（{d} 次平均）===
        \\默认栈(16 MiB) : {d:.1} us/次
        \\128 KiB 栈     : {d:.1} us/次
        \\
    , .{ n, per_default, per_small });
}

const WorkerStat = struct {
    ok: bool = false,
    lat_ns: []u64 = &.{},
    total_ns: u64 = 0,
};

fn workerMain(
    io: std.Io,
    gpa: std.mem.Allocator,
    addr: std.Io.net.IpAddress,
    payload: []const u8,
    iterations: usize,
    stat: *WorkerStat,
) void {
    var stream = addr.connect(io, .{ .mode = .stream }) catch return;
    defer stream.close(io);
    zgrpc.socket_timeout.setNoDelay(io, stream, true) catch {};

    var rbuf: [16 * 1024]u8 = undefined;
    var wbuf: [16 * 1024]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    var sw = stream.writer(io, &wbuf);

    var client = zgrpc.client.Client.init(gpa, &sr.interface, &sw.interface, .{ .authority = "127.0.0.1" });
    defer client.deinit();

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    // 预热
    for (0..50) |_| {
        _ = arena_state.reset(.retain_capacity);
        _ = client.unary(method, payload, arena_state.allocator()) catch return;
    }

    const t0 = zgrpc.clock.monotonicNs();
    for (0..iterations) |i| {
        _ = arena_state.reset(.retain_capacity);
        const a = zgrpc.clock.monotonicNs();
        const res = client.unary(method, payload, arena_state.allocator()) catch return;
        const b = zgrpc.clock.monotonicNs();
        if (res.messages.len != 1) return;
        stat.lat_ns[i] = @intCast(@max(b - a, 0));
    }
    stat.total_ns = @intCast(zgrpc.clock.monotonicNs() - t0);
    stat.ok = true;
}

/// 并发模式：`conns` 条独立连接同时打，衡量**吞吐**（这才是 async/协程收益体现的场景）。
fn runConcurrent(io: std.Io, gpa: std.mem.Allocator, addr: std.Io.net.IpAddress, cfg: Args) !void {
    const n = @min(cfg.conns, 64);
    const payload = try gpa.alloc(u8, cfg.payload);
    defer gpa.free(payload);
    @memset(payload, 'x');

    var stats: [64]WorkerStat = undefined;
    var lats: [64][]u64 = undefined;
    for (0..n) |i| {
        lats[i] = try gpa.alloc(u64, cfg.iterations);
        stats[i] = .{ .lat_ns = lats[i] };
    }
    defer for (lats[0..n]) |l| gpa.free(l);

    var threads: [64]std.Thread = undefined;
    const t_all0 = zgrpc.clock.monotonicNs();
    for (0..n) |i| {
        threads[i] = try std.Thread.spawn(.{}, workerMain, .{
            io, gpa, addr, payload, cfg.iterations, &stats[i],
        });
    }
    for (threads[0..n]) |th| th.join();
    const wall_ns: u64 = @intCast(zgrpc.clock.monotonicNs() - t_all0);

    var total_req: usize = 0;
    for (stats[0..n]) |s| {
        if (s.ok) total_req += cfg.iterations;
    }
    var merged: usize = 0;
    var sum: u64 = 0;
    for (stats[0..n]) |s| {
        for (s.lat_ns[0..cfg.iterations]) |v| {
            sum += v;
            merged += 1;
        }
    }
    std.debug.print(
        \\
        \\=== zgrpc unary 基准（并发 {d} 连接 × {d} 次）===
        \\完成请求   : {d}
        \\墙钟总耗时 : {d:.2} ms
        \\总 QPS     : {d:.0}
        \\延迟 avg   : {d:.1} us
        \\
    , .{
        n,
        cfg.iterations,
        total_req,
        @as(f64, @floatFromInt(wall_ns)) / 1e6,
        @as(f64, @floatFromInt(total_req)) * 1e9 / @as(f64, @floatFromInt(@max(wall_ns, 1))),
        @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(@max(merged, 1))) / 1000.0,
    });
}