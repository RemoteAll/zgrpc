//! 裸 TCP ping-pong 延迟基准：establishing「这台机器上任何 socket RPC 的地板」。
//!
//! 为什么要有它：gRPC 的往返里既有「协议处理」也有「操作系统成本」。
//! 不把 OS 地板量出来，就无法判断剩下的时间还有没有优化空间、
//! 更无法回答「能不能做到 ns 级」这种问题。
//!
//! 用法：`zig build-exe tools/tcp_floor.zig -O ReleaseFast -femit-bin=zig-out/bin/tcp_floor.exe`
//!      `tcp_floor.exe [iterations]`
//!
//! 服务端在本进程里起一个线程，客户端在主线程，两者走 127.0.0.1 回环，
//! 每次只发 1 字节、收回 1 字节 —— 除系统调用外没有任何工作。

const std = @import("std");

const Iterations = 20000;

fn serverMain(ctx: *ServerCtx) void {
    const io = ctx.io;
    const stream = ctx.stream;
    defer stream.close(io);

    var buf: [64]u8 = undefined;
    var sr = stream.reader(io, &buf);
    var r: *std.Io.Reader = &sr.interface;

    var out: [64]u8 = undefined;
    var sw = stream.writer(io, &out);
    var w: *std.Io.Writer = &sw.interface;

    // 回声：读 1 字节，立刻写回去并 flush（等价于最朴素的 request/response）
    while (true) {
        var one: [1]u8 = undefined;
        r.readSliceAll(&one) catch return;
        w.writeAll(&one) catch return;
        w.flush() catch return;
    }
}

const ServerCtx = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    var iterations: usize = Iterations;
    {
        var it = std.process.Args.Iterator.initAllocator(init.minimal.args, gpa) catch return;
        defer it.deinit();
        _ = it.skip();
        if (it.next()) |a| iterations = std.fmt.parseInt(usize, a, 10) catch iterations;
    }
    if (iterations == 0) iterations = Iterations;

    const io = init.io;

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", 21999);
    var listener = try addr.listen(io, .{ .reuse_address = true, .kernel_backlog = 128 });
    defer listener.deinit(io);

    // 服务端线程：accept 一条连接后开始回声
    const Srv = struct {
        fn run(io2: std.Io, l: *std.Io.net.Server, out: *std.Io.net.Stream) void {
            out.* = l.accept(io2) catch return;
            var ctx = ServerCtx{ .io = io2, .stream = out.* };
            serverMain(&ctx);
        }
    };
    var srv_stream: std.Io.net.Stream = undefined;
    const th = try std.Thread.spawn(.{}, Srv.run, .{ io, &listener, &srv_stream });
    th.detach();

    var stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var recv_buf: [64]u8 = undefined;
    var send_buf: [64]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);
    var r: *std.Io.Reader = &sr.interface;
    var w: *std.Io.Writer = &sw.interface;

    const one = [_]u8{'x'};

    // 预热
    for (0..200) |_| {
        try w.writeAll(&one);
        try w.flush();
        var got: [1]u8 = undefined;
        try r.readSliceAll(&got);
    }

    var total_ns: u128 = 0;
    var min_ns: u128 = std.math.maxInt(u128);
    for (0..iterations) |_| {
        const t0 = std.Io.Clock.now(.boot, io);
        try w.writeAll(&one);
        try w.flush();
        var got: [1]u8 = undefined;
        try r.readSliceAll(&got);
        const t1 = std.Io.Clock.now(.boot, io);
        const dt: u128 = @intCast(t1.toNanoseconds() - t0.toNanoseconds());
        total_ns += dt;
        min_ns = @min(min_ns, dt);
    }

    const n: f64 = @floatFromInt(iterations);
    std.debug.print(
        "[tcp-floor] iterations={d} avg_us={d:.2} min_us={d:.2}\n",
        .{
            iterations,
            @as(f64, @floatFromInt(total_ns)) / n / 1000.0,
            @as(f64, @floatFromInt(min_ns)) / 1000.0,
        },
    );
}
