//! keepalive 探针：验证服务端会**主动**发 PING（而不是只回 ACK）。
//!
//! 做法：起一个配了 `keepalive_interval_ms = 100` 的服务端，客户端完成 h2 握手后
//! **什么都不发**，只读帧 —— 若在 1.5 秒内收到多个 PING，说明保活生效。
//!
//! 为什么需要它：NAT / 四层 LB 会把「空闲」连接掐掉（常见 5~60 分钟），
//! 保活必须由服务端或客户端周期发帧；只回 ACK 不算保活。

const std = @import("std");
const zgrpc = @import("zgrpc");

const interval_ms: u32 = 100;
const observe_ms: u64 = 1500;
const min_pings: usize = 3; // 100ms 间隔在 1.5s 内应发 ~15 个，留足余量

var dummy: u8 = 0;

fn handleConn(allocator: std.mem.Allocator, io: std.Io, stream: std.Io.net.Stream, server: *zgrpc.grpc.Server) void {
    server.serveConn(allocator, io, stream);
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // 服务端：只配 keepalive，不注册任何路由（本探针只关心 PING）
    var no_routes = [_]zgrpc.grpc.Route{};
    var no_streams = [_]zgrpc.grpc.StreamRoute{};
    var server = zgrpc.grpc.Server{
        .routes = &no_routes,
        .stream_routes = &no_streams,
        .outer_ctx = &dummy,
        .keepalive_interval_ms = interval_ms,
    };

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", 0); // 端口 0 = 让系统分配
    var listener = try addr.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);
    const port = listener.socket.address.getPort();
    std.debug.print("[keepalive-probe] 服务端监听 127.0.0.1:{d}，keepalive={d}ms\n", .{ port, interval_ms });

    // keepalive 线程
    const ka = try server.spawnKeepalive(allocator);
    defer {
        server.stopKeepalive();
        ka.join();
    }

    // accept 线程
    // accept 循环阻塞在 `accept(io)` 上，join 会永远等不到 —— 直接 detach，
    // 进程退出时由 OS 回收（探针是一次性程序，不需要优雅收尾）。
    const acc = try std.Thread.spawn(.{ .stack_size = zgrpc.grpc.Server.conn_stack_size }, acceptLoop, .{ allocator, io, &listener, &server });
    acc.detach();

    // 客户端：握手后静默
    const cli_addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var stream = try cli_addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var wbuf: [4096]u8 = undefined;
    var rbuf: [4096]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    var sr = stream.reader(io, &rbuf);

    // HTTP/2 连接前言 + 一个空 SETTINGS 帧（服务端收到它才算连接就绪）
    try sw.interface.writeAll("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    try sw.interface.writeAll(&.{ 0, 0, 0, 0x4, 0, 0, 0, 0, 0 });
    try sw.interface.flush();

    // 只读帧：统计 PING 数量
    var pings: usize = 0;
    var acks: usize = 0;
    var got_settings = false;
    var frames: usize = 0;
    const deadline = zgrpc.clock.monotonicNs() + @as(i128, observe_ms) * std.time.ns_per_ms;

    while (zgrpc.clock.monotonicNs() < deadline) {
        var hdr: [9]u8 = undefined;
        sr.interface.readSliceAll(&hdr) catch |e| {
            std.debug.print("[keepalive-probe] 读帧头结束：{s}\n", .{@errorName(e)});
            break;
        };
        const len = (@as(usize, hdr[0]) << 16) | (@as(usize, hdr[1]) << 8) | @as(usize, hdr[2]);
        const ftype = hdr[3];
        const flags = hdr[4];
        var payload: [256]u8 = undefined;
        if (len <= payload.len) {
            sr.interface.readSliceAll(payload[0..len]) catch break;
        }
        frames += 1;
        switch (ftype) {
            0x4 => got_settings = true,
            0x6 => {
                if (flags & 0x1 != 0) {
                    acks += 1;
                } else {
                    pings += 1;
                    // 回 ACK，模拟正常对端
                    var echo: [9 + 8]u8 = undefined;
                    echo[0] = 0;
                    echo[1] = 0;
                    echo[2] = 8;
                    echo[3] = 0x6;
                    echo[4] = 0x1;
                    @memset(echo[5..9], 0);
                    if (len == 8) @memcpy(echo[9..17], payload[0..8]) else @memset(echo[9..17], 0);
                    sw.interface.writeAll(&echo) catch break;
                    sw.interface.flush() catch break;
                }
            },
            else => {},
        }
    }

    std.debug.print("[keepalive-probe] 收到 {d} 帧：SETTINGS={} PING(无 ACK)={d} PING ACK={d}\n", .{ frames, got_settings, pings, acks });
    std.debug.print("[keepalive-probe] 服务端活跃连接数={d}\n", .{server.connCount()});

    if (!got_settings) {
        std.debug.print("[keepalive-probe] FAIL：没收到服务端 SETTINGS\n", .{});
        return error.NoSettings;
    }
    if (pings < min_pings) {
        std.debug.print("[keepalive-probe] FAIL：{d}ms 内只收到 {d} 个 PING（期望 ≥{d}）\n", .{ observe_ms, pings, min_pings });
        return error.TooFewPings;
    }
    std.debug.print("[keepalive-probe] PASS：服务端主动保活（{d}ms 内 {d} 个 PING）\n", .{ observe_ms, pings });
    _ = init;
}

fn acceptLoop(allocator: std.mem.Allocator, io: std.Io, listener: *std.Io.net.Server, server: *zgrpc.grpc.Server) void {
    while (true) {
        const stream = listener.accept(io) catch |e| {
            std.debug.print("[keepalive-probe] accept 结束：{s}\n", .{@errorName(e)});
            return;
        };
        const th = std.Thread.spawn(.{ .stack_size = zgrpc.grpc.Server.conn_stack_size }, handleConn, .{ allocator, io, stream, server }) catch {
            stream.close(io);
            continue;
        };
        th.detach();
    }
}
