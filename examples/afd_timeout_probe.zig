//! 验证 **Windows AFD 原生读超时**（overlapped + `NtWaitForSingleObject`）。
//!
//! 运行：`zig build afd-probe`
//!
//! 分三步，逐步排除：
//! ① 服务端先发一小段数据 → 用 Zig 原生 `stream.read` 读掉（证明连接可用）；
//! ② 服务端沉默 → `afd_reader.read` 带 200ms 时限，期望 `error.Timeout`；
//! ③ 打印耗时，确认是「到点返回」而不是「立即报错」。

const std = @import("std");
const zgrpc = @import("zgrpc");

const port: u16 = 18053;
const timeout_ms: u32 = 200;
const max_elapsed_ms: i128 = 2000;
const silent_ns: u64 = 3 * std.time.ns_per_s;

/// 接受连接 → 先发 8 字节问候 → 然后一直沉默。
fn greeterAccept(io: std.Io, listener: *std.Io.net.Server) void {
    const stream = listener.accept(io) catch return;
    defer stream.close(io);
    var wbuf: [64]u8 = undefined;
    var w = stream.writer(io, &wbuf);
    w.interface.writeAll("GREETING") catch return;
    w.interface.flush() catch return;
    // 1.5s 后再发一次：给「不带 event 的同步读」一个返回机会
    zgrpc.clock.sleepNs(1500 * std.time.ns_per_ms);
    w.interface.writeAll("SECOND!!") catch return;
    w.interface.flush() catch return;
    zgrpc.clock.sleepNs(silent_ns);
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var listener = addr.listen(io, .{ .reuse_address = true }) catch |e| {
        std.debug.print("[afd-probe] 监听 {d} 失败：{s}\n", .{ port, @errorName(e) });
        return e;
    };
    defer listener.deinit(io);

    const th = std.Thread.spawn(.{}, greeterAccept, .{ io, &listener }) catch |e| {
        std.debug.print("[afd-probe] 创建线程失败：{s}\n", .{@errorName(e)});
        return e;
    };
    th.detach();

    var stream = addr.connect(io, .{ .mode = .stream }) catch |e| {
        std.debug.print("[afd-probe] 连接失败：{s}\n", .{@errorName(e)});
        return e;
    };
    defer stream.close(io);

    // ---------- ① 用 Zig 原生路径读掉问候语 ----------
    var greet: [16]u8 = undefined;
    var gdatas = [1][]u8{&greet};
    const n = stream.read(io, &gdatas) catch |e| {
        std.debug.print("[afd-probe] ① Zig read 失败：{s}\n", .{@errorName(e)});
        return e;
    };
    std.debug.print("[afd-probe] ① Zig read 成功：{d} 字节（{s}）\n", .{ n, greet[0..n] });

    // ---------- ①.5 诊断A：不传 event（Zig 同款语义，同步阻塞） ----------
    // 服务端 1.5s 后会再发一次数据，所以这里应阻塞后成功
    var dbuf: [64]u8 = undefined;
    const tA0 = zgrpc.clock.monotonicNs();
    const ra = zgrpc.afd_reader.readDiagnostic(stream.socket.handle, &dbuf, false);
    const a_ms = @divTrunc(zgrpc.clock.monotonicNs() - tA0, std.time.ns_per_ms);
    if (ra) |got| {
        std.debug.print("[afd-probe] ①.5 A(无event) 成功：{d} 字节，{d}ms\n", .{ got, a_ms });
    } else |e| {
        std.debug.print("[afd-probe] ①.5 A(无event) 失败：{s}，{d}ms\n", .{ @errorName(e), a_ms });
    }

    // ---------- ② overlapped + 时限等待（此时对端已沉默） ----------
    var buf: [1024]u8 = undefined;
    const t0 = zgrpc.clock.monotonicNs();
    const r = zgrpc.afd_reader.read(stream.socket.handle, &buf, timeout_ms);
    const elapsed_ms = @divTrunc(zgrpc.clock.monotonicNs() - t0, std.time.ns_per_ms);

    if (r) |got| {
        std.debug.print("[afd-probe] ② 失败：读到 {d} 字节（对端不该再发），{d}ms\n", .{ got, elapsed_ms });
        return error.Unexpected;
    } else |err| {
        std.debug.print("[afd-probe] ② afd_reader.read 返回 {s}，耗时 {d}ms\n", .{ @errorName(err), elapsed_ms });
        if (err != error.Timeout) {
            std.debug.print("[afd-probe] ② 失败：期望 error.Timeout\n", .{});
            return error.Unexpected;
        }
    }

    if (elapsed_ms < timeout_ms or elapsed_ms > max_elapsed_ms) {
        std.debug.print("[afd-probe] ② 失败：耗时 {d}ms 不在 [{d}ms, {d}ms]\n", .{
            elapsed_ms, timeout_ms, max_elapsed_ms,
        });
        return error.Unexpected;
    }

    std.debug.print("[afd-probe] PASS：原生读超时生效，{d}ms 返回（设定 {d}ms），零线程零轮询\n", .{
        elapsed_ms, timeout_ms,
    });
}
