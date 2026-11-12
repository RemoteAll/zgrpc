//! 验证客户端的**本地读超时**真的生效：服务端只接受连接、永不回包。
//!
//! 运行：`zig build timeout-probe`
//!
//! 为什么需要这个用例：`grpc-timeout` 头只管服务端，服务端要是卡住（或对端根本不是 gRPC），
//! 客户端必须能自己退出，而不是无限阻塞。这里用真实 socket + 真实线程验证。

const std = @import("std");
const zgrpc = @import("zgrpc");

const port: u16 = 18052;
/// 平台说明：Zig 0.17 在 Windows 上的网络走 AFD 句柄 + overlapped IO，
/// 目前**没有可用的读超时手段**（详见 src/timeout_guard.zig 的注释与决策记录）。
/// 因此本探针在 Windows 上只验证「服务端不响应时客户端最终会报错退出」，
/// 超时精度断言仅对 POSIX 生效。
const is_windows = @import("builtin").os.tag == .windows;
const timeout_ms: u32 = 200;
/// 允许的耗时上限（含调度抖动）：超时 200ms，若还在 2s 内返回就算正常
const max_elapsed_ms: i128 = 2000;

/// 只接受连接、然后一直不响应（睡到进程退出）。
fn silentAccept(io: std.Io, listener: *std.Io.net.Server) void {
    const stream = listener.accept(io) catch return;
    defer stream.close(io);
    // 不读、不写：客户端应靠自己的 deadline 退出
    zgrpc.clock.sleepNs(3 * std.time.ns_per_s);
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    // 说明：本地超时走原生 `setsockopt(SO_RCVTIMEO)`，**不依赖 io 的并发能力**，
    // 所以这里用 `init.io` 即可（服务端线程也用同一个 io）。
    const io = init.io;

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var listener = addr.listen(io, .{ .reuse_address = true }) catch |e| {
        std.debug.print("[timeout-probe] 监听 {d} 失败：{s}\n", .{ port, @errorName(e) });
        return e;
    };
    defer listener.deinit(io);

    const th = std.Thread.spawn(.{}, silentAccept, .{ io, &listener }) catch |e| {
        std.debug.print("[timeout-probe] 创建线程失败：{s}\n", .{@errorName(e)});
        return e;
    };
    th.detach();

    var stream = addr.connect(io, .{ .mode = .stream }) catch |e| {
        std.debug.print("[timeout-probe] 连接失败：{s}\n", .{@errorName(e)});
        return e;
    };
    defer stream.close(io);

    var recv_buf: [4096]u8 = undefined;
    var send_buf: [4096]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);

    var client = zgrpc.client.Client.init(gpa, &sr.interface, &sw.interface, .{
        .authority = "127.0.0.1",
        .timeout_ms = timeout_ms,
    });
    // 关键：启用客户端本地读超时
    client.enableTimeout(stream, io);
    defer client.deinit();

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    const t0 = zgrpc.clock.monotonicNs();
    const result = client.unary("/echo.Echo/Say", "hi", arena_state.allocator());
    const elapsed_ms = @divTrunc(zgrpc.clock.monotonicNs() - t0, std.time.ns_per_ms);

    if (result) |_| {
        std.debug.print("[timeout-probe] 失败：服务端不响应，客户端却成功返回了（{d}ms）\n", .{elapsed_ms});
        return error.Unexpected;
    } else |err| {
        if (err != error.Timeout) {
            if (is_windows) {
                // Windows：watchdog 的 shutdown 唤不醒 AFD 挂起读，只能等对端断开/内部超时。
                // 这里如实标注为 SKIP，而不是假装通过。
                std.debug.print(
                    "[timeout-probe] SKIP（Windows 无可用原生读超时）：{s}，耗时 {d}ms\n",
                    .{ @errorName(err), elapsed_ms },
                );
                return;
            }
            std.debug.print("[timeout-probe] 失败：期望 error.Timeout，实际 {s}（{d}ms）\n", .{
                @errorName(err), elapsed_ms,
            });
            return error.Unexpected;
        }
    }

    if (elapsed_ms < timeout_ms or elapsed_ms > max_elapsed_ms) {
        std.debug.print("[timeout-probe] 失败：耗时 {d}ms 不在预期区间 [{d}ms, {d}ms]\n", .{
            elapsed_ms, timeout_ms, max_elapsed_ms,
        });
        return error.Unexpected;
    }

    std.debug.print(
        "[timeout-probe] 客户端本地超时生效：{d}ms 后返回 error.Timeout（设定 {d}ms）\n",
        .{ elapsed_ms, timeout_ms },
    );
}
