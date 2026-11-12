//! 验证客户端的**本地读超时**真的生效（内核层，零线程）。
//!
//! 运行：`zig build timeout-probe`
//!
//! 为什么需要这个用例：`grpc-timeout` 头只管服务端，服务端要是卡住（或对端根本不是 gRPC），
//! 客户端必须能自己退出，而不是无限阻塞。这里用真实 socket 验证。
//!
//! 三段断言：
//! ① 服务端只 accept 不回包 → 客户端按 `timeout_ms` 返回 `error.Timeout`；
//! ② 超时由**内核层**产生（`Client.native_timeout` = true），全程**没有读线程**；
//! ③ 超时后 IRP 已收尾 → socket 可以安全 `close`（不再有「关连接导致读线程崩进程」的坑）。

const std = @import("std");
const zgrpc = @import("zgrpc");

const port: u16 = 18052;
const timeout_ms: u32 = 200;
/// 允许的耗时上限（含调度抖动）：超时 200ms，若还在 2s 内返回就算正常
const max_elapsed_ms: i128 = 2000;
/// 服务端沉默时长（之后关闭连接）
const silent_ns: u64 = 3 * std.time.ns_per_s;

/// 只接受连接、然后一直不响应（保持到 `silent_ns` 后关闭）。
fn silentAccept(io: std.Io, listener: *std.Io.net.Server) void {
    const stream = listener.accept(io) catch return;
    defer stream.close(io);
    // 不读、不写：客户端应靠自己的 deadline 退出
    zgrpc.clock.sleepNs(silent_ns);
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
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
    // 现在可以安全关闭：超时走内核，取消后 IRP 已收尾，不存在「关连接导致读线程拿到
    // CONNECTION_ABORTED 而 panic」的问题（那是旧读线程方案的局限）。
    defer stream.close(io);

    var recv_buf: [4096]u8 = undefined;
    var send_buf: [4096]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);

    var client = zgrpc.client.Client.init(gpa, &sr.interface, &sw.interface, .{
        .authority = "127.0.0.1",
        .timeout_ms = timeout_ms,
    });
    client.enableTimeout(stream, io);
    defer client.deinit();

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    // ---------- ① 超时生效 ----------
    const t0 = zgrpc.clock.monotonicNs();
    const result = client.unary("/echo.Echo/Say", "hi", arena_state.allocator());
    const elapsed_ms = @divTrunc(zgrpc.clock.monotonicNs() - t0, std.time.ns_per_ms);

    if (result) |_| {
        std.debug.print("[timeout-probe] ① 失败：服务端不响应，客户端却成功返回（{d}ms）\n", .{elapsed_ms});
        return error.Unexpected;
    } else |err| {
        if (err != error.Timeout) {
            std.debug.print("[timeout-probe] ① 失败：期望 error.Timeout，实际 {s}（{d}ms）\n", .{
                @errorName(err), elapsed_ms,
            });
            return error.Unexpected;
        }
    }

    if (elapsed_ms < timeout_ms or elapsed_ms > max_elapsed_ms) {
        std.debug.print("[timeout-probe] ① 失败：耗时 {d}ms 不在 [{d}ms, {d}ms]\n", .{
            elapsed_ms, timeout_ms, max_elapsed_ms,
        });
        return error.Unexpected;
    }
    std.debug.print(
        "[timeout-probe] ① 超时生效：{d}ms 后返回 error.Timeout（设定 {d}ms）\n",
        .{ elapsed_ms, timeout_ms },
    );

    // ---------- ② 内核层实现，无读线程 ----------
    if (!client.native_timeout) {
        std.debug.print("[timeout-probe] ② 失败：native_timeout 未启用\n", .{});
        return error.Unexpected;
    }
    std.debug.print("[timeout-probe] ② 由内核层产生（native_timeout=true），全程零读线程\n", .{});

    // ---------- ③ 超时后句柄仍可安全关闭 ----------
    // 若内核里还有挂起的 IRP，close 会让后续操作拿到 CONNECTION_ABORTED；
    // 这里由 defer 关闭，进程正常退出即为通过。
    std.debug.print("[timeout-probe] PASS\n", .{});
}
