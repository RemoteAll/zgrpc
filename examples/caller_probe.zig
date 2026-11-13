//! Caller 探针：验证**拦截器 / 重试 / 负载均衡**三件事真的生效。
//!
//! 场景：
//! 1. 正常调用 → status 0，只尝试 1 次；
//! 2. 调未注册方法 → 服务端回 UNIMPLEMENTED(12)，把 12 放进 `retry_on` 后应尝试 `max_attempts` 次；
//! 3. 拦截器统计：on_request / on_response 次数必须与尝试次数一致；
//! 4. 负载均衡：两个 endpoint 指向同一端口，建连时按 rr 轮转（观察选中的下标）。

const std = @import("std");
const zgrpc = @import("zgrpc");

var dummy: u8 = 0;

/// 拦截器状态（放全局，回调只拿到 ctx）
const Stats = struct {
    requests: usize = 0,
    responses: usize = 0,
    last_status: u32 = 999,
    attempts_seen: [8]u8 = @splat(0),
};

fn onRequest(ctx: ?*anyopaque, path: []const u8, attempt: u8) void {
    _ = path;
    const s: *Stats = @ptrCast(@alignCast(ctx.?));
    s.requests += 1;
    if (attempt <= 8) s.attempts_seen[attempt - 1] += 1;
}

fn onResponse(ctx: ?*anyopaque, path: []const u8, status: u32, elapsed_ns: i128, attempt: u8) void {
    _ = path;
    _ = elapsed_ns;
    _ = attempt;
    const s: *Stats = @ptrCast(@alignCast(ctx.?));
    s.responses += 1;
    s.last_status = status;
}

fn sayHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    try call.sendMessage(msg.payload);
}

fn handleConn(allocator: std.mem.Allocator, io: std.Io, stream: std.Io.net.Stream, server: *zgrpc.grpc.Server) void {
    server.serveConn(allocator, io, stream);
}

fn acceptLoop(allocator: std.mem.Allocator, io: std.Io, listener: *std.Io.net.Server, server: *zgrpc.grpc.Server) void {
    while (true) {
        const stream = listener.accept(io) catch return;
        const th = std.Thread.spawn(.{ .stack_size = zgrpc.grpc.Server.conn_stack_size }, handleConn, .{ allocator, io, stream, server }) catch {
            stream.close(io);
            continue;
        };
        th.detach();
    }
}

pub fn main(init: std.process.Init) !void {
    _ = init;
    const allocator = std.heap.page_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // ---- 服务端 ----
    var routes = [_]zgrpc.grpc.Route{
        .{ .path = "/probe.Probe/Say", .handler = sayHandler },
    };
    var no_streams = [_]zgrpc.grpc.StreamRoute{};
    var server = zgrpc.grpc.Server{ .routes = &routes, .stream_routes = &no_streams, .outer_ctx = &dummy };

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try addr.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);
    const port = listener.socket.address.getPort();
    std.debug.print("[caller-probe] 服务端 127.0.0.1:{d}\n", .{port});

    const acc = try std.Thread.spawn(.{ .stack_size = zgrpc.grpc.Server.conn_stack_size }, acceptLoop, .{ allocator, io, &listener, &server });
    acc.detach();

    // ---- Caller：两个 endpoint 指向同一端口，验证 rr 轮转 ----
    var stats = Stats{};
    const eps = [_]zgrpc.caller.Endpoint{
        .{ .host = "127.0.0.1", .port = port },
        .{ .host = "127.0.0.1", .port = port },
    };
    var caller = zgrpc.caller.Caller.init(allocator, io, &eps, .{
        .timeout_ms = 2000,
        .retry = .{
            .max_attempts = 3,
            .initial_backoff_ms = 10,
            .max_backoff_ms = 50,
            .retry_on = &.{12}, // 只把 UNIMPLEMENTED 当成可重试，方便观测
        },
        .interceptors = &.{.{ .ctx = &stats, .on_request = onRequest, .on_response = onResponse }},
    });
    defer caller.deinit();

    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // ---- ① 正常调用：1 次尝试、status 0 ----
    const ok_res = try caller.unary("/probe.Probe/Say", "hello", a);
    if (!ok_res.ok()) {
        std.debug.print("[caller-probe] FAIL：正常调用 status={d} msg={s}\n", .{ ok_res.status, ok_res.message });
        return error.UnexpectedStatus;
    }
    if (caller.attempts_total != 1 or caller.retries_total != 0) {
        std.debug.print("[caller-probe] FAIL：正常调用 attempts={d} retries={d}（期望 1/0）\n", .{ caller.attempts_total, caller.retries_total });
        return error.BadCounters;
    }
    std.debug.print("[caller-probe] ① 正常调用 PASS（attempts={d} retries={d} status={d}）\n", .{ caller.attempts_total, caller.retries_total, ok_res.status });

    // ---- ② 未注册方法 → UNIMPLEMENTED(12) 在 retry_on 里 → 重试到 max_attempts ----
    const before = caller.attempts_total;
    const bad = try caller.unary("/probe.Probe/Nope", "x", a);
    const tried = caller.attempts_total - before;
    if (bad.status != 12) {
        std.debug.print("[caller-probe] FAIL：未注册方法 status={d}（期望 12）\n", .{bad.status});
        return error.UnexpectedStatus;
    }
    if (tried != 3) {
        std.debug.print("[caller-probe] FAIL：可重试 status 只尝试了 {d} 次（期望 3）\n", .{tried});
        return error.BadRetryCount;
    }
    std.debug.print("[caller-probe] ② 重试 PASS（status=12，尝试 {d} 次，累计 retries={d}）\n", .{ tried, caller.retries_total });

    // ---- ③ 拦截器：次数与尝试次数一致 ----
    if (stats.requests != caller.attempts_total or stats.responses != caller.attempts_total) {
        std.debug.print("[caller-probe] FAIL：拦截器 req={d} rsp={d}，尝试={d}（应相等）\n", .{ stats.requests, stats.responses, caller.attempts_total });
        return error.BadInterceptorCount;
    }
    std.debug.print("[caller-probe] ③ 拦截器 PASS（on_request={d} on_response={d} last_status={d}）\n", .{ stats.requests, stats.responses, stats.last_status });

    // ---- ④ 负载均衡：连接被丢弃后重连时按 rr 轮转 ----
    // 正常调用复用现有连接 → rr 不动；新建 Caller 后第一次建连才推进 rr。
    var caller2 = zgrpc.caller.Caller.init(allocator, io, &eps, .{ .timeout_ms = 2000 });
    defer caller2.deinit();
    const eps2_first = eps[caller2.rr % eps.len].port;
    _ = try caller2.unary("/probe.Probe/Say", "again", a);
    const eps2_after = caller2.rr;
    if (eps2_first != port or eps2_after != 1) {
        std.debug.print("[caller-probe] FAIL：rr 未按预期轮转（first={d} after={d}）\n", .{ eps2_first, eps2_after });
        return error.BadRoundRobin;
    }
    std.debug.print("[caller-probe] ④ 负载均衡 PASS（建连时 rr 前进到 {d}）\n", .{eps2_after});

    std.debug.print("[caller-probe] PASS：拦截器 / 重试 / 负载均衡均生效\n", .{});
}
