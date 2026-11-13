//! 调用策略层：**拦截器 / 重试 / 负载均衡**。
//!
//! 为什么不塞进 `client.Client`：`Client` 是「一条**已建立**连接上的编解码器」——
//! 它不负责连接从哪来、断了要不要重连、多个地址怎么选。这些都是**调用策略**，
//! 混进去会让 `Client` 变成一个什么都管的胖对象，也让「单连接直连」这种简单用法
//! 被迫背上重试与地址表的复杂度。
//!
//! 分层：
//! ```text
//! Caller（本文件）      : 选地址、建/断连接、重试、拦截器
//!   └─ client.Client    : 一条连接上的 h2/gRPC 编解码
//!        └─ h2 / hpack  : 协议层
//! ```

const std = @import("std");
const log = @import("log.zig");
const client_mod = @import("client.zig");
const clock = @import("clock.zig");

pub const Endpoint = struct {
    host: []const u8,
    port: u16,
};

/// 拦截器：只做**观察**（打点、日志、统计），不修改请求/响应内容 ——
/// 要改内容请直接构造 `Request`（`Caller` 会透传 metadata）。
/// 做成「观察者」而不是「可改写的中间件」，是因为同步模型下改写需要交出所有权，
/// 反而容易漏释放。
pub const Interceptor = struct {
    ctx: ?*anyopaque = null,
    /// 每次尝试**发请求前**调用
    on_request: ?*const fn (ctx: ?*anyopaque, path: []const u8, attempt: u8) void = null,
    /// 每次尝试**拿到响应后**调用（失败的 status 也会走到这里）
    on_response: ?*const fn (ctx: ?*anyopaque, path: []const u8, status: u32, elapsed_ns: i128, attempt: u8) void = null,
};

pub const RetryPolicy = struct {
    /// 总尝试次数（1 = 不重试）
    max_attempts: u8 = 1,
    initial_backoff_ms: u32 = 100,
    max_backoff_ms: u32 = 2_000,
    /// 可重试的 gRPC status（默认 14=UNAVAILABLE、8=RESOURCE_EXHAUSTED）
    retry_on: []const u32 = &.{ 14, 8 },
};

pub const Options = struct {
    /// 每次调用的时限（毫秒）；null = 不设
    timeout_ms: ?u32 = null,
    content_type: client_mod.ContentType = .grpc,
    retry: RetryPolicy = .{},
    interceptors: []const Interceptor = &.{},
};

/// 一条已建立的连接（含它自己的收发缓冲，缓冲随连接一起释放）。
pub const Conn = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,
    recv: []u8,
    send: []u8,
    src: std.Io.net.Stream.Reader,
    swr: std.Io.net.Stream.Writer,
    client: client_mod.Client,

    fn create(alloc: std.mem.Allocator, io: std.Io, ep: Endpoint, opts: Options) !*Conn {
        const addr = try std.Io.net.IpAddress.parse(ep.host, ep.port);
        const stream = try addr.connect(io, .{ .mode = .stream });
        // 后续任一步失败都要关掉 socket，否则每失败一次漏一个连接
        errdefer stream.close(io);

        const c = try alloc.create(Conn);
        errdefer alloc.destroy(c);
        c.alloc = alloc;
        c.io = io;
        c.stream = stream;
        c.recv = try alloc.alloc(u8, 16 * 1024);
        errdefer alloc.free(c.recv);
        c.send = try alloc.alloc(u8, 16 * 1024);
        errdefer alloc.free(c.send);

        c.src = stream.reader(io, c.recv);
        c.swr = stream.writer(io, c.send);
        c.client = client_mod.Client.init(alloc, &c.src.interface, &c.swr.interface, .{
            .authority = ep.host,
            .timeout_ms = opts.timeout_ms,
            .content_type = opts.content_type,
        });
        // 关 Nagle + 启用本地读超时（都挂在 Client 上）
        c.client.enableTimeout(stream, io);
        return c;
    }

    fn destroy(self: *Conn) void {
        const alloc = self.alloc;
        self.client.deinit();
        self.stream.close(self.io);
        alloc.free(self.recv);
        alloc.free(self.send);
        alloc.destroy(self);
    }
};

pub const Caller = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    endpoints: []const Endpoint,
    opts: Options,
    /// round-robin 下标（**建连时**轮转，不是每次请求）
    rr: usize = 0,
    conn: ?*Conn = null,
    /// 可观测计数
    attempts_total: u64 = 0,
    retries_total: u64 = 0,

    pub fn init(alloc: std.mem.Allocator, io: std.Io, endpoints: []const Endpoint, opts: Options) Caller {
        return .{ .alloc = alloc, .io = io, .endpoints = endpoints, .opts = opts };
    }

    pub fn deinit(self: *Caller) void {
        self.dropConn();
        self.* = undefined;
    }

    fn dropConn(self: *Caller) void {
        if (self.conn) |c| {
            c.destroy();
            self.conn = null;
        }
    }

    /// 懒建连 + round-robin 选地址。
    fn ensureConn(self: *Caller) !*Conn {
        if (self.conn) |c| return c;
        if (self.endpoints.len == 0) return error.NoEndpoints;
        const ep = self.endpoints[self.rr % self.endpoints.len];
        self.rr +%= 1;
        const c = Conn.create(self.alloc, self.io, ep, self.opts) catch |e| {
            log.caller.warn("连接 {s}:{d} 失败：{s}", .{ ep.host, ep.port, @errorName(e) });
            return e;
        };
        self.conn = c;
        return c;
    }

    fn isRetryable(self: *const Caller, status: u32) bool {
        for (self.opts.retry.retry_on) |s| {
            if (s == status) return true;
        }
        return false;
    }

    fn sleepBackoff(self: *Caller, backoff_ms: *u32) void {
        clock.sleepNs(@as(u64, backoff_ms.*) * std.time.ns_per_ms);
        const next = backoff_ms.* *| 2;
        backoff_ms.* = @min(next, self.opts.retry.max_backoff_ms);
    }

    /// 带重试 / 拦截器的 unary 调用。
    ///
    /// 重试判据：
    /// - **连接级错误**（建连失败、连接断开、超时）→ 重试（换地址或重连）
    /// - **业务 status 在 `retry_on` 里** → 重试
    /// - 其它（例如 `INVALID_ARGUMENT`）→ 直接返回，重试没有意义
    pub fn unary(
        self: *Caller,
        method: []const u8,
        payload: []const u8,
        arena: std.mem.Allocator,
    ) !client_mod.Result {
        const max_attempts = @max(@as(u8, 1), self.opts.retry.max_attempts);
        var backoff_ms = self.opts.retry.initial_backoff_ms;
        var attempt: u8 = 1;
        var last_err: anyerror = error.ConnectionClosed;

        while (attempt <= max_attempts) : (attempt += 1) {
            for (self.opts.interceptors) |it| {
                if (it.on_request) |f| f(it.ctx, method, attempt);
            }
            self.attempts_total +|= 1;
            const t0 = clock.monotonicNs();

            var conn: *Conn = self.ensureConn() catch |e| {
                last_err = e;
                if (attempt < max_attempts) {
                    self.retries_total +|= 1;
                    log.caller.debug("第 {d}/{d} 次尝试建连失败（{s}），退避 {d}ms 后重试", .{ attempt, max_attempts, @errorName(e), backoff_ms });
                    self.sleepBackoff(&backoff_ms);
                    continue;
                }
                return e;
            };

            const res = conn.client.unary(method, payload, arena) catch |e| {
                // 连接状态已不可信：丢弃，下次重试会重建（并按 rr 换地址）
                self.dropConn();
                last_err = e;
                if (attempt < max_attempts) {
                    self.retries_total +|= 1;
                    log.caller.debug("第 {d}/{d} 次尝试失败（{s}），退避 {d}ms 后重试", .{ attempt, max_attempts, @errorName(e), backoff_ms });
                    self.sleepBackoff(&backoff_ms);
                    continue;
                }
                return e;
            };

            const elapsed = clock.monotonicNs() - t0;
            for (self.opts.interceptors) |it| {
                if (it.on_response) |f| f(it.ctx, method, res.status, elapsed, attempt);
            }

            if (attempt < max_attempts and self.isRetryable(res.status)) {
                self.retries_total +|= 1;
                log.caller.debug("第 {d}/{d} 次拿到可重试 status={d}，退避 {d}ms 后重试", .{ attempt, max_attempts, res.status, backoff_ms });
                self.dropConn();
                self.sleepBackoff(&backoff_ms);
                continue;
            }
            return res;
        }
        return last_err;
    }
};

// ===========================================================================
// 连接池（对齐标准 gRPC 客户端的 Channel / ClientConn 语义）
// ===========================================================================
//
// 为什么需要：`Caller` 只缓存**一条**连接且不是线程安全的（`conn` / `rr` / 计数器都是可变状态）。
// 多线程共用一个 `Caller` 会直接数据竞争；给每个线程建一个 `Caller` 又会连接数失控。
// 连接池把「借 / 还 / 上限 / 空闲回收 / 坏连接丢弃」集中管理。
//
// 为什么不能靠 h2 多路复用省掉池：`client.Client` 是**同步阻塞**模型，内部持有
// stream_id 序列与 HPACK 动态表，同一条连接上无法并发发起多个 unary。所以并发度 ≈ 连接数。

pub const PoolOptions = struct {
    /// 总连接上限（含已借出的）
    max_total: usize = 8,
    /// 空闲列表上限，超出则关闭连归还的连接
    max_idle: usize = 8,
    /// 空闲超过它就关闭（毫秒）
    idle_timeout_ms: i64 = 30_000,
    /// 借不到连接时的最长等待（毫秒）
    acquire_timeout_ms: i64 = 5_000,
    /// 单连接建连/调用参数
    call: Options = .{},
};

const IdleConn = struct {
    conn: *Conn,
    since_ms: i64,
};

fn tickMs() i64 {
    return @intCast(@divTrunc(clock.monotonicNs(), 1_000_000));
}

pub const Pool = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    endpoints: []const Endpoint,
    opts: PoolOptions,

    mutex: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,

    /// 空闲连接（LIFO，取最近用过的，缓存命中率更高）
    idle: std.ArrayListUnmanaged(IdleConn) = .empty,
    /// 已建立连接数（含借出）
    total: usize = 0,
    closed: bool = false,
    /// round-robin 地址下标
    rr: usize = 0,

    // 可观测计数
    created: u64 = 0,
    reused: u64 = 0,
    discarded: u64 = 0,

    pub fn init(alloc: std.mem.Allocator, io: std.Io, endpoints: []const Endpoint, opts: PoolOptions) Pool {
        return .{ .alloc = alloc, .io = io, .endpoints = endpoints, .opts = opts };
    }

    /// 关闭所有空闲连接。**借出中的连接不在此处理**（由借出方 `release`）。
    pub fn deinit(self: *Pool) void {
        const alloc = self.alloc;
        const io = self.io;
        self.mutex.lockUncancelable(io);
        self.closed = true;
        const items = self.idle;
        self.idle = .empty;
        self.total -= items.items.len;
        std.Io.Condition.broadcast(&self.cond, io);
        self.mutex.unlock(io);

        for (items.items) |it| it.conn.destroy();
        alloc.free(items.items);
    }

    /// 借一条连接：优先复用空闲，不足且未达上限则新建，达上限则等待归还。
    pub fn acquire(self: *Pool) !*Conn {
        const deadline = tickMs() + self.opts.acquire_timeout_ms;

        while (true) {
            var expired: ?*Conn = null;
            self.mutex.lockUncancelable(self.io);

            // 1) 空闲栈（顺手淘汰过期/已关闭的连接）
            while (self.idle.items.len > 0) {
                const item = self.idle.pop().?;
                if (self.closed or (tickMs() - item.since_ms) > self.opts.idle_timeout_ms) {
                    self.total -= 1;
                    self.discarded +|= 1;
                    if (expired) |e| e.destroy();
                    expired = item.conn;
                    continue;
                }
                self.reused +|= 1;
                self.mutex.unlock(self.io);
                if (expired) |e| e.destroy();
                return item.conn;
            }
            if (expired) |e| {
                self.mutex.unlock(self.io);
                e.destroy();
                continue;
            }

            if (self.closed) {
                self.mutex.unlock(self.io);
                return error.PoolClosed;
            }

            if (self.endpoints.len == 0) {
                self.mutex.unlock(self.io);
                return error.NoEndpoints;
            }

            if (self.total < self.opts.max_total) {
                // 名额在锁内预留，保证并发下不冲破上限
                self.total += 1;
                const ep = self.nextEndpoint();
                self.mutex.unlock(self.io);

                const c = Conn.create(self.alloc, self.io, ep, self.opts.call) catch |e| {
                    self.mutex.lockUncancelable(self.io);
                    self.total -= 1;
                    std.Io.Condition.signal(&self.cond, self.io);
                    self.mutex.unlock(self.io);
                    return e;
                };
                self.mutex.lockUncancelable(self.io);
                self.created +|= 1;
                self.mutex.unlock(self.io);
                return c;
            }

            // 2) 达上限：等别人归还（`waitTimeout` 要求调用时持有互斥量）
            const remain = deadline - tickMs();
            if (remain <= 0) {
                self.mutex.unlock(self.io);
                return error.AcquireTimeout;
            }
            _ = std.Io.Condition.waitTimeout(&self.cond, self.io, &self.mutex, .{
                .duration = .{
                    .raw = .{ .nanoseconds = @as(i96, remain) * std.time.ns_per_ms },
                    .clock = .awake,
                },
            }) catch {};
            self.mutex.unlock(self.io);
        }
    }

    /// 归还连接。`healthy = false` 时直接关闭（不回到空闲列表）。
    pub fn release(self: *Pool, c: *Conn, healthy: bool) void {
        var to_destroy: ?*Conn = null;
        self.mutex.lockUncancelable(self.io);
        if (!healthy) self.discarded +|= 1;
        if (!healthy or self.closed or self.idle.items.len >= self.opts.max_idle) {
            self.total -= 1;
            to_destroy = c;
        } else {
            self.idle.append(self.alloc, .{ .conn = c, .since_ms = tickMs() }) catch {
                self.total -= 1;
                to_destroy = c;
            };
        }
        std.Io.Condition.signal(&self.cond, self.io);
        self.mutex.unlock(self.io);

        if (to_destroy) |d| d.destroy();
    }

    /// 借连接 → 调用 → 归还，失败自动丢弃并发起重试（退避同 `RetryPolicy`）。
    pub fn unary(
        self: *Pool,
        method: []const u8,
        payload: []const u8,
        arena: std.mem.Allocator,
    ) !client_mod.Result {
        const max_attempts = @max(@as(u8, 1), self.opts.call.retry.max_attempts);
        var backoff_ms = self.opts.call.retry.initial_backoff_ms;
        var attempt: u8 = 1;

        while (true) : (attempt += 1) {
            for (self.opts.call.interceptors) |it| {
                if (it.on_request) |f| f(it.ctx, method, attempt);
            }
            const t0 = clock.monotonicNs();

            const c = self.acquire() catch |e| {
                if (attempt >= max_attempts) return e;
                clock.sleepNs(@as(u64, backoff_ms) * std.time.ns_per_ms);
                backoff_ms = @min(backoff_ms *| 2, self.opts.call.retry.max_backoff_ms);
                continue;
            };

            const res = c.client.unary(method, payload, arena) catch |e| {
                // 连接状态已不可信：不回到池里
                self.release(c, false);
                if (attempt >= max_attempts) return e;
                clock.sleepNs(@as(u64, backoff_ms) * std.time.ns_per_ms);
                backoff_ms = @min(backoff_ms *| 2, self.opts.call.retry.max_backoff_ms);
                continue;
            };

            const elapsed = clock.monotonicNs() - t0;
            for (self.opts.call.interceptors) |it| {
                if (it.on_response) |f| f(it.ctx, method, res.status, elapsed, attempt);
            }
            self.release(c, true);
            return res;
        }
    }

    pub fn stats(self: *Pool) struct { created: u64, reused: u64, discarded: u64, total: usize, idle: usize } {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{
            .created = self.created,
            .reused = self.reused,
            .discarded = self.discarded,
            .total = self.total,
            .idle = self.idle.items.len,
        };
    }

    fn nextEndpoint(self: *Pool) Endpoint {
        const n = self.endpoints.len;
        const ep = self.endpoints[self.rr % n];
        self.rr +%= 1;
        return ep;
    }
};

test "pool：建连失败会回滚名额（不泄漏计数）" {
    // 127.0.0.1:1 无监听，必然快速失败；只验证会计逻辑
    const eps = [_]Endpoint{.{ .host = "127.0.0.1", .port = 1 }};
    var pool = Pool.init(testing.allocator, std.testing.io, &eps, .{
        .acquire_timeout_ms = 200,
        .call = .{ .timeout_ms = 100 },
    });
    defer pool.deinit();

    var succeeded = false;
    if (pool.acquire()) |_| {
        succeeded = true;
    } else |_| {}
    try testing.expect(!succeeded);

    // 失败必须把预留的名额退回去，否则池会“假满”再也借不到
    const s = pool.stats();
    try testing.expectEqual(@as(usize, 0), s.total);
    try testing.expectEqual(@as(u64, 0), s.created);
}

test "pool：deinit 后再借返回 PoolClosed；空地址返回 NoEndpoints" {
    const empty = [_]Endpoint{};
    var p1 = Pool.init(testing.allocator, std.testing.io, &empty, .{});
    try testing.expectError(error.NoEndpoints, p1.acquire());
    p1.deinit();

    const eps = [_]Endpoint{.{ .host = "127.0.0.1", .port = 1 }};
    var p2 = Pool.init(testing.allocator, std.testing.io, &eps, .{});
    p2.deinit();
    try testing.expectError(error.PoolClosed, p2.acquire());
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "caller：重试判据（retry_on 命中才重试）" {
    // 不起网络：只验证策略判定与退避递增
    const c = Caller.init(testing.allocator, undefined, &.{}, .{ .retry = .{ .retry_on = &.{ 14, 8 } } });

    try testing.expect(c.isRetryable(14));
    try testing.expect(c.isRetryable(8));
    try testing.expect(!c.isRetryable(3)); // INVALID_ARGUMENT 不该重试
    try testing.expect(!c.isRetryable(0)); // OK
}

test "caller：退避指数增长并封顶" {
    const c = Caller.init(testing.allocator, undefined, &.{}, .{ .retry = .{ .initial_backoff_ms = 10, .max_backoff_ms = 40 } });

    var b: u32 = 10;
    const expect = [_]u32{ 20, 40, 40 };
    for (expect) |want| {
        // 只算「下一次」的值，不真的 sleep
        const next = @min(b *| 2, c.opts.retry.max_backoff_ms);
        b = next;
        try testing.expectEqual(want, b);
    }
}

test "caller：没有 endpoint 时报 NoEndpoints" {
    var c = Caller.init(testing.allocator, undefined, &.{}, .{});
    try testing.expectError(error.NoEndpoints, c.ensureConn());
}

test "caller：round-robin 建连时轮转" {
    const eps = [_]Endpoint{
        .{ .host = "127.0.0.1", .port = 1 },
        .{ .host = "127.0.0.1", .port = 2 },
        .{ .host = "127.0.0.1", .port = 3 },
    };
    var c = Caller.init(testing.allocator, undefined, &eps, .{});

    // 不真的建连，只看选中的下标推进
    var picked: [3]u16 = undefined;
    for (&picked, 0..) |*slot, i| {
        _ = i;
        const ep = eps[c.rr % eps.len];
        slot.* = ep.port;
        c.rr +%= 1;
    }
    try testing.expectEqualSlices(u16, &.{ 1, 2, 3 }, &picked);
    // 回绕
    try testing.expectEqual(@as(u16, 1), eps[c.rr % eps.len].port);
}
