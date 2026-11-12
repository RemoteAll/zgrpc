//! gRPC 客户端（明文 h2c）：握手、发起调用、读响应。
//!
//! 定位：给 zgrpc 补上"另一半"—— 之前只有服务端，连自测都要手写帧。
//! `callMany` 会在**同一条连接**上并发发起多条流（不同 stream_id），配合 h2 的多路复用。
//!
//! 限制（明确）：不做异步运行时；`callMany` 是"先全部发出、再统一收完"的批量模型，
//! 交互式双向流（bidi）需要自己基于 `h2` 层构建，见 README「未做」。

const std = @import("std");
const builtin = @import("builtin");
const h2 = @import("h2.zig");
const hpack = @import("hpack.zig");
const grpc = @import("grpc.zig");

const afd_reader = @import("afd_reader.zig");
const socket_timeout = @import("socket_timeout.zig");
const clock = @import("clock.zig");

pub const Request = struct {
    /// 完整方法路径，如 `/echo.Echo/Say`
    method: []const u8,
    payload: []const u8 = "",
    /// 单次调用的超时（毫秒）；空则用 `Options.timeout_ms`。发出时为 `grpc-timeout` 头。
    timeout_ms: ?u32 = null,
    /// 自定义请求 metadata（`-bin` 结尾的请自行 base64；可用 `grpc.encodeBase64NoPad`）
    metadata: []const hpack.Header = &.{},
};

pub const Result = struct {
    stream_id: u32,
    /// gRPC 状态码（0 = OK）；未收到 trailer 时为 0，但 `aborted` 为 true
    status: u32 = 0,
    /// `grpc-message`（已 percent-decode；可能为空）
    message: []const u8 = "",
    /// 已解出（去分帧）的响应消息；unary 通常 1 条
    messages: []const []const u8 = &.{},
    /// 收到 `RST_STREAM`，或流结束时没有 `grpc-status`
    aborted: bool = false,
    /// 响应的 `:status`（非 200 时说明对端回的不是 gRPC 响应，详见 `message`）
    http_status: []const u8 = "",
    /// 响应头帧里的全部头（含 `content-type` 等，业务自行挑）
    headers: std.ArrayListUnmanaged(hpack.Header) = .empty,
    /// trailers 里的自定义 metadata（`grpc-status` / `grpc-message` 已单独抽出）
    trailers: std.ArrayListUnmanaged(hpack.Header) = .empty,

    pub fn ok(self: Result) bool {
        return !self.aborted and self.status == 0;
    }

    /// 读一个响应头（同名多次时返回第一个）。
    pub fn header(self: Result, name: []const u8) ?[]const u8 {
        for (self.headers.items) |h| {
            if (std.mem.eql(u8, h.name, name)) return h.value;
        }
        return null;
    }

    /// 读一个 trailers metadata（同名多次时返回第一个）。
    pub fn trailer(self: Result, name: []const u8) ?[]const u8 {
        for (self.trailers.items) |h| {
            if (std.mem.eql(u8, h.name, name)) return h.value;
        }
        return null;
    }

    /// 读一个**二进制** metadata（`-bin` 结尾，trailers 优先、再找响应头）：base64 解码。
    pub fn binaryMetadata(self: Result, arena: std.mem.Allocator, name: []const u8) !?[]const u8 {
        const raw = self.trailer(name) orelse self.header(name) orelse return null;
        const first = if (std.mem.indexOfScalar(u8, raw, ',')) |i| raw[0..i] else raw;
        return try grpc.decodeBase64Loose(arena, std.mem.trim(u8, first, " \t"));
    }
};

/// 请求/响应的 content-type 形态。
pub const ContentType = enum {
    /// 原生 gRPC over HTTP/2
    grpc,
    /// gRPC-Web（二进制帧，trailers 走 body 尾帧）
    grpc_web,
    /// gRPC-Web 文本模式（base64，浏览器 XHR 回退用）
    grpc_web_text,

    pub fn header(self: ContentType) []const u8 {
        return switch (self) {
            .grpc => "application/grpc",
            .grpc_web => "application/grpc-web+proto",
            .grpc_web_text => "application/grpc-web-text+proto",
        };
    }
};

pub const Options = struct {
    authority: []const u8 = "localhost",
    scheme: []const u8 = "http",
    /// 单条流响应体上限
    max_body: usize = 16 * 1024 * 1024,
    /// 默认超时（毫秒）；空 = 不设（同规范：缺省视为无限超时）
    timeout_ms: ?u32 = null,
    /// 规范建议的 UA 格式：`grpc-<lang>/<ver>`
    user_agent: []const u8 = "grpc-zgrpc/0.2.1",
    /// 用哪种 content-type 形态（web 系列会自动做尾帧与 base64 处理）
    content_type: ContentType = .grpc,
};

pub const Client = struct {
    alloc: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    opts: Options = .{},
    decoder: hpack.Decoder,
    /// 下一个可用流 id（客户端发起的流必须是奇数）
    next_stream_id: u32 = 1,
    started: bool = false,
    /// 本地超时所需的 socket（`enableTimeout` 提供）
    timeout_stream: ?std.Io.net.Stream = null,
    timeout_io: ?std.Io = null,
    /// 是否已启用**原生读超时**（Windows：AFD overlapped；POSIX：`SO_RCVTIMEO`）
    native_timeout: bool = false,
    /// 超时实现路径：先试 std.Io 带超时操作，不支持时降级到平台原生
    timeout_mode: TimeoutMode = .auto,
    /// 本次调用的绝对截止时刻（单调时钟纳秒）；null = 不限
    deadline_ns: ?i128 = null,

    const TimeoutMode = enum {
        /// 尚未探测：先试 `io.operateTimeout`
        auto,
        /// 已确认 `io.operateTimeout` 可用（异步后端如 zio）
        io_api,
        /// 已确认不可用，走平台原生方案
        platform,
    };

    pub fn init(
        alloc: std.mem.Allocator,
        reader: *std.Io.Reader,
        writer: *std.Io.Writer,
        opts: Options,
    ) Client {
        return .{
            .alloc = alloc,
            .reader = reader,
            .writer = writer,
            .opts = opts,
            .decoder = hpack.Decoder.init(alloc),
        };
    }

    pub fn deinit(self: *Client) void {
        self.stopGuard();
        // 本地超时走的是**内核层**（AFD overlapped / `SO_RCVTIMEO`）：
        // 不启线程、不占额外内存，因此这里没有任何需要 join / 回收的东西。
        self.decoder.deinit();
    }

    /// 启用**本地读超时**（内核层，零线程）。
    ///
    /// 同时会顺手关闭 Nagle（`TCP_NODELAY`）：gRPC 是 HEADERS+DATA+TRAILERS 多小包交互，
    /// 开着 Nagle 与对端延迟 ACK 叠加，会造成随机 10~200ms 延迟尖峰（实测 p95 从 14.7ms 降到 0.35ms，
    /// QPS 提升约 10 倍）。
    ///
    /// - **Windows**：在 AFD 句柄上自己发起 overlapped `IOCTL_AFD_RECEIVE`，
    ///   再用 `NtWaitForSingleObject(句柄)` 带时限等待（见 `afd_reader.zig`，实测 201ms 生效）。
    /// - **POSIX**：`setsockopt(SO_RCVTIMEO)`，到点 `read` 返回 `EAGAIN`。
    pub fn enableTimeout(self: *Client, stream: std.Io.net.Stream, io: std.Io) void {
        if (self.native_timeout) return;
        self.timeout_stream = stream;
        self.timeout_io = io;
        self.tuneSocket(stream, io);
        // Windows 无需预先设置超时值：时限在每次读时作为参数传入。
        if (builtin.os.tag != .windows) {
            socket_timeout.setRecvTimeout(io, stream, 0) catch |e| {
                std.debug.print("[zgrpc.client] 设置 SO_RCVTIMEO 失败，本地超时不可用：{s}\n", .{@errorName(e)});
                return;
            };
        }
        self.native_timeout = true;
    }

    /// 套接字调优（不启用超时也可以单独调）：目前是关闭 Nagle。
    ///
    /// **建议每个客户端连接都调一次**：不调的话小包往返会撞上 Nagle × 延迟 ACK，
    /// 表现为「大部分很快、但随机一批卡 10~200ms」。
    pub fn tuneSocket(self: *Client, stream: std.Io.net.Stream, io: std.Io) void {
        _ = self;
        socket_timeout.setNoDelay(io, stream, true) catch |e| {
            std.debug.print("[zgrpc.client] 关闭 Nagle 失败（延迟可能偏高）：{s}\n", .{@errorName(e)});
        };
    }
    /// 读满 `dest`；启用本地超时后按**剩余时间**喂超时，到点返回 `error.Timeout`。
    ///
    /// 优先级：
    /// 1. **先试 `io.operateTimeout(net_read)`** —— zio 等实现了完整 `std.Io` 的后端支持它，
    ///    此时底层是真正的异步（Linux io_uring / Windows IOCP / macOS kqueue），无线程；
    /// 2. 后端报 `ConcurrencyUnavailable`（std 自带的 Threaded 就这样）→ 记录一下，
    ///    后续全部回退到**平台原生方案**（Windows AFD overlapped / POSIX `SO_RCVTIMEO`）。
    ///
    /// 这样 zgrpc 自己不依赖任何异步运行时，但**换一个 Io 实例就能拿到真异步**。
    fn readExact(self: *Client, dest: []u8) !void {
        if (!self.native_timeout) return self.reader.readSliceAll(dest);
        const stream = self.timeout_stream orelse return self.reader.readSliceAll(dest);
        const io = self.timeout_io orelse return self.reader.readSliceAll(dest);

        var off: usize = 0;
        while (off < dest.len) {
            if (self.isExpired()) return error.Timeout;
            const remain = self.remainingTimeoutMs();
            const slice = dest[off..];

            // ① std.Io 原生带超时读（异步后端）
            if (self.timeout_mode != .platform) {
                if (try self.tryIoTimeoutRead(io, stream, slice, remain)) |got| {
                    if (got == 0) return error.ConnectionClosed;
                    off += got;
                    continue;
                }
            }

            // ② 平台原生回退
            var got: usize = 0;
            if (builtin.os.tag == .windows) {
                got = afd_reader.read(stream.socket.handle, slice, remain) catch |e| switch (e) {
                    error.Timeout => return error.Timeout,
                    error.ConnectionResetByPeer, error.SocketUnconnected => return error.ConnectionClosed,
                    else => |other| return other,
                };
            } else {
                if (remain) |ms| socket_timeout.setRecvTimeout(io, stream, ms) catch {};
                var datas = [1][]u8{slice};
                got = stream.read(io, &datas) catch |e| {
                    if (socket_timeout.isReadTimeout(e)) return error.Timeout;
                    return e;
                };
            }
            if (got == 0) return error.ConnectionClosed; // 对端已关闭
            off += got;
        }
    }

    /// 用 `io.operateTimeout` 做一次带时限的读。
    ///
    /// 返回 `null` 表示「当前 Io 后端不支持带超时操作」（已把 `timeout_mode` 降级为 `.platform`），
    /// 调用方应回退到平台原生方案。
    fn tryIoTimeoutRead(
        self: *Client,
        io: std.Io,
        stream: std.Io.net.Stream,
        dest: []u8,
        remain_ms: ?u32,
    ) !?usize {
        var bufs = [1][]u8{dest};
        const op = std.Io.Operation{ .net_read = .{
            .socket_handle = stream.socket.handle,
            .data = &bufs,
        } };
        const timeout: std.Io.Timeout = if (remain_ms) |ms| .{ .duration = .{
            .raw = .{ .nanoseconds = @as(u64, ms) * std.time.ns_per_ms },
            .clock = .awake,
        } } else .none;

        const res = io.operateTimeout(op, timeout) catch |e| switch (e) {
            error.ConcurrencyUnavailable => {
                self.timeout_mode = .platform;
                std.debug.print(
                    "[zgrpc.client] 当前 Io 后端不支持带超时操作，回退到平台原生读超时（异步后端可省掉这层）\n",
                    .{},
                );
                return null;
            },
            error.Timeout => return error.Timeout,
            else => return e,
        };
        self.timeout_mode = .io_api;
        const rr = res.net_read catch |e| switch (e) {
            error.ConnectionTimedOut => return error.Timeout,
            else => |other| return other,
        };
        return rr.data_len;
    }
    /// 距离截止时刻还剩多少毫秒（向上取整，至少 1）；未设截止时返回 null。
    fn remainingTimeoutMs(self: *Client) ?u32 {
        const deadline = self.deadline_ns orelse return null;
        const remain = deadline - clock.monotonicNs();
        if (remain <= 0) return 1;
        const ms = @divTrunc(remain + std.time.ns_per_ms - 1, std.time.ns_per_ms);
        return @intCast(@min(ms, @as(i128, std.math.maxInt(u32))));
    }

    fn isExpired(self: *Client) bool {
        const deadline = self.deadline_ns orelse return false;
        return clock.monotonicNs() >= deadline;
    }

    /// 按本次调用计算绝对 deadline（多流共用一条 socket，取**最短**的超时）。
    fn startGuard(self: *Client, reqs: []const Request) void {
        self.deadline_ns = null;
        if (!self.native_timeout) return;
        var ms: ?u32 = self.opts.timeout_ms;
        for (reqs) |r| {
            if (r.timeout_ms) |t| ms = if (ms) |m| @min(m, t) else t;
        }
        const v = ms orelse return;
        if (v == 0) return;
        self.deadline_ns = clock.monotonicNs() + @as(i128, v) * std.time.ns_per_ms;
    }

    /// 清除 deadline（恢复无限等待）。
    fn stopGuard(self: *Client) void {
        self.deadline_ns = null;
    }

    /// 读失败时把本地的超时错误统一成 `error.Timeout`。
    fn mapReadError(self: *Client, err: anyerror) anyerror {
        _ = self;
        if (err == error.Timeout) return error.Timeout;
        return err;
    }

    /// 发连接前言与本端 SETTINGS（幂等）。
    pub fn handshake(self: *Client) !void {
        if (self.started) return;
        try self.writer.writeAll(h2.client_preface);
        try writeFrame(self.writer, .settings, 0, 0, &.{});
        try self.writer.flush();
        self.started = true;
    }

    /// unary 调用（发一条消息、收一条消息）。
    pub fn unary(
        self: *Client,
        method: []const u8,
        payload: []const u8,
        arena: std.mem.Allocator,
    ) !Result {
        const results = try self.callMany(&.{.{ .method = method, .payload = payload }}, arena);
        return results[0];
    }

    /// 在同一条连接上并发发起多个调用（每条一个流），返回与 `reqs` 同序的结果。
    pub fn callMany(
        self: *Client,
        reqs: []const Request,
        arena: std.mem.Allocator,
    ) ![]Result {
        try self.handshake();
        if (reqs.len == 0) return &.{};
        // 本次调用的读 deadline（有效期从现在算起，跨整个 callMany 共用）
        self.startGuard(reqs);
        defer self.stopGuard();

        const results = try arena.alloc(Result, reqs.len);
        const bodies = try arena.alloc(std.ArrayListUnmanaged(u8), reqs.len);
        const done = try arena.alloc(bool, reqs.len);
        const saw_status = try arena.alloc(bool, reqs.len);

        for (results, 0..) |*r, i| {
            r.* = .{ .stream_id = self.next_stream_id + @as(u32, @intCast(i)) * 2 };
            bodies[i] = .empty;
            done[i] = false;
            saw_status[i] = false;
        }
        self.next_stream_id += @as(u32, @intCast(reqs.len)) * 2;

        // 先全部发 HEADERS（模拟并发发起），再发各自 DATA
        for (reqs, results) |req, r| {
            try self.writeHeaders(
                r.stream_id,
                req.method,
                req.timeout_ms orelse self.opts.timeout_ms,
                req.metadata,
                arena,
            );
        }
        for (reqs, results) |req, r| {
            var body: std.ArrayListUnmanaged(u8) = .empty;
            try grpc.encodeMessage(&body, arena, req.payload);
            const data = if (self.opts.content_type == .grpc_web_text)
                try grpc.encodeBase64Padded(arena, body.items)
            else
                body.items;
            try writeFrame(self.writer, .data, h2.Flags.end_stream, r.stream_id, data);
        }
        try self.writer.flush();

        var remaining = reqs.len;
        while (remaining > 0) {
            const hdr = readFrameHeader(self) catch |e| {
                const mapped = self.mapReadError(e);
                if (mapped == error.Timeout) {
                    std.debug.print("[zgrpc.client] 读响应超时（本地 timeout 到期）\n", .{});
                    return error.Timeout;
                }
                std.debug.print("[zgrpc.client] 读响应帧结束（{s}）\n", .{@errorName(e)});
                return error.ConnectionClosed;
            };
            const payload = try arena.alloc(u8, hdr.length);
            try self.readExact(payload);

            switch (hdr.ftype) {
                .settings => {
                    if (hdr.flags & h2.Flags.ack == 0) {
                        try writeFrame(self.writer, .settings, h2.Flags.ack, 0, &.{});
                        try self.writer.flush();
                    }
                },
                .ping => {
                    if (hdr.flags & h2.Flags.ack == 0) {
                        try writeFrame(self.writer, .ping, h2.Flags.ack, 0, payload);
                        try self.writer.flush();
                    }
                },
                .data => {
                    const idx = indexOf(results, hdr.stream_id) orelse continue;
                    if (done[idx]) continue;
                    if (bodies[idx].items.len + payload.len > self.opts.max_body) {
                        return error.BodyTooLarge;
                    }
                    try bodies[idx].appendSlice(arena, payload);
                    try self.refundWindow(hdr.stream_id, payload.len);
                    // gRPC-Web：流结束靠 DATA 的 END_STREAM（原生 gRPC 靠 trailers HEADERS）
                    if (hdr.flags & h2.Flags.end_stream != 0) {
                        done[idx] = true;
                        remaining -= 1;
                    }
                },
                .headers => {
                    const idx = indexOf(results, hdr.stream_id) orelse continue;
                    var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                    self.decoder.decode(payload, &hs, arena) catch |e| {
                        std.debug.print("[zgrpc.client] HPACK 解码失败（流 {d}）：{s}\n", .{
                            hdr.stream_id, @errorName(e),
                        });
                        return e;
                    };
                    try applyHeaders(
                        &results[idx],
                        hs.items,
                        &saw_status[idx],
                        hdr.flags & h2.Flags.end_stream != 0,
                        arena,
                    );
                    if (hdr.flags & h2.Flags.end_stream != 0) {
                        if (!done[idx]) {
                            done[idx] = true;
                            remaining -= 1;
                        }
                    }
                },
                .rst_stream => {
                    const idx = indexOf(results, hdr.stream_id) orelse continue;
                    if (!done[idx]) {
                        done[idx] = true;
                        results[idx].aborted = true;
                        remaining -= 1;
                    }
                },
                .goaway => return error.ConnectionClosed,
                else => {},
            }
        }

        // 文本模式：整段 base64 → 二进制帧（忽略 in-stream padding）
        if (self.opts.content_type == .grpc_web_text) {
            for (bodies) |*b| {
                if (b.items.len == 0) continue;
                const decoded = try grpc.decodeBase64Stream(arena, b.items);
                // 旧 items 也在 arena 上，不需要单独释放
                b.* = std.ArrayListUnmanaged(u8).empty;
                try b.appendSlice(arena, decoded);
            }
        }
        // gRPC-Web：状态与自定义 metadata 在 body 尾帧里（不是 HTTP trailers）
        if (self.opts.content_type != .grpc) {
            for (results, bodies, 0..) |*r, b, i| {
                try parseWebTrailers(r, b.items, &saw_status[i], arena);
            }
        }
        // 仍未拿到状态（原生缺 trailers / web 缺尾帧）→ 规范要求合成，不能静默认成功
        for (results, 0..) |*r, i| {
            if (!saw_status[i]) try synthesizeStatus(r, arena);
        }

        // 去掉 gRPC 长度前缀，解出消息（压缩消息自动解压）
        for (results, bodies) |*r, b| {
            var list: std.ArrayListUnmanaged([]const u8) = .empty;
            var it = grpc.MessageIterator{ .data = b.items, .alloc = arena };
            while (it.next()) |m| try list.append(arena, m.payload);
            r.messages = list.items;
        }
        return results;
    }

    /// 客户端流 / bidi（批量形式）：把多条请求消息一次性发完，收全部响应消息。    ///
    /// 服务端若在每条消息到达时就回一条（bidi），这里能一次拿回多条 —— 这正是
    /// `grpc.Server.streamHooks()` 的配套验证场景。
    pub fn clientStream(
        self: *Client,
        method: []const u8,
        messages: []const []const u8,
        arena: std.mem.Allocator,
    ) !Result {
        try self.handshake();
        self.startGuard(&.{.{ .method = method, .timeout_ms = self.opts.timeout_ms }});
        defer self.stopGuard();
        const sid = self.next_stream_id;
        self.next_stream_id += 2;

        try self.writeHeaders(sid, method, self.opts.timeout_ms, &.{}, arena);
        if (messages.len == 0) {
            try writeFrame(self.writer, .data, h2.Flags.end_stream, sid, &.{});
        } else {
            for (messages, 0..) |m, i| {
                var body: std.ArrayListUnmanaged(u8) = .empty;
                try grpc.encodeMessage(&body, arena, m);
                const flags: u8 = if (i + 1 == messages.len) h2.Flags.end_stream else 0;
                const data = if (self.opts.content_type == .grpc_web_text)
                    try grpc.encodeBase64Padded(arena, body.items)
                else
                    body.items;
                try writeFrame(self.writer, .data, flags, sid, data);
            }
        }
        try self.writer.flush();

        var result = Result{ .stream_id = sid };
        var saw_status = false;
        var resp: std.ArrayListUnmanaged(u8) = .empty;

        while (true) {
            const hdr = readFrameHeader(self) catch |e| {
                const mapped = self.mapReadError(e);
                if (mapped == error.Timeout) {
                    std.debug.print("[zgrpc.client] 读响应超时（本地 timeout 到期）\n", .{});
                    return error.Timeout;
                }
                std.debug.print("[zgrpc.client] 读响应帧结束（{s}）\n", .{@errorName(e)});
                return error.ConnectionClosed;
            };
            const payload = try arena.alloc(u8, hdr.length);
            try self.readExact(payload);

            switch (hdr.ftype) {
                .settings => {
                    if (hdr.flags & h2.Flags.ack == 0) {
                        try writeFrame(self.writer, .settings, h2.Flags.ack, 0, &.{});
                        try self.writer.flush();
                    }
                },
                .ping => {
                    if (hdr.flags & h2.Flags.ack == 0) {
                        try writeFrame(self.writer, .ping, h2.Flags.ack, 0, payload);
                        try self.writer.flush();
                    }
                },
                .data => {
                    if (hdr.stream_id == sid) {
                        if (resp.items.len + payload.len > self.opts.max_body) return error.BodyTooLarge;
                        try resp.appendSlice(arena, payload);
                        try self.refundWindow(sid, payload.len);
                    }
                },
                .headers => {
                    if (hdr.stream_id != sid) continue;
                    var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                    try self.decoder.decode(payload, &hs, arena);
                    try applyHeaders(&result, hs.items, &saw_status, hdr.flags & h2.Flags.end_stream != 0, arena);
                    if (hdr.flags & h2.Flags.end_stream != 0) {
                        if (!saw_status) try synthesizeStatus(&result, arena);
                        break;
                    }
                },
                .rst_stream => {
                    if (hdr.stream_id == sid) {
                        result.aborted = true;
                        break;
                    }
                },
                .goaway => return error.ConnectionClosed,
                else => {},
            }
        }

        var raw: []const u8 = resp.items;
        if (self.opts.content_type == .grpc_web_text and resp.items.len > 0) {
            raw = try grpc.decodeBase64Stream(arena, resp.items);
        }
        if (self.opts.content_type != .grpc) {
            try parseWebTrailers(&result, raw, &saw_status, arena);
        }
        if (!saw_status) try synthesizeStatus(&result, arena);

        var list: std.ArrayListUnmanaged([]const u8) = .empty;
        var it = grpc.MessageIterator{ .data = raw, .alloc = arena };
        while (it.next()) |m| try list.append(arena, m.payload);
        result.messages = list.items;
        return result;
    }

    fn writeHeaders(
        self: *Client,
        stream_id: u32,
        method: []const u8,
        timeout_ms: ?u32,
        metadata: []const hpack.Header,
        arena: std.mem.Allocator,
    ) !void {
        var block: std.ArrayListUnmanaged(u8) = .empty;
        var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
        try hs.appendSlice(arena, &.{
            .{ .name = ":method", .value = "POST" },
            .{ .name = ":path", .value = method },
            .{ .name = ":scheme", .value = self.opts.scheme },
            .{ .name = ":authority", .value = self.opts.authority },
        });
        // 规范：Timeout 应紧跟在保留头之后、Call-Definition 之前
        if (timeout_ms) |ms| {
            const v = try std.fmt.allocPrint(arena, "{d}m", .{ms});
            try hs.append(arena, .{ .name = "grpc-timeout", .value = v });
        }
        try hs.appendSlice(arena, &.{
            .{ .name = "content-type", .value = self.opts.content_type.header() },
            .{ .name = "te", .value = "trailers" },
            .{ .name = "user-agent", .value = self.opts.user_agent },
            // 声明可接受 gzip：否则服务端永远不会压缩响应（大消息白耗带宽）
            .{ .name = "grpc-accept-encoding", .value = "gzip" },
        });
        // 自定义请求 metadata 放在 Call-Definition 之后（规范：Call-Definition 在前）
        try hs.appendSlice(arena, metadata);
        try hpack.encode(hs.items, &block, arena);
        try writeFrame(self.writer, .headers, h2.Flags.end_headers, stream_id, block.items);
    }

    fn refundWindow(self: *Client, stream_id: u32, amount: usize) !void {
        if (amount == 0) return;
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, @intCast(amount), .big);
        try writeFrame(self.writer, .window_update, 0, 0, &buf);
        try writeFrame(self.writer, .window_update, 0, stream_id, &buf);
        try self.writer.flush();
    }
};

fn indexOf(results: []const Result, stream_id: u32) ?usize {
    for (results, 0..) |r, i| {
        if (r.stream_id == stream_id) return i;
    }
    return null;
}

/// 把 HEADERS 帧里与 gRPC 相关的字段写进 `r`：`:status` / `grpc-status` / `grpc-message`。
///
/// `grpc-message` 按规范是 UTF-8 的 percent-encoding（RFC 3986 §2.1 的受限形式），
/// 这里统一解码；非法 `%xx` 原样保留（规范明确：解码方 MUST NOT 因此报错）。
fn applyHeaders(
    r: *Result,
    hs: []const hpack.Header,
    saw_status: *bool,
    is_trailer: bool,
    arena: std.mem.Allocator,
) !void {
    for (hs) |h| {
        if (std.mem.eql(u8, h.name, "grpc-status")) {
            r.status = std.fmt.parseInt(u32, h.value, 10) catch 2;
            saw_status.* = true;
        } else if (std.mem.eql(u8, h.name, "grpc-message")) {
            r.message = try grpc.percentDecode(arena, h.value);
        } else if (std.mem.eql(u8, h.name, ":status")) {
            r.http_status = h.value;
        } else if (is_trailer) {
            try r.trailers.append(arena, h);
        } else {
            try r.headers.append(arena, h);
        }
    }
}

/// gRPC-Web：`grpc-status` 与自定义 metadata 在 body 的**最后一个尾帧**里（首字节 MSB=1，
/// 后面是 4B 大端长度 + `k: v\r\n` 文本）。
fn parseWebTrailers(
    r: *Result,
    data: []const u8,
    saw_status: *bool,
    arena: std.mem.Allocator,
) !void {
    var pos: usize = 0;
    while (pos + 5 <= data.len) {
        const flag = data[pos];
        const len = std.mem.readInt(u32, data[pos + 1 ..][0..4], .big);
        if (flag & 0x80 != 0) {
            const end = @min(data.len, pos + 5 + len);
            const text = data[pos + 5 .. end];
            var it = std.mem.splitSequence(u8, text, "\r\n");
            while (it.next()) |line| {
                const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
                const name = std.mem.trim(u8, line[0..colon], " ");
                const value = std.mem.trim(u8, line[colon + 1 ..], " ");
                if (std.mem.eql(u8, name, "grpc-status")) {
                    r.status = std.fmt.parseInt(u32, value, 10) catch 2;
                    saw_status.* = true;
                } else if (std.mem.eql(u8, name, "grpc-message")) {
                    r.message = try grpc.percentDecode(arena, value);
                } else if (name.len > 0) {
                    try r.trailers.append(arena, .{ .name = name, .value = value });
                }
            }
            return;
        }
        pos += 5 + @as(usize, len);
    }
}

/// 规范：对端返回非 200、或流结束时没有 `grpc-status` 时，客户端必须**合成**
/// Status 与 Status-Message 交给应用层，而不是静默当成功。
fn synthesizeStatus(r: *Result, arena: std.mem.Allocator) !void {
    if (r.aborted) return;
    const st: []const u8 = if (r.http_status.len == 0) "200" else r.http_status;
    if (!std.mem.eql(u8, st, "200")) {
        r.status = @intFromEnum(grpc.Status.unavailable);
        r.message = try std.fmt.allocPrint(arena, "对端返回 HTTP {s}（不是 gRPC 响应）", .{st});
        r.aborted = true;
        return;
    }
    r.status = @intFromEnum(grpc.Status.unknown_error);
    r.message = "流已结束但没有 grpc-status（对端不是合规的 gRPC 实现）";
}

fn writeFrame(
    w: *std.Io.Writer,
    ftype: h2.FrameType,
    flags: u8,
    stream_id: u32,
    payload: []const u8,
) !void {
    var hdr: [9]u8 = undefined;
    hdr[0] = @intCast((payload.len >> 16) & 0xff);
    hdr[1] = @intCast((payload.len >> 8) & 0xff);
    hdr[2] = @intCast(payload.len & 0xff);
    hdr[3] = @intFromEnum(ftype);
    hdr[4] = flags;
    std.mem.writeInt(u32, hdr[5..9], stream_id & 0x7fff_ffff, .big);
    try w.writeAll(&hdr);
    if (payload.len > 0) try w.writeAll(payload);
}

const FrameHeader = struct {
    length: usize,
    ftype: h2.FrameType,
    flags: u8,
    stream_id: u32,
};

fn readFrameHeader(c: *Client) !FrameHeader {
    var b: [9]u8 = undefined;
    try c.readExact(&b);
    return .{
        .length = (@as(usize, b[0]) << 16) | (@as(usize, b[1]) << 8) | @as(usize, b[2]),
        .ftype = @enumFromInt(b[3]),
        .flags = b[4],
        .stream_id = std.mem.readInt(u32, b[5..9], .big) & 0x7fff_ffff,
    };
}

// ---------------------------------------------------------------------------
// 测试：用构造的"服务端响应字节流"验证客户端解析
// ---------------------------------------------------------------------------

const testing = std.testing;

/// 构造一个最小的服务端响应序列（SETTINGS + HEADERS + DATA + trailers）。
fn buildServerResponse(
    a: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    stream_id: u32,
    payload: []const u8,
    status: []const u8,
) !void {
    try h2.appendFrame(out, a, .settings, 0, 0, &.{});

    var h1: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":status", .value = "200" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, &h1, a);
    try h2.appendFrame(out, a, .headers, h2.Flags.end_headers, stream_id, h1.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try grpc.encodeMessage(&body, a, payload);
    try h2.appendFrame(out, a, .data, 0, stream_id, body.items);

    var h2block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = "grpc-status", .value = status },
        .{ .name = "grpc-message", .value = "" },
    }, &h2block, a);
    try h2.appendFrame(out, a, .headers, h2.Flags.end_headers | h2.Flags.end_stream, stream_id, h2block.items);
}

test "client：unary 解析出 grpc-status 与响应消息" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try buildServerResponse(a, &input, 1, "pong", "0");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var c = Client.init(testing.allocator, &r, &aw.writer, .{});
    defer c.deinit();

    const res = try c.unary("/t.T/Call", "ping", a);
    try testing.expect(res.ok());
    try testing.expectEqual(@as(u32, 0), res.status);
    try testing.expectEqual(@as(usize, 1), res.messages.len);
    try testing.expectEqualStrings("pong", res.messages[0]);
}

test "client：并发两流按 stream_id 归位" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // 服务端响应：先流 1 的完整序列，再流 3 的完整序列（顺序到达）
    var input: std.ArrayListUnmanaged(u8) = .empty;
    try buildServerResponse(a, &input, 1, "one", "0");
    try buildServerResponse(a, &input, 3, "three", "0");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var c = Client.init(testing.allocator, &r, &aw.writer, .{});
    defer c.deinit();

    const res = try c.callMany(&.{
        .{ .method = "/t.T/A", .payload = "a" },
        .{ .method = "/t.T/B", .payload = "b" },
    }, a);
    try testing.expectEqual(@as(usize, 2), res.len);
    try testing.expectEqual(@as(u32, 1), res[0].stream_id);
    try testing.expectEqual(@as(u32, 3), res[1].stream_id);
    try testing.expectEqualStrings("one", res[0].messages[0]);
    try testing.expectEqualStrings("three", res[1].messages[0]);
}

test "client：非 0 状态码不算 ok" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try buildServerResponse(a, &input, 1, "", "12");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var c = Client.init(testing.allocator, &r, &aw.writer, .{});
    defer c.deinit();

    const res = try c.unary("/t.T/Call", "", a);
    try testing.expect(!res.ok());
    try testing.expectEqual(@as(u32, 12), res.status);
}

/// 构造 gRPC-Web **文本模式**的服务端响应：整个 body 是 base64（消息帧 + 尾帧）。
fn buildWebTextResponse(
    a: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    stream_id: u32,
    payload: []const u8,
    status: []const u8,
) !void {
    try h2.appendFrame(out, a, .settings, 0, 0, &.{});

    var h1: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":status", .value = "200" },
        .{ .name = "content-type", .value = "application/grpc-web-text+proto" },
    }, &h1, a);
    try h2.appendFrame(out, a, .headers, h2.Flags.end_headers, stream_id, h1.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try grpc.encodeMessage(&body, a, payload);
    // 尾帧：0x80 + 4B 长度 + "grpc-status: N\r\n"
    try body.append(a, 0x80);
    const text = try std.fmt.allocPrint(a, "grpc-status: {s}\r\n", .{status});
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(text.len), .big);
    try body.appendSlice(a, &len);
    try body.appendSlice(a, text);

    const enc = try grpc.encodeBase64Padded(a, body.items);
    try h2.appendFrame(out, a, .data, h2.Flags.end_stream, stream_id, enc);
}

test "client：grpc-web-text 往返（请求 base64 发出、状态取自 body 尾帧）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try buildWebTextResponse(a, &input, 1, "pong", "0");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var c = Client.init(testing.allocator, &r, &aw.writer, .{ .content_type = .grpc_web_text });
    defer c.deinit();

    const res = try c.unary("/t.T/Call", "ping", a);
    try testing.expect(res.ok());
    try testing.expectEqualStrings("pong", res.messages[0]);
    try testing.expectEqualStrings("application/grpc-web-text+proto", res.header("content-type").?);

    // 请求体也必须是 base64：解出来应是 [压缩位=0][长度=4]["ping"]
    var pos: usize = h2.client_preface.len; // 跳过连接前言再看帧
    var found = false;
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype != .data) continue;
        found = true;
        const raw = try grpc.decodeBase64Stream(a, f.payload);
        try testing.expectEqual(@as(u8, 0), raw[0]);
        try testing.expectEqual(@as(u32, 4), std.mem.readInt(u32, raw[1..5], .big));
        try testing.expectEqualStrings("ping", raw[5..]);
    }
    try testing.expect(found);
}
