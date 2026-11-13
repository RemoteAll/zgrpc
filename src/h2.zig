//! HTTP/2（RFC 9113 / RFC 7540）**明文 h2c（prior-knowledge）服务端**。
//!
//! 只做 gRPC 需要的那部分，刻意不做的事（都写在「未支持」里，避免误解为完整实现）：
//! - 不做 TLS/ALPN（由前置反代终止 TLS，或以明文供服务间直连）；
//! - 不做服务端推送（`PUSH_PROMISE` 只跳过）；
//! - 不做优先级调度（`PRIORITY` 只跳过）；
//! - handler 仍是**同步串行执行**（一次跑一个流的业务），但**流的接收是多路复用的**：
//!   多个流可以同时处于半开状态（已收 HEADERS、body 未收完），互不踢掉 —— 真实 gRPC 客户端
//!   在一条连接上并发调用时不会被打回 `REFUSED_STREAM`；
//! - 发送受流控阻塞时，期间读到的业务帧进 `pending` 队列延后处理（不会丢，也不会误拒）。
//!
//! 已支持：`SETTINGS`(含 ACK/并发流上限)、`HEADERS`/`CONTINUATION`（含 PADDED/PRIORITY）、
//! `DATA`（含 PADDED、流控回补）、`WINDOW_UPDATE`（连接级 + 每流）、`PING`(ACK)、`RST_STREAM`、`GOAWAY`。

const std = @import("std");
const log = @import("log.zig");
const hpack = @import("hpack.zig");

/// 客户端连接前言（RFC 9113 §3.4）。
pub const client_preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub const FrameType = enum(u8) {
    data = 0x0,
    headers = 0x1,
    priority = 0x2,
    rst_stream = 0x3,
    settings = 0x4,
    push_promise = 0x5,
    ping = 0x6,
    goaway = 0x7,
    window_update = 0x8,
    continuation = 0x9,
    _,
};

pub const Flags = struct {
    pub const end_stream: u8 = 0x1;
    /// SETTINGS 与 PING 复用同一位
    pub const ack: u8 = 0x1;
    pub const end_headers: u8 = 0x4;
    pub const padded: u8 = 0x8;
    pub const priority: u8 = 0x20;
};

pub const ErrorCode = enum(u32) {
    no_error = 0x0,
    protocol_error = 0x1,
    internal_error = 0x2,
    flow_control_error = 0x3,
    settings_timeout = 0x4,
    stream_closed = 0x5,
    frame_size_error = 0x6,
    refused_stream = 0x7,
    cancel = 0x8,
    compression_error = 0x9,
    connect_error = 0xa,
    enhance_your_calm = 0xb,
    inadequate_security = 0xc,
    http_1_1_required = 0xd,
    _,
};

pub const default_max_frame_size: usize = 16384;
pub const default_window_size: i64 = 65535;

/// handler 嵌套深度上限（发送阻塞 → 读帧 → 又派发新请求 → 再阻塞…）。
const max_dispatch_depth: u8 = 4;

pub const FrameHeader = struct {
    length: usize,
    ftype: FrameType,
    flags: u8,
    stream_id: u32,
};

pub const Options = struct {
    /// 单个 header block（含 CONTINUATION 拼接）上限，防 HPACK 炸弹
    max_header_block: usize = 64 * 1024,
    /// 单请求体上限
    max_body: usize = 4 * 1024 * 1024,
    /// 我们自己声明的 `SETTINGS_INITIAL_WINDOW_SIZE`
    initial_window_size: u32 = @intCast(default_window_size),
    /// 我们自己声明的 `SETTINGS_MAX_CONCURRENT_STREAMS`
    max_concurrent_streams: usize = 100,
};

pub const Request = struct {
    stream_id: u32,
    method: []const u8 = "",
    path: []const u8 = "",
    scheme: []const u8 = "",
    authority: []const u8 = "",
    /// 全部请求头（含 `:method` 等伪头），已从 HPACK 解开
    headers: []const hpack.Header = &.{},
    /// 完整请求体（已跨 DATA 帧拼接）
    body: []const u8 = "",

    /// 大小写不敏感取头（h2 要求小写，这里仍兼容大小写）
    pub fn header(self: Request, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

/// 响应写入器：`begin` → `sendData`(可多次) → `end`。
pub const Responder = struct {
    conn: *Connection,
    stream: *StreamState,
    begun: bool = false,
    ended: bool = false,

    pub fn streamId(self: *const Responder) u32 {
        return self.stream.id;
    }

    /// 发送响应头块（`status` 如 "200"；`extra` 里不要含 `:status`）。
    pub fn begin(self: *Responder, status: []const u8, extra: []const hpack.Header) !void {
        if (self.begun) return error.AlreadyBegun;
        const a = self.stream.allocator();

        var list: std.ArrayListUnmanaged(hpack.Header) = .empty;
        try list.append(a, .{ .name = ":status", .value = status });
        try list.appendSlice(a, extra);

        var block: std.ArrayListUnmanaged(u8) = .empty;
        try hpack.encode(list.items, &block, a);

        try self.conn.writeHeaderBlock(self.stream.id, block.items, 0);
        self.begun = true;
    }

    /// 发送正文（自动按对端 `MAX_FRAME_SIZE` 与流控窗口切片）。
    pub fn sendData(self: *Responder, bytes: []const u8) !void {
        if (!self.begun) try self.begin("200", &.{});
        if (self.ended) return error.AlreadyEnded;

        var off: usize = 0;
        while (off < bytes.len) {
            const room = @min(self.conn.send_window, self.stream.send_window);
            if (room <= 0) {
                try self.conn.pumpForWindow(self.stream);
                continue;
            }
            const want: usize = @intCast(@min(
                @as(i64, @intCast(@min(bytes.len - off, self.conn.peer_max_frame_size))),
                room,
            ));
            try self.conn.writeFrame(.data, 0, self.stream.id, bytes[off .. off + want]);
            self.conn.send_window -= @as(i64, @intCast(want));
            self.stream.send_window -= @as(i64, @intCast(want));
            off += want;
        }
    }

    /// 结束响应：`trailers` 非空则放在尾部 HEADERS（gRPC 的 `grpc-status` 就走这里）。
    pub fn end(self: *Responder, trailers: []const hpack.Header) !void {
        if (!self.begun) try self.begin("200", &.{});
        if (self.ended) return error.AlreadyEnded;
        const a = self.stream.allocator();
        var block: std.ArrayListUnmanaged(u8) = .empty;
        try hpack.encode(trailers, &block, a);
        try self.conn.writeHeaderBlock(self.stream.id, block.items, Flags.end_stream);
        self.ended = true;
    }

    /// 发送最后一段正文并结束流（gRPC-Web 把 trailers 放在 body 尾帧，需要这种收尾方式）。
    pub fn endData(self: *Responder, bytes: []const u8) !void {
        if (self.ended) return error.AlreadyEnded;
        if (!self.begun) try self.begin("200", &.{});
        if (bytes.len == 0) {
            try self.conn.writeFrame(.data, Flags.end_stream, self.stream.id, &.{});
            self.ended = true;
            return;
        }
        var off: usize = 0;
        while (off < bytes.len) {
            const room = @min(self.conn.send_window, self.stream.send_window);
            if (room <= 0) {
                try self.conn.pumpForWindow(self.stream);
                continue;
            }
            const want: usize = @intCast(@min(
                @as(i64, @intCast(@min(bytes.len - off, self.conn.peer_max_frame_size))),
                room,
            ));
            const last = off + want == bytes.len;
            try self.conn.writeFrame(
                .data,
                if (last) Flags.end_stream else 0,
                self.stream.id,
                bytes[off .. off + want],
            );
            self.conn.send_window -= @as(i64, @intCast(want));
            self.stream.send_window -= @as(i64, @intCast(want));
            off += want;
        }
        self.ended = true;
    }
};

pub const HandlerFn = *const fn (
    ctx: *anyopaque,
    req: Request,
    rsp: *Responder,
    arena: std.mem.Allocator,
) anyerror!void;

/// 流式处理钩子（对应 gRPC 的客户端流 / bidi）：
/// `onHeaders` 在请求头就绪时调一次，`onData` 在**每个 DATA 帧到达时**立即调（不等 END_STREAM）。
pub const StreamHooks = struct {
    ctx: *anyopaque,
    onHeaders: *const fn (ctx: *anyopaque, req: Request, rsp: *Responder) anyerror!void,
    onData: *const fn (ctx: *anyopaque, rsp: *Responder, data: []const u8, end_stream: bool) anyerror!void,
    /// 流被对端 `RST_STREAM` 重置（可选）
    onReset: ?*const fn (ctx: *anyopaque, stream_id: u32) void = null,
};

/// 一条流的全部请求状态。**自带 arena**：流结束时整体释放，
/// 因此多流并存时不会互相污染（不用连接级 arena 的 reset 时机去赌）。
pub const StreamState = struct {
    id: u32,
    arena: std.heap.ArenaAllocator,
    header_block: std.ArrayListUnmanaged(u8) = .empty,
    headers: std.ArrayListUnmanaged(hpack.Header) = .empty,
    body: std.ArrayListUnmanaged(u8) = .empty,
    headers_done: bool = false,
    method: []const u8 = "",
    path: []const u8 = "",
    scheme: []const u8 = "",
    authority: []const u8 = "",
    /// 该流剩余发送窗口
    send_window: i64 = default_window_size,
    /// 流式模式下的响应器（**必须活到流结束**，不能在栈上建：handler 会跨回调持有它）
    rsp: ?*Responder = null,
    /// 业务挂载点（流式模式：gRPC 的 Call）
    user_data: ?*anyopaque = null,

    pub fn allocator(self: *StreamState) std.mem.Allocator {
        return self.arena.allocator();
    }
};

const PendingFrame = struct {
    hdr: FrameHeader,
    payload: []u8,
};

/// 极简自旋锁：临界区只有「写一帧 + flush」（微秒级）。
/// Zig 0.17.1 的 `std.Io.Mutex` 需要 `io` 参数且可取消，而 `writeFrame` 没有 io 上下文 ——
/// 为一把只用几微秒的锁引入 io 依赖不值得。
pub const SpinLock = struct {
    locked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn lock(self: *SpinLock) void {
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    /// 尝试拿锁；拿不到立刻返回 false。
    /// keepalive 用它跳过「正忙」的连 —— 有业务流量时本来就不需要 PING。
    pub fn tryLock(self: *SpinLock) bool {
        return self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) == null;
    }

    pub fn unlock(self: *SpinLock) void {
        self.locked.store(false, .release);
    }
};

pub const Connection = struct {
    /// 连接级长期分配（HPACK 动态表、流对象与延后帧）
    alloc: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    /// 帧写入互斥：keepalive 线程可能与业务线程同时发帧，
    /// 一帧的「头 + 体 + flush」必须原子，否则会交错出非法帧。
    write_mutex: SpinLock = .{},
    opts: Options = .{},
    decoder: hpack.Decoder,

    /// 我方剩余发送窗口（连接级）
    send_window: i64 = default_window_size,
    /// 对端 `SETTINGS_MAX_FRAME_SIZE`
    peer_max_frame_size: usize = default_max_frame_size,
    /// 对端 `SETTINGS_INITIAL_WINDOW_SIZE`（新流的发送窗口初值）
    peer_initial_window: i64 = default_window_size,

    /// 活跃流（多路复用：同一连接可同时存在多个半开流）
    streams: std.ArrayListUnmanaged(*StreamState) = .empty,
    /// 发送受流控阻塞期间读到的业务帧：延后到主循环处理（不丢、不误拒）
    pending: std.ArrayListUnmanaged(PendingFrame) = .empty,

    serve_ctx: ?*anyopaque = null,
    serve_handler: ?HandlerFn = null,
    /// 非空时进入**流式模式**：DATA 到达即回调，不攒到 END_STREAM
    stream_hooks: ?StreamHooks = null,
    dispatch_depth: u8 = 0,
    /// 已收到对端 SETTINGS（收到前不能算连接就绪）
    got_peer_settings: bool = false,
    /// 收到对端 PING ACK 的单调时刻（0 = 从未收到）
    last_ping_ack_ns: i128 = 0,
    /// 已发 / 已确认的 PING 计数
    pings_sent: u64 = 0,
    pings_acked: u64 = 0,

    pub fn init(
        alloc: std.mem.Allocator,
        reader: *std.Io.Reader,
        writer: *std.Io.Writer,
        opts: Options,
    ) Connection {
        return .{
            .alloc = alloc,
            .reader = reader,
            .writer = writer,
            .opts = opts,
            .decoder = hpack.Decoder.init(alloc),
        };
    }

    pub fn deinit(self: *Connection) void {
        for (self.streams.items) |s| {
            s.arena.deinit();
            self.alloc.destroy(s);
        }
        self.streams.deinit(self.alloc);
        for (self.pending.items) |pf| self.alloc.free(pf.payload);
        self.pending.deinit(self.alloc);
        self.decoder.deinit();
    }

    /// 当前活跃流数（可观测/测试用）。
    pub fn activeStreams(self: *const Connection) usize {
        return self.streams.items.len;
    }

    fn findStream(self: *Connection, id: u32) ?*StreamState {
        for (self.streams.items) |s| {
            if (s.id == id) return s;
        }
        return null;
    }

    fn createStream(self: *Connection, id: u32) !*StreamState {
        const s = try self.alloc.create(StreamState);
        s.* = .{
            .id = id,
            .arena = std.heap.ArenaAllocator.init(self.alloc),
            .send_window = self.peer_initial_window,
        };
        errdefer {
            s.arena.deinit();
            self.alloc.destroy(s);
        }
        try self.streams.append(self.alloc, s);
        return s;
    }

    fn destroyStream(self: *Connection, s: *StreamState) void {
        for (self.streams.items, 0..) |x, i| {
            if (x == s) {
                _ = self.streams.orderedRemove(i);
                break;
            }
        }
        s.arena.deinit();
        self.alloc.destroy(s);
    }

    /// 阻塞服务一条连接，直到对端 `GOAWAY`/断开。返回 `error.ConnectionClosed` 属正常收尾。
    pub fn serve(self: *Connection, ctx: *anyopaque, handler: HandlerFn) !void {
        self.serve_ctx = ctx;
        self.serve_handler = handler;
        self.stream_hooks = null;
        defer {
            self.serve_ctx = null;
            self.serve_handler = null;
        }
        return self.runLoop();
    }

    /// 流式服务： `hooks.onData` 在 DATA 到达时立即回调（支持客户端流 / bidi）。
    pub fn serveStreaming(self: *Connection, hooks: StreamHooks) !void {
        self.stream_hooks = hooks;
        self.serve_ctx = null;
        self.serve_handler = null;
        defer self.stream_hooks = null;
        return self.runLoop();
    }

    fn runLoop(self: *Connection) !void {
        var pf: [client_preface.len]u8 = undefined;
        self.reader.readSliceAll(&pf) catch |e| {
            log.h2.warn("读连接前言失败：{s}", .{@errorName(e)});
            return e;
        };
        if (!std.mem.eql(u8, &pf, client_preface)) {
            log.h2.warn("连接前言不匹配（可能是 HTTP/1.1 打到 h2 端口）", .{});
            return error.BadPreface;
        }

        // 服务端必须先发 SETTINGS（RFC 9113 §3.4）
        try self.sendSettings();

        while (true) {
            // 先消化"发送阻塞期间"暂存的业务帧
            if (self.pending.items.len > 0) {
                const p = self.pending.orderedRemove(0);
                defer self.alloc.free(p.payload);
                try self.handleFrame(p.hdr, p.payload);
                continue;
            }

            const hdr = self.readFrameHeader() catch |e| {
                // 对端关闭/半包 → 正常收尾；其它读错误也在这里定位
                log.h2.debug("读帧结束（{s}），连接收尾", .{@errorName(e)});
                return error.ConnectionClosed;
            };

            const payload = try self.alloc.alloc(u8, hdr.length);
            self.reader.readSliceAll(payload) catch |e| {
                self.alloc.free(payload);
                log.h2.warn("读帧体失败（流 {d}，{d} 字节）：{s}", .{
                    hdr.stream_id, hdr.length, @errorName(e),
                });
                return error.ConnectionClosed;
            };
            defer self.alloc.free(payload);

            try self.handleFrame(hdr, payload);
        }
    }

    fn handleFrame(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        switch (hdr.ftype) {
            .settings => try self.onSettings(hdr, payload),
            .ping => try self.onPing(hdr, payload),
            .window_update => try self.onWindowUpdate(hdr, payload),
            .headers => try self.onHeaders(hdr, payload),
            .continuation => try self.onContinuation(hdr, payload),
            .data => try self.onData(hdr, payload),
            .rst_stream => try self.onRstStream(hdr, payload),
            .goaway => return error.ConnectionClosed,
            // PRIORITY / PUSH_PROMISE / 未知帧：按 RFC 必须忽略
            else => {},
        }
    }

    // -----------------------------------------------------------------------
    // 帧读写
    // -----------------------------------------------------------------------

    fn readFrameHeader(self: *Connection) !FrameHeader {
        var b: [9]u8 = undefined;
        try self.reader.readSliceAll(&b);
        const len = (@as(usize, b[0]) << 16) | (@as(usize, b[1]) << 8) | @as(usize, b[2]);
        return .{
            .length = len,
            .ftype = @fromBackingInt(@intCast(b[3])),
            .flags = b[4],
            .stream_id = std.mem.readInt(u32, b[5..9], .big) & 0x7fff_ffff,
        };
    }

    /// 发送 header block，并按需分片为 `HEADERS + CONTINUATION…`。
    ///
    /// RFC 9113 §6.10：一个 header block 的 HEADERS 与它后面的 CONTINUATION 之间
    /// **不允许插入任何帧**（PING 也不行），否则对端会当协议错误。因此整段序列
    /// 必须在同一把写锁内完成 —— 这也正是 keepalive 能安全并发发 PING 的前提。
    ///
    /// `extra_flags` 只允许 `END_STREAM`（HEADERS 专属）；`END_HEADERS` 由这里控制，
    /// 而且只能出现在末帧上。未超帧长时直接一帧发出（常见路径不变）。
    fn writeHeaderBlock(
        self: *Connection,
        stream_id: u32,
        block: []const u8,
        extra_flags: u8,
    ) !void {
        const max = self.peer_max_frame_size;
        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        if (block.len <= max) {
            try self.writeFrameBody(.headers, extra_flags | Flags.end_headers, stream_id, block);
            return;
        }
        // 首片带 END_STREAM（若有），不带 END_HEADERS；其余走 CONTINUATION，末片才带 END_HEADERS
        try self.writeFrameBody(.headers, extra_flags & Flags.end_stream, stream_id, block[0..max]);
        var off: usize = max;
        while (off < block.len) {
            const n = @min(max, block.len - off);
            const last = off + n == block.len;
            try self.writeFrameBody(
                .continuation,
                if (last) Flags.end_headers else 0,
                stream_id,
                block[off .. off + n],
            );
            off += n;
        }
    }

    fn writeFrame(
        self: *Connection,
        ftype: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !void {
        // 头 + 体 + flush 必须原子（keepalive 线程可能并发发 PING）
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        return self.writeFrameBody(ftype, flags, stream_id, payload);
    }

    /// 非阻塞写帧：拿不到写锁（连接正忙）就返回 false，不排队。
    fn writeFrameTry(
        self: *Connection,
        ftype: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !bool {
        if (!self.write_mutex.tryLock()) return false;
        defer self.write_mutex.unlock();
        try self.writeFrameBody(ftype, flags, stream_id, payload);
        return true;
    }

    fn writeFrameBody(
        self: *Connection,
        ftype: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !void {
        if (payload.len > 0x00ff_ffff) return error.FrameTooLarge;

        var hdr: [9]u8 = undefined;
        hdr[0] = @intCast((payload.len >> 16) & 0xff);
        hdr[1] = @intCast((payload.len >> 8) & 0xff);
        hdr[2] = @intCast(payload.len & 0xff);
        hdr[3] = @backingInt(ftype);
        hdr[4] = flags;
        std.mem.writeInt(u32, hdr[5..9], stream_id & 0x7fff_ffff, .big);

        try self.writer.writeAll(&hdr);
        if (payload.len > 0) try self.writer.writeAll(payload);
        try self.writer.flush();
    }

    fn sendSettings(self: *Connection) !void {
        var payload: [12]u8 = undefined;
        // MAX_CONCURRENT_STREAMS
        std.mem.writeInt(u16, payload[0..2], 0x0003, .big);
        std.mem.writeInt(u32, payload[2..6], @intCast(self.opts.max_concurrent_streams), .big);
        // INITIAL_WINDOW_SIZE
        std.mem.writeInt(u16, payload[6..8], 0x0004, .big);
        std.mem.writeInt(u32, payload[8..12], self.opts.initial_window_size, .big);
        try self.writeFrame(.settings, 0, 0, &payload);
    }

    fn errBytes(code: ErrorCode) [4]u8 {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, @backingInt(code), .big);
        return b;
    }

    fn sendRst(self: *Connection, stream_id: u32, code: ErrorCode) !void {
        if (stream_id == 0) return;
        const payload = errBytes(code);
        try self.writeFrame(.rst_stream, 0, stream_id, &payload);
    }

    fn sendGoAway(self: *Connection, code: ErrorCode) !void {
        var payload: [8]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], 0, .big); // last_stream_id
        std.mem.writeInt(u32, payload[4..8], @backingInt(code), .big);
        // 刻意吞掉：GOAWAY 是「尽力告知」，能走到这里通常已经是对端断了/写不出去，
        // 没有后续动作可做，上层已拿到原始错误，这里再报错只会掩盖真正原因。
        self.writeFrame(.goaway, 0, 0, &payload) catch {};
    }

    // -----------------------------------------------------------------------
    // 控制帧
    // -----------------------------------------------------------------------

    fn onSettings(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        if (hdr.flags & Flags.ack != 0) {
            if (payload.len != 0) return error.ProtocolError;
            self.got_peer_settings = true;
            return;
        }
        if (payload.len % 6 != 0) return error.FrameSizeError;

        var i: usize = 0;
        while (i + 6 <= payload.len) : (i += 6) {
            const id = std.mem.readInt(u16, payload[i..][0..2], .big);
            const val = std.mem.readInt(u32, payload[i + 2 ..][0..4], .big);
            switch (id) {
                0x0001 => {}, // HEADER_TABLE_SIZE：本库编码不用动态表，忽略
                0x0004 => { // INITIAL_WINDOW_SIZE：按差值调整所有现存流的发送窗口
                    const new_win: i64 = @intCast(val);
                    const delta = new_win - self.peer_initial_window;
                    for (self.streams.items) |s| s.send_window += delta;
                    self.peer_initial_window = new_win;
                },
                0x0005 => { // MAX_FRAME_SIZE
                    if (val < 16384 or val > 16_777_215) return error.ProtocolError;
                    self.peer_max_frame_size = val;
                },
                else => {},
            }
        }
        self.got_peer_settings = true;
        try self.writeFrame(.settings, Flags.ack, 0, &.{});
    }

    fn onPing(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        if (payload.len != 8) return error.FrameSizeError;
        if (hdr.flags & Flags.ack != 0) {
            // 对端回了我们的 PING：记下时间与计数，供调用方判活
            self.last_ping_ack_ns = @import("clock.zig").monotonicNs();
            self.pings_acked +|= 1;
            return;
        }
        try self.writeFrame(.ping, Flags.ack, 0, payload);
    }

    /// 主动发一个 PING（`payload` 为空则自动填时间戳字节）。
    ///
    /// 发送与业务帧共用写锁，因此可以从 keepalive 线程调用。
    pub fn sendPing(self: *Connection, payload: ?[8]u8) !void {
        const buf = pingPayload(payload);
        self.pings_sent +|= 1;
        try self.writeFrame(.ping, 0, 0, &buf);
    }

    /// 非阻塞版：连接正忙（有业务帧在写）时返回 false ——
    /// 「正在传数据」本身就达到了保活目的，没必要排队等写锁。
    pub fn trySendPing(self: *Connection, payload: ?[8]u8) !bool {
        const buf = pingPayload(payload);
        const wrote = try self.writeFrameTry(.ping, 0, 0, &buf);
        if (wrote) self.pings_sent +|= 1;
        return wrote;
    }

    fn pingPayload(payload: ?[8]u8) [8]u8 {
        return payload orelse blk: {
            var b: [8]u8 = undefined;
            std.mem.writeInt(u64, &b, @bitCast(@as(i64, @truncate(@import("clock.zig").monotonicNs()))), .big);
            break :blk b;
        };
    }

    /// 收到过多少个 PING ACK（调用方可据此判活；0 表示对端从未回应）。
    pub fn pingAcks(self: *const Connection) u64 {
        return self.pings_acked;
    }

    fn onWindowUpdate(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        if (payload.len != 4) return error.FrameSizeError;
        const inc: i64 = @intCast(std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff);
        if (hdr.stream_id == 0) {
            self.send_window += inc;
        } else if (self.findStream(hdr.stream_id)) |s| {
            s.send_window += inc;
        }
    }

    fn onRstStream(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        // RFC 9113 §6.4：RST_STREAM 载荷必为 4 字节，否则是连接级 FRAME_SIZE_ERROR
        if (payload.len != 4) return error.FrameSizeError;
        if (self.findStream(hdr.stream_id)) |s| {
            log.h2.info("对端重置流 {d}，丢弃其状态", .{hdr.stream_id});
            if (self.stream_hooks) |hooks| {
                if (hooks.onReset) |cb| cb(hooks.ctx, hdr.stream_id);
            }
            self.destroyStream(s);
        }
    }

    /// 发送受阻时推进窗口：只立即处理控制帧；业务帧进 `pending` 延后（不丢）。
    fn pumpForWindow(self: *Connection, stream: *StreamState) !void {
        while (self.send_window <= 0 or stream.send_window <= 0) {
            const hdr = self.readFrameHeader() catch |e| {
                log.h2.debug("等窗口时读帧结束（{s}）", .{@errorName(e)});
                return error.ConnectionClosed;
            };
            const payload = try self.alloc.alloc(u8, hdr.length);
            self.reader.readSliceAll(payload) catch |e| {
                self.alloc.free(payload);
                log.h2.warn("等窗口时读帧体失败：{s}", .{@errorName(e)});
                return error.ConnectionClosed;
            };

            switch (hdr.ftype) {
                .window_update, .ping, .settings => {
                    defer self.alloc.free(payload);
                    try self.handleFrame(hdr, payload);
                },
                .rst_stream => {
                    defer self.alloc.free(payload);
                    const reset_id = hdr.stream_id;
                    try self.handleFrame(hdr, payload);
                    if (reset_id == stream.id) return error.StreamReset;
                },
                .goaway => {
                    self.alloc.free(payload);
                    return error.ConnectionClosed;
                },
                else => try self.pending.append(self.alloc, .{ .hdr = hdr, .payload = payload }),
            }
        }
    }

    // -----------------------------------------------------------------------
    // 请求帧（多路复用）
    // -----------------------------------------------------------------------

    fn onHeaders(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        // RFC 9113 §5.1.1：客户端发起的流号必须是正奇数（偶数是服务端推送专用，本库不用）
        if (hdr.stream_id == 0 or hdr.stream_id % 2 == 0) {
            log.h2.warn("非法流号 {d}（客户端发起必须为奇数）", .{hdr.stream_id});
            try self.sendGoAway(.protocol_error);
            return error.ProtocolError;
        }
        const s = self.findStream(hdr.stream_id) orelse blk: {
            if (self.streams.items.len >= self.opts.max_concurrent_streams) {
                log.h2.warn(
                    "并发流超上限 {d}，拒绝流 {d}",
                    .{ self.opts.max_concurrent_streams, hdr.stream_id },
                );
                try self.sendRst(hdr.stream_id, .refused_stream);
                return;
            }
            break :blk try self.createStream(hdr.stream_id);
        };

        if (s.headers_done) {
            // 同流二次 HEADERS（trailers）本库不支持
            try self.sendRst(hdr.stream_id, .protocol_error);
            self.destroyStream(s);
            return;
        }

        const a = s.allocator();
        const data = stripHeaderExtras(hdr, payload) orelse {
            try self.sendRst(hdr.stream_id, .protocol_error);
            self.destroyStream(s);
            return;
        };

        try s.header_block.appendSlice(a, data);
        if (s.header_block.items.len > self.opts.max_header_block) {
            log.h2.warn("header block 超上限 {d} 字节", .{self.opts.max_header_block});
            try self.sendRst(hdr.stream_id, .enhance_your_calm);
            self.destroyStream(s);
            return;
        }

        if (hdr.flags & Flags.end_headers != 0) {
            try self.afterHeaders(s, hdr.flags & Flags.end_stream != 0);
        }
    }

    fn onContinuation(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        const s = self.findStream(hdr.stream_id) orelse {
            // 流已不存在（比如上一条 HEADERS 已因超限被 RST/销毁）：**忽略该 CONTINUATION**。
            // 这里不能返回错误——那会把整条连接拆掉，连带弄死其它正常流（流级错误就按流处理）。
            log.h2.debug("忽略孤立的 CONTINUATION（流 {d} 不存在）", .{hdr.stream_id});
            return;
        };
        if (s.headers_done) {
            try self.sendRst(hdr.stream_id, .protocol_error);
            self.destroyStream(s);
            return;
        }
        const a = s.allocator();
        try s.header_block.appendSlice(a, payload);
        if (s.header_block.items.len > self.opts.max_header_block) {
            try self.sendRst(hdr.stream_id, .enhance_your_calm);
            self.destroyStream(s);
            return;
        }
        if (hdr.flags & Flags.end_headers != 0) {
            try self.afterHeaders(s, hdr.flags & Flags.end_stream != 0);
        }
    }

    fn skipContinuation(self: *Connection) !void {
        _ = self;
    }

    fn makeRequest(s: *StreamState) Request {
        return .{
            .stream_id = s.id,
            .method = s.method,
            .path = s.path,
            .scheme = s.scheme,
            .authority = s.authority,
            .headers = s.headers.items,
            .body = s.body.items,
        };
    }

    /// 请求头就绪：整请求模式攒到 END_STREAM 再派发；流式模式立刻回调 `onHeaders`。
    fn afterHeaders(self: *Connection, s: *StreamState, end_stream: bool) !void {
        try self.finishHeaders(s);

        if (self.stream_hooks) |hooks| {
            // Responder 必须长期存活（业务会跨多次回调持有它）→ 放在流的 arena 里
            const rsp = try s.allocator().create(Responder);
            rsp.* = .{ .conn = self, .stream = s };
            s.rsp = rsp;

            hooks.onHeaders(hooks.ctx, makeRequest(s), rsp) catch |e| {
                log.h2.err("流式 onHeaders 失败（流 {d}）：{s}", .{ s.id, @errorName(e) });
                if (!rsp.begun) rsp.begin("500", &.{}) catch {};
                if (!rsp.ended) rsp.end(&.{}) catch {};
            };

            // 无请求体的流：立刻通知"结束"
            if (end_stream) try self.deliverData(s, &.{}, true);
            return;
        }

        if (end_stream) try self.dispatch(s);
    }

    /// 流式模式：把一段请求数据交给业务；`end_stream` 为真时收尾并销毁流。
    fn deliverData(self: *Connection, s: *StreamState, data: []const u8, end_stream: bool) !void {
        const hooks = self.stream_hooks orelse return;
        const rsp = s.rsp orelse blk: {
            const r = try s.allocator().create(Responder);
            r.* = .{ .conn = self, .stream = s };
            s.rsp = r;
            break :blk r;
        };

        hooks.onData(hooks.ctx, rsp, data, end_stream) catch |e| {
            log.h2.err("流式 onData 失败（流 {d}，{d} 字节）：{s}", .{
                s.id, data.len, @errorName(e),
            });
            if (!rsp.begun) rsp.begin("500", &.{}) catch {};
            if (!rsp.ended) rsp.end(&.{}) catch {};
        };

        if (end_stream) {
            // 业务未显式收尾时补上，保证每个流都有 END_STREAM
            if (!rsp.ended) rsp.end(&.{}) catch {};
            self.destroyStream(s);
        }
    }

    fn finishHeaders(self: *Connection, s: *StreamState) !void {
        const a = s.allocator();
        self.decoder.decode(s.header_block.items, &s.headers, a) catch |e| {
            log.h2.err("HPACK 解码失败（流 {d}）：{s}", .{ s.id, @errorName(e) });
            try self.sendGoAway(.compression_error);
            return error.CompressionError;
        };
        s.headers_done = true;
        for (s.headers.items) |h| {
            if (std.mem.eql(u8, h.name, ":method")) s.method = h.value;
            if (std.mem.eql(u8, h.name, ":path")) s.path = h.value;
            if (std.mem.eql(u8, h.name, ":scheme")) s.scheme = h.value;
            if (std.mem.eql(u8, h.name, ":authority")) s.authority = h.value;
        }
    }

    fn onData(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        const s = self.findStream(hdr.stream_id) orelse {
            // 流不存在（已结束/从未开启）：回补连接窗口即可，不必回 RST（可能已关闭）
            try self.refundWindow(hdr.stream_id, payload.len, false);
            return;
        };

        var data = payload;
        if (hdr.flags & Flags.padded != 0) {
            // pad 长度越界：只拒绝该流（不能把错误抛上去，否则一条坏帧就能拆掉整条连接、
            // 连带弄死其它正常流。RFC 9113 也要求按流处理协议错误）。
            if (payload.len == 0 or @as(usize, payload[0]) > payload.len - 1) {
                log.h2.warn("流 {d} 的 PADDED 帧 pad 长度越界（载荷 {d} 字节），拒绝该流", .{
                    hdr.stream_id, payload.len,
                });
                try self.refundWindow(hdr.stream_id, payload.len, true);
                try self.sendRst(hdr.stream_id, .protocol_error);
                self.destroyStream(s);
                return;
            }
            const pad = @as(usize, payload[0]);
            data = payload[1 .. payload.len - pad];
        }

        // 无论是否超限都要回补窗口，否则对端会卡住
        try self.refundWindow(hdr.stream_id, payload.len, true);

        // 流式模式：不累积 body（业务自己维护重组缓冲），DATA 到达即回调
        if (self.stream_hooks != null) {
            try self.deliverData(s, data, hdr.flags & Flags.end_stream != 0);
            return;
        }

        const a = s.allocator();
        if (s.body.items.len + data.len > self.opts.max_body) {
            log.h2.warn("请求体超上限 {d} 字节（流 {d}）", .{ self.opts.max_body, s.id });
            try self.sendRst(hdr.stream_id, .enhance_your_calm);
            self.destroyStream(s);
            return;
        }
        try s.body.appendSlice(a, data);

        if (hdr.flags & Flags.end_stream != 0) try self.dispatch(s);
    }

    /// 收到 DATA 后把窗口补回去（连接级 + 流级）。
    fn refundWindow(self: *Connection, stream_id: u32, amount: usize, with_stream: bool) !void {
        if (amount == 0) return;
        const inc: u32 = @intCast(@min(amount, 0x7fff_ffff));
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, inc, .big);
        try self.writeFrame(.window_update, 0, 0, &buf);
        if (with_stream) try self.writeFrame(.window_update, 0, stream_id, &buf);
    }

    fn dispatch(self: *Connection, s: *StreamState) !void {
        const ctx = self.serve_ctx orelse return;
        const handler = self.serve_handler orelse return;
        defer self.destroyStream(s);

        if (self.dispatch_depth >= max_dispatch_depth) {
            log.h2.warn("派发嵌套过深（{d}），拒绝流 {d}", .{ self.dispatch_depth, s.id });
            try self.sendRst(s.id, .refused_stream);
            return;
        }
        self.dispatch_depth += 1;
        defer self.dispatch_depth -= 1;

        var rsp = Responder{ .conn = self, .stream = s };
        const req = Request{
            .stream_id = s.id,
            .method = s.method,
            .path = s.path,
            .scheme = s.scheme,
            .authority = s.authority,
            .headers = s.headers.items,
            .body = s.body.items,
        };

        handler(ctx, req, &rsp, s.allocator()) catch |e| {
            log.h2.err("处理 {s} {s} 失败（流 {d}）：{s}", .{
                req.method, req.path, s.id, @errorName(e),
            });
            if (!rsp.begun) rsp.begin("500", &.{}) catch {};
            if (!rsp.ended) rsp.end(&.{}) catch {};
            return;
        };

        // handler 没显式收尾时补上，保证每个流都有 END_STREAM
        if (!rsp.begun) try rsp.begin("200", &.{});
        if (!rsp.ended) try rsp.end(&.{});
    }
};

/// 去掉 HEADERS 帧的 PADDED / PRIORITY 前缀；非法返回 null。
fn stripHeaderExtras(hdr: FrameHeader, payload: []const u8) ?[]const u8 {
    var start: usize = 0;
    var end: usize = payload.len;
    if (hdr.flags & Flags.padded != 0) {
        if (payload.len == 0) return null;
        const pad = @as(usize, payload[0]);
        start += 1;
        if (pad > end - start) return null;
        end -= pad;
    }
    if (hdr.flags & Flags.priority != 0) {
        start += 5;
        if (start > end) return null;
    }
    return payload[start..end];
}

// ---------------------------------------------------------------------------
// 测试：明文 h2c 端到端（构造真实客户端字节流，逐帧断言响应）
// ---------------------------------------------------------------------------

const testing = std.testing;

pub fn appendFrame(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: std.mem.Allocator,
    ftype: FrameType,
    flags: u8,
    sid: u32,
    payload: []const u8,
) !void {
    try buf.append(alloc, @intCast((payload.len >> 16) & 0xff));
    try buf.append(alloc, @intCast((payload.len >> 8) & 0xff));
    try buf.append(alloc, @intCast(payload.len & 0xff));
    try buf.append(alloc, @backingInt(ftype));
    try buf.append(alloc, flags);
    var sidb: [4]u8 = undefined;
    std.mem.writeInt(u32, &sidb, sid, .big);
    try buf.appendSlice(alloc, &sidb);
    try buf.appendSlice(alloc, payload);
}

pub const ParsedFrame = struct {
    ftype: FrameType,
    flags: u8,
    stream_id: u32,
    payload: []const u8,
};

pub fn nextFrame(data: []const u8, pos: *usize) ?ParsedFrame {
    if (pos.* + 9 > data.len) return null;
    const b = data[pos.*..];
    const len = (@as(usize, b[0]) << 16) | (@as(usize, b[1]) << 8) | @as(usize, b[2]);
    if (pos.* + 9 + len > data.len) return null;
    const f = ParsedFrame{
        .ftype = @fromBackingInt(@intCast(b[3])),
        .flags = b[4],
        .stream_id = std.mem.readInt(u32, b[5..9], .big) & 0x7fff_ffff,
        .payload = b[9 .. 9 + len],
    };
    pos.* += 9 + len;
    return f;
}

fn echoHandler(
    ctx: *anyopaque,
    req: Request,
    rsp: *Responder,
    arena: std.mem.Allocator,
) anyerror!void {
    _ = ctx;
    _ = arena;
    try rsp.begin("200", &.{.{ .name = "content-type", .value = "text/plain" }});
    try rsp.sendData(req.body);
    try rsp.end(&.{.{ .name = "x-done", .value = "1" }});
}

fn encodeReqHeaders(a: std.mem.Allocator, path: []const u8) !std.ArrayListUnmanaged(u8) {
    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = path },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "text/plain" },
    }, &block, a);
    return block;
}

test "h2：明文 h2c 端到端（preface + SETTINGS + HEADERS/DATA → 响应帧齐全）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});

    const block = try encodeReqHeaders(a, "/echo");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, block.items);
    try appendFrame(&input, a, .data, Flags.end_stream, 1, "hello-h2");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    const out = aw.written();
    var pos: usize = 0;
    var saw_settings = false;
    var saw_settings_ack = false;
    var saw_headers = false;
    var saw_status_200 = false;
    var got_body: []const u8 = "";
    var saw_trailers = false;
    var saw_trailer_value = false;

    while (nextFrame(out, &pos)) |f| {
        switch (f.ftype) {
            .settings => {
                if (f.flags & Flags.ack != 0) saw_settings_ack = true else saw_settings = true;
            },
            .window_update => {},
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, ":status") and std.mem.eql(u8, h.value, "200")) {
                        saw_status_200 = true;
                    }
                    if (std.mem.eql(u8, h.name, "x-done") and std.mem.eql(u8, h.value, "1")) {
                        saw_trailer_value = true;
                    }
                }
                if (f.flags & Flags.end_stream != 0) {
                    saw_trailers = true;
                } else {
                    saw_headers = true;
                }
            },
            .data => got_body = f.payload,
            else => {},
        }
    }

    try testing.expect(saw_settings);
    try testing.expect(saw_settings_ack);
    try testing.expect(saw_headers);
    try testing.expect(saw_status_200);
    try testing.expectEqualStrings("hello-h2", got_body);
    try testing.expect(saw_trailers);
    try testing.expect(saw_trailer_value);
}

test "h2：PING 必须回 ACK（同 payload）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    try appendFrame(&input, a, .ping, 0, 0, "12345678");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var ack_payload: []const u8 = "";
    while (nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype == .ping and f.flags & Flags.ack != 0) ack_payload = f.payload;
    }
    try testing.expectEqualStrings("12345678", ack_payload);
}

test "h2：收到 DATA 必须回补窗口（连接级 + 流级）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const block = try encodeReqHeaders(a, "/echo");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, block.items);
    try appendFrame(&input, a, .data, Flags.end_stream, 1, "1234567890"); // 10 字节

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var conn_inc: u32 = 0;
    var stream_inc: u32 = 0;
    while (nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype == .window_update) {
            const inc = std.mem.readInt(u32, f.payload[0..4], .big);
            if (f.stream_id == 0) conn_inc += inc else stream_inc += inc;
        }
    }
    try testing.expectEqual(@as(u32, 10), conn_inc);
    try testing.expectEqual(@as(u32, 10), stream_inc);
}

test "h2：多路复用（两个流交错收头/收体，各自独立响应）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});

    const b1 = try encodeReqHeaders(a, "/echo/one");
    const b3 = try encodeReqHeaders(a, "/echo/three");

    // 交错：两条流都先发 HEADERS（不带 END_STREAM），再分别发 DATA
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    try appendFrame(&input, a, .headers, Flags.end_headers, 3, b3.items);
    try appendFrame(&input, a, .data, Flags.end_stream, 1, "one");
    try appendFrame(&input, a, .data, Flags.end_stream, 3, "three");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var status_count: usize = 0;
    var body1: []const u8 = "";
    var body3: []const u8 = "";
    var end1 = false;
    var end3 = false;
    var refused = false;

    while (nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .headers => {
                if (f.stream_id == 0) continue;
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                var is_status = false;
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, ":status")) is_status = true;
                }
                if (is_status) status_count += 1;
                if (f.flags & Flags.end_stream != 0) {
                    if (f.stream_id == 1) end1 = true;
                    if (f.stream_id == 3) end3 = true;
                }
            },
            .data => {
                if (f.stream_id == 1) body1 = f.payload;
                if (f.stream_id == 3) body3 = f.payload;
            },
            .rst_stream => refused = true,
            else => {},
        }
    }

    // 两条流都必须被处理（不是被 REFUSED_STREAM 打回）
    try testing.expect(!refused);
    try testing.expectEqual(@as(usize, 2), status_count);
    try testing.expectEqualStrings("one", body1);
    try testing.expectEqualStrings("three", body3);
    try testing.expect(end1);
    try testing.expect(end3);
}
// ---------------------------------------------------------------------------
// 健壮性：畸形/超限输入必须「明确拒绝」或「干净收尾」，绝不能 panic 或越界读
// ---------------------------------------------------------------------------

/// 把一段构造好的客户端字节流喂给连接，返回服务端写入的字节（-ConnectionClosed 视为正常收尾）。
fn driveConn(a: std.mem.Allocator, input: []const u8, opts: Options) ![]const u8 {
    var r = std.Io.Reader.fixed(input);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();
    var conn = Connection.init(testing.allocator, &r, &aw.writer, opts);
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };
    return try a.dupe(u8, aw.written());
}

/// 统计输出里指定流收到的 RST_STREAM 的错误码（没有则返回 null）。
fn findRstCode(out: []const u8, sid: u32) ?u32 {
    var pos: usize = 0;
    while (nextFrame(out, &pos)) |f| {
        if (f.ftype == .rst_stream and f.stream_id == sid and f.payload.len >= 4) {
            return std.mem.readInt(u32, f.payload[0..4], .big);
        }
    }
    return null;
}

test "h2健壮性：连接前言不对 → 返回 BadPreface（不 panic）" {
    const bad = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";
    var r = std.Io.Reader.fixed(bad);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();
    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    try testing.expectError(error.BadPreface, conn.serve(&dummy, echoHandler));
}

test "h2健壮性：并发流超上限 → REFUSED_STREAM(7)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    // 两条都只发 HEADERS（不 END_STREAM）→ 第二条会超过上限 1
    const b1 = try encodeReqHeaders(a, "/one");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    const b3 = try encodeReqHeaders(a, "/three");
    try appendFrame(&input, a, .headers, Flags.end_headers, 3, b3.items);

    const out = try driveConn(a, input.items, .{ .max_concurrent_streams = 1 });
    try testing.expectEqual(@as(?u32, 7), findRstCode(out, 3)); // REFUSED_STREAM
}

test "h2健壮性：header block 超上限 → ENHANCE_YOUR_CALM(11)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b1 = try encodeReqHeaders(a, "/big-header");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);

    // 上限压到 16 字节：上面那块的 HPACK 编码必然超
    const out = try driveConn(a, input.items, .{ .max_header_block = 16 });
    try testing.expectEqual(@as(?u32, 11), findRstCode(out, 1)); // ENHANCE_YOUR_CALM
}

test "h2健壮性：请求体超上限 → ENHANCE_YOUR_CALM(11)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b1 = try encodeReqHeaders(a, "/too-big");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    try appendFrame(&input, a, .data, 0, 1, "0123456789abcdef"); // 16 字节

    const out = try driveConn(a, input.items, .{ .max_body = 8 });
    // 实现用 ENHANCE_YOUR_CALM(11)：语义是「请求体太激进」，比 REFUSED_STREAM 更准确
    try testing.expectEqual(@as(?u32, 11), findRstCode(out, 1));
}

test "h2健壮性：帧声明长度大于实际字节 → 干净收尾（不 panic）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    // 头部声明 1024 字节，但只给 3 字节
    try input.appendSlice(a, &.{ 0x00, 0x04, 0x00, @backingInt(FrameType.data), 0, 0, 0, 0, 1 });
    try input.appendSlice(a, "abc");

    _ = try driveConn(a, input.items, .{});
}

test "h2健壮性：PADDED 帧 pad 长度越界 → 拒绝该流（不越界读）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b1 = try encodeReqHeaders(a, "/padded");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    // PADDED 标志 + pad length=200（远超实际载荷）
    try appendFrame(&input, a, .data, Flags.padded, 1, &.{ 200, 'x' });

    const out = try driveConn(a, input.items, .{});
    // 必须给出明确拒绝（协议错误），而不是读到缓冲区外
    try testing.expect(findRstCode(out, 1) != null);
}
test "h2健壮性：孤立 CONTINUATION（流不存在）不拆连接，后续帧仍被处理" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    // 流 3 从未建立 → 这条 CONTINUATION 是孤立的
    try appendFrame(&input, a, .continuation, Flags.end_headers, 3, "junk");
    // 同一条连接上的 PING 必须仍然被回应（说明连接没被拆）
    try appendFrame(&input, a, .ping, 0, 0, "12345678");

    const out = try driveConn(a, input.items, .{});
    var pos: usize = 0;
    var got_ping_ack = false;
    while (nextFrame(out, &pos)) |f| {
        if (f.ftype == .ping and f.flags & Flags.ack != 0) got_ping_ack = true;
    }
    try testing.expect(got_ping_ack);
}

test "h2健壮性：偶数流号（客户端非法）→ GOAWAY + ProtocolError" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b2 = try encodeReqHeaders(a, "/even");
    try appendFrame(&input, a, .headers, Flags.end_headers, 2, b2.items); // 偶数流号

    try testing.expectError(error.ProtocolError, driveConn(a, input.items, .{}));
}

test "h2健壮性：RST_STREAM 载荷不是 4 字节 → FRAME_SIZE_ERROR" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    try appendFrame(&input, a, .rst_stream, 0, 1, &.{ 0x00, 0x01 }); // 只给 2 字节

    try testing.expectError(error.FrameSizeError, driveConn(a, input.items, .{}));
}

test "SpinLock：tryLock 拿不到立刻返回 false（不自旋阻塞）" {
    var l: SpinLock = .{};
    try testing.expect(l.tryLock());
    // 已被占住 → 不能再拿到
    try testing.expect(!l.tryLock());
    l.unlock();
    try testing.expect(l.tryLock());
    l.unlock();
}

test "keepalive：连接正忙时 trySendPing 立刻放弃（不阻塞、不计数）" {
    var r = std.Io.Reader.fixed("");
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();

    // 空闲连接：PING 正常发出并计数
    try testing.expect(try conn.trySendPing(null));
    try testing.expectEqual(@as(u64, 1), conn.pings_sent);

    // 模拟「另一线程正在写业务帧」：写锁被占
    conn.write_mutex.lock();
    defer conn.write_mutex.unlock();
    // 必须立刻返回 false（而不是排队等锁）——这是高并发下 pingAll 不拖死别人的前提
    try testing.expect(!(try conn.trySendPing(null)));
    // 跳过的那次不计数
    try testing.expectEqual(@as(u64, 1), conn.pings_sent);
}
