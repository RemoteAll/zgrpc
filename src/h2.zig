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

        try self.conn.writeFrame(.headers, Flags.end_headers, self.stream.id, block.items);
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
        try self.conn.writeFrame(
            .headers,
            Flags.end_headers | Flags.end_stream,
            self.stream.id,
            block.items,
        );
        self.ended = true;
    }
};

pub const HandlerFn = *const fn (
    ctx: *anyopaque,
    req: Request,
    rsp: *Responder,
    arena: std.mem.Allocator,
) anyerror!void;

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

    pub fn allocator(self: *StreamState) std.mem.Allocator {
        return self.arena.allocator();
    }
};

const PendingFrame = struct {
    hdr: FrameHeader,
    payload: []u8,
};

pub const Connection = struct {
    /// 连接级长期分配（HPACK 动态表、流对象与延后帧）
    alloc: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
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
    dispatch_depth: u8 = 0,
    /// 已收到对端 SETTINGS（收到前不能算连接就绪）
    got_peer_settings: bool = false,

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
        defer {
            self.serve_ctx = null;
            self.serve_handler = null;
        }

        var pf: [client_preface.len]u8 = undefined;
        self.reader.readSliceAll(&pf) catch |e| {
            std.debug.print("[zgrpc.h2] 读连接前言失败：{s}\n", .{@errorName(e)});
            return e;
        };
        if (!std.mem.eql(u8, &pf, client_preface)) {
            std.debug.print("[zgrpc.h2] 连接前言不匹配（可能是 HTTP/1.1 打到 h2 端口）\n", .{});
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
                std.debug.print("[zgrpc.h2] 读帧结束（{s}），连接收尾\n", .{@errorName(e)});
                return error.ConnectionClosed;
            };

            const payload = try self.alloc.alloc(u8, hdr.length);
            self.reader.readSliceAll(payload) catch |e| {
                self.alloc.free(payload);
                std.debug.print("[zgrpc.h2] 读帧体失败（流 {d}，{d} 字节）：{s}\n", .{
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
            .ftype = @enumFromInt(b[3]),
            .flags = b[4],
            .stream_id = std.mem.readInt(u32, b[5..9], .big) & 0x7fff_ffff,
        };
    }

    fn writeFrame(
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
        hdr[3] = @intFromEnum(ftype);
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
        std.mem.writeInt(u32, &b, @intFromEnum(code), .big);
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
        std.mem.writeInt(u32, payload[4..8], @intFromEnum(code), .big);
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
        if (hdr.flags & Flags.ack != 0) return;
        try self.writeFrame(.ping, Flags.ack, 0, payload);
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
        _ = payload;
        if (self.findStream(hdr.stream_id)) |s| {
            std.debug.print("[zgrpc.h2] 对端重置流 {d}，丢弃其状态\n", .{hdr.stream_id});
            self.destroyStream(s);
        }
    }

    /// 发送受阻时推进窗口：只立即处理控制帧；业务帧进 `pending` 延后（不丢）。
    fn pumpForWindow(self: *Connection, stream: *StreamState) !void {
        while (self.send_window <= 0 or stream.send_window <= 0) {
            const hdr = self.readFrameHeader() catch |e| {
                std.debug.print("[zgrpc.h2] 等窗口时读帧结束（{s}）\n", .{@errorName(e)});
                return error.ConnectionClosed;
            };
            const payload = try self.alloc.alloc(u8, hdr.length);
            self.reader.readSliceAll(payload) catch |e| {
                self.alloc.free(payload);
                std.debug.print("[zgrpc.h2] 等窗口时读帧体失败：{s}\n", .{@errorName(e)});
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
        const s = self.findStream(hdr.stream_id) orelse blk: {
            if (self.streams.items.len >= self.opts.max_concurrent_streams) {
                std.debug.print(
                    "[zgrpc.h2] 并发流超上限 {d}，拒绝流 {d}\n",
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
            std.debug.print("[zgrpc.h2] header block 超上限 {d} 字节\n", .{self.opts.max_header_block});
            try self.sendRst(hdr.stream_id, .enhance_your_calm);
            self.destroyStream(s);
            return;
        }

        if (hdr.flags & Flags.end_headers != 0) {
            try self.finishHeaders(s);
            if (hdr.flags & Flags.end_stream != 0) try self.dispatch(s);
        }
    }

    fn onContinuation(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        const s = self.findStream(hdr.stream_id) orelse {
            try self.skipContinuation();
            return error.ProtocolError;
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
            try self.finishHeaders(s);
            if (hdr.flags & Flags.end_stream != 0) try self.dispatch(s);
        }
    }

    fn skipContinuation(self: *Connection) !void {
        _ = self;
    }

    fn finishHeaders(self: *Connection, s: *StreamState) !void {
        const a = s.allocator();
        self.decoder.decode(s.header_block.items, &s.headers, a) catch |e| {
            std.debug.print("[zgrpc.h2] HPACK 解码失败（流 {d}）：{s}\n", .{ s.id, @errorName(e) });
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
            if (payload.len == 0) return error.ProtocolError;
            const pad = @as(usize, payload[0]);
            if (pad > payload.len - 1) return error.ProtocolError;
            data = payload[1 .. payload.len - pad];
        }

        // 无论是否超限都要回补窗口，否则对端会卡住
        try self.refundWindow(hdr.stream_id, payload.len, true);

        const a = s.allocator();
        if (s.body.items.len + data.len > self.opts.max_body) {
            std.debug.print("[zgrpc.h2] 请求体超上限 {d} 字节（流 {d}）\n", .{ self.opts.max_body, s.id });
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
            std.debug.print("[zgrpc.h2] 派发嵌套过深（{d}），拒绝流 {d}\n", .{ self.dispatch_depth, s.id });
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
            std.debug.print("[zgrpc.h2] 处理 {s} {s} 失败（流 {d}）：{s}\n", .{
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
    try buf.append(alloc, @intFromEnum(ftype));
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
        .ftype = @enumFromInt(b[3]),
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