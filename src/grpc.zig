//! gRPC 语义层（跑在 `h2` 之上）：消息分帧、状态码、路由、unary 与服务端流式。
//!
//! 协议要点（gRPC over HTTP/2）：
//! - 请求：`POST /{service}/{method}`、`content-type: application/grpc*`、`te: trailers`；
//! - 消息分帧：`[压缩标志:1][长度:4 大端][payload]`，可在一帧内多条（流式）；
//! - 响应：`:status 200` + `content-type: application/grpc`，**状态放在 trailers 的 `grpc-status`**；
//! - 本库不做 protobuf 编解码：业务拿到/给出的是**已序列化的字节**，由调用方自带 protobuf 实现
//!   （服务间自控两端时通常是零成本；要对接现成 .proto 再接一个 codegen 即可，不必改动本层）。
//!
//! 未支持（明确写出，避免误解）：压缩（`grpc-encoding`）、客户端流/bidi 的背压、
//! `grpc-timeout` 强制取消、`grpc-message` 的 percent-encoding（仅透传 ASCII）。

const std = @import("std");
const h2 = @import("h2.zig");
const hpack = @import("hpack.zig");

/// gRPC 状态码（https://grpc.io/docs/guides/status-codes/）。
pub const Status = enum(u32) {
    ok = 0,
    cancelled = 1,
    unknown_error = 2,
    invalid_argument = 3,
    deadline_exceeded = 4,
    not_found = 5,
    already_exists = 6,
    permission_denied = 7,
    resource_exhausted = 8,
    failed_precondition = 9,
    aborted = 10,
    out_of_range = 11,
    unimplemented = 12,
    internal = 13,
    unavailable = 14,
    data_loss = 15,
    unauthenticated = 16,
};

pub const Message = struct {
    compressed: bool,
    payload: []const u8,
};

/// 遍历一个 gRPC 请求体里的全部消息。
pub const MessageIterator = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn next(self: *MessageIterator) ?Message {
        if (self.pos + 5 > self.data.len) return null;
        const compressed = self.data[self.pos] != 0;
        const len = std.mem.readInt(u32, self.data[self.pos + 1 ..][0..4], .big);
        const start = self.pos + 5;
        if (len > self.data.len - start) return null;
        self.pos = start + len;
        return .{ .compressed = compressed, .payload = self.data[start..self.pos] };
    }
};

/// 把一条消息编码成 gRPC 帧（未压缩）追加到 `out`。
pub fn encodeMessage(
    out: *std.ArrayListUnmanaged(u8),
    alloc: std.mem.Allocator,
    payload: []const u8,
) !void {
    try out.append(alloc, 0);
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(payload.len), .big);
    try out.appendSlice(alloc, &len);
    try out.appendSlice(alloc, payload);
}

const grpc_content_type = [_]hpack.Header{
    .{ .name = "content-type", .value = "application/grpc" },
};

/// 一次调用：业务通过它读请求消息、写响应消息、设状态。
pub const Call = struct {
    req: h2.Request,
    rsp: *h2.Responder,
    alloc: std.mem.Allocator,
    status: Status = .ok,
    /// 失败时回给客户端的文本（放在 trailers 的 `grpc-message`）
    error_message: []const u8 = "",
    sent_any: bool = false,
    finished: bool = false,

    // --- 流式模式状态（由 `Server.streamHooks` 驱动）---
    /// 跨 DATA 帧的累积缓冲（整请求路由存原始帧，流式路由存待切分帧）
    accum: std.ArrayListUnmanaged(u8) = .empty,
    stream_route: ?*const StreamRoute = null,
    unary_route: ?Route = null,
    route_ctx: *anyopaque = undefined,

    /// unary 调用取第一条消息。
    pub fn firstMessage(self: *Call) ?Message {
        var it = MessageIterator{ .data = self.req.body };
        return it.next();
    }

    pub fn messages(self: *Call) MessageIterator {
        return .{ .data = self.req.body };
    }

    /// 发送一条响应消息（服务端流式可多次调用）。
    pub fn sendMessage(self: *Call, payload: []const u8) !void {
        if (self.finished) return error.AlreadyFinished;
        if (!self.sent_any) {
            try self.rsp.begin("200", &grpc_content_type);
            self.sent_any = true;
        }
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        try encodeMessage(&buf, self.alloc, payload);
        try self.rsp.sendData(buf.items);
    }

    pub fn setStatus(self: *Call, status: Status, message: []const u8) void {
        self.status = status;
        self.error_message = message;
    }

    /// 收尾：把 `grpc-status` 写进 trailers 并结束流。幂等。
    pub fn finish(self: *Call) !void {
        if (self.finished) return;
        self.finished = true;

        const a = self.alloc;
        var trailers: std.ArrayListUnmanaged(hpack.Header) = .empty;
        var code_buf: [8]u8 = undefined;
        const code = try std.fmt.bufPrint(&code_buf, "{d}", .{@intFromEnum(self.status)});
        try trailers.append(a, .{ .name = "grpc-status", .value = code });
        if (self.error_message.len > 0) {
            try trailers.append(a, .{ .name = "grpc-message", .value = self.error_message });
        }

        if (!self.sent_any) {
            try self.rsp.begin("200", &grpc_content_type);
            self.sent_any = true;
        }
        try self.rsp.end(trailers.items);
    }
};

pub const HandlerFn = *const fn (
    ctx: *anyopaque,
    call: *Call,
    arena: std.mem.Allocator,
) anyerror!void;

pub const Route = struct {
    /// 完整方法路径，如 `/echo.Echo/Say`
    path: []const u8,
    handler: HandlerFn,
    /// 该路由专用上下文；为空则用 `Server` 的 ctx
    ctx: ?*anyopaque = null,
};

/// 流式方法（客户端流 / bidi）：消息逐条回调，可在回调里随时 `call.sendMessage`。
pub const StreamRoute = struct {
    path: []const u8,
    /// 收到一条**完整** gRPC 消息（跨 DATA 帧的重组由本层负责）
    on_message: *const fn (ctx: *anyopaque, call: *Call, msg: []const u8) anyerror!void,
    /// 请求流结束（收到 END_STREAM）
    on_end: ?*const fn (ctx: *anyopaque, call: *Call) anyerror!void = null,
    ctx: ?*anyopaque = null,
};

/// 单条消息上限（流式重组时防炸）。
const max_stream_message: usize = 4 * 1024 * 1024;

/// gRPC 路由表；把它作为 `h2.Connection.serve` 的 handler 使用：
/// ```zig
/// var server = zgrpc.grpc.Server{ .routes = &routes };
/// try conn.serve(server.context(), server.handler());
/// ```
pub const Server = struct {
    /// 整请求路由（unary / 服务端流式）
    routes: []const Route = &.{},
    /// 流式路由（客户端流 / bidi）
    stream_routes: []const StreamRoute = &.{},
    /// 路由未指定 `ctx` 时使用
    outer_ctx: *anyopaque,

    pub fn context(self: *Server) *anyopaque {
        return @ptrCast(self);
    }

    /// 整请求模式（也是 `serve` 的旧入口）。
    pub fn handler(self: *Server) h2.HandlerFn {
        _ = self;
        return handle;
    }

    /// 流式模式：DATA 到达即回调，支持客户端流 / bidi；同时兼容整请求路由。
    pub fn streamHooks(self: *Server) h2.StreamHooks {
        return .{
            .ctx = @ptrCast(self),
            .onHeaders = streamOnHeaders,
            .onData = streamOnData,
            .onReset = streamOnReset,
        };
    }

    fn findStreamRoute(self: *Server, path: []const u8) ?*const StreamRoute {
        for (self.stream_routes) |*r| {
            if (std.mem.eql(u8, r.path, path)) return r;
        }
        return null;
    }

    fn find(self: *Server, path: []const u8) ?Route {
        for (self.routes) |r| {
            if (std.mem.eql(u8, r.path, path)) return r;
        }
        return null;
    }

    fn handle(server_ctx: *anyopaque, req: h2.Request, rsp: *h2.Responder, arena: std.mem.Allocator) anyerror!void {
        const self: *Server = @ptrCast(@alignCast(server_ctx));
        var call = Call{ .req = req, .rsp = rsp, .alloc = arena };

        if (!std.mem.eql(u8, req.method, "POST")) {
            call.setStatus(.unimplemented, "gRPC 只支持 POST");
            return call.finish();
        }
        const ct = req.header("content-type") orelse "";
        if (!std.mem.startsWith(u8, ct, "application/grpc")) {
            call.setStatus(.invalid_argument, "content-type 必须是 application/grpc");
            return call.finish();
        }

        const route = self.find(req.path) orelse {
            std.debug.print("[zgrpc.grpc] 未注册的方法：{s}\n", .{req.path});
            call.setStatus(.unimplemented, "未注册的方法");
            return call.finish();
        };

        const route_ctx = route.ctx orelse self.outer_ctx;
        route.handler(route_ctx, &call, arena) catch |e| {
            // 业务未显式设过状态时兜底为 internal，并留下可定位日志
            if (call.status == .ok) call.setStatus(.internal, @errorName(e));
            std.debug.print("[zgrpc.grpc] 处理 {s} 失败：{s}\n", .{ req.path, @errorName(e) });
        };
        return call.finish();
    }

    // -----------------------------------------------------------------------
    // 流式模式（客户端流 / bidi）：DATA 到达即回调
    // -----------------------------------------------------------------------

    fn streamOnHeaders(server_ctx: *anyopaque, req: h2.Request, rsp: *h2.Responder) anyerror!void {
        const self: *Server = @ptrCast(@alignCast(server_ctx));
        const a = rsp.stream.allocator();
        const call = try a.create(Call);
        call.* = .{ .req = req, .rsp = rsp, .alloc = a };
        rsp.stream.user_data = call;

        if (!std.mem.eql(u8, req.method, "POST")) {
            call.setStatus(.unimplemented, "gRPC 只支持 POST");
            return call.finish();
        }
        const ct = req.header("content-type") orelse "";
        if (!std.mem.startsWith(u8, ct, "application/grpc")) {
            call.setStatus(.invalid_argument, "content-type 必须是 application/grpc");
            return call.finish();
        }

        if (self.findStreamRoute(req.path)) |sr| {
            call.stream_route = sr;
            call.route_ctx = sr.ctx orelse self.outer_ctx;
            return;
        }
        if (self.find(req.path)) |r| {
            // 整请求路由也走流式驱动：先攒满 body，END_STREAM 时调用
            call.unary_route = r;
            call.route_ctx = r.ctx orelse self.outer_ctx;
            return;
        }

        std.debug.print("[zgrpc.grpc] 未注册的方法：{s}\n", .{req.path});
        call.setStatus(.unimplemented, "未注册的方法");
        return call.finish();
    }

    fn streamOnData(server_ctx: *anyopaque, rsp: *h2.Responder, data: []const u8, end_stream: bool) anyerror!void {
        _ = server_ctx;
        const p = rsp.stream.user_data orelse return;
        const call: *Call = @ptrCast(@alignCast(p));
        if (call.finished) return;

        // ① 整请求路由：原样累积（含 gRPC 长度前缀），END_STREAM 时一次交付
        if (call.unary_route) |route| {
            try call.accum.appendSlice(call.alloc, data);
            if (call.accum.items.len > max_stream_message) return error.BodyTooLarge;
            if (end_stream) {
                call.req.body = call.accum.items;
                route.handler(call.route_ctx, call, call.alloc) catch |e| {
                    if (call.status == .ok) call.setStatus(.internal, @errorName(e));
                    std.debug.print("[zgrpc.grpc] 处理 {s} 失败：{s}\n", .{ call.req.path, @errorName(e) });
                };
                return call.finish();
            }
            return;
        }

        // ② 流式路由：跨 DATA 帧重组，逐条完整消息回调
        const sr = call.stream_route orelse return;
        try call.accum.appendSlice(call.alloc, data);
        if (call.accum.items.len > max_stream_message) return error.BodyTooLarge;

        var pos: usize = 0;
        while (true) {
            const avail = call.accum.items.len - pos;
            if (avail < 5) break;
            const len = std.mem.readInt(u32, call.accum.items[pos + 1 ..][0..4], .big);
            if (avail - 5 < len) break;
            const msg = call.accum.items[pos + 5 .. pos + 5 + len];
            sr.on_message(call.route_ctx, call, msg) catch |e| {
                if (call.status == .ok) call.setStatus(.internal, @errorName(e));
                std.debug.print("[zgrpc.grpc] 流式处理 {s} 失败：{s}\n", .{ call.req.path, @errorName(e) });
                return call.finish();
            };
            pos += 5 + len;
        }
        if (pos > 0) {
            const rest = call.accum.items.len - pos;
            std.mem.copyForwards(u8, call.accum.items[0..rest], call.accum.items[pos..]);
            call.accum.shrinkRetainingCapacity(rest);
        }

        if (end_stream) {
            if (sr.on_end) |f| {
                f(call.route_ctx, call) catch |e| {
                    if (call.status == .ok) call.setStatus(.internal, @errorName(e));
                    std.debug.print("[zgrpc.grpc] 流式 on_end 失败：{s}\n", .{@errorName(e)});
                };
            }
            return call.finish();
        }
    }

    fn streamOnReset(ctx: *anyopaque, stream_id: u32) void {
        _ = ctx;
        std.debug.print("[zgrpc.grpc] 客户端取消了流 {d}\n", .{stream_id});
    }
};

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "grpc：消息编码与迭代往返" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&buf, a, "hello");
    try encodeMessage(&buf, a, "world");

    // 首字节是压缩标志，随后 4 字节是大端长度
    try testing.expectEqual(@as(u8, 0), buf.items[0]);
    try testing.expectEqual(@as(u32, 5), std.mem.readInt(u32, buf.items[1..5], .big));

    var it = MessageIterator{ .data = buf.items };
    try testing.expectEqualStrings("hello", it.next().?.payload);
    try testing.expectEqualStrings("world", it.next().?.payload);
    try testing.expect(it.next() == null);
}

test "grpc：截断的消息体不会被误判为完整消息" {
    var it = MessageIterator{ .data = "\x00\x00\x00\x00\x09abc" };
    try testing.expect(it.next() == null);
}

// ---------------------------------------------------------------------------
// 端到端：h2 + gRPC 完整链路（真实客户端字节流 → 断言 DATA 与 trailers）
// ---------------------------------------------------------------------------

fn pingHandler(ctx: *anyopaque, call: *Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    if (msg.compressed) {
        call.setStatus(.unimplemented, "不支持压缩");
        return;
    }
    try call.sendMessage(if (std.mem.eql(u8, msg.payload, "ping")) "pong" else "?");
}

test "grpc：h2 上的 unary 端到端（grpc-status 走 trailers）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Echo/Ping" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "te", .value = "trailers" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&body, a, "ping");
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, body.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Ping", .handler = pingHandler }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var content_type_ok = false;
    var status_200 = false;
    var resp_body: []const u8 = "";
    var grpc_status: []const u8 = "";
    var saw_end_stream = false;

    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, ":status") and std.mem.eql(u8, h.value, "200")) status_200 = true;
                    if (std.mem.eql(u8, h.name, "content-type") and std.mem.eql(u8, h.value, "application/grpc")) content_type_ok = true;
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
                if (f.flags & h2.Flags.end_stream != 0) saw_end_stream = true;
            },
            .data => resp_body = f.payload,
            else => {},
        }
    }

    try testing.expect(status_200);
    try testing.expect(content_type_ok);
    try testing.expectEqualStrings("0", grpc_status); // OK
    try testing.expect(saw_end_stream);

    var it = MessageIterator{ .data = resp_body };
    try testing.expectEqualStrings("pong", it.next().?.payload);
    try testing.expect(it.next() == null);
}

test "grpc：未注册方法返回 UNIMPLEMENTED(12)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/nope.Unknown/Call" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers | h2.Flags.end_stream, 1, block.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Ping", .handler = pingHandler }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var grpc_status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype != .headers) continue;
        var dec = hpack.Decoder.init(testing.allocator);
        defer dec.deinit();
        var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
        try dec.decode(f.payload, &hs, a);
        for (hs.items) |h| {
            if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
        }
    }
    try testing.expectEqualStrings("12", grpc_status);
}
// ---------------------------------------------------------------------------
// 端到端：流式（bidi / 客户端流）—— DATA 到达即回调，消息跨帧重组
// ---------------------------------------------------------------------------

fn chatOnMessage(ctx: *anyopaque, call: *Call, msg: []const u8) anyerror!void {
    _ = ctx;
    var buf: [512]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "echo:{s}", .{msg});
    try call.sendMessage(out);
}

test "grpc：bidi 流式（每条消息即时响应 + 跨 DATA 帧重组）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Stream/Chat" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, &block, a);
    // 注意：HEADERS 不带 END_STREAM —— 客户端流
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var msg1: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&msg1, a, "hello");
    var msg2: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&msg2, a, "world");

    // 第 1 条完整；第 2 条被切成两帧（验证跨帧重组）
    try h2.appendFrame(&input, a, .data, 0, 1, msg1.items);
    const cut = msg2.items.len / 2;
    try h2.appendFrame(&input, a, .data, 0, 1, msg2.items[0..cut]);
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, msg2.items[cut..]);

    var routes = [_]Route{};
    var stream_routes = [_]StreamRoute{
        .{ .path = "/test.Stream/Chat", .on_message = chatOnMessage },
    };
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .stream_routes = &stream_routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serveStreaming(server.streamHooks()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    // 汇总响应：DATA 拼接 + trailers
    var resp: std.ArrayListUnmanaged(u8) = .empty;
    var pos: usize = 0;
    var grpc_status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .data => try resp.appendSlice(a, f.payload),
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
            },
            else => {},
        }
    }

    var it = MessageIterator{ .data = resp.items };
    try testing.expectEqualStrings("echo:hello", it.next().?.payload);
    try testing.expectEqualStrings("echo:world", it.next().?.payload);
    try testing.expect(it.next() == null);
    try testing.expectEqualStrings("0", grpc_status);
}

test "grpc：流式模式下整请求路由仍可用（累积到 END_STREAM）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Echo/Ping" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&body, a, "ping");
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, body.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Ping", .handler = pingHandler }};
    var stream_routes = [_]StreamRoute{};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .stream_routes = &stream_routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serveStreaming(server.streamHooks()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var resp: std.ArrayListUnmanaged(u8) = .empty;
    var pos: usize = 0;
    var grpc_status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .data => try resp.appendSlice(a, f.payload),
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
            },
            else => {},
        }
    }

    var it = MessageIterator{ .data = resp.items };
    try testing.expectEqualStrings("pong", it.next().?.payload);
    try testing.expectEqualStrings("0", grpc_status);
}