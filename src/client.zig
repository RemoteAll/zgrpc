//! gRPC 客户端（明文 h2c）：握手、发起调用、读响应。
//!
//! 定位：给 zgrpc 补上"另一半"—— 之前只有服务端，连自测都要手写帧。
//! `callMany` 会在**同一条连接**上并发发起多条流（不同 stream_id），配合 h2 的多路复用。
//!
//! 限制（明确）：不做异步运行时；`callMany` 是"先全部发出、再统一收完"的批量模型，
//! 交互式双向流（bidi）需要自己基于 `h2` 层构建，见 README「未做」。

const std = @import("std");
const h2 = @import("h2.zig");
const hpack = @import("hpack.zig");
const grpc = @import("grpc.zig");

pub const Request = struct {
    /// 完整方法路径，如 `/echo.Echo/Say`
    method: []const u8,
    payload: []const u8 = "",
};

pub const Result = struct {
    stream_id: u32,
    /// gRPC 状态码（0 = OK）；未收到 trailer 时为 0，但 `aborted` 为 true
    status: u32 = 0,
    /// `grpc-message`（可能为空）
    message: []const u8 = "",
    /// 已解出（去分帧）的响应消息；unary 通常 1 条
    messages: []const []const u8 = &.{},
    /// 收到 `RST_STREAM`，或流结束时没有 `grpc-status`
    aborted: bool = false,

    pub fn ok(self: Result) bool {
        return !self.aborted and self.status == 0;
    }
};

pub const Options = struct {
    authority: []const u8 = "localhost",
    scheme: []const u8 = "http",
    /// 单条流响应体上限
    max_body: usize = 16 * 1024 * 1024,
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
        self.decoder.deinit();
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

        const results = try arena.alloc(Result, reqs.len);
        const bodies = try arena.alloc(std.ArrayListUnmanaged(u8), reqs.len);
        const done = try arena.alloc(bool, reqs.len);

        for (results, 0..) |*r, i| {
            r.* = .{ .stream_id = self.next_stream_id + @as(u32, @intCast(i)) * 2 };
            bodies[i] = .empty;
            done[i] = false;
        }
        self.next_stream_id += @as(u32, @intCast(reqs.len)) * 2;

        // 先全部发 HEADERS（模拟并发发起），再发各自 DATA
        for (reqs, results) |req, r| try self.writeHeaders(r.stream_id, req.method, arena);
        for (reqs, results) |req, r| {
            var body: std.ArrayListUnmanaged(u8) = .empty;
            try grpc.encodeMessage(&body, arena, req.payload);
            try writeFrame(self.writer, .data, h2.Flags.end_stream, r.stream_id, body.items);
        }
        try self.writer.flush();

        var remaining = reqs.len;
        while (remaining > 0) {
            const hdr = readFrameHeader(self.reader) catch |e| {
                std.debug.print("[zgrpc.client] 读响应帧结束（{s}）\n", .{@errorName(e)});
                return error.ConnectionClosed;
            };
            const payload = try arena.alloc(u8, hdr.length);
            try self.reader.readSliceAll(payload);

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
                    for (hs.items) |h| {
                        if (std.mem.eql(u8, h.name, "grpc-status")) {
                            results[idx].status = std.fmt.parseInt(u32, h.value, 10) catch 2;
                        } else if (std.mem.eql(u8, h.name, "grpc-message")) {
                            results[idx].message = h.value;
                        }
                    }
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

        // 去掉 gRPC 长度前缀，解出消息
        for (results, bodies) |*r, b| {
            var list: std.ArrayListUnmanaged([]const u8) = .empty;
            var it = grpc.MessageIterator{ .data = b.items };
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
        const sid = self.next_stream_id;
        self.next_stream_id += 2;

        try self.writeHeaders(sid, method, arena);
        if (messages.len == 0) {
            try writeFrame(self.writer, .data, h2.Flags.end_stream, sid, &.{});
        } else {
            for (messages, 0..) |m, i| {
                var body: std.ArrayListUnmanaged(u8) = .empty;
                try grpc.encodeMessage(&body, arena, m);
                const flags: u8 = if (i + 1 == messages.len) h2.Flags.end_stream else 0;
                try writeFrame(self.writer, .data, flags, sid, body.items);
            }
        }
        try self.writer.flush();

        var result = Result{ .stream_id = sid };
        var resp: std.ArrayListUnmanaged(u8) = .empty;

        while (true) {
            const hdr = readFrameHeader(self.reader) catch |e| {
                std.debug.print("[zgrpc.client] 读响应帧结束（{s}）\n", .{@errorName(e)});
                return error.ConnectionClosed;
            };
            const payload = try arena.alloc(u8, hdr.length);
            try self.reader.readSliceAll(payload);

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
                    for (hs.items) |h| {
                        if (std.mem.eql(u8, h.name, "grpc-status")) {
                            result.status = std.fmt.parseInt(u32, h.value, 10) catch 2;
                        } else if (std.mem.eql(u8, h.name, "grpc-message")) {
                            result.message = h.value;
                        }
                    }
                    if (hdr.flags & h2.Flags.end_stream != 0) break;
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

        var list: std.ArrayListUnmanaged([]const u8) = .empty;
        var it = grpc.MessageIterator{ .data = resp.items };
        while (it.next()) |m| try list.append(arena, m.payload);
        result.messages = list.items;
        return result;
    }

    fn writeHeaders(self: *Client, stream_id: u32, method: []const u8, arena: std.mem.Allocator) !void {        var block: std.ArrayListUnmanaged(u8) = .empty;
        try hpack.encode(&.{
            .{ .name = ":method", .value = "POST" },
            .{ .name = ":path", .value = method },
            .{ .name = ":scheme", .value = self.opts.scheme },
            .{ .name = ":authority", .value = self.opts.authority },
            .{ .name = "content-type", .value = "application/grpc" },
            .{ .name = "te", .value = "trailers" },
        }, &block, arena);
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

fn readFrameHeader(r: *std.Io.Reader) !FrameHeader {
    var b: [9]u8 = undefined;
    try r.readSliceAll(&b);
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
