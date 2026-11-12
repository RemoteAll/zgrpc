//! gRPC echo 客户端示例：在**同一条 TCP 连接**上并发发起两个 unary 调用（明文 h2c）。
//!
//! 运行（需先起 `zig build echo-server`）：`zig build echo-client`
//!
//! 为什么故意做成"并发双流"：它用来证明服务端在真实 socket 上**多路复用**可用 ——
//! 两条流的 HEADERS 都先到、body 后到，互不踢掉，各自独立拿到 `grpc-status=0`。
//! 同时也是一份可读的 h2 客户端样例（贴着帧写）。

const std = @import("std");
const zgrpc = @import("zgrpc");

const port: u16 = 18051;
const method = "/echo.Echo/Say";

const Call = struct {
    stream_id: u32,
    payload: []const u8,
    body: []const u8 = "",
    grpc_status: []const u8 = "",
    ended: bool = false,
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var stream = addr.connect(io, .{ .mode = .stream }) catch |e| {
        std.debug.print(
            "[zgrpc] 连接 127.0.0.1:{d} 失败（{s}）——请先运行 `zig build echo-server`\n",
            .{ port, @errorName(e) },
        );
        return e;
    };
    defer stream.close(io);

    var recv_buf: [16 * 1024]u8 = undefined;
    var send_buf: [16 * 1024]u8 = undefined;
    var sr = stream.reader(io, &recv_buf);
    var sw = stream.writer(io, &send_buf);
    const r = &sr.interface;
    const w = &sw.interface;

    // 1. 连接前言 + 我们的 SETTINGS
    try w.writeAll(zgrpc.h2.client_preface);
    try writeFrame(w, .settings, 0, 0, &.{});

    var calls = [_]Call{
        .{ .stream_id = 1, .payload = "stream-one" },
        .{ .stream_id = 3, .payload = "stream-three" },
    };

    // 2. 两条流的 HEADERS 先全部发出（模拟并发发起）
    for (&calls) |*c| {
        try writeHeaders(w, a, c.stream_id);
    }
    // 3. 再发各自的 DATA（gRPC 长度前缀消息）
    for (&calls) |*c| {
        var body: std.ArrayListUnmanaged(u8) = .empty;
        try zgrpc.grpc.encodeMessage(&body, a, c.payload);
        try writeFrame(w, .data, zgrpc.h2.Flags.end_stream, c.stream_id, body.items);
    }
    try w.flush();

    // 4. 读响应帧，按 stream_id 归位
    while (true) {
        const hdr = readFrameHeader(r) catch break; // 对端关闭

        var pbuf: [64 * 1024]u8 = undefined;
        if (hdr.length > pbuf.len) {
            std.debug.print("[zgrpc] 响应帧过大（{d} 字节），未处理\n", .{hdr.length});
            return error.FrameTooLarge;
        }
        try r.readSliceAll(pbuf[0..hdr.length]);
        const body_bytes = pbuf[0..hdr.length];

        switch (hdr.ftype) {
            .settings => {
                if (hdr.flags & zgrpc.h2.Flags.ack == 0) {
                    try writeFrame(w, .settings, zgrpc.h2.Flags.ack, 0, &.{});
                    try w.flush();
                }
            },
            .headers => {
                if (hdr.stream_id == 0) continue;
                var dec = zgrpc.hpack.Decoder.init(a);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(zgrpc.hpack.Header) = .empty;
                try dec.decode(body_bytes, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "grpc-status")) {
                        for (&calls) |*c| {
                            if (c.stream_id == hdr.stream_id) c.grpc_status = h.value;
                        }
                    }
                }
                if (hdr.flags & zgrpc.h2.Flags.end_stream != 0) {
                    for (&calls) |*c| {
                        if (c.stream_id == hdr.stream_id) c.ended = true;
                    }
                }
            },
            .data => {
                for (&calls) |*c| {
                    if (c.stream_id == hdr.stream_id) c.body = try a.dupe(u8, body_bytes);
                }
            },
            .goaway => break,
            else => {},
        }

        if (allEnded(&calls)) break;
    }

    // 5. 汇总
    var ok = true;
    for (calls) |c| {
        var it = zgrpc.grpc.MessageIterator{ .data = c.body };
        const text = if (it.next()) |m| m.payload else "<无消息>";
        std.debug.print("[zgrpc] 流 {d}：grpc-status={s} 响应={s}\n", .{ c.stream_id, c.grpc_status, text });
        if (!std.mem.eql(u8, c.grpc_status, "0") or !c.ended) ok = false;
    }
    if (!ok) {
        std.debug.print("[zgrpc] 存在失败调用\n", .{});
        return error.GrpcStatusNotOk;
    }
    std.debug.print("[zgrpc] 两条并发流均成功（同一条 TCP 连接）\n", .{});
}

fn allEnded(calls: []const Call) bool {
    for (calls) |c| {
        if (!c.ended) return false;
    }
    return true;
}

fn writeHeaders(w: *std.Io.Writer, a: std.mem.Allocator, sid: u32) !void {
    var block: std.ArrayListUnmanaged(u8) = .empty;
    try zgrpc.hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = method },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "127.0.0.1" },
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "te", .value = "trailers" },
    }, &block, a);
    try writeFrame(w, .headers, zgrpc.h2.Flags.end_headers, sid, block.items);
}

fn writeFrame(
    w: *std.Io.Writer,
    ftype: zgrpc.h2.FrameType,
    flags: u8,
    sid: u32,
    bytes: []const u8,
) !void {
    var hdr: [9]u8 = undefined;
    hdr[0] = @intCast((bytes.len >> 16) & 0xff);
    hdr[1] = @intCast((bytes.len >> 8) & 0xff);
    hdr[2] = @intCast(bytes.len & 0xff);
    hdr[3] = @intFromEnum(ftype);
    hdr[4] = flags;
    std.mem.writeInt(u32, hdr[5..9], sid & 0x7fff_ffff, .big);
    try w.writeAll(&hdr);
    if (bytes.len > 0) try w.writeAll(bytes);
}

const FrameHeader = struct {
    length: usize,
    ftype: zgrpc.h2.FrameType,
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
