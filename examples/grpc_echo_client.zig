//! gRPC echo 客户端示例：跑一次真实 TCP 上的 unary 调用（明文 h2c）。
//!
//! 运行（需先起 `zig build echo-server`）：`zig build echo-client`
//!
//! 这个客户端刻意写得「贴着帧」——它同时是 h2 客户端的可读样例，
//! 也用来证明服务端在真实 socket 上可用（而不只是内存管道里的单元测试）。

const std = @import("std");
const zgrpc = @import("zgrpc");

const port: u16 = 18051;
const method = "/echo.Echo/Say";
const payload = "hello-grpc";

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

    // 2. HEADERS（伪头 + gRPC 元数据）
    var block: std.ArrayListUnmanaged(u8) = .empty;
    try zgrpc.hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = method },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "127.0.0.1" },
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "te", .value = "trailers" },
    }, &block, a);
    try writeFrame(w, .headers, zgrpc.h2.Flags.end_headers, 1, block.items);

    // 3. DATA（gRPC 长度前缀消息）
    var body: std.ArrayListUnmanaged(u8) = .empty;
    try zgrpc.grpc.encodeMessage(&body, a, payload);
    try writeFrame(w, .data, zgrpc.h2.Flags.end_stream, 1, body.items);
    try w.flush();

    // 4. 读响应帧
    var got_data: []const u8 = "";
    var grpc_status: []const u8 = "";
    var saw_end = false;

    while (!saw_end) {
        const hdr = try readFrameHeader(r);
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
                var dec = zgrpc.hpack.Decoder.init(a);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(zgrpc.hpack.Header) = .empty;
                try dec.decode(body_bytes, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
                if (hdr.flags & zgrpc.h2.Flags.end_stream != 0) saw_end = true;
            },
            .data => got_data = try a.dupe(u8, body_bytes),
            .goaway => saw_end = true,
            else => {},
        }
    }

    var it = zgrpc.grpc.MessageIterator{ .data = got_data };
    if (it.next()) |msg| {
        std.debug.print("[zgrpc] 调用成功：grpc-status={s} 响应={s}\n", .{ grpc_status, msg.payload });
    } else {
        std.debug.print("[zgrpc] 调用未返回消息：grpc-status={s}\n", .{grpc_status});
        return error.NoResponseMessage;
    }

    if (!std.mem.eql(u8, grpc_status, "0")) {
        std.debug.print("[zgrpc] 调用失败：grpc-status={s}\n", .{grpc_status});
        return error.GrpcStatusNotOk;
    }
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
    session_id: u32,
};

fn readFrameHeader(r: *std.Io.Reader) !FrameHeader {
    var b: [9]u8 = undefined;
    try r.readSliceAll(&b);
    return .{
        .length = (@as(usize, b[0]) << 16) | (@as(usize, b[1]) << 8) | @as(usize, b[2]),
        .ftype = @enumFromInt(b[3]),
        .flags = b[4],
        .session_id = std.mem.readInt(u32, b[5..9], .big) & 0x7fff_ffff,
    };
}
