//! gzip 编解码（gRPC 消息压缩用；`grpc-encoding: gzip`）。
//!
//! 零依赖：走 Zig 标准库 `std.compress.flate`。服务端与客户端共用这一份，
//! 避免两处各写一套导致行为漂移。

const std = @import("std");

/// gzip 压缩。
///
/// 注意：`Compress.init` 断言输出缓冲 > 8 字节，所以必须用 `initCapacity` 预分配；
/// `Compress` 结构体约 224K，放堆上避免大栈帧。
pub fn compress(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var output = try std.Io.Writer.Allocating.initCapacity(alloc, 4096);
    errdefer output.deinit();

    const window = try alloc.alloc(u8, std.compress.flate.max_window_len);
    defer alloc.free(window);

    const compressor = try alloc.create(std.compress.flate.Compress);
    defer alloc.destroy(compressor);
    compressor.* = try std.compress.flate.Compress.init(&output.writer, window, .gzip, .default);
    try compressor.writer.writeAll(input);
    try compressor.finish();

    return try output.toOwnedSlice();
}

/// gzip 解压。
pub fn decompress(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var input_reader: std.Io.Reader = .fixed(input);
    var decompressor = std.compress.flate.Decompress.init(&input_reader, .gzip, &.{});

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    _ = try decompressor.reader.streamRemaining(&output.writer);
    if (decompressor.err) |err| return err;

    return try output.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "gzip：压缩后可解压回原文" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // `**` 数组重复运算符在 Zig 0.17.1 已移除，手写重复
    const base = "the quick brown fox jumps over the lazy dog";
    var buf: [base.len * 8]u8 = undefined;
    for (0..8) |i| @memcpy(buf[i * base.len ..][0..base.len], base);
    const plain: []const u8 = &buf;
    const gz = try compress(a, plain);
    try testing.expect(gz.len < plain.len);
    // gzip 魔数
    try testing.expectEqual(@as(u8, 0x1F), gz[0]);
    try testing.expectEqual(@as(u8, 0x8B), gz[1]);

    const back = try decompress(a, gz);
    try testing.expectEqualStrings(plain, back);
}

test "gzip：空输入也能往返" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const gz = try compress(a, "");
    const back = try decompress(a, gz);
    try testing.expectEqualStrings("", back);
}
