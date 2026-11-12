//! 验证 `tools/proto_gen.zig` 生成的代码**真的能用**（不只是语法正确）。
//!
//! 这个文件由 `zig build test` 一起跑；生成物是 `examples/echo.pb.zig`
//! （由 `zig run tools/proto_gen.zig -- examples/echo.proto examples/echo.pb.zig` 产出）。

const std = @import("std");
const echo = @import("echo.pb.zig");

test "proto_gen：标量 + 字符串往返" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const req = echo.BumpRequest{ .value = 41, .tag = "gen" };
    const bytes = try req.encode(a);
    const back = try echo.BumpRequest.decode(bytes, a);
    try std.testing.expectEqual(@as(i32, 41), back.value);
    try std.testing.expectEqualStrings("gen", back.tag);

    // 默认值不写盘（proto3）：空 message 编码后应为空字节串
    const empty: echo.BumpRequest = .{};
    try std.testing.expectEqual(@as(usize, 0), (try empty.encode(a)).len);
}

test "proto_gen：负数 / 枚举 / 嵌套 message / repeated / double 往返" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const s = echo.Summary{
        .note = "note-中文",
        .status = .OK,
        .last = .{ .value = -7, .tag = "inner", .calls = 9 },
        .samples = &.{ -1, 0, 42 },
        .counters = &.{ 1, 300, 70000 },
        .done = true,
        .ratio = 0.5,
    };
    const bytes = try s.encode(a);
    const back = try echo.Summary.decode(bytes, a);
    try std.testing.expectEqualStrings("note-中文", back.note);
    try std.testing.expectEqual(echo.Status.OK, back.status.?);
    try std.testing.expectEqual(@as(i32, -7), back.last.?.value);
    try std.testing.expectEqualStrings("inner", back.last.?.tag);
    try std.testing.expectEqual(@as(u64, 9), back.last.?.calls);
    try std.testing.expectEqualSlices(i32, &.{ -1, 0, 42 }, back.samples);
    try std.testing.expectEqualSlices(u64, &.{ 1, 300, 70000 }, back.counters);
    try std.testing.expect(back.done);
    try std.testing.expectEqual(@as(f64, 0.5), back.ratio);
}

test "proto_gen：service 路由常量" {
    try std.testing.expectEqualStrings("echo.Echo", echo.Echo.name);
    try std.testing.expectEqualStrings("/echo.Echo/Say", echo.Echo.Say);
    try std.testing.expectEqualStrings("/echo.Echo/Chat", echo.Echo.Chat);
}

test "proto_gen：repeated string / message / enum 往返" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const s = echo.Summary{
        .tags = &.{ "a", "中文" },
        .items = &.{ .{ .value = 1 }, .{ .value = 2, .tag = "x" } },
        .states = &.{ .OK, .FAILED },
    };
    const bytes = try s.encode(a);
    const back = try echo.Summary.decode(bytes, a);

    try std.testing.expectEqual(@as(usize, 2), back.tags.len);
    try std.testing.expectEqualStrings("a", back.tags[0]);
    try std.testing.expectEqualStrings("中文", back.tags[1]);

    try std.testing.expectEqual(@as(usize, 2), back.items.len);
    try std.testing.expectEqual(@as(i32, 1), back.items[0].value);
    try std.testing.expectEqual(@as(i32, 2), back.items[1].value);
    try std.testing.expectEqualStrings("x", back.items[1].tag);

    try std.testing.expectEqualSlices(echo.Status, &.{ .OK, .FAILED }, back.states);
}

test "proto_gen：repeated 数字兼容非 packed 编码（对端 [packed=false]）" {
    const pb = @import("zgrpc").pb;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // 手工拼出「同一字段号出现两次、各一个 varint」的非 packed 形态
    var w = pb.Writer.init(a);
    try w.int32(4, 7);
    try w.int32(4, 9);

    const back = try echo.Summary.decode(w.bytes(), a);
    try std.testing.expectEqualSlices(i32, &.{ 7, 9 }, back.samples);
}

test "proto_gen：map<string,int32> 与 map<int32,message> 往返" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const s = echo.Summary{
        .counts = &.{ .{ .key = "a", .value = 1 }, .{ .key = "中文", .value = -2 } },
        .by_id = &.{ .{ .key = 7, .value = .{ .value = 70, .tag = "seven" } } },
    };
    const bytes = try s.encode(a);
    const back = try echo.Summary.decode(bytes, a);

    try std.testing.expectEqual(@as(usize, 2), back.counts.len);
    try std.testing.expectEqual(@as(i32, 1), back.getCounts("a").?);
    try std.testing.expectEqual(@as(i32, -2), back.getCounts("中文").?);
    try std.testing.expect(back.getCounts("missing") == null);

    try std.testing.expectEqual(@as(usize, 1), back.by_id.len);
    const seven = back.getById(7).?;
    try std.testing.expectEqual(@as(i32, 70), seven.value);
    try std.testing.expectEqualStrings("seven", seven.tag);
    try std.testing.expect(back.getById(8) == null);
}
