//! 验证「带 import 的 codegen」：`shop.pb.zig` 通过 `@import("common/types.pb.zig")`
//! 引用跨文件类型。这个文件由 `zig build test` 一起跑。

const std = @import("std");
const shop = @import("proto_import/shop.pb.zig");
const common = @import("proto_import/common/types.pb.zig");

test "import codegen：跨文件 message / enum / repeated / map 往返" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const o = shop.Order{
        .id = "A1",
        .total = .{ .cents = 100, .currency = "CNY" },
        .region = .CN,
        .lines = &.{ .{ .cents = 5 }, .{ .cents = 6, .currency = "USD" } },
        .by_sku = &.{ .{ .key = "s1", .value = .{ .cents = 9 } } },
    };
    const bytes = try o.encode(a);
    const back = try shop.Order.decode(bytes, a);

    try std.testing.expectEqualStrings("A1", back.id);
    try std.testing.expectEqual(@as(i64, 100), back.total.?.cents);
    try std.testing.expectEqualStrings("CNY", back.total.?.currency);
    try std.testing.expectEqual(common.Region.CN, back.region.?);
    try std.testing.expectEqual(@as(usize, 2), back.lines.len);
    try std.testing.expectEqual(@as(i64, 6), back.lines[1].cents);
    try std.testing.expectEqualStrings("USD", back.lines[1].currency);
    try std.testing.expectEqual(@as(i64, 9), back.getBySku("s1").?.cents);
    try std.testing.expect(back.getBySku("nope") == null);
}

test "import codegen：依赖文件自己的类型也能单独用" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const m = common.Money{ .cents = -7, .currency = "EUR" };
    const back = try common.Money.decode(try m.encode(a), a);
    try std.testing.expectEqual(@as(i64, -7), back.cents);
    try std.testing.expectEqualStrings("EUR", back.currency);
}
