//! 由 zgrpc tools/proto_gen.zig 生成，**请勿手改**。
//! 来源：common/types.proto
//! package：common

const std = @import("std");
const pb = @import("zgrpc").pb;

pub const Region = enum(i32) {
    GLOBAL = 0,
    CN = 1,
    US = 2,
};

pub const Money = struct {
    cents: i64 = 0,
    currency: []const u8 = "",

    pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {
        var w = pb.Writer.init(a);
        if (self.cents != 0) try w.int64(1, self.cents);
        if (self.currency.len > 0) try w.string(2, self.currency);
        return w.bytes();
    }

    pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {
        _ = a; // 该 message 的字段不需要分配
        var self: @This() = .{};
        var r = pb.Reader{ .data = bytes };
        while (r.next()) |f| switch (f.number) {
            1 => self.cents = f.asInt64(),
            2 => self.currency = f.asBytes(),
            else => {},
        };
        return self;
    }
};

