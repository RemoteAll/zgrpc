//! 由 zgrpc tools/proto_gen.zig 生成，**请勿手改**。
//! 来源：shop.proto
//! package：shop

const std = @import("std");
const pb = @import("zgrpc").pb;

const common_types = @import("common/types.pb.zig");

pub const Order = struct {
    /// `map<string, ..> by_sku = 5` 的条目
    pub const BySkuEntry = struct { key: []const u8, value: common_types.Money };
    id: []const u8 = "",
    total: ?common_types.Money = null,
    region: ?common_types.Region = null,
    lines: []const common_types.Money = &.{},
    by_sku: []const BySkuEntry = &.{},

    pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {
        var w = pb.Writer.init(a);
        if (self.id.len > 0) try w.string(1, self.id);
        if (self.total) |v| try w.message(2, try v.encode(a));
        if (self.region) |v| { if (@intFromEnum(v) != 0) try w.enumField(3, v); }
        for (self.lines) |v| try w.message(4, try v.encode(a));
        for (self.by_sku) |e| {
            var ew = pb.Writer.init(a);
            try ew.string(1, e.key);
            try ew.message(2, try e.value.encode(a));
            try w.message(5, ew.bytes());
        }
        return w.bytes();
    }

    pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {
        var self: @This() = .{};
        var lst_lines: std.ArrayListUnmanaged(common_types.Money) = .empty;
        var lst_by_sku: std.ArrayListUnmanaged(BySkuEntry) = .empty;
        var r = pb.Reader{ .data = bytes };
        while (r.next()) |f| switch (f.number) {
            1 => self.id = f.asBytes(),
            2 => self.total = try common_types.Money.decode(f.asBytes(), a),
            3 => self.region = f.asEnum(common_types.Region),
            4 => try lst_lines.append(a, try common_types.Money.decode(f.asBytes(), a)),
            5 => {
                var ek: []const u8 = "";
                var ev: common_types.Money = .{};
                var er = pb.Reader{ .data = f.asBytes() };
                while (er.next()) |ef| switch (ef.number) {
                    1 => ek = ef.asBytes(),
                    2 => ev = try common_types.Money.decode(ef.asBytes(), a),
                    else => {},
                };
                try lst_by_sku.append(a, .{ .key = ek, .value = ev });
            },
            else => {},
        };
        self.lines = lst_lines.items;
        self.by_sku = lst_by_sku.items;
        return self;
    }

    /// `by_sku` 取一个键（线性查找，未命中返回 null）
    pub fn getBySku(self: @This(), k: []const u8) ?common_types.Money {
        for (self.by_sku) |e| if (std.mem.eql(u8, e.key, k)) return e.value;
        return null;
    }
};

/// service shop.Shop（路径前缀 `/shop.Shop/`）
pub const Shop = struct {
    pub const name = "shop.Shop";
    pub const Get = "/shop.Shop/Get";
};

