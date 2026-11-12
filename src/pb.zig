//! 最小 protobuf 读写运行时（proto3，零依赖）。
//!
//! 为什么需要它：zgrpc 只做传输层，业务拿到的是**已序列化的字节**。之前没有这一层，
//! 业务要么自己手写 varint（极易错），要么引一个第三方 protobuf 库（打破零依赖）。
//! 这里把最容易写错的部分固化下来：varint/zigzag、tag、length-delimited、packed。
//!
//! 设计取舍：
//! - **不做 `.proto` 解析与代码生成**：那是独立工具的事；这里只给「手写 message 编解码」
//!   提供正确、够用的原语（每个 message 大约 10 行代码，见下面示例）。
//! - **不分配解码结果**：`Reader.next` 返回的 `Field.data` 直接指向输入缓冲区，
//!   需要长期持有再自行复制（服务端场景通常 arena 内即用即弃）。
//! - **宽容未知字段**：`next` 会自动跳过不认识的 wire type，符合 proto3 前向兼容要求。
//!
//! 示例（手写一个 `EchoRequest { string text = 1; int32 seq = 2; }`）：
//! ```zig
//! fn encodeEcho(a: std.mem.Allocator, text: []const u8, seq: i32) ![]const u8 {
//!     var w = zgrpc.pb.Writer.init(a);
//!     try w.string(1, text);
//!     try w.int32(2, seq);
//!     return w.bytes();
//! }
//!
//! fn decodeEcho(bytes: []const u8) !struct { text: []const u8, seq: i32 } {
//!     var text: []const u8 = "";
//!     var seq: i32 = 0;
//!     var r = zgrpc.pb.Reader{ .data = bytes };
//!     while (r.next()) |f| switch (f.number) {
//!         1 => text = f.asString(),
//!         2 => seq = f.asInt32(),
//!         else => {}, // 未知字段已被跳过
//!     };
//!     return .{ .text = text, .seq = seq };
//! }
//! ```

const std = @import("std");

/// protobuf wire type（RFC 见 https://protobuf.dev/programming-guides/encoding/）。
pub const WireType = enum(u3) {
    varint = 0,
    fixed64 = 1,
    len = 2,
    start_group = 3,
    end_group = 4,
    fixed32 = 5,
};

pub const max_field_number: u32 = (1 << 29) - 1;

// ---------------------------------------------------------------------------
// 写
// ---------------------------------------------------------------------------

/// 编码器：把字段追加进内部缓冲区。
///
/// 分配器由调用方持有（服务端一般在流的 arena 上），`deinit` 可选。
pub const Writer = struct {
    alloc: std.mem.Allocator,
    list: std.ArrayListUnmanaged(u8) = .empty,

    pub fn init(alloc: std.mem.Allocator) Writer {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Writer) void {
        self.list.deinit(self.alloc);
    }

    /// 已编码的字节（生命周期跟随本 Writer 的分配器）。
    pub fn bytes(self: *const Writer) []const u8 {
        return self.list.items;
    }

    pub fn len(self: *const Writer) usize {
        return self.list.items.len;
    }

    pub fn rawByte(self: *Writer, b: u8) !void {
        try self.list.append(self.alloc, b);
    }

    pub fn rawSlice(self: *Writer, s: []const u8) !void {
        try self.list.appendSlice(self.alloc, s);
    }

    /// 无符号 varint（1~10 字节）。
    pub fn writeVarint(self: *Writer, v: u64) !void {
        var x = v;
        while (x >= 0x80) {
            try self.list.append(self.alloc, @as(u8, @truncate(x)) | 0x80);
            x >>= 7;
        }
        try self.list.append(self.alloc, @truncate(x));
    }

    fn writeTag(self: *Writer, number: u32, wire: WireType) !void {
        std.debug.assert(number >= 1 and number <= max_field_number);
        try self.writeVarint((@as(u64, number) << 3) | @intFromEnum(wire));
    }

    /// varint 字段（`uint64` / `int64` / `int32` 等的通用底座；负 int32/int64 会被符号扩展成 10 字节）。
    pub fn uint64(self: *Writer, number: u32, v: u64) !void {
        try self.writeTag(number, .varint);
        try self.writeVarint(v);
    }

    pub fn int64(self: *Writer, number: u32, v: i64) !void {
        // proto3：负数按 64 位补码编码（固定 10 字节）
        try self.uint64(number, @bitCast(v));
    }

    pub fn int32(self: *Writer, number: u32, v: i32) !void {
        try self.uint64(number, @bitCast(@as(i64, v)));
    }

    pub fn uint32(self: *Writer, number: u32, v: u32) !void {
        try self.uint64(number, v);
    }

    /// zigzag（`sint32`），负数更省空间。
    pub fn sint64(self: *Writer, number: u32, v: i64) !void {
        try self.uint64(number, zigzagEncode(v));
    }

    pub fn sint32(self: *Writer, number: u32, v: i32) !void {
        try self.sint64(number, v);
    }

    pub fn boolField(self: *Writer, number: u32, v: bool) !void {
        try self.uint64(number, if (v) 1 else 0);
    }

    /// 枚举按 int32 编码。
    pub fn enumField(self: *Writer, number: u32, v: anytype) !void {
        try self.int32(number, @intCast(@intFromEnum(v)));
    }

    pub fn fixed32(self: *Writer, number: u32, v: u32) !void {
        try self.writeTag(number, .fixed32);
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, v, .little);
        try self.list.appendSlice(self.alloc, &b);
    }

    pub fn fixed64(self: *Writer, number: u32, v: u64) !void {
        try self.writeTag(number, .fixed64);
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, v, .little);
        try self.list.appendSlice(self.alloc, &b);
    }

    pub fn floatField(self: *Writer, number: u32, v: f32) !void {
        try self.fixed32(number, @bitCast(v));
    }

    pub fn doubleField(self: *Writer, number: u32, v: f64) !void {
        try self.fixed64(number, @bitCast(v));
    }

    /// 任意字节串（`bytes`）。
    pub fn bytesField(self: *Writer, number: u32, v: []const u8) !void {
        try self.writeTag(number, .len);
        try self.writeVarint(v.len);
        try self.list.appendSlice(self.alloc, v);
    }

    /// 字符串（UTF-8；本层不校验编码有效性，与多数实现一致）。
    pub fn string(self: *Writer, number: u32, v: []const u8) !void {
        try self.bytesField(number, v);
    }

    /// 嵌套 message：`sub` 已编码好的字节。
    pub fn message(self: *Writer, number: u32, sub: []const u8) !void {
        try self.bytesField(number, sub);
    }

    /// packed repeated（数值类型）：proto3 默认打包。
    pub fn packedUint64(self: *Writer, number: u32, values: []const u64) !void {
        if (values.len == 0) return; // proto3：空 repeated 不写
        var inner = Writer.init(self.alloc);
        for (values) |v| try inner.writeVarint(v);
        try self.message(number, inner.bytes());
    }

    pub fn packedInt32(self: *Writer, number: u32, values: []const i32) !void {
        if (values.len == 0) return;
        var inner = Writer.init(self.alloc);
        for (values) |v| try inner.writeVarint(@bitCast(@as(i64, v)));
        try self.message(number, inner.bytes());
    }
};

/// zigzag 编码（`sint32` / `sint64`）。
pub fn zigzagEncode(v: i64) u64 {
    const bits: u64 = @bitCast(v);
    return (bits << 1) ^ @as(u64, @bitCast(@as(i64, v) >> 63));
}

/// zigzag 解码。
pub fn zigzagDecode(v: u64) i64 {
    const half = v >> 1;
    const sign: u64 = 0 -% (v & 1);
    return @bitCast(half ^ sign);
}

// ---------------------------------------------------------------------------
// 读
// ---------------------------------------------------------------------------

/// 一个已解析的字段：`data` 仅对 `len` 类型有值（指向输入缓冲区，不复制）。
pub const Field = struct {
    number: u32,
    wire: WireType,
    /// varint / fixed 类型的原始数值
    value: u64 = 0,
    /// len 类型的载荷（未复制）
    data: []const u8 = "",

    pub fn asString(self: Field) []const u8 {
        return self.data;
    }

    pub fn asBytes(self: Field) []const u8 {
        return self.data;
    }

    pub fn asUint64(self: Field) u64 {
        return self.value;
    }

    pub fn asInt64(self: Field) i64 {
        return @bitCast(self.value);
    }

    pub fn asInt32(self: Field) i32 {
        // proto3：负数被符号扩展到 64 位，截断取低 32 位
        return @truncate(@as(i64, @bitCast(self.value)));
    }

    pub fn asUint32(self: Field) u32 {
        return @truncate(self.value);
    }

    pub fn asSint64(self: Field) i64 {
        return zigzagDecode(self.value);
    }

    pub fn asSint32(self: Field) i32 {
        return @truncate(zigzagDecode(self.value));
    }

    pub fn asBool(self: Field) bool {
        return self.value != 0;
    }

    pub fn asEnum(self: Field, comptime E: type) ?E {
        const raw = self.asInt32();
        // Zig 0.17 的 Enum TypeInfo 是 field_values（不再是 fields）
        const info = @typeInfo(E).@"enum";
        inline for (info.field_values) |v| {
            if (v == raw) return @enumFromInt(raw);
        }
        return null;
    }

    pub fn asFloat(self: Field) f32 {
        return @bitCast(@as(u32, @truncate(self.value)));
    }

    pub fn asDouble(self: Field) f64 {
        return @bitCast(self.value);
    }

    /// 解析 packed repeated varint 的载荷。
    pub fn packedVarints(self: Field, alloc: std.mem.Allocator) ![]u64 {
        var out: std.ArrayListUnmanaged(u64) = .empty;
        var pos: usize = 0;
        while (pos < self.data.len) {
            const v = readVarintAt(self.data, &pos) orelse return error.InvalidVarint;
            try out.append(alloc, v);
        }
        return out.items;
    }

    /// 解析 packed repeated sint（zigzag）。
    pub fn packedSints(self: Field, alloc: std.mem.Allocator) ![]i64 {
        const raw = try self.packedVarints(alloc);
        for (raw) |*v| v.* = @bitCast(zigzagDecode(v.*)); // 原地还原（u64 与 i64 同宽）
        return raw;
    }
};

pub const ReadError = error{InvalidVarint};

/// 解码器：按字段顺序遍历。
pub const Reader = struct {
    data: []const u8,
    pos: usize = 0,

    /// 取下一个字段；未知/不支持的 wire type 会被**跳过**（proto3 前向兼容）。
    /// 数据损坏（varint 截断、长度越界）返回 null —— 调用方按「消息不完整」处理。
    pub fn next(self: *Reader) ?Field {
        while (self.pos < self.data.len) {
            const key = readVarintAt(self.data, &self.pos) orelse return null;
            const number: u32 = @intCast(key >> 3);
            const wire_raw: u3 = @truncate(key & 0x7);
            // WireType 只定义了 0~5；6/7 保留 → 无法确定长度，终止
            if (wire_raw > @intFromEnum(WireType.fixed32)) return null;
            const wire: WireType = @enumFromInt(wire_raw);
            switch (wire) {
                .varint => {
                    const v = readVarintAt(self.data, &self.pos) orelse return null;
                    return .{ .number = number, .wire = wire, .value = v };
                },
                .fixed64 => {
                    if (self.pos + 8 > self.data.len) return null;
                    const v = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
                    self.pos += 8;
                    return .{ .number = number, .wire = wire, .value = v };
                },
                .fixed32 => {
                    if (self.pos + 4 > self.data.len) return null;
                    const v = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
                    self.pos += 4;
                    return .{ .number = number, .wire = wire, .value = v };
                },
                .len => {
                    const n = readVarintAt(self.data, &self.pos) orelse return null;
                    if (n > self.data.len - self.pos) return null;
                    const d = self.data[self.pos .. self.pos + @as(usize, @intCast(n))];
                    self.pos += @intCast(n);
                    return .{ .number = number, .wire = wire, .data = d };
                },
                // group 已废弃（proto2 遗留），直接结束遍历
                .start_group, .end_group => return null,
            }
        }
        return null;
    }
};

/// 从 `pos.*` 读一个 varint；成功时推进 `pos.*`。
fn readVarintAt(data: []const u8, pos: *usize) ?u64 {
    var shift: u7 = 0;
    var v: u64 = 0;
    var i = pos.*;
    while (i < data.len and shift < 64) {
        const b = data[i];
        i += 1;
        v |= @as(u64, b & 0x7f) << @intCast(shift);
        if (b & 0x80 == 0) {
            pos.* = i;
            return v;
        }
        shift += 7;
    }
    return null;
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "pb：varint 边界与 zigzag" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cases = [_]u64{ 0, 1, 127, 128, 300, 16383, 16384, std.math.maxInt(u32), std.math.maxInt(u64) };
    for (cases) |v| {
        var w = Writer.init(a);
        try w.writeVarint(v);
        var pos: usize = 0;
        try testing.expectEqual(v, readVarintAt(w.bytes(), &pos).?);
        try testing.expectEqual(w.bytes().len, pos);
    }

    // zigzag：0,-1,1,-2 应编码成 0,1,2,3
    try testing.expectEqual(@as(u64, 0), zigzagEncode(0));
    try testing.expectEqual(@as(u64, 1), zigzagEncode(-1));
    try testing.expectEqual(@as(u64, 2), zigzagEncode(1));
    try testing.expectEqual(@as(u64, 3), zigzagEncode(-2));
    try testing.expectEqual(@as(i64, -2), zigzagDecode(3));
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), zigzagDecode(zigzagEncode(std.math.minInt(i64))));
}

test "pb：标量/字符串/嵌套 message 往返" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // 内层：Inner { int32 id = 1; string name = 2; }
    var inner = Writer.init(a);
    try inner.int32(1, -7);
    try inner.string(2, "内层");

    // 外层：Outer { string text = 1; bool ok = 2; Inner sub = 3; sint32 delta = 4; }
    var w = Writer.init(a);
    try w.string(1, "hello");
    try w.boolField(2, true);
    try w.message(3, inner.bytes());
    try w.sint32(4, -100);

    var r = Reader{ .data = w.bytes() };
    var text: []const u8 = "";
    var ok = false;
    var sub: []const u8 = "";
    var delta: i32 = 0;
    var seen: usize = 0;
    while (r.next()) |f| {
        seen += 1;
        switch (f.number) {
            1 => text = f.asString(),
            2 => ok = f.asBool(),
            3 => sub = f.asBytes(),
            4 => delta = f.asSint32(),
            else => try testing.expect(false),
        }
    }
    try testing.expectEqual(@as(usize, 4), seen);
    try testing.expectEqualStrings("hello", text);
    try testing.expect(ok);
    try testing.expectEqual(@as(i32, -100), delta);

    // 内层独立解
    var ir = Reader{ .data = sub };
    var id: i32 = 0;
    var name: []const u8 = "";
    while (ir.next()) |f| switch (f.number) {
        1 => id = f.asInt32(),
        2 => name = f.asString(),
        else => {},
    };
    try testing.expectEqual(@as(i32, -7), id);
    try testing.expectEqualStrings("内层", name);
}

test "pb：未知字段被跳过（前向兼容）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = Writer.init(a);
    try w.uint32(1, 42);
    try w.string(99, "未来字段"); // 未知
    try w.fixed32(100, 0xdeadbeef); // 未知
    try w.string(2, "known");

    var r = Reader{ .data = w.bytes() };
    var got1: u32 = 0;
    var got2: []const u8 = "";
    while (r.next()) |f| switch (f.number) {
        1 => got1 = f.asUint32(),
        2 => got2 = f.asString(),
        else => {},
    };
    try testing.expectEqual(@as(u32, 42), got1);
    try testing.expectEqualStrings("known", got2);
}

test "pb：packed repeated" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = Writer.init(a);
    try w.packedUint64(1, &.{ 1, 2, 300 });
    try w.packedInt32(2, &.{ -1, 0, 1 });

    var r = Reader{ .data = w.bytes() };
    var ok = false;
    while (r.next()) |f| switch (f.number) {
        1 => {
            const vals = try f.packedVarints(a);
            try testing.expectEqualSlices(u64, &.{ 1, 2, 300 }, vals);
            ok = true;
        },
        2 => {
            const vals = try f.packedVarints(a);
            try testing.expectEqualSlices(u64, &.{
                @bitCast(@as(i64, -1)),
                0,
                1,
            }, vals);
        },
        else => {},
    };
    try testing.expect(ok);
}

test "pb：截断数据不 panic（返回 null）" {
    var r = Reader{ .data = &.{ 0x0a, 0x05, 'a' } }; // 声明 5 字节只给 1 字节
    try testing.expect(r.next() == null);
}
