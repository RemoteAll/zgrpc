//! 由 zgrpc tools/proto_gen.zig 生成，**请勿手改**。
//! 来源：examples/echo.proto
//! package：echo

const std = @import("std");
const pb = @import("zgrpc").pb;

pub const Status = enum(i32) {
    UNKNOWN = 0,
    OK = 1,
    FAILED = 2,
};

pub const BumpRequest = struct {
    value: i32 = 0,
    tag: []const u8 = "",

    pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {
        var w = pb.Writer.init(a);
        if (self.value != 0) try w.int32(1, self.value);
        if (self.tag.len > 0) try w.string(2, self.tag);
        return w.bytes();
    }

    pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {
        _ = a; // 该 message 的字段不需要分配
        var self: @This() = .{};
        var r = pb.Reader{ .data = bytes };
        while (r.next()) |f| switch (f.number) {
            1 => self.value = f.asInt32(),
            2 => self.tag = f.asBytes(),
            else => {},
        };
        return self;
    }
};

pub const BumpResponse = struct {
    value: i32 = 0,
    tag: []const u8 = "",
    calls: u64 = 0,

    pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {
        var w = pb.Writer.init(a);
        if (self.value != 0) try w.int32(1, self.value);
        if (self.tag.len > 0) try w.string(2, self.tag);
        if (self.calls != 0) try w.uint64(3, self.calls);
        return w.bytes();
    }

    pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {
        _ = a; // 该 message 的字段不需要分配
        var self: @This() = .{};
        var r = pb.Reader{ .data = bytes };
        while (r.next()) |f| switch (f.number) {
            1 => self.value = f.asInt32(),
            2 => self.tag = f.asBytes(),
            3 => self.calls = f.asUint64(),
            else => {},
        };
        return self;
    }
};

pub const Summary = struct {
    note: []const u8 = "",
    status: ?Status = null,
    last: ?BumpResponse = null,
    samples: []const i32 = &.{},
    counters: []const u64 = &.{},
    done: bool = false,
    ratio: f64 = 0,

    pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {
        var w = pb.Writer.init(a);
        if (self.note.len > 0) try w.string(1, self.note);
        if (self.status) |v| try w.enumField(2, v) else try w.enumField(2, Status.UNKNOWN);
        if (self.last) |v| try w.message(3, try v.encode(a));
        if (self.samples.len > 0) {
            try w.packedInt32(4, self.samples);
        }
        if (self.counters.len > 0) {
            try w.packedUint64(5, self.counters);
        }
        if (self.done) try w.boolField(6, true);
        if (self.ratio != 0) try w.doubleField(7, self.ratio);
        return w.bytes();
    }

    pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {
        var self: @This() = .{};
        var r = pb.Reader{ .data = bytes };
        while (r.next()) |f| switch (f.number) {
            1 => self.note = f.asBytes(),
            2 => self.status = f.asEnum(Status),
            3 => self.last = try BumpResponse.decode(f.asBytes(), a),
            4 => {
                const vals = try f.packedVarints(a);
                var tmp: std.ArrayListUnmanaged(i32) = .empty;
                for (vals) |v| try tmp.append(a, @truncate(@as(i64, @bitCast(v))));
                self.samples = tmp.items;
            },
            5 => {
                const vals = try f.packedVarints(a);
                var tmp: std.ArrayListUnmanaged(u64) = .empty;
                for (vals) |v| try tmp.append(a, v);
                self.counters = tmp.items;
            },
            6 => self.done = f.asBool(),
            7 => self.ratio = f.asDouble(),
            else => {},
        };
        return self;
    }
};

/// service echo.Echo（路径前缀 `/echo.Echo/`）
pub const Echo = struct {
    pub const name = "echo.Echo";
    pub const Say = "/echo.Echo/Say";
    pub const Chat = "/echo.Echo/Chat";
    // Chat：client_stream=true server_stream=true
};

