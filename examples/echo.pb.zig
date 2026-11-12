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
    tags: []const []const u8 = &.{},
    items: []const BumpResponse = &.{},
    states: []const Status = &.{},

    pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {
        var w = pb.Writer.init(a);
        if (self.note.len > 0) try w.string(1, self.note);
        if (self.status) |v| try w.enumField(2, v) else try w.enumField(2, Status.UNKNOWN);
        if (self.last) |v| try w.message(3, try v.encode(a));
        try w.packedVarint(4, self.samples);
        try w.packedVarint(5, self.counters);
        if (self.done) try w.boolField(6, true);
        if (self.ratio != 0) try w.doubleField(7, self.ratio);
        for (self.tags) |v| try w.bytesField(8, v);
        for (self.items) |v| try w.message(9, try v.encode(a));
        for (self.states) |v| try w.enumField(10, v);
        return w.bytes();
    }

    pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {
        var self: @This() = .{};
        var lst_samples: std.ArrayListUnmanaged(i32) = .empty;
        var lst_counters: std.ArrayListUnmanaged(u64) = .empty;
        var lst_tags: std.ArrayListUnmanaged([]const u8) = .empty;
        var lst_items: std.ArrayListUnmanaged(BumpResponse) = .empty;
        var lst_states: std.ArrayListUnmanaged(Status) = .empty;
        var r = pb.Reader{ .data = bytes };
        while (r.next()) |f| switch (f.number) {
            1 => self.note = f.asBytes(),
            2 => self.status = f.asEnum(Status),
            3 => self.last = try BumpResponse.decode(f.asBytes(), a),
            4 => if (f.wire == .len) {
                for (try f.packedVarints(a)) |v| try lst_samples.append(a, @truncate(@as(i64, @bitCast(v))));
            } else {
                const v = f.value;
                try lst_samples.append(a, @truncate(@as(i64, @bitCast(v))));
            },
            5 => if (f.wire == .len) {
                for (try f.packedVarints(a)) |v| try lst_counters.append(a, v);
            } else {
                const v = f.value;
                try lst_counters.append(a, v);
            },
            6 => self.done = f.asBool(),
            7 => self.ratio = f.asDouble(),
            8 => try lst_tags.append(a, f.asBytes()),
            9 => try lst_items.append(a, try BumpResponse.decode(f.asBytes(), a)),
            10 => if (f.asEnum(Status)) |v| try lst_states.append(a, v),
            else => {},
        };
        self.samples = lst_samples.items;
        self.counters = lst_counters.items;
        self.tags = lst_tags.items;
        self.items = lst_items.items;
        self.states = lst_states.items;
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

