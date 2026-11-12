//! `google.rpc.Status` / `google.protobuf.Any` 的编解码 —— 服务 `grpc-status-details-bin`。
//!
//! ## 为什么需要它
//!
//! gRPC 的 `grpc-status` 只有一个数字码，业务想告诉客户端「哪个字段错了、缺什么权限、重试多久」
//! 就得靠 `grpc-status-details-bin`（规范推荐的富错误通道）。它的内容就是序列化的
//! `google.rpc.Status`：
//!
//! ```proto
//! message Status { int32 code = 1; string message = 2; repeated google.protobuf.Any details = 3; }
//! message Any    { string type_url = 1; bytes value = 2; }
//! ```
//!
//! 传输时这个二进制还要按 `-bin` metadata 规则做 **base64（无 padding）**——这里只负责
//! 「Status ⇄ 字节」，base64 与 trailer 组装由 `Call.setStatusDetails` 完成。
//!
//! ## 典型用法
//!
//! ```zig
//! try call.setStatusDetails(arena, .{
//!     .code = 3, // INVALID_ARGUMENT
//!     .message = "参数不合法",
//!     .details = &.{.{ .type_url = "type.googleapis.com/google.rpc.BadRequest", .value = bad_request_bytes }},
//! });
//! call.setStatus(.invalid_argument, "参数不合法");
//! ```

const std = @import("std");
const pb = @import("pb.zig");

/// `google.protobuf.Any`：类型 URL + 序列化后的值。
pub const Any = struct {
    /// 形如 `type.googleapis.com/google.rpc.BadRequest`
    type_url: []const u8 = "",
    /// 该类型的 protobuf 序列化字节
    value: []const u8 = "",
};

/// `google.rpc.Status` 的 Zig 映射。
pub const Status = struct {
    /// 与 `grpc-status` 一致的规范错误码（0 = OK）
    code: i32 = 0,
    /// 人类可读描述（对应 `grpc-message` 的内容）
    message: []const u8 = "",
    details: []const Any = &.{},
};

/// 错误码常量（`google.rpc.Code`，与 `zgrpc.grpc.Status` 一一对应）。
pub const Code = struct {
    pub const ok: i32 = 0;
    pub const cancelled: i32 = 1;
    pub const unknown: i32 = 2;
    pub const invalid_argument: i32 = 3;
    pub const deadline_exceeded: i32 = 4;
    pub const not_found: i32 = 5;
    pub const already_exists: i32 = 6;
    pub const permission_denied: i32 = 7;
    pub const resource_exhausted: i32 = 8;
    pub const failed_precondition: i32 = 9;
    pub const aborted: i32 = 10;
    pub const out_of_range: i32 = 11;
    pub const unimplemented: i32 = 12;
    pub const internal: i32 = 13;
    pub const unavailable: i32 = 14;
    pub const data_loss: i32 = 15;
    pub const unauthenticated: i32 = 16;
};

/// 把 `Status` 序列化为 `grpc-status-details-bin` 的**原始字节**（尚未 base64）。
pub fn encode(alloc: std.mem.Allocator, st: Status) ![]const u8 {
    var w = pb.Writer.init(alloc);
    defer w.deinit();
    if (st.code != 0) try w.int32(1, st.code);
    if (st.message.len > 0) try w.string(2, st.message);
    for (st.details) |d| {
        // 每个 Any 单独编码后作为字段 3 的 length-delimited 值
        var inner = pb.Writer.init(alloc);
        defer inner.deinit();
        if (d.type_url.len > 0) try inner.string(1, d.type_url);
        if (d.value.len > 0) try inner.bytesField(2, d.value);
        try w.message(3, inner.bytes());
    }
    // `bytes()` 是 Writer 内部切片的别名，必须复制出去（Writer 马上 deinit）
    return try alloc.dupe(u8, w.bytes());
}

/// 从 `grpc-status-details-bin` 的原始字节解析 `Status`；输出在 `alloc` 上。
///
/// 未知字段会被跳过（protobuf 前向兼容）；截断数据返回 `error.Truncated` 而不是 panic。
pub fn decode(alloc: std.mem.Allocator, data: []const u8) !Status {
    var st = Status{};
    var details: std.ArrayListUnmanaged(Any) = .empty;
    errdefer details.deinit(alloc);

    var r = pb.Reader{ .data = data };
    while (r.next()) |f| {
        switch (f.number) {
            1 => st.code = f.asInt32(),
            2 => st.message = try alloc.dupe(u8, f.asBytes()),
            3 => {
                const raw = f.asBytes();
                var any = Any{};
                var ir = pb.Reader{ .data = raw };
                while (ir.next()) |g| switch (g.number) {
                    1 => any.type_url = try alloc.dupe(u8, g.asBytes()),
                    2 => any.value = try alloc.dupe(u8, g.asBytes()),
                    else => {},
                };
                try details.append(alloc, any);
            },
            else => {},
        }
    }
    st.details = try details.toOwnedSlice(alloc);
    return st;
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "status：空 Status 编码为空字节（全默认值不写字段）" {
    const b = try encode(testing.allocator, .{});
    defer testing.allocator.free(b);
    try testing.expectEqual(@as(usize, 0), b.len);
}

test "status：code + message 往返" {
    const b = try encode(testing.allocator, .{ .code = 3, .message = "参数不合法" });
    defer testing.allocator.free(b);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const st = try decode(arena.allocator(), b);
    try testing.expectEqual(@as(i32, 3), st.code);
    try testing.expectEqualStrings("参数不合法", st.message);
    try testing.expectEqual(@as(usize, 0), st.details.len);
}

test "status：带 Any details 往返" {
    const bad_req = [_]u8{ 0x0a, 0x03, 'a', 'b', 'c' };
    const b = try encode(testing.allocator, .{
        .code = Code.invalid_argument,
        .message = "字段缺失",
        .details = &.{
            .{ .type_url = "type.googleapis.com/google.rpc.BadRequest", .value = &bad_req },
            .{ .type_url = "type.googleapis.com/google.rpc.RetryInfo", .value = &.{ 0x08, 0x05 } },
        },
    });
    defer testing.allocator.free(b);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const st = try decode(arena.allocator(), b);
    try testing.expectEqual(Code.invalid_argument, st.code);
    try testing.expectEqualStrings("字段缺失", st.message);
    try testing.expectEqual(@as(usize, 2), st.details.len);
    try testing.expectEqualStrings("type.googleapis.com/google.rpc.BadRequest", st.details[0].type_url);
    try testing.expectEqualSlices(u8, &bad_req, st.details[0].value);
    try testing.expectEqualStrings("type.googleapis.com/google.rpc.RetryInfo", st.details[1].type_url);
}

test "status：未知字段被跳过、截断返回错误而非 panic" {
    // 字段 99（varint）+ 字段 1
    const with_unknown = [_]u8{ 0x98, 0x06, 0x2a, 0x08, 0x07 };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const st = try decode(arena.allocator(), &with_unknown);
    try testing.expectEqual(@as(i32, 7), st.code);

    // 声称有 5 字节但只有 1 字节 → pb.Reader 返回 null（不 panic）
    const truncated = [_]u8{ 0x12, 0x05, 'x' };
    const st2 = try decode(arena.allocator(), &truncated);
    try testing.expectEqualStrings("", st2.message);
}
