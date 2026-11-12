//! 标准健康检查服务 `grpc.health.v1.Health`（零依赖，最简 protobuf 手写编解码）。
//!
//! 为什么做：这是 gRPC 生态的事实标准——k8s probe、Envoy/nginx 负载均衡、
//! `grpcurl` / `grpc-health-probe` 都直接调它。有了它，zgrpc 服务不必自己发明健康接口。
//!
//! 对应 proto（`grpc/health/v1/health.proto`）：
//! ```proto
//! message HealthCheckRequest  { string service = 1; }
//! message HealthCheckResponse { ServingStatus status = 1; }
//! enum ServingStatus { UNKNOWN = 0; SERVING = 1; NOT_SERVING = 2; SERVICE_UNKNOWN = 3; }
//! service Health {
//!   rpc Check(HealthCheckRequest) returns (HealthCheckResponse);
//!   rpc Watch(HealthCheckRequest) returns (stream HealthCheckResponse);
//! }
//! ```
//!
//! 用法：
//! ```zig
//! var checker = zgrpc.health.Checker{ .check = myCheck };
//! var rs = zgrpc.health.routes(&checker);
//! var server = zgrpc.grpc.Server{ .routes = &rs, .outer_ctx = undefined };
//! ```
//!
//! 限制（明确）：本库的 h2 是**同步单线程**模型，`Watch` 无法长期挂起推送；
//! 默认只推一次当前状态就结束流（对 k8s probe 足够），需要持续推送请用 `watch_pushes > 1`，
//! 但要清楚它会在间隔期间**占住整条连接**。

const std = @import("std");
const grpc = @import("grpc.zig");
const clock = @import("clock.zig");

/// `grpc.health.v1.HealthCheckResponse.ServingStatus`
pub const ServingStatus = enum(u8) {
    unknown = 0,
    serving = 1,
    not_serving = 2,
    service_unknown = 3,
};

/// 业务返回某个 service 的健康状态；`service` 是请求里的服务名（空串表示整体）。
pub const Checker = struct {
    ctx: ?*anyopaque = null,
    check: *const fn (ctx: ?*anyopaque, service: []const u8) ServingStatus,
};

pub const Options = struct {
    /// `Watch` 推送次数（1 = 推一次即结束，同步模型下的安全默认值）
    watch_pushes: u32 = 1,
    /// 多次推送之间的间隔
    watch_interval_ns: u64 = std.time.ns_per_s,
    /// 推给 `Watch` 的最大次数上限（防止业务传超大值卡住连接）
    watch_max: u32 = 60,
};

pub const check_path = "/grpc.health.v1.Health/Check";
pub const watch_path = "/grpc.health.v1.Health/Watch";

/// 健康检查服务：把 checker 与行为选项打包，作为路由上下文传给 handler。
pub const Service = struct {
    checker: *const Checker,
    opts: Options = .{},

    /// 生成 Check / Watch 两条路由。返回的数组是**值**，调用方需保证其生命周期覆盖 server
    /// （通常放在 `main` 的作用域里即可）。
    pub fn routes(self: *Service) [2]grpc.Route {
        return .{
            .{ .path = check_path, .handler = handleCheck, .ctx = @ptrCast(self) },
            .{ .path = watch_path, .handler = handleWatch, .ctx = @ptrCast(self) },
        };
    }
};

/// 便捷形式：只用默认选项的 `Service`。
///
/// 注意：必须由调用方持有 `Service` 的生命周期（它会被当作路由上下文），
/// 所以这里只提供「套一步」的写法，而不是内部藏一份静态变量。
pub fn routes(svc: *Service) [2]grpc.Route {
    return svc.routes();
}

fn handleCheck(ctx: ?*anyopaque, call: *grpc.Call, arena: std.mem.Allocator) anyerror!void {
    const svc: *Service = @ptrCast(@alignCast(ctx orelse {
        call.setStatus(.internal, "health: 未绑定 checker");
        return;
    }));
    const req = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "health: 空请求");
        return;
    };
    const service = try decodeService(arena, req.payload);
    const status = svc.checker.check(svc.checker.ctx, service);
    if (status == .service_unknown) {
        // 规范建议：未知服务名回 NOT_FOUND，而不是在响应里塞 SERVICE_UNKNOWN
        call.setStatus(.not_found, "health: 未注册的服务");
        return;
    }
    try call.sendMessage(try encodeResponse(arena, status));
}

fn handleWatch(ctx: ?*anyopaque, call: *grpc.Call, arena: std.mem.Allocator) anyerror!void {
    const svc: *Service = @ptrCast(@alignCast(ctx orelse {
        call.setStatus(.internal, "health: 未绑定 checker");
        return;
    }));
    const req = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "health: 空请求");
        return;
    };
    const service = try decodeService(arena, req.payload);

    // 同步模型下无法长挂：推 N 次（默认 1 次）后结束，变化才有意义
    const pushes = @min(@max(svc.opts.watch_pushes, 1), svc.opts.watch_max);
    var last: ?ServingStatus = null;
    var i: u32 = 0;
    while (i < pushes) : (i += 1) {
        const status = svc.checker.check(svc.checker.ctx, service);
        if (last == null or last.? != status) {
            try call.sendMessage(try encodeResponse(arena, status));
            last = status;
        }
        if (i + 1 < pushes) clock.sleepNs(svc.opts.watch_interval_ns);
        if (call.expired()) {
            call.setStatus(.deadline_exceeded, "health: Watch 超过 grpc-timeout");
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// 最简 protobuf：只覆盖上面两个 message（字段 1：string / enum）
// ---------------------------------------------------------------------------

fn writeVarint(out: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, value: u64) !void {
    var v = value;
    while (v >= 0x80) {
        try out.append(a, @as(u8, @truncate(v)) | 0x80);
        v >>= 7;
    }
    try out.append(a, @truncate(v));
}

fn readVarint(bytes: []const u8, pos: *usize) ?u64 {
    var shift: u6 = 0;
    var v: u64 = 0;
    while (pos.* < bytes.len and shift < 64) {
        const b = bytes[pos.*];
        pos.* += 1;
        v |= @as(u64, b & 0x7f) << shift;
        if (b & 0x80 == 0) return v;
        shift += 7;
    }
    return null;
}

/// 解 `HealthCheckRequest`：取字段 1（service 字符串）。
/// 其它字段按 wire type 跳过（宽容：未知字段不得导致失败）。
pub fn decodeService(a: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var pos: usize = 0;
    while (pos < bytes.len) {
        const key = readVarint(bytes, &pos) orelse return "";
        const field = key >> 3;
        const wire = key & 0x7;
        switch (wire) {
            0 => _ = readVarint(bytes, &pos) orelse return "",
            1 => pos += 8,
            5 => pos += 4,
            2 => {
                const len = readVarint(bytes, &pos) orelse return "";
                if (len > bytes.len - pos) return "";
                const val = bytes[pos .. pos + @as(usize, @intCast(len))];
                pos += @intCast(len);
                if (field == 1) return val;
            },
            else => return "",
        }
    }
    _ = a;
    return "";
}

/// 编 `HealthCheckResponse`：字段 1（enum，wire type 0）。
pub fn encodeResponse(a: std.mem.Allocator, status: ServingStatus) ![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    try out.append(a, 0x08); // field 1, wire type 0 (varint)
    try writeVarint(&out, a, @intFromEnum(status));
    return out.items;
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "health：encodeResponse / decodeService 往返" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const body = try encodeResponse(a, .serving);
    try testing.expectEqualSlices(u8, &.{ 0x08, 0x01 }, body);

    // 手工构造 HealthCheckRequest{ service = "echo.Echo" }
    var req: std.ArrayListUnmanaged(u8) = .empty;
    try req.append(a, 0x0a);
    try writeVarint(&req, a, "echo.Echo".len);
    try req.appendSlice(a, "echo.Echo");
    try testing.expectEqualStrings("echo.Echo", try decodeService(a, req.items));

    // 空请求 → 空 service（表示整体健康）
    try testing.expectEqualStrings("", try decodeService(a, &.{}));

    // 未知字段必须被跳过而不是报错：先塞一个 field 2 varint，再给 field 1
    var with_unknown: std.ArrayListUnmanaged(u8) = .empty;
    try with_unknown.append(a, 0x10); // field 2, wire type 0
    try writeVarint(&with_unknown, a, 42);
    try with_unknown.appendSlice(a, req.items);
    try testing.expectEqualStrings("echo.Echo", try decodeService(a, with_unknown.items));
}

fn testCheck(ctx: ?*anyopaque, service: []const u8) ServingStatus {
    _ = ctx;
    if (std.mem.eql(u8, service, "unknown.Service")) return .service_unknown;
    return .serving;
}

test "health：路由表路径与 checker 回调" {
    var checker = Checker{ .check = testCheck };
    var svc = Service{ .checker = &checker };
    const rs = svc.routes();
    try testing.expectEqualStrings("/grpc.health.v1.Health/Check", rs[0].path);
    try testing.expectEqualStrings("/grpc.health.v1.Health/Watch", rs[1].path);
    try testing.expectEqual(ServingStatus.serving, testCheck(null, ""));
    try testing.expectEqual(ServingStatus.service_unknown, testCheck(null, "unknown.Service"));
}
