//! gRPC **服务反射**（`grpc.reflection.v1.ServerReflection`），同时兼容 `v1alpha`。
//!
//! 有了它，`grpcurl` / `ghz` 这类工具**不需要 `.proto` 文件**就能自省服务：
//!
//! ```bash
//! grpcurl -plaintext localhost:18051 list
//! grpcurl -plaintext localhost:18051 describe echo.Echo
//! grpcurl -plaintext -d '{"text":"hi"}' localhost:18051 echo.Echo/Say
//! ```
//!
//! ## 协议要点（两个版本的消息编码完全一致，只有 service 全名不同）
//!
//! ```proto
//! service ServerReflection { rpc ServerReflectionInfo(stream ServerReflectionRequest) returns (stream ServerReflectionResponse); }
//!
//! message ServerReflectionRequest {
//!   string host = 1;
//!   oneof message_request {
//!     string file_by_filename = 3;
//!     string file_containing_symbol = 4;
//!     ExtensionRequest file_containing_extension = 5;
//!     string all_extension_numbers_of_type = 6;
//!     string list_services = 7;          // 值无意义，只看字段出现
//!   }
//! }
//! message ServerReflectionResponse {
//!   string valid_host = 1;
//!   ServerReflectionRequest original_request = 2;
//!   oneof message_response {
//!     FileDescriptorResponse file_descriptor_response = 4;
//!     ExtensionNumberResponse all_extension_numbers_response = 5;
//!     ListServiceResponse list_services_response = 6;
//!     ErrorResponse error_response = 7;
//!   }
//! }
//! message FileDescriptorResponse { repeated bytes file_descriptor_proto = 1; }
//! message ListServiceResponse    { repeated ServiceResponse service = 1; }
//! message ServiceResponse        { string name = 1; }
//! message ErrorResponse          { int32 error_code = 1; string error_message = 2; }
//! ```
//!
//! ## 用法
//!
//! ```zig
//! var refl = try zgrpc.reflection.Service.init(alloc, &.{
//!     .{ .file_name = "echo.proto", .source = @embedFile("echo.proto") },
//! });
//! defer refl.deinit();
//!
//! var stream_routes = [_]zgrpc.grpc.StreamRoute{
//!     refl.streamRoute(),        // grpc.reflection.v1
//!     refl.streamRouteV1Alpha(), // grpc.reflection.v1alpha（老工具）
//! };
//! ```
//!
//! 只实现了 **list_services / file_by_filename / file_containing_symbol** 三种请求；
//! 扩展（extension）相关请求返回 `UNIMPLEMENTED` 错误响应（proto3 不使用扩展）。

const std = @import("std");
const grpc = @import("grpc.zig");
const proto = @import("proto.zig");
const proto_loader = @import("proto_loader.zig");
const descriptor = @import("descriptor.zig");
const pb = @import("pb.zig");

pub const path_v1 = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo";
pub const path_v1alpha = "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo";

/// 一份 `.proto` 的登记项。
const Entry = struct {
    file_name: []const u8,
    /// `FileDescriptorProto` 二进制
    bytes: []const u8,
    /// 完全限定符号（不带前导点）：服务名、服务名.方法名、消息名、枚举名
    symbols: []const []const u8,
    /// 只含服务全名（供 list_services 使用）
    service_names: []const []const u8,
};

pub const Source = struct {
    file_name: []const u8,
    source: []const u8,
};

pub const Service = struct {
    /// 所有登记数据都从内部 arena 分配：`deinit` 一次释放，不可能漏。
    /// （反射数据是**只读、长期存活**的，arena 正合适。）
    arena: std.heap.ArenaAllocator,
    entries: []Entry,

    pub fn init(parent: std.mem.Allocator, sources: []const Source) !Service {
        var arena = std.heap.ArenaAllocator.init(parent);
        errdefer arena.deinit();
        const a = arena.allocator();

        const entries = try a.alloc(Entry, sources.len);
        for (sources, 0..) |s, i| {
            const parsed = try proto.parse(a, s.source);
            entries[i] = .{
                .file_name = try a.dupe(u8, s.file_name),
                .bytes = try descriptor.encodeFile(a, s.file_name, parsed),
                .symbols = try collectSymbols(a, parsed),
                .service_names = try collectServiceNames(a, parsed),
            };
        }
        return .{ .arena = arena, .entries = entries };
    }

    /// 从 `proto_loader` 的结果建反射服务（**支持 import**：每个文件一份 descriptor）。
    ///
    /// `loaded.files` 已按依赖拓扑排序（被依赖的在前），客户端 `file_by_filename`
    /// 拉依赖时能逐层拿全。
    pub fn fromLoaded(parent: std.mem.Allocator, loaded: proto_loader.Loaded) !Service {
        var arena = std.heap.ArenaAllocator.init(parent);
        errdefer arena.deinit();
        const a = arena.allocator();

        // 跨包引用的 enum / message 判定要能查到同批登记的其他文件
        const all = try a.alloc(proto.Proto, loaded.files.len);
        for (loaded.files, 0..) |f, i| all[i] = f.proto;

        const entries = try a.alloc(Entry, loaded.files.len);
        for (loaded.files, 0..) |f, i| {
            entries[i] = .{
                .file_name = try a.dupe(u8, f.name),
                .bytes = try descriptor.encodeFileWith(a, f.name, f.proto, all),
                .symbols = try collectSymbols(a, f.proto),
                .service_names = try collectServiceNames(a, f.proto),
            };
        }
        return .{ .arena = arena, .entries = entries };
    }

    pub fn deinit(self: *Service) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// 内部数据使用的分配器（`onMessage` 用它做临时分配）。
    fn scratch(self: *Service) std.mem.Allocator {
        return self.arena.allocator();
    }

    /// `grpc.reflection.v1` 的流式路由（带 per-route ctx，可与业务路由共存）。
    pub fn streamRoute(self: *Service) grpc.StreamRoute {
        return .{ .path = path_v1, .on_message = onMessage, .ctx = @ptrCast(self) };
    }

    /// `grpc.reflection.v1alpha` 的流式路由（消息编码与 v1 相同，仅为兼容老工具）。
    pub fn streamRouteV1Alpha(self: *Service) grpc.StreamRoute {
        return .{ .path = path_v1alpha, .on_message = onMessage, .ctx = @ptrCast(self) };
    }

    /// 供 `Server.outer_ctx` 之外使用：直接拿到上下文指针。
    pub fn context(self: *Service) *anyopaque {
        return @ptrCast(self);
    }
};

/// 递归收集嵌套 message / enum 的符号（`pkg.Outer.Nested`）。
fn collectNestedSymbols(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]const u8),
    prefix: []const u8,
    m: proto.Message,
) !void {
    const full = if (prefix.len == 0)
        try alloc.dupe(u8, m.name)
    else
        try std.fmt.allocPrint(alloc, "{s}.{s}", .{ prefix, m.name });
    try list.append(alloc, full);
    for (m.enums) |e| try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}.{s}", .{ full, e.name }));
    for (m.nested) |n| try collectNestedSymbols(alloc, list, full, n);
}

fn collectSymbols(alloc: std.mem.Allocator, p: proto.Proto) ![]const []const u8 {
    var list: std.ArrayListUnmanaged([]const u8) = .empty;
    const prefix = if (p.package.len > 0) try std.fmt.allocPrint(alloc, "{s}.", .{p.package}) else "";

    for (p.messages) |m| try collectNestedSymbols(alloc, &list, prefix[0..prefix.len -| 1], m);
    for (p.enums) |e| try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}{s}", .{ prefix, e.name }));
    for (p.services) |s| {
        try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}{s}", .{ prefix, s.name }));
        for (s.rpcs) |r| {
            try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}{s}.{s}", .{ prefix, s.name, r.name }));
        }
    }
    return list.toOwnedSlice(alloc);
}

fn collectServiceNames(alloc: std.mem.Allocator, p: proto.Proto) ![]const []const u8 {
    var list: std.ArrayListUnmanaged([]const u8) = .empty;
    for (p.services) |s| {
        if (p.package.len > 0) {
            try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}.{s}", .{ p.package, s.name }));
        } else {
            try list.append(alloc, try alloc.dupe(u8, s.name));
        }
    }
    return list.toOwnedSlice(alloc);
}

fn findByName(self: *Service, name: []const u8) ?*const Entry {
    for (self.entries) |*e| {
        if (std.mem.eql(u8, e.file_name, name)) return e;
    }
    return null;
}

fn findBySymbol(self: *Service, symbol_in: []const u8) ?*const Entry {
    // protobuf 的符号可以带前导点（`.pkg.Svc`），我们登记时不带
    var symbol = symbol_in;
    while (symbol.len > 0 and symbol[0] == '.') symbol = symbol[1..];
    for (self.entries) |*e| {
        for (e.symbols) |s| {
            if (std.mem.eql(u8, s, symbol)) return e;
        }
    }
    return null;
}

fn onMessage(ctx: *anyopaque, call: *grpc.Call, msg: []const u8) anyerror!void {
    const self: *Service = @ptrCast(@alignCast(ctx));

    var arena_state = std.heap.ArenaAllocator.init(self.scratch());
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const Request = struct {
        kind: enum { none, by_filename, by_symbol, list_services } = .none,
        arg: []const u8 = "",
    };
    var req = Request{};
    var r = pb.Reader{ .data = msg };
    while (r.next()) |f| switch (f.number) {
        3 => {
            req.kind = .by_filename;
            req.arg = f.asBytes();
        },
        4 => {
            req.kind = .by_symbol;
            req.arg = f.asBytes();
        },
        7 => req.kind = .list_services,
        else => {},
    };

    var w = pb.Writer.init(a);

    switch (req.kind) {
        .by_filename => {
            if (findByName(self, req.arg)) |e| {
                try writeFileDescriptorResponse(&w, a, &.{e.bytes});
            } else {
                try writeError(&w, a, grpc.Status.not_found, "未找到该文件");
            }
        },
        .by_symbol => {
            if (findBySymbol(self, req.arg)) |e| {
                try writeFileDescriptorResponse(&w, a, &.{e.bytes});
            } else {
                try writeError(&w, a, grpc.Status.not_found, "未找到该符号");
            }
        },
        .list_services => {
            var resp = pb.Writer.init(a);
            defer resp.deinit();
            for (self.entries) |e| {
                for (e.service_names) |sym| {
                    var sr = pb.Writer.init(a);
                    defer sr.deinit();
                    try sr.string(1, sym);
                    try resp.message(1, sr.bytes()); // ListServiceResponse.service
                }
            }
            try w.message(6, resp.bytes()); // list_services_response
        },
        .none => try writeError(&w, a, grpc.Status.unimplemented, "不支持的反射请求"),
    }

    try call.sendMessage(w.bytes());
}

fn writeFileDescriptorResponse(w: *pb.Writer, a: std.mem.Allocator, files: []const []const u8) !void {
    var inner = pb.Writer.init(a);
    defer inner.deinit();
    for (files) |b| try inner.bytesField(1, b); // repeated bytes file_descriptor_proto
    try w.message(4, inner.bytes()); // file_descriptor_response
}

fn writeError(w: *pb.Writer, a: std.mem.Allocator, code: grpc.Status, msg: []const u8) !void {
    var inner = pb.Writer.init(a);
    defer inner.deinit();
    try inner.int32(1, @intCast(@backingInt(code)));
    try inner.string(2, msg);
    try w.message(7, inner.bytes()); // error_response
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "reflection：init 后能按符号与文件名索引" {
    const src =
        \\syntax = "proto3";
        \\package echo;
        \\message Req { int32 id = 1; }
        \\service Echo { rpc Say(Req) returns (Req); }
    ;
    var refl = try Service.init(testing.allocator, &.{.{ .file_name = "echo.proto", .source = src }});
    defer refl.deinit();

    try testing.expectEqual(@as(usize, 1), refl.entries.len);
    try testing.expect(findByName(&refl, "echo.proto") != null);
    try testing.expect(findByName(&refl, "nope.proto") == null);
    try testing.expect(findBySymbol(&refl, ".echo.Echo") != null);
    try testing.expect(findBySymbol(&refl, "echo.Echo.Say") != null);
    try testing.expect(findBySymbol(&refl, ".echo.Req") != null);
    try testing.expect(findBySymbol(&refl, ".echo.Nope") == null);
}
