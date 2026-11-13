//! `Proto` 语法树 → `google.protobuf.FileDescriptorProto` 二进制。
//!
//! 服务反射（`grpc.reflection.v1.ServerReflection`）返回的就是这份二进制：
//! grpcurl / ghz 拿到它之后就能在**没有 .proto 文件**的情况下知道有哪些服务、方法、消息字段。
//!
//! ## 字段号来源
//!
//! 全部来自 `google/protobuf/descriptor.proto`（protobuf 官方，20 余年未变）：
//!
//! | 消息 | 字段 |
//! |---|---|
//! | FileDescriptorProto | name=1, package=2, dependency=3, message_type=4, enum_type=5, service=6, syntax=12 |
//! | DescriptorProto | name=1, field=2, nested_type=3, enum_type=4 |
//! | FieldDescriptorProto | name=1, number=3, label=4, type=5, type_name=6, json_name=10 |
//! | EnumDescriptorProto | name=1, value=2；(EnumValueDescriptorProto) name=1, number=2 |
//! | ServiceDescriptorProto | name=1, method=2 |
//! | MethodDescriptorProto | name=1, input_type=2, output_type=3, client_streaming=5, server_streaming=6 |
//!
//! `type` 的枚举值同样来自官方（`FieldDescriptorProto.Type`），见 `fieldTypeOf`。

const std = @import("std");
const proto = @import("proto.zig");
const pb = @import("pb.zig");

/// `FieldDescriptorProto.Label`
const Label = struct {
    const optional: i32 = 1;
    const repeated: i32 = 3;
};

/// `FieldDescriptorProto.Type`（只列我们支持的标量 + message/enum）
/// 名字是否指向**本文件**定义的 enum。
/// protobuf 的 `type` 必须与 `type_name` 指向的真实定义一致：enum→14、message→11。
/// 写错会导致 grpcurl/protoreflect 报
/// `resolved "X", but it is not an message`。
///
/// `all` 是本次一起登记的全部文件：**跨包引用**（`common.Region`）的定义不在本文件里，
/// 不查它就会被当成 message（→ type=11），protoreflect 直接报错。
fn isEnumName(p: *const proto.Proto, all: []const proto.Proto, name: []const u8) bool {
    if (proto.findEnum(p, name) != null) return true; // 递归查找，嵌套定义也能识别
    for (all) |*o| {
        if (proto.findEnum(o, name) != null) return true;
    }
    return false;
}

fn fieldTypeOf(p: *const proto.Proto, all: []const proto.Proto, f: proto.Field) i32 {
    return switch (f.ftype) {
        .scalar => |s| scalarTypeCode(s),
        .named => |n| if (isEnumName(p, all, n)) 14 else 11,
        // map 字段在 wire/描述符层面就是一个 message（指向自动生成的 `XxxEntry`）
        .map => 11,
    };
}

fn scalarTypeCode(s: proto.Scalar) i32 {
    return switch (s) {
        .double => 1,
        .float => 2,
        .int64 => 3,
        .uint64 => 4,
        .int32 => 5,
        .fixed64 => 6,
        .fixed32 => 7,
        .bool => 8,
        .string => 9,
        .bytes => 12,
        .uint32 => 13,
        .sfixed32 => 15,
        .sfixed64 => 16,
        .sint32 => 17,
        .sint64 => 18,
    };
}

/// 完全限定名前缀：protobuf 约定类型引用必须以 `.` 开头。
fn fullyQualified(alloc: std.mem.Allocator, package: []const u8, name: []const u8) ![]const u8 {
    if (std.mem.lastIndexOfScalar(u8, name, '.') != null) {
        // 已经是 a.b.C 形式
        return std.fmt.allocPrint(alloc, ".{s}", .{name});
    }
    if (package.len == 0) return std.fmt.allocPrint(alloc, ".{s}", .{name});
    return std.fmt.allocPrint(alloc, ".{s}.{s}", .{ package, name });
}

fn writeField(w: *pb.Writer, alloc: std.mem.Allocator, package: []const u8, msg_name: []const u8, p: *const proto.Proto, all: []const proto.Proto, f: proto.Field, oneof_index: ?u32) !void {
    var inner = pb.Writer.init(alloc);
    defer inner.deinit();

    try inner.string(1, f.name); // name
    try inner.int32(3, @intCast(f.number)); // number
    try inner.int32(4, if (f.repeated) Label.repeated else Label.optional); // label
    try inner.int32(5, fieldTypeOf(p, all, f)); // type
    switch (f.ftype) {
        .scalar => {},
        .named => |n| {
            // 嵌套定义要用相对全名：`Nested` → `.pkg.Container.Nested`
            if (try proto.qualifiedName(alloc, p, n)) |q| {
                const abs = if (package.len > 0)
                    try std.fmt.allocPrint(alloc, ".{s}.{s}", .{ package, q })
                else
                    try std.fmt.allocPrint(alloc, ".{s}", .{q});
                try inner.string(6, abs); // type_name
            } else {
                try inner.string(6, try fullyQualified(alloc, package, n)); // type_name
            }
        },
        .map => {
            // map 字段在描述符里是 `repeated <Msg>.<Field>Entry`
            const entry = try mapEntryName(alloc, f.name);
            const type_name = if (package.len > 0)
                try std.fmt.allocPrint(alloc, ".{s}.{s}.{s}", .{ package, msg_name, entry })
            else
                try std.fmt.allocPrint(alloc, ".{s}.{s}", .{ msg_name, entry });
            try inner.string(6, type_name);
        },
    }
    // json_name：protobuf 的默认规则是下划线转驼峰，客户端不传也能推导，故省略。
    if (oneof_index) |oi| try inner.int32(9, @intCast(oi)); // FieldDescriptorProto.oneof_index = 9
    if (f.proto3_optional) try inner.boolField(17, true); // FieldDescriptorProto.proto3_optional = 17

    try w.message(2, inner.bytes()); // DescriptorProto.field = 2
}

/// `counts` → `CountsEntry`（protobuf 的命名规则：字段名转 PascalCase + `Entry`）。
fn mapEntryName(alloc: std.mem.Allocator, field_name: []const u8) ![]const u8 {
    const base = try proto.toPascalCase(alloc, field_name);
    return std.fmt.allocPrint(alloc, "{s}Entry", .{base});
}

/// `optional` 字段在「合成 oneof 区」里的序号（按字段出现顺序，从 0 开始）。
fn syntheticIndex(m: proto.Message, target: proto.Field) u32 {
    var i: u32 = 0;
    for (m.fields) |f| {
        if (!f.proto3_optional) continue;
        if (f.number == target.number) return i;
        i += 1;
    }
    return i;
}

/// `map<K,V>` 对应的嵌套类型：`key = 1` / `value = 2`，并打 `options.map_entry = true`。
/// 字段号依据官方 `descriptor.proto`：`DescriptorProto.nested_type = 3`、`options = 7`，
/// `MessageOptions.map_entry = 7`。
fn writeMapEntry(
    w: *pb.Writer,
    alloc: std.mem.Allocator,
    package: []const u8,
    p: *const proto.Proto,
    all: []const proto.Proto,
    mt: proto.MapType,
    field_name: []const u8,
) !void {
    var entry = pb.Writer.init(alloc);
    defer entry.deinit();

    try entry.string(1, try mapEntryName(alloc, field_name));

    {
        var kf = pb.Writer.init(alloc);
        defer kf.deinit();
        try kf.string(1, "key");
        try kf.int32(3, 1);
        try kf.int32(4, Label.optional);
        try kf.int32(5, scalarTypeCode(mt.key));
        try entry.message(2, kf.bytes());
    }
    {
        var vf = pb.Writer.init(alloc);
        defer vf.deinit();
        try vf.string(1, "value");
        try vf.int32(3, 2);
        try vf.int32(4, Label.optional);
        switch (mt.value.*) {
            .scalar => |s| try vf.int32(5, scalarTypeCode(s)),
            .named => |n| {
                try vf.int32(5, if (isEnumName(p, all, n)) 14 else 11);
                try vf.string(6, try fullyQualified(alloc, package, n));
            },
            // protobuf 不允许 map 的 value 又是 map（parser 已挡住，这里只是穷尽 switch）
            .map => return error.Unsupported,
        }
        try entry.message(2, vf.bytes());
    }
    {
        var opts = pb.Writer.init(alloc);
        defer opts.deinit();
        try opts.boolField(7, true);
        try entry.message(7, opts.bytes()); // DescriptorProto.options = 7
    }

    try w.message(3, entry.bytes()); // DescriptorProto.nested_type = 3
}

fn writeMessage(w: *pb.Writer, alloc: std.mem.Allocator, package: []const u8, p: *const proto.Proto, all: []const proto.Proto, m: proto.Message, outer: []const u8) !void {
    try writeMessageAs(w, alloc, package, p, all, m, outer, 4); // 顶层：FileDescriptorProto.message_type = 4
}

/// `field_num`：顶层用 4，嵌套用 3（`DescriptorProto.nested_type`）。
/// 写错会让 protoreflect 把嵌套 message 当成 enum 解析。
fn writeMessageAs(
    w: *pb.Writer,
    alloc: std.mem.Allocator,
    package: []const u8,
    p: *const proto.Proto,
    all: []const proto.Proto,
    m: proto.Message,
    outer: []const u8,
    field_num: u32,
) !void {
    var inner = pb.Writer.init(alloc);
    defer inner.deinit();

    // 限定名（嵌套时是 `Outer.Inner`）：map Entry 与嵌套类型引用都要用它
    const full_name = if (outer.len == 0)
        m.name
    else
        try std.fmt.allocPrint(alloc, "{s}.{s}", .{ outer, m.name });

    try inner.string(1, m.name);
    // 先排真实 oneof，再排 `optional` 字段的合成 oneof（索引必须与实际 oneof_decl 顺序一致）
    for (m.fields) |f| {
        if (f.proto3_optional) {
            try writeField(&inner, alloc, package, full_name, p, all, f, @intCast(m.oneofs.len + syntheticIndex(m, f)));
        } else {
            try writeField(&inner, alloc, package, full_name, p, all, f, f.oneof_index);
        }
    }
    // 嵌套定义（message / enum）：`DescriptorProto.nested_type = 3`、`enum_type = 4`
    for (m.nested) |n| try writeMessageAs(&inner, alloc, package, p, all, n, full_name, 3);
    for (m.enums) |e| try writeEnumAs(&inner, alloc, e, 4);
    // map 字段要额外生成嵌套 Entry 类型（`nested_type`）
    for (m.fields) |f| {
        switch (f.ftype) {
            .map => |mt| try writeMapEntry(&inner, alloc, package, p, all, mt, f.name),
            else => {},
        }
    }
    // oneof 声明：`DescriptorProto.oneof_decl = 8`，`OneofDescriptorProto.name = 1`
    for (m.oneofs) |oname| {
        var od = pb.Writer.init(alloc);
        defer od.deinit();
        try od.string(1, oname);
        try inner.message(8, od.bytes());
    }
    // proto3 `optional` 的合成 oneof（名字是 `_<字段名>`，protoc 就是这么干的）
    for (m.fields) |f| {
        if (!f.proto3_optional) continue;
        var od = pb.Writer.init(alloc);
        defer od.deinit();
        try od.string(1, try std.fmt.allocPrint(alloc, "_{s}", .{f.name}));
        try inner.message(8, od.bytes());
    }

    try w.message(field_num, inner.bytes());
}

fn writeEnum(w: *pb.Writer, alloc: std.mem.Allocator, e: proto.EnumDef) !void {
    try writeEnumAs(w, alloc, e, 5); // 顶层：FileDescriptorProto.enum_type = 5
}

/// 把 enum 写进外层容器：顶层用 5，嵌套用 4（`DescriptorProto.enum_type`）。
fn writeEnumAs(w: *pb.Writer, alloc: std.mem.Allocator, e: proto.EnumDef, field_num: u32) !void {
    var inner = pb.Writer.init(alloc);
    defer inner.deinit();

    try inner.string(1, e.name);
    for (e.values) |v| {
        var iv = pb.Writer.init(alloc);
        defer iv.deinit();
        try iv.string(1, v.name);
        try iv.int32(2, v.value);
        try inner.message(2, iv.bytes()); // EnumDescriptorProto.value = 2
    }

    try w.message(field_num, inner.bytes());
}

fn writeService(w: *pb.Writer, alloc: std.mem.Allocator, package: []const u8, s: proto.Service) !void {
    var inner = pb.Writer.init(alloc);
    defer inner.deinit();

    try inner.string(1, s.name);
    for (s.rpcs) |r| {
        var im = pb.Writer.init(alloc);
        defer im.deinit();
        try im.string(1, r.name);
        try im.string(2, try fullyQualified(alloc, package, r.req)); // input_type
        try im.string(3, try fullyQualified(alloc, package, r.rsp)); // output_type
        if (r.client_stream) try im.boolField(5, true); // client_streaming
        if (r.server_stream) try im.boolField(6, true); // server_streaming
        try inner.message(2, im.bytes()); // ServiceDescriptorProto.method = 2
    }

    try w.message(6, inner.bytes()); // FileDescriptorProto.service = 6
}

/// 把语法树编码成 `FileDescriptorProto` 字节。输出在 `alloc` 上，调用方负责释放。
///
/// `file_name` 就是客户端后面用来 `file_by_filename` 查询的名字（通常用 `.proto` 的文件名或任意标识）。
pub fn encodeFile(alloc: std.mem.Allocator, file_name: []const u8, p: proto.Proto) ![]const u8 {
    return encodeFileWith(alloc, file_name, p, &.{});
}

/// 带跨文件上下文：`all` 是本次一起登记的全部文件。
/// 跨包引用（`common.Region`）要靠它判定 enum / message，否则会被当成 message 编码。
pub fn encodeFileWith(
    alloc: std.mem.Allocator,
    file_name: []const u8,
    p: proto.Proto,
    all: []const proto.Proto,
) ![]const u8 {
    var w = pb.Writer.init(alloc);
    defer w.deinit();

    try w.string(1, file_name); // name
    if (p.package.len > 0) try w.string(2, p.package); // package
    for (p.imports) |imp| try w.string(3, imp.path); // dependency（按 import 原样路径）
    for (p.messages) |m| try writeMessage(&w, alloc, p.package, &p, all, m, "");
    for (p.enums) |e| try writeEnum(&w, alloc, e);
    for (p.services) |s| try writeService(&w, alloc, p.package, s);
    try w.string(12, "proto3"); // syntax

    return try alloc.dupe(u8, w.bytes());
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

fn findField(r: *pb.Reader, number: u32) ?pb.Field {
    var rr = r.*;
    while (rr.next()) |f| {
        if (f.number == number) return f;
    }
    return null;
}

test "descriptor：编码出合法的 FileDescriptorProto 主干" {
    const src =
        \\syntax = "proto3";
        \\package echo;
        \\message Req { int32 id = 1; string name = 2; }
        \\message Res { bytes data = 1; }
        \\service Echo { rpc Say(Req) returns (Res); }
        \\enum Kind { A = 0; }
    ;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const parsed = try proto.parse(a, src);
    const bytes = try encodeFile(a, "echo.proto", parsed);

    var r = pb.Reader{ .data = bytes };
    var got_name: []const u8 = "";
    var got_package: []const u8 = "";
    var got_syntax: []const u8 = "";
    var service_count: usize = 0;
    var message_count: usize = 0;
    var enum_count: usize = 0;

    while (r.next()) |f| switch (f.number) {
        1 => got_name = f.asBytes(),
        2 => got_package = f.asBytes(),
        4 => message_count += 1,
        5 => enum_count += 1,
        6 => service_count += 1,
        12 => got_syntax = f.asBytes(),
        else => {},
    };

    try testing.expectEqualStrings("echo.proto", got_name);
    try testing.expectEqualStrings("echo", got_package);
    try testing.expectEqualStrings("proto3", got_syntax);
    try testing.expectEqual(@as(usize, 2), message_count);
    try testing.expectEqual(@as(usize, 1), enum_count);
    try testing.expectEqual(@as(usize, 1), service_count);
}

test "descriptor：ServiceDescriptorProto 的 method 字段正确（含流式标记）" {
    const src =
        \\syntax = "proto3";
        \\package demo;
        \\message Req { int32 a = 1; }
        \\service Api { rpc Bidi(stream Req) returns (stream Req); }
    ;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = try encodeFile(a, "demo.proto", try proto.parse(a, src));

    // 找到 service(6)
    var r = pb.Reader{ .data = bytes };
    var svc_raw: []const u8 = "";
    while (r.next()) |f| {
        if (f.number == 6) svc_raw = f.asBytes();
    }
    try testing.expect(svc_raw.len > 0);

    var sr = pb.Reader{ .data = svc_raw };
    var svc_name: []const u8 = "";
    var method_raw: []const u8 = "";
    while (sr.next()) |f| switch (f.number) {
        1 => svc_name = f.asBytes(),
        2 => method_raw = f.asBytes(),
        else => {},
    };
    try testing.expectEqualStrings("Api", svc_name);

    var mr = pb.Reader{ .data = method_raw };
    var mname: []const u8 = "";
    var in_type: []const u8 = "";
    var out_type: []const u8 = "";
    var client_streaming = false;
    var server_streaming = false;
    while (mr.next()) |f| switch (f.number) {
        1 => mname = f.asBytes(),
        2 => in_type = f.asBytes(),
        3 => out_type = f.asBytes(),
        5 => client_streaming = f.asBool(),
        6 => server_streaming = f.asBool(),
        else => {},
    };
    try testing.expectEqualStrings("Bidi", mname);
    try testing.expectEqualStrings(".demo.Req", in_type);
    try testing.expectEqualStrings(".demo.Req", out_type);
    try testing.expect(client_streaming);
    try testing.expect(server_streaming);
}
