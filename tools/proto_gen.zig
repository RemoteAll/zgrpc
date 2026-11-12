//! zgrpc 的 proto3 **子集**代码生成器：把 `.proto` 编译成 Zig 结构体 + 编解码 + 路由常量。
//!
//! 运行：
//! ```text
//! zig run tools/proto_gen.zig -- examples/echo.proto examples/echo.pb.zig
//! ```
//!
//! 为什么只做子集：完整 protobuf 生态（import / well-known types / map / oneof / Any / 反射）
//! 体量远超一个传输层库该背的范围。这里覆盖「服务间自控两端」的常见写法，
//! 生成结果直接跑在 `zgrpc.pb` 上，零依赖。
//!
//! 支持的语法：
//! - `syntax = "proto3";`、`package a.b.c;`
//! - `message Name { ... }`：标量字段、`string` / `bytes`、`enum` 字段、嵌套 message 字段、`repeated` 标量
//! - `enum Name { A = 0; ... }`
//! - `service Name { rpc M(Req) returns (Res); rpc S(stream Req) returns (stream Res); }`
//!
//! 明确不支持（遇到会报错并指出行号，不会静默忽略）：`import`、`map<...>`、`oneof`、`optional`、
//! `reserved`、`extend`、`option`、`group`、`repeated` 非标量（含 message）。

const std = @import("std");

const zproto = @import("zgrpc").proto;

// 解析器与语法树统一在 `src/proto.zig`（`zgrpc.proto`）：codegen 与服务反射共用同一份实现，
// 避免两处漂移。这里只做「语法树 → Zig 代码」。
const Scalar = zproto.Scalar;
const Field = zproto.Field;
const Message = zproto.Message;
const EnumDef = zproto.EnumDef;
const Service = zproto.Service;
const Rpc = zproto.Rpc;
const Proto = zproto.Proto;
const scalarFromName = zproto.scalarFromName;
const lastSegment = zproto.lastSegment;
const proto_toPascalCase = zproto.toPascalCase;
// ---------------------------------------------------------------------------
// 代码生成
// ---------------------------------------------------------------------------

/// `counts` → `CountsEntry`（与 descriptor 里的嵌套类型同名）
fn mapEntryTypeName(a: std.mem.Allocator, field_name: []const u8) ![]const u8 {
    const base = try proto_toPascalCase(a, field_name);
    return std.fmt.allocPrint(a, "{s}Entry", .{base});
}

fn mapKeyType(a: std.mem.Allocator, mt: zproto.MapType) ![]const u8 {
    return elemType(a, .{ .name = "", .number = 0, .ftype = .{ .scalar = mt.key }, .repeated = false });
}

fn mapValueType(a: std.mem.Allocator, v: zproto.FieldType) ![]const u8 {
    return elemType(a, .{ .name = "", .number = 0, .ftype = v, .repeated = false });
}

/// 默认值：map 为空片。
fn mapDefault() []const u8 {
    return "&.{}";
}

/// map 的 key/value 局部变量初值（解码时用）
fn scalarZero(s: Scalar) []const u8 {
    return switch (s) {
        .bool => "false",
        .string, .bytes => "\"\"",
        else => "0",
    };
}

fn namedZero(proto: Proto, n: []const u8) []const u8 {
    // enum 用第一个枚举值；message 用空字面量（`.{}`）
    return if (isEnum(proto, n)) "undefined" else ".{}";
}

/// `scalarReadExpr` 的表达式里写死了接收者 `f.`；map 条目内部遍历用的是 `ef`，
/// 所以要把前缀换掉（不做字符串替换就只能把整个 switch 再写一遍）。
fn retarget(a: std.mem.Allocator, expr: []const u8, recv: []const u8) ![]const u8 {
    const tail = expr[2..]; // 跳过 `f.`
    return std.fmt.allocPrint(a, "{s}.{s}", .{ recv, tail });
}

/// 单个元素（非 repeated）的 Zig 类型写法。
fn elemType(a: std.mem.Allocator, f: Field) ![]const u8 {
    return switch (f.ftype) {
        .scalar => |s| switch (s) {
            .double => "f64",
            .float => "f32",
            .int32, .sint32, .sfixed32 => "i32",
            .int64, .sint64, .sfixed64 => "i64",
            .uint32, .fixed32 => "u32",
            .uint64, .fixed64 => "u64",
            .bool => "bool",
            .string, .bytes => "[]const u8",
        },
        // 元素是**值**类型（不是 `?T`）
        .named => |n| try std.fmt.allocPrint(a, "{s}", .{lastSegment(n)}),
        // map 只在字段层处理；作为「元素」不可能出现（parser 已挡），这里只是穷尽 switch
        .map => unreachable,
    };
}

/// 生成字段的 Zig 类型写法（named 类型需要拼接，故用分配器）。
fn zigType(a: std.mem.Allocator, proto: Proto, f: Field) ![]const u8 {
    _ = proto; // named 类型只取末段名字，不需要查表
    if (f.repeated) {
        switch (f.ftype) {
            .map => return std.fmt.allocPrint(a, "[]const {s}", .{try mapEntryTypeName(a, f.name)}),
            else => return std.fmt.allocPrint(a, "[]const {s}", .{try elemType(a, f)}),
        }
    }
    return switch (f.ftype) {
        .scalar => elemType(a, f),
        .named => |n| try std.fmt.allocPrint(a, "?{s}", .{lastSegment(n)}),
        .map => std.fmt.allocPrint(a, "[]const {s}", .{try mapEntryTypeName(a, f.name)}),
    };
}

/// repeated 数字/bool 的 packed 编码函数名。
/// 注意 wire 类型不同：float/fixed32/sfixed32 是 4 字节定长，double/fixed64/sfixed64 是 8 字节定长，
/// sint32/sint64 用 zigzag，其余是普通 varint。
fn packedFn(s: Scalar) []const u8 {
    return switch (s) {
        .sint32, .sint64 => "packedSint",
        .float, .fixed32, .sfixed32 => "packedFixed32",
        .double, .fixed64, .sfixed64 => "packedFixed64",
        else => "packedVarint",
    };
}

/// repeated 数字/bool 解码 packed 载荷时用的 `Field` 方法名。
fn packedReadFn(s: Scalar) []const u8 {
    return switch (s) {
        .sint32, .sint64 => "packedSints",
        .float, .fixed32, .sfixed32 => "packedFixed32s",
        .double, .fixed64, .sfixed64 => "packedFixed64s",
        else => "packedVarints",
    };
}

fn isEnum(proto: Proto, name: []const u8) bool {
    const short = lastSegment(name);
    for (proto.enums) |e| {
        if (std.mem.eql(u8, e.name, short)) return true;
    }
    return false;
}

fn isMessage(proto: Proto, name: []const u8) bool {
    const short = lastSegment(name);
    for (proto.messages) |m| {
        if (std.mem.eql(u8, m.name, short)) return true;
    }
    return false;
}


fn defaultValue(proto: Proto, f: Field) []const u8 {
    if (f.repeated) return "&.{}";
    return switch (f.ftype) {
        .scalar => |s| switch (s) {
            .bool => "false",
            .string, .bytes => "\"\"",
            .double, .float => "0",
            else => "0",
        },
        .named => |n| if (isEnum(proto, n)) "null" else "null",
        .map => mapDefault(),
    };
}

fn writeEncodeField(w: anytype, proto: Proto, f: Field, pad: []const u8) !void {
    const fnum = f.number;
    if (f.repeated) {
        switch (f.ftype) {
            // map：每个条目编码成一个 Entry 子消息（key=1 / value=2）
            .map => |mt| {
                try w.print("{s}for (self.{s}) |e| {{\n", .{ pad, f.name });
                try w.print("{s}    var ew = pb.Writer.init(a);\n", .{pad});
                try w.print("{s}    try ew.{s}(1, e.key);\n", .{ pad, scalarWriteFn(mt.key) });
                switch (mt.value.*) {
                    .scalar => |s| try w.print("{s}    try ew.{s}(2, e.value);\n", .{ pad, scalarWriteFn(s) }),
                    .named => |n| if (isEnum(proto, n))
                        try w.print("{s}    try ew.enumField(2, e.value);\n", .{pad})
                    else
                        try w.print("{s}    try ew.message(2, try e.value.encode(a));\n", .{pad}),
                    .map => return error.Unsupported,
                }
                try w.print("{s}    try w.message({d}, ew.bytes());\n", .{ pad, fnum });
                try w.print("{s}}}\n", .{pad});
            },
            .scalar => |s| {
                if (s == .string or s == .bytes) {
                    // 非 packed：每条一个 tag + 长度
                    try w.print("{s}for (self.{s}) |v| try w.bytesField({d}, v);\n", .{ pad, f.name, fnum });
                } else {
                    // packed 数值：一次写完整片（空则不写）
                    try w.print("{s}try w.{s}({d}, self.{s});\n", .{ pad, packedFn(s), fnum, f.name });
                }
            },
            .named => |n| {
                if (isEnum(proto, n)) {
                    try w.print("{s}for (self.{s}) |v| try w.enumField({d}, v);\n", .{ pad, f.name, fnum });
                } else {
                    try w.print("{s}for (self.{s}) |v| try w.message({d}, try v.encode(a));\n", .{ pad, f.name, fnum });
                }
            },
        }
        return;
    }
    switch (f.ftype) {
        .scalar => |s| switch (s) {
            .string, .bytes => try w.print(
                "{s}if (self.{s}.len > 0) try w.string({d}, self.{s});\n",
                .{ pad, f.name, fnum, f.name },
            ),
            .bool => try w.print("{s}if (self.{s}) try w.boolField({d}, true);\n", .{ pad, f.name, fnum }),
            .double => try w.print("{s}if (self.{s} != 0) try w.doubleField({d}, self.{s});\n", .{ pad, f.name, fnum, f.name }),
            .float => try w.print("{s}if (self.{s} != 0) try w.floatField({d}, self.{s});\n", .{ pad, f.name, fnum, f.name }),
            else => try w.print("{s}if (self.{s} != 0) try w.{s}({d}, self.{s});\n", .{ pad, f.name, scalarWriteFn(s), fnum, f.name }),
        },
        .named => |n| {
            if (isEnum(proto, n)) {
                const t = lastSegment(n);
                try w.print("{s}if (self.{s}) |v| try w.enumField({d}, v) else try w.enumField({d}, {s}.{s});\n", .{
                    pad, f.name, fnum, fnum, t, proto.enums[enumIndexOf(proto, n)].values[0].name,
                });
            } else {
                try w.print("{s}if (self.{s}) |v| try w.message({d}, try v.encode(a));\n", .{ pad, f.name, fnum });
            }
        },
        .map => {},
    }
}

fn scalarWriteFn(s: Scalar) []const u8 {
    return switch (s) {
        .double => "doubleField",
        .float => "floatField",
        .int32 => "int32",
        .int64 => "int64",
        .uint32 => "uint32",
        .uint64 => "uint64",
        .sint32 => "sint32",
        .sint64 => "sint64",
        .fixed32 => "fixed32",
        .fixed64 => "fixed64",
        .sfixed32 => "fixed32",
        .sfixed64 => "fixed64",
        .bool => "boolField",
        .string, .bytes => "string",
    };
}

fn enumIndexOf(proto: Proto, name: []const u8) usize {
    const short = lastSegment(name);
    for (proto.enums, 0..) |e, i| {
        if (std.mem.eql(u8, e.name, short)) return i;
    }
    return 0;
}

fn writeDecodeField(a: std.mem.Allocator, w: anytype, proto: Proto, f: Field, pad: []const u8) !void {
    const fnum = f.number;
    if (f.repeated) {
        // 反复出现的同号字段（以及 packed / 非 packed 混用）都累积到局部 list
        switch (f.ftype) {
            .scalar => |s| {
                if (s == .string or s == .bytes) {
                    try w.print("{s}{d} => try lst_{s}.append(a, f.asBytes()),\n", .{ pad, fnum, f.name });
                } else {
                    try w.print("{s}{d} => if (f.wire == .len) {{\n", .{ pad, fnum });
                    try w.print("{s}    for (try f.{s}(a)) |v| try lst_{s}.append(a, {s});\n", .{ pad, packedReadFn(s), f.name, castExpr(s) });
                    // 非 packed：单条 varint/fixed 值
                    try w.print("{s}}} else {{\n", .{pad});
                    try w.print("{s}    const v = f.value;\n", .{pad});
                    try w.print("{s}    try lst_{s}.append(a, {s});\n", .{ pad, f.name, castExpr(s) });
                    try w.print("{s}}},\n", .{pad});
                }
            },
            .named => |n| {
                const t = lastSegment(n);
                if (isEnum(proto, n)) {
                    try w.print("{s}{d} => if (f.asEnum({s})) |v| try lst_{s}.append(a, v),\n", .{ pad, fnum, t, f.name });
                } else {
                    try w.print("{s}{d} => try lst_{s}.append(a, try {s}.decode(f.asBytes(), a)),\n", .{ pad, fnum, f.name, t });
                }
            },
            .map => |mt| {
                // 每个条目是独立子消息：先在局部累积 key/value，再 append
                try w.print("{s}{d} => {{\n", .{ pad, fnum });
                try w.print("{s}    var ek: {s} = {s};\n", .{ pad, try mapKeyType(a, mt), scalarZero(mt.key) });
                try w.print("{s}    var ev: {s} = {s};\n", .{ pad, try mapValueType(a, mt.value.*), switch (mt.value.*) {
                    .scalar => |s| scalarZero(s),
                    .named => |n| namedZero(proto, n),
                    .map => return error.Unsupported,
                } });
                try w.print("{s}    var er = pb.Reader{{ .data = f.asBytes() }};\n", .{pad});
                try w.print("{s}    while (er.next()) |ef| switch (ef.number) {{\n", .{pad});
                try w.print("{s}        1 => ek = {s},\n", .{ pad, try retarget(a, scalarReadExpr(mt.key), "ef") });
                switch (mt.value.*) {
                    .scalar => |s| try w.print("{s}        2 => ev = {s},\n", .{ pad, try retarget(a, scalarReadExpr(s), "ef") }),
                    .named => |n| if (isEnum(proto, n))
                        try w.print("{s}        2 => if (ef.asEnum({s})) |v| ev = v,\n", .{ pad, lastSegment(n) })
                    else
                        try w.print("{s}        2 => ev = try {s}.decode(ef.asBytes(), a),\n", .{ pad, lastSegment(n) }),
                    .map => return error.Unsupported,
                }
                try w.print("{s}        else => {{}},\n", .{pad});
                try w.print("{s}    }};\n", .{pad});
                try w.print("{s}    try lst_{s}.append(a, .{{ .key = ek, .value = ev }});\n", .{ pad, f.name });
                try w.print("{s}}},\n", .{pad});
            },
        }
        return;
    }
    switch (f.ftype) {
        .scalar => |s| {
            const expr = scalarReadExpr(s);
            try w.print("{s}{d} => self.{s} = {s},\n", .{ pad, fnum, f.name, expr });
        },
        .named => |n| {
            if (isEnum(proto, n)) {
                const t = lastSegment(n);
                try w.print("{s}{d} => self.{s} = f.asEnum({s}),\n", .{ pad, fnum, f.name, t });
            } else {
                const t = lastSegment(n);
                try w.print("{s}{d} => self.{s} = try {s}.decode(f.asBytes(), a),\n", .{ pad, fnum, f.name, t });
            }
        },
        .map => {},
    }
}

fn castExpr(s: Scalar) []const u8 {
    return switch (s) {
        .double => "@bitCast(@as(u64, v))",
        .float => "@bitCast(@as(u32, @truncate(v)))",
        .int32, .sint32, .sfixed32 => "@truncate(@as(i64, @bitCast(v)))",
        .int64, .sint64, .sfixed64 => "@bitCast(v)",
        .uint32, .fixed32 => "@truncate(v)",
        .uint64, .fixed64 => "v",
        .bool => "v != 0",
        .string, .bytes => "v",
    };
}

fn scalarReadExpr(s: Scalar) []const u8 {
    return switch (s) {
        .double => "f.asDouble()",
        .float => "f.asFloat()",
        .int32 => "f.asInt32()",
        .int64 => "f.asInt64()",
        .uint32 => "f.asUint32()",
        .uint64 => "f.asUint64()",
        .sint32 => "f.asSint32()",
        .sint64 => "f.asSint64()",
        .fixed32 => "f.asUint32()",
        .fixed64 => "f.asUint64()",
        .sfixed32 => "f.asInt32()",
        .sfixed64 => "f.asInt64()",
        .bool => "f.asBool()",
        .string, .bytes => "f.asBytes()",
    };
}

/// 该 message 的 decode 是否需要分配器（有 repeated 或嵌套 message 才需要）。
fn decodeNeedsAlloc(proto: Proto, m: Message) bool {
    for (m.fields) |f| {
        if (f.repeated) return true;
        switch (f.ftype) {
            .named => |n| if (!isEnum(proto, n)) return true,
            .scalar => {},
            .map => return true,
        }
    }
    return false;
}

fn generate(a: std.mem.Allocator, proto: Proto, src_path: []const u8) ![]const u8 {
    var aw = std.Io.Writer.Allocating.init(a);
    const w = &aw.writer;

    // 注意：`//!` 只能出现在文件最开头，所以来源信息必须写在这两块 import 之前
    try w.writeAll("//! 由 zgrpc tools/proto_gen.zig 生成，**请勿手改**。\n");
    try w.print("//! 来源：{s}\n", .{src_path});
    if (proto.package.len > 0) try w.print("//! package：{s}\n", .{proto.package});
    try w.writeAll(
        \\
        \\const std = @import("std");
        \\const pb = @import("zgrpc").pb;
        \\
        \\
    );

    // 枚举
    for (proto.enums) |e| {
        try w.print("pub const {s} = enum(i32) {{\n", .{e.name});
        for (e.values) |v| try w.print("    {s} = {d},\n", .{ v.name, v.value });
        try w.writeAll("};\n\n");
    }

    // 消息
    for (proto.messages) |m| {
        try w.print("pub const {s} = struct {{\n", .{m.name});
        // map 字段的条目类型（与描述符里的 `XxxEntry` 同名，便于双向对照）
        for (m.fields) |f| {
            switch (f.ftype) {
                .map => |mt| {
                    try w.print("    /// `map<{s}, ..> {s} = {d}` 的条目\n", .{ @tagName(mt.key), f.name, f.number });
                    try w.print("    pub const {s} = struct {{ key: {s}, value: {s} }};\n", .{
                        try mapEntryTypeName(a, f.name),
                        try mapKeyType(a, mt),
                        try mapValueType(a, mt.value.*),
                    });
                },
                else => {},
            }
        }
        for (m.fields) |f| {
            try w.print("    {s}: {s} = {s},\n", .{ f.name, try zigType(a, proto, f), defaultValue(proto, f) });
        }
        try w.writeAll("\n    pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {\n");
        try w.writeAll("        var w = pb.Writer.init(a);\n");
        for (m.fields) |f| try writeEncodeField(w, proto, f, "        ");
        try w.writeAll("        return w.bytes();\n    }\n\n");
        try w.writeAll("    pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {\n");
        if (!decodeNeedsAlloc(proto, m)) {
            try w.writeAll("        _ = a; // 该 message 的字段不需要分配\n");
        }
        try w.writeAll("        var self: @This() = .{};\n");
        // repeated 字段用局部 list 累积：同号字段可重复出现，packed 与非 packed 也能混用
        for (m.fields) |f| {
            if (!f.repeated) continue;
            const elem = switch (f.ftype) {
                .map => try mapEntryTypeName(a, f.name),
                else => try elemType(a, f),
            };
            try w.print("        var lst_{s}: std.ArrayListUnmanaged({s}) = .empty;\n", .{ f.name, elem });
        }
        try w.writeAll("        var r = pb.Reader{ .data = bytes };\n");
        try w.writeAll("        while (r.next()) |f| switch (f.number) {\n");
        for (m.fields) |f| try writeDecodeField(a, w, proto, f, "            ");
        try w.writeAll("            else => {},\n");
        try w.writeAll("        };\n");
        for (m.fields) |f| {
            if (!f.repeated) continue;
            try w.print("        self.{s} = lst_{s}.items;\n", .{ f.name, f.name });
        }
        try w.writeAll("        return self;\n    }\n");
        // map 的键查找辅助（零依赖；小 map 线性查找足够，不值得引入 hash 容器）
        for (m.fields) |f| {
            switch (f.ftype) {
                .map => |mt| {
                    const pascal = try proto_toPascalCase(a, f.name);
                    const kt = try mapKeyType(a, mt);
                    const vt = try mapValueType(a, mt.value.*);
                    try w.print("\n    /// `{s}` 取一个键（线性查找，未命中返回 null）\n", .{f.name});
                    try w.print("    pub fn get{s}(self: @This(), k: {s}) ?{s} {{\n", .{ pascal, kt, vt });
                    if (mt.key == .string) {
                        try w.print("        for (self.{s}) |e| if (std.mem.eql(u8, e.key, k)) return e.value;\n", .{f.name});
                    } else {
                        try w.print("        for (self.{s}) |e| if (e.key == k) return e.value;\n", .{f.name});
                    }
                    try w.writeAll("        return null;\n    }\n");
                },
                else => {},
            }
        }
        try w.writeAll("};\n\n");
    }

    // 服务：路由常量
    for (proto.services) |s| {
        const full = if (proto.package.len > 0)
            try std.fmt.allocPrint(a, "{s}.{s}", .{ proto.package, s.name })
        else
            s.name;
        try w.print("/// service {s}（路径前缀 `/{s}/`）\npub const {s} = struct {{\n", .{ full, full, s.name });
        try w.print("    pub const name = \"{s}\";\n", .{full});
        for (s.rpcs) |r| {
            try w.print("    pub const {s} = \"/{s}/{s}\";\n", .{ r.name, full, r.name });
            if (r.client_stream or r.server_stream) {
                try w.print("    // {s}：client_stream={} server_stream={}\n", .{ r.name, r.client_stream, r.server_stream });
            }
        }
        try w.writeAll("};\n\n");
    }

    return aw.written();
}

// ---------------------------------------------------------------------------
// 入口
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var it = std.process.Args.Iterator.initAllocator(init.minimal.args, a) catch |e| {
        std.debug.print("[proto_gen] 解析命令行参数失败：{s}\n", .{@errorName(e)});
        return e;
    };
    defer it.deinit();
    _ = it.skip(); // 程序名
    const in_path = it.next() orelse {
        std.debug.print("用法：zig run tools/proto_gen.zig -- <input.proto> <output.zig>\n", .{});
        return error.MissingArgs;
    };
    const out_path = it.next() orelse {
        std.debug.print("用法：zig run tools/proto_gen.zig -- <input.proto> <output.zig>\n", .{});
        return error.MissingArgs;
    };

    const src = std.Io.Dir.cwd().readFileAlloc(io, in_path, a, .limited(4 * 1024 * 1024)) catch |e| {
        std.debug.print("[proto_gen] 读 {s} 失败：{s}\n", .{ in_path, @errorName(e) });
        return e;
    };

    const parsed = try zproto.parse(a, src);

    // 校验：named 类型必须已定义
    for (parsed.messages) |m| {
        for (m.fields) |f| {
            switch (f.ftype) {
                .scalar => {},
                .named => |n| if (!isMessage(parsed, n) and !isEnum(parsed, n)) {
                    std.debug.print("[proto_gen] {s} 的字段 {s} 引用了未定义类型 {s}\n", .{ m.name, f.name, n });
                    return error.UnknownType;
                },
                .map => |mt| switch (mt.value.*) {
                    .scalar => {},
                    .named => |n| if (!isMessage(parsed, n) and !isEnum(parsed, n)) {
                        std.debug.print("[proto_gen] {s} 的 map 字段 {s} 取值类型未定义：{s}\n", .{ m.name, f.name, n });
                        return error.UnknownType;
                    },
                    .map => {
                        std.debug.print("[proto_gen] {s} 的字段 {s}：map 的 value 不能再是 map\n", .{ m.name, f.name });
                        return error.Unsupported;
                    },
                },
            }
        }
    }

    const code = try generate(a, parsed, in_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = out_path, .data = code });
    std.debug.print("[proto_gen] 已生成 {s}（{d} 字节，{d} 个 message / {d} 个 enum / {d} 个 service）\n", .{
        out_path, code.len, parsed.messages.len, parsed.enums.len, parsed.services.len,
    });
}

test "proto_gen：解析 proto3 子集" {
    const src =
        \\syntax = "proto3";
        \\package demo.v1;
        \\
        \\enum Color { RED = 0; GREEN = 1; }
        \\
        \\message Inner { int32 id = 1; }
        \\
        \\message Outer {
        \\  string text = 1;
        \\  Color color = 2;
        \\  Inner inner = 3;
        \\  repeated int32 nums = 4;
        \\  bool ok = 5;
        \\}
        \\
        \\service Demo {
        \\  rpc Say(Outer) returns (Outer);
        \\  rpc Chat(stream Outer) returns (stream Outer);
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const parsed = try zproto.parse(a, src);
    try std.testing.expectEqualStrings("demo.v1", parsed.package);
    try std.testing.expectEqual(@as(usize, 2), parsed.messages.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.enums.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.services.len);
    try std.testing.expectEqual(@as(usize, 5), parsed.messages[1].fields.len);
    try std.testing.expect(parsed.messages[1].fields[3].repeated);
    try std.testing.expect(parsed.services[0].rpcs[1].client_stream);
    try std.testing.expect(parsed.services[0].rpcs[1].server_stream);

    const code = try generate(a, parsed, "demo.proto");
    try std.testing.expect(std.mem.indexOf(u8, code, "pub const Outer = struct {") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "pub const Color = enum(i32)") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "\"/demo.v1.Demo/Say\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "pub fn encode") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "pub fn decode") != null);
}
