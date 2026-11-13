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
const proto_loader = @import("zgrpc").proto_loader;

// ---------------------------------------------------------------------------
// 生成上下文：本文件类型用裸名，跨文件类型用 `<别名>.<名>`
// ---------------------------------------------------------------------------

/// 单个输出文件的类型上下文。
///
/// 设计取舍（protoc 风格）：**每个 `.proto` 生成自己的 `.pb.zig`**，跨文件引用走
/// `@import("<相对路径>")` 的别名。好处是**单文件场景的生成结果完全不变**，
/// 不会为了支持 import 而给所有类型加上包名前缀。
const Ctx = struct {
    a: std.mem.Allocator,
    /// 本文件 package（用于把 `pkg.Type` 归一成 `Type`）
    package: []const u8,
    /// 本文件定义的类型：相对全名（`Outer.Inner`）→ 引用写法（裸名）
    own: std.StringHashMapUnmanaged(TypeRef) = .empty,
    /// 其他文件定义的类型：`<pkg>.<相对全名>` → 引用写法（`别名.Outer.Inner`）
    global: std.StringHashMapUnmanaged(TypeRef) = .empty,
    /// 需要 `@import` 的依赖
    imports: []const ImportDecl = &.{},

    pub const TypeRef = struct { zig: []const u8, is_enum: bool, from_own_file: bool };
    pub const ImportDecl = struct { alias: []const u8, path: []const u8 };

    /// 解析类型引用。
    ///
    /// 查找顺序（任一命中即返回）：
    /// 1. 本文件裸相对名（`Outer.Inner` / `BumpResponse`）
    /// 2. 去掉本文件 package 前缀后（`echo.BumpResponse` → `BumpResponse`）
    /// 3. 全局绝对名（`common.Money`）
    /// 4. 同包跨文件（`<本文件 package>.<name>`）
    pub fn lookup(self: *const Ctx, name: []const u8) ?TypeRef {
        if (self.own.get(name)) |r| return r;
        if (stripPackageStr(self.package, name)) |n| {
            if (self.own.get(n)) |r| return r;
        }
        if (self.global.get(name)) |r| return r;
        if (self.package.len > 0) {
            const with_pkg = std.fmt.allocPrint(self.a, "{s}.{s}", .{ self.package, name }) catch return null;
            if (self.global.get(with_pkg)) |r| return r;
        }
        return null;
    }

    pub fn isEnum(self: *const Ctx, name: []const u8) bool {
        return if (self.lookup(name)) |r| r.is_enum else false;
    }

    pub fn isKnown(self: *const Ctx, name: []const u8) bool {
        return self.lookup(name) != null;
    }
};

/// `pkg.Name` → `Name`（仅当确实以 pkg 开头）
fn stripPackageStr(package: []const u8, n: []const u8) ?[]const u8 {
    if (package.len == 0 or n.len <= package.len + 1) return null;
    if (!std.mem.startsWith(u8, n, package)) return null;
    if (n[package.len] != '.') return null;
    return n[package.len + 1 ..];
}

/// `common/types.proto` → `common_types`（`@import` 的局部名）
fn aliasOf(a: std.mem.Allocator, file_name: []const u8) ![]const u8 {
    const base = if (std.mem.endsWith(u8, file_name, ".proto"))
        file_name[0 .. file_name.len - ".proto".len]
    else
        file_name;
    const out = try a.alloc(u8, base.len + 1);
    var n: usize = 0;
    var prev_us = true; // 抑制连续下划线
    for (base) |c| {
        if (std.ascii.isAlphanumeric(c)) {
            out[n] = c;
            n += 1;
            prev_us = false;
        } else if (!prev_us) {
            out[n] = '_';
            n += 1;
            prev_us = true;
        }
    }
    if (n == 0 or out[n - 1] == '_') {
        if (n > 0) n -= 1;
    }
    return out[0..n];
}

/// 依赖文件对应生成物的 `@import` 路径（相对输出文件所在目录，也即 root 目录）
fn importPathOf(a: std.mem.Allocator, file_name: []const u8) ![]const u8 {
    const base = if (std.mem.endsWith(u8, file_name, ".proto"))
        file_name[0 .. file_name.len - ".proto".len]
    else
        file_name;
    return std.fmt.allocPrint(a, "{s}.pb.zig", .{base});
}

/// 递归登记一个文件里的全部 message / enum（含嵌套）。
/// 登记一个类型，并同时登记它的各级「后缀名」。
///
/// 为什么需要后缀：字段里写的是 `Nested`（在 `Container` 内）或 `Nested.Level`，
/// 而登记的全名是 `Container.Nested` / `Container.Nested.Level`。后缀匹配让两者都能解析。
/// 同名冲突时**先到先得**（proto3 里同文件同名类型本就该报错，这里不静默错配）。
fn registerOne(
    a: std.mem.Allocator,
    table: *std.StringHashMapUnmanaged(Ctx.TypeRef),
    full_rel: []const u8,
    package: []const u8,
    alias: ?[]const u8,
    is_enum: bool,
) !void {
    var idx: usize = 0;
    while (true) {
        const suffix = if (idx == 0) full_rel else full_rel[idx..];
        if (alias) |al| {
            const zig = try std.fmt.allocPrint(a, "{s}.{s}", .{ al, suffix });
            if (package.len > 0) {
                const key = try std.fmt.allocPrint(a, "{s}.{s}", .{ package, suffix });
                if (!table.contains(key)) try table.put(a, key, .{ .zig = zig, .is_enum = is_enum, .from_own_file = false });
            }
            if (!table.contains(suffix)) try table.put(a, suffix, .{ .zig = zig, .is_enum = is_enum, .from_own_file = false });
        } else {
            if (!table.contains(suffix)) try table.put(a, suffix, .{ .zig = suffix, .is_enum = is_enum, .from_own_file = true });
        }
        const dot = std.mem.indexOfScalarPos(u8, full_rel, idx, '.') orelse break;
        idx = dot + 1;
    }
}

/// 递归登记一个文件里的全部 message / enum（含嵌套）。
fn registerTypes(
    a: std.mem.Allocator,
    table: *std.StringHashMapUnmanaged(Ctx.TypeRef),
    package: []const u8,
    prefix: []const u8,
    alias: ?[]const u8,
    m: Message,
) !void {
    const rel = if (prefix.len == 0)
        try a.dupe(u8, m.name)
    else
        try std.fmt.allocPrint(a, "{s}.{s}", .{ prefix, m.name });
    try registerOne(a, table, rel, package, alias, false);

    for (m.enums) |e| {
        const erel = try std.fmt.allocPrint(a, "{s}.{s}", .{ rel, e.name });
        try registerOne(a, table, erel, package, alias, true);
    }
    for (m.nested) |n| try registerTypes(a, table, package, rel, alias, n);
}

fn registerFile(
    a: std.mem.Allocator,
    table: *std.StringHashMapUnmanaged(Ctx.TypeRef),
    p: Proto,
    alias: ?[]const u8,
) !void {
    for (p.messages) |m| try registerTypes(a, table, p.package, "", alias, m);
    for (p.enums) |e| try registerOne(a, table, e.name, p.package, alias, true);
}

/// 为第 `fi` 个文件构造生成上下文。
fn buildCtx(a: std.mem.Allocator, files: []const proto_loader.File, fi: usize) !Ctx {
    var ctx = Ctx{ .a = a, .package = files[fi].proto.package };

    try registerFile(a, &ctx.own, files[fi].proto, null);
    // 本文件的绝对名也登记进 own（`echo.BumpResponse` 这种写法）
    if (ctx.package.len > 0) {
        var it = ctx.own.iterator();
        var extra: std.ArrayListUnmanaged(struct { []const u8, Ctx.TypeRef }) = .empty;
        while (it.next()) |e| {
            if (stripPackageStr(ctx.package, e.key_ptr.*) != null) continue; // 已是绝对名
            const abs = try std.fmt.allocPrint(a, "{s}.{s}", .{ ctx.package, e.key_ptr.* });
            try extra.append(a, .{ abs, e.value_ptr.* });
        }
        for (extra.items) |kv| try ctx.own.put(a, kv[0], kv[1]);
    }

    // 其他文件：**只登记本文件直接 import 的那些**（protobuf 也要求显式 import 才能引用）。
    // 把同批次的兄弟文件全登记进来会让 `types.pb.zig` 反向 import `shop.pb.zig`，形成无意义依赖。
    var imports: std.ArrayListUnmanaged(Ctx.ImportDecl) = .empty;
    for (files[fi].proto.imports) |imp| {
        for (files, 0..) |g, gi| {
            if (gi == fi) continue;
            if (!std.mem.eql(u8, g.name, imp.path)) continue;
            const alias = try aliasOf(a, g.name);
            try imports.append(a, .{ .alias = alias, .path = try importPathOf(a, g.name) });
            try registerFile(a, &ctx.global, g.proto, alias);
            break;
        }
    }
    ctx.imports = imports.items;
    return ctx;
}

fn mapEntryTypeName(a: std.mem.Allocator, field_name: []const u8) ![]const u8 {
    const base = try proto_toPascalCase(a, field_name);
    return std.fmt.allocPrint(a, "{s}Entry", .{base});
}

// ---------------------------------------------------------------------------
// 代码生成
// ---------------------------------------------------------------------------

/// 去掉 package 前缀，保留限定路径：`demo.Outer.Inner` → `Outer.Inner`。
/// 嵌套定义在 Zig 里就是嵌套声明，必须写成 `Outer.Inner` 才能从其他顶层类型引用。
fn mapKeyType(a: std.mem.Allocator, ctx: *const Ctx, mt: zproto.MapType) ![]const u8 {
    return elemType(a, ctx, .{ .name = "", .number = 0, .ftype = .{ .scalar = mt.key }, .repeated = false });
}

fn mapValueType(a: std.mem.Allocator, ctx: *const Ctx, v: zproto.FieldType) ![]const u8 {
    return elemType(a, ctx, .{ .name = "", .number = 0, .ftype = v, .repeated = false });
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

fn namedZero(ctx: *const Ctx, n: []const u8) []const u8 {
    // enum 用第一个枚举值；message 用空字面量（`.{}`）
    return if (ctx.isEnum(n)) "undefined" else ".{}";
}

/// `scalarReadExpr` 的表达式里写死了接收者 `f.`；map 条目内部遍历用的是 `ef`，
/// 所以要把前缀换掉（不做字符串替换就只能把整个 switch 再写一遍）。
fn retarget(a: std.mem.Allocator, expr: []const u8, recv: []const u8) ![]const u8 {
    const tail = expr[2..]; // 跳过 `f.`
    return std.fmt.allocPrint(a, "{s}.{s}", .{ recv, tail });
}

/// 单个元素（非 repeated）的 Zig 类型写法。
fn elemType(a: std.mem.Allocator, ctx: *const Ctx, f: Field) ![]const u8 {
    _ = a; // 类型字符串都在 buildCtx 里预先算好
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
        // 元素是**值**类型（不是 `?T`）；嵌套定义 / 跨文件引用由 ctx 统一解析
        .named => |n| (ctx.lookup(n) orelse return error.UnknownType).zig,
        // map 只在字段层处理；作为「元素」不可能出现（parser 已挡），这里只是穷尽 switch
        .map => unreachable,
    };
}

/// 生成字段的 Zig 类型写法（named 类型需要拼接，故用分配器）。
fn zigType(a: std.mem.Allocator, ctx: *const Ctx, f: Field) ![]const u8 {
    if (f.repeated) {
        switch (f.ftype) {
            .map => return std.fmt.allocPrint(a, "[]const {s}", .{try mapEntryTypeName(a, f.name)}),
            else => return std.fmt.allocPrint(a, "[]const {s}", .{try elemType(a, ctx, f)}),
        }
    }
    const base = try elemType(a, ctx, f);
    return switch (f.ftype) {
        // message 引用本来就是 ?T（表示未设置）；enum / 标量在 `optional` 时才包成 ?T
        .named => std.fmt.allocPrint(a, "?{s}", .{base}),
        .scalar => if (f.proto3_optional) std.fmt.allocPrint(a, "?{s}", .{base}) else base,
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

fn isMessage(ctx: *const Ctx, name: []const u8) bool {
    return ctx.isKnown(name);
}

fn defaultValue(ctx: *const Ctx, f: Field) []const u8 {
    if (f.repeated) return "&.{}";
    if (f.proto3_optional) return "null";
    return switch (f.ftype) {
        .scalar => |s| switch (s) {
            .bool => "false",
            .string, .bytes => "\"\"",
            .double, .float => "0",
            else => "0",
        },
        .named => |n| if (ctx.isEnum(n)) "null" else "null",
        .map => mapDefault(),
    };
}

fn writeEncodeField(w: anytype, ctx: *const Ctx, f: Field, pad: []const u8) !void {
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
                    .named => |n| if (ctx.isEnum(n))
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
                if (ctx.isEnum(n)) {
                    try w.print("{s}for (self.{s}) |v| try w.enumField({d}, v);\n", .{ pad, f.name, fnum });
                } else {
                    try w.print("{s}for (self.{s}) |v| try w.message({d}, try v.encode(a));\n", .{ pad, f.name, fnum });
                }
            },
        }
        return;
    }
    // proto3 `optional`：有值就写，**即使值是默认值**（否则无法表达显式 presence）
    if (f.proto3_optional) {
        var pad_buf: [64]u8 = undefined;
        const pad2 = std.fmt.bufPrint(&pad_buf, "{s}    ", .{pad}) catch pad;
        try w.print("{s}if (self.{s}) |v| {{\n", .{ pad, f.name });
        try writeValueStmt(w, ctx, f, "v", pad2);
        try w.print("{s}}}\n", .{pad});
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
            if (ctx.isEnum(n)) {
                // proto3：默认值（0）不序列化；`null` 表示跟随默认值，同样不写。
                // oneof 成员不走这里（见 `writeValueStmt`）——它们显式设置就必须写。
                try w.print("{s}if (self.{s}) |v| {{ if (@intFromEnum(v) != 0) try w.enumField({d}, v); }}\n", .{ pad, f.name, fnum });
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

/// 把单个值按字段号写入（不带「!= 0」判断）。
/// oneof 成员被显式设置时**即使值是默认值也要写**（用于区分「未设置」），所以不能用
/// `writeEncodeField` 那套“零值不写”的规则。
fn writeValueStmt(w: anytype, ctx: *const Ctx, f: Field, val: []const u8, pad: []const u8) !void {
    const fnum = f.number;
    switch (f.ftype) {
        .scalar => |s| switch (s) {
            .string, .bytes => try w.print("{s}try w.string({d}, {s});\n", .{ pad, fnum, val }),
            .bool => try w.print("{s}try w.boolField({d}, {s});\n", .{ pad, fnum, val }),
            .double => try w.print("{s}try w.doubleField({d}, {s});\n", .{ pad, fnum, val }),
            .float => try w.print("{s}try w.floatField({d}, {s});\n", .{ pad, fnum, val }),
            else => try w.print("{s}try w.{s}({d}, {s});\n", .{ pad, scalarWriteFn(s), fnum, val }),
        },
        .named => |n| if (ctx.isEnum(n))
            try w.print("{s}try w.enumField({d}, {s});\n", .{ pad, fnum, val })
        else
            try w.print("{s}try w.message({d}, try {s}.encode(a));\n", .{ pad, fnum, val }),
        .map => unreachable,
    }
}

fn writeDecodeField(a: std.mem.Allocator, w: anytype, ctx: *const Ctx, m: Message, f: Field, pad: []const u8) !void {
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
                const t = (ctx.lookup(n) orelse return error.UnknownType).zig;
                if (ctx.isEnum(n)) {
                    try w.print("{s}{d} => if (f.asEnum({s})) |v| try lst_{s}.append(a, v),\n", .{ pad, fnum, t, f.name });
                } else {
                    try w.print("{s}{d} => try lst_{s}.append(a, try {s}.decode(f.asBytes(), a)),\n", .{ pad, fnum, f.name, t });
                }
            },
            .map => |mt| {
                // 每个条目是独立子消息：先在局部累积 key/value，再 append
                try w.print("{s}{d} => {{\n", .{ pad, fnum });
                try w.print("{s}    var ek: {s} = {s};\n", .{ pad, try mapKeyType(a, ctx, mt), scalarZero(mt.key) });
                try w.print("{s}    var ev: {s} = {s};\n", .{ pad, try mapValueType(a, ctx, mt.value.*), switch (mt.value.*) {
                    .scalar => |s| scalarZero(s),
                    .named => |n| namedZero(ctx, n),
                    .map => return error.Unsupported,
                } });
                try w.print("{s}    var er = pb.Reader{{ .data = f.asBytes() }};\n", .{pad});
                try w.print("{s}    while (er.next()) |ef| switch (ef.number) {{\n", .{pad});
                try w.print("{s}        1 => ek = {s},\n", .{ pad, try retarget(a, scalarReadExpr(mt.key), "ef") });
                switch (mt.value.*) {
                    .scalar => |s| try w.print("{s}        2 => ev = {s},\n", .{ pad, try retarget(a, scalarReadExpr(s), "ef") }),
                    .named => |n| {
                        const vt = (ctx.lookup(n) orelse return error.UnknownType).zig;
                        if (ctx.isEnum(n)) {
                            try w.print("{s}        2 => if (ef.asEnum({s})) |v| ev = v,\n", .{ pad, vt });
                        } else {
                            try w.print("{s}        2 => ev = try {s}.decode(ef.asBytes(), a),\n", .{ pad, vt });
                        }
                    },
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
            if (f.oneof_index) |oi| {
                try w.print("{s}{d} => self.{s} = .{{ .{s} = {s} }},\n", .{ pad, fnum, m.oneofs[@intCast(oi)], f.name, expr });
            } else {
                try w.print("{s}{d} => self.{s} = {s},\n", .{ pad, fnum, f.name, expr });
            }
        },
        .named => |n| {
            const t = (ctx.lookup(n) orelse return error.UnknownType).zig;
            if (ctx.isEnum(n)) {
                if (f.oneof_index) |oi| {
                    try w.print("{s}{d} => if (f.asEnum({s})) |v| {{ self.{s} = .{{ .{s} = v }}; }},\n", .{ pad, fnum, t, m.oneofs[@intCast(oi)], f.name });
                } else {
                    try w.print("{s}{d} => self.{s} = f.asEnum({s}),\n", .{ pad, fnum, f.name, t });
                }
            } else {
                if (f.oneof_index) |oi| {
                    try w.print("{s}{d} => self.{s} = .{{ .{s} = try {s}.decode(f.asBytes(), a) }},\n", .{ pad, fnum, m.oneofs[@intCast(oi)], f.name, t });
                } else {
                    try w.print("{s}{d} => self.{s} = try {s}.decode(f.asBytes(), a),\n", .{ pad, fnum, f.name, t });
                }
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
fn decodeNeedsAlloc(ctx: *const Ctx, m: Message) bool {
    for (m.fields) |f| {
        if (f.repeated) return true;
        switch (f.ftype) {
            .named => |n| if (!ctx.isEnum(n)) return true,
            .scalar => {},
            .map => return true,
        }
    }
    return false;
}

/// 递归生成一个 message 类型：嵌套 enum / 嵌套 message / map 条目 / oneof union / 字段 / 编解码。
fn writeMessageType(a: std.mem.Allocator, w: anytype, ctx: *const Ctx, m: Message, pad: []const u8) !void {
    const p1 = try std.fmt.allocPrint(a, "{s}    ", .{pad});
    const p2 = try std.fmt.allocPrint(a, "{s}    ", .{p1});
    const p3 = try std.fmt.allocPrint(a, "{s}    ", .{p2});
    const p4 = try std.fmt.allocPrint(a, "{s}    ", .{p3});

    try w.print("{s}pub const {s} = struct {{\n", .{ pad, m.name });

    // 嵌套 enum
    for (m.enums) |e| {
        try w.print("{s}pub const {s} = enum(i32) {{\n", .{ p1, e.name });
        for (e.values) |v| try w.print("{s}{s} = {d},\n", .{ p2, v.name, v.value });
        try w.print("{s}}};\n", .{p1});
    }
    // 嵌套 message（Zig 里就是嵌套声明，引用写 `Outer.Inner`）
    for (m.nested) |n| try writeMessageType(a, w, ctx, n, p1);

    // map 条目类型（与描述符里的 `XxxEntry` 同名，便于双向对照）
    for (m.fields) |f| {
        switch (f.ftype) {
            .map => |mt| {
                try w.print("{s}/// `map<{s}, ..> {s} = {d}` 的条目\n", .{ p1, @tagName(mt.key), f.name, f.number });
                try w.print("{s}pub const {s} = struct {{ key: {s}, value: {s} }};\n", .{
                    p1,
                    try mapEntryTypeName(a, f.name),
                    try mapKeyType(a, ctx, mt),
                    try mapValueType(a, ctx, mt.value.*),
                });
            },
            else => {},
        }
    }
    // oneof → Zig 的 tagged union（语义天然对应：同时只能有一个）
    for (m.oneofs, 0..) |oname, oi| {
        try w.print("{s}/// oneof `{s}`\n", .{ p1, oname });
        try w.print("{s}pub const {s} = union(enum) {{\n", .{ p1, try proto_toPascalCase(a, oname) });
        for (m.fields) |f| {
            const fi = f.oneof_index orelse continue;
            if (@as(usize, fi) != oi) continue;
            try w.print("{s}{s}: {s},\n", .{ p2, f.name, try elemType(a, ctx, f) });
        }
        try w.print("{s}}};\n", .{p1});
    }
    // 普通字段
    for (m.fields) |f| {
        if (f.oneof_index != null) continue; // oneof 成员在 union 里，不是独立字段
        try w.print("{s}{s}: {s} = {s},\n", .{ p1, f.name, try zigType(a, ctx, f), defaultValue(ctx, f) });
    }
    // oneof 字段本体
    for (m.oneofs) |oname| {
        try w.print("{s}{s}: ?{s} = null,\n", .{ p1, oname, try proto_toPascalCase(a, oname) });
    }

    try w.print("{s}\n{s}pub fn encode(self: @This(), a: std.mem.Allocator) ![]const u8 {{\n", .{ pad, p1 });
    try w.print("{s}var w = pb.Writer.init(a);\n", .{p2});
    for (m.fields) |f| {
        if (f.oneof_index != null) continue;
        try writeEncodeField(w, ctx, f, p2);
    }
    for (m.oneofs, 0..) |oname, oi| {
        try w.print("{s}if (self.{s}) |c| switch (c) {{\n", .{ p2, oname });
        for (m.fields) |f| {
            const fi = f.oneof_index orelse continue;
            if (@as(usize, fi) != oi) continue;
            try w.print("{s}.{s} => |v| {{\n", .{ p3, f.name });
            try writeValueStmt(w, ctx, f, "v", p4);
            try w.print("{s}}},\n", .{p3});
        }
        try w.print("{s}}};\n", .{p2});
    }
    try w.print("{s}return w.bytes();\n{s}}}\n\n", .{ p2, p1 });

    try w.print("{s}pub fn decode(bytes: []const u8, a: std.mem.Allocator) !@This() {{\n", .{p1});
    if (!decodeNeedsAlloc(ctx, m)) {
        try w.print("{s}_ = a; // 该 message 的字段不需要分配\n", .{p2});
    }
    try w.print("{s}var self: @This() = .{{}};\n", .{p2});
    // repeated 字段用局部 list 累积：同号字段可重复出现，packed 与非 packed 也能混用
    for (m.fields) |f| {
        if (!f.repeated) continue;
        const elem = switch (f.ftype) {
            .map => try mapEntryTypeName(a, f.name),
            else => try elemType(a, ctx, f),
        };
        try w.print("{s}var lst_{s}: std.ArrayListUnmanaged({s}) = .empty;\n", .{ p2, f.name, elem });
    }
    try w.print("{s}var r = pb.Reader{{ .data = bytes }};\n", .{p2});
    try w.print("{s}while (r.next()) |f| switch (f.number) {{\n", .{p2});
    for (m.fields) |f| try writeDecodeField(a, w, ctx, m, f, p3);
    try w.print("{s}else => {{}},\n{s}}};\n", .{ p3, p2 });
    for (m.fields) |f| {
        if (!f.repeated) continue;
        try w.print("{s}self.{s} = lst_{s}.items;\n", .{ p2, f.name, f.name });
    }
    try w.print("{s}return self;\n{s}}}\n", .{ p2, p1 });

    // map 的键查找辅助（零依赖；小 map 线性查找足够，不值得引入 hash 容器）
    for (m.fields) |f| {
        switch (f.ftype) {
            .map => |mt| {
                const pascal = try proto_toPascalCase(a, f.name);
                const kt = try mapKeyType(a, ctx, mt);
                const vt = try mapValueType(a, ctx, mt.value.*);
                try w.print("\n{s}/// `{s}` 取一个键（线性查找，未命中返回 null）\n", .{ p1, f.name });
                try w.print("{s}pub fn get{s}(self: @This(), k: {s}) ?{s} {{\n", .{ p1, pascal, kt, vt });
                if (mt.key == .string) {
                    try w.print("{s}for (self.{s}) |e| if (std.mem.eql(u8, e.key, k)) return e.value;\n", .{ p2, f.name });
                } else {
                    try w.print("{s}for (self.{s}) |e| if (e.key == k) return e.value;\n", .{ p2, f.name });
                }
                try w.print("{s}return null;\n{s}}}\n", .{ p2, p1 });
            },
            else => {},
        }
    }

    try w.print("{s}}};\n\n", .{pad});
}

/// 校验：所有 named 引用都能在生成上下文里解析（含 import 进来的类型）。
fn validate(ctx: *const Ctx, p: Proto) !void {
    for (p.messages) |m| {
        for (m.fields) |f| {
            switch (f.ftype) {
                .scalar => {},
                .named => |n| if (!ctx.isKnown(n)) {
                    std.debug.print("[proto_gen] {s} 的字段 {s} 引用了未定义类型 {s}（检查 import 是否齐全）\n", .{ m.name, f.name, n });
                    return error.UnknownType;
                },
                .map => |mt| switch (mt.value.*) {
                    .scalar => {},
                    .named => |n| if (!ctx.isKnown(n)) {
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
}

fn generate(a: std.mem.Allocator, ctx: *const Ctx, proto: Proto, src_path: []const u8) ![]const u8 {
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

    // 跨文件依赖：每个 import 一个别名（`const common_types = @import("common/types.pb.zig");`）
    if (ctx.imports.len > 0) {
        for (ctx.imports) |imp| {
            try w.print("const {s} = @import(\"{s}\");\n", .{ imp.alias, imp.path });
        }
        try w.writeAll("\n");
    }

    // 枚举
    for (proto.enums) |e| {
        try w.print("pub const {s} = enum(i32) {{\n", .{e.name});
        for (e.values) |v| try w.print("    {s} = {d},\n", .{ v.name, v.value });
        try w.writeAll("};\n\n");
    }

    // 消息（递归生成嵌套定义）
    for (proto.messages) |m| try writeMessageType(a, w, ctx, m, "");

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

    // 1) 加载 root 及其 import（拓扑排序：被依赖的在前，root 在最后）
    var loader = proto_loader.Loader.init(a, io);
    const loaded = loader.loadRoot(in_path) catch |e| {
        std.debug.print("[proto_gen] 加载 {s} 失败：{s}\n", .{ in_path, @errorName(e) });
        return e;
    };

    const out_dir = std.fs.path.dirname(out_path) orelse "";

    // 2) 逐文件生成自己的 `.pb.zig`（root 用 out_path，依赖用 out_dir/<name>.pb.zig）
    for (loaded.files, 0..) |f, i| {
        const is_root = i == loaded.files.len - 1;
        const ctx = try buildCtx(a, loaded.files, i);
        try validate(&ctx, f.proto);

        const code = try generate(a, &ctx, f.proto, f.name);
        const dep_name = try importPathOf(a, f.name);
        const target = if (is_root)
            out_path
        else if (out_dir.len == 0)
            dep_name
        else
            try std.fs.path.join(a, &.{ out_dir, dep_name });

        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = target, .data = code });
        std.debug.print("[proto_gen] 已生成 {s}（{d} 字节，{d} message / {d} enum / {d} service）\n", .{
            target, code.len, f.proto.messages.len, f.proto.enums.len, f.proto.services.len,
        });
    }
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
