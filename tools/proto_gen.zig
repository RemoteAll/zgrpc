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

// ---------------------------------------------------------------------------
// 词法
// ---------------------------------------------------------------------------

const TokenKind = enum { ident, number, string, sym, eof };

const Token = struct {
    kind: TokenKind,
    text: []const u8,
    line: usize,
};

const Lexer = struct {
    src: []const u8,
    pos: usize = 0,
    line: usize = 1,

    fn next(self: *Lexer) !Token {
        try self.skipSpaceAndComments();
        if (self.pos >= self.src.len) return .{ .kind = .eof, .text = "", .line = self.line };
        const start = self.pos;
        const c = self.src[self.pos];

        if (std.ascii.isAlphabetic(c) or c == '_') {
            while (self.pos < self.src.len) {
                const d = self.src[self.pos];
                if (!(std.ascii.isAlphanumeric(d) or d == '_' or d == '.')) break;
                self.pos += 1;
            }
            return .{ .kind = .ident, .text = self.src[start..self.pos], .line = self.line };
        }
        if (std.ascii.isDigit(c)) {
            while (self.pos < self.src.len and (std.ascii.isDigit(self.src[self.pos]) or self.src[self.pos] == 'x')) self.pos += 1;
            return .{ .kind = .number, .text = self.src[start..self.pos], .line = self.line };
        }
        if (c == '"' or c == '\'') {
            const quote = c;
            self.pos += 1;
            const s = self.pos;
            while (self.pos < self.src.len and self.src[self.pos] != quote) self.pos += 1;
            const text = self.src[s..self.pos];
            if (self.pos < self.src.len) self.pos += 1;
            return .{ .kind = .string, .text = text, .line = self.line };
        }
        self.pos += 1;
        return .{ .kind = .sym, .text = self.src[start..self.pos], .line = self.line };
    }

    fn skipSpaceAndComments(self: *Lexer) !void {
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            if (c == '\n') {
                self.line += 1;
                self.pos += 1;
            } else if (c == ' ' or c == '\t' or c == '\r') {
                self.pos += 1;
            } else if (c == '/' and self.pos + 1 < self.src.len and self.src[self.pos + 1] == '/') {
                while (self.pos < self.src.len and self.src[self.pos] != '\n') self.pos += 1;
            } else if (c == '/' and self.pos + 1 < self.src.len and self.src[self.pos + 1] == '*') {
                self.pos += 2;
                while (self.pos + 1 < self.src.len and !(self.src[self.pos] == '*' and self.src[self.pos + 1] == '/')) {
                    if (self.src[self.pos] == '\n') self.line += 1;
                    self.pos += 1;
                }
                self.pos += 2;
            } else break;
        }
    }
};

// ---------------------------------------------------------------------------
// 语法树
// ---------------------------------------------------------------------------

const Scalar = enum { double, float, int32, int64, uint32, uint64, sint32, sint64, fixed32, fixed64, sfixed32, sfixed64, bool, string, bytes };

const FieldType = union(enum) {
    scalar: Scalar,
    /// 引用同文件内的 message / enum（按末段名字查）
    named: []const u8,
};

const Field = struct {
    name: []const u8,
    number: u32,
    ftype: FieldType,
    repeated: bool,
};

const Message = struct {
    name: []const u8,
    fields: []Field,
};

const EnumValue = struct { name: []const u8, value: i32 };
const EnumDef = struct { name: []const u8, values: []EnumValue };

const Rpc = struct {
    name: []const u8,
    req: []const u8,
    rsp: []const u8,
    client_stream: bool,
    server_stream: bool,
};

const Service = struct { name: []const u8, rpcs: []Rpc };

const Proto = struct {
    package: []const u8 = "",
    messages: []Message = &.{},
    enums: []EnumDef = &.{},
    services: []Service = &.{},
};

// ---------------------------------------------------------------------------
// 语法分析
// ---------------------------------------------------------------------------

const ParseError = error{ Syntax, Unsupported, InvalidNumber, OutOfMemory };

const Parser = struct {
    lex: Lexer,
    tok: Token = .{ .kind = .eof, .text = "", .line = 0 },
    a: std.mem.Allocator,

    fn init(a: std.mem.Allocator, src: []const u8) !Parser {
        var p = Parser{ .lex = .{ .src = src }, .a = a };
        p.tok = try p.lex.next();
        return p;
    }

    fn advance(self: *Parser) !void {
        self.tok = try self.lex.next();
    }

    fn fail(self: *Parser, comptime msg: []const u8) ParseError {
        std.debug.print("[proto_gen] 第 {d} 行：{s}（遇到 `{s}`）\n", .{ self.tok.line, msg, self.tok.text });
        return error.Syntax;
    }

    fn expectSym(self: *Parser, s: []const u8) !void {
        if (self.tok.kind != .sym or !std.mem.eql(u8, self.tok.text, s)) {
            std.debug.print("[proto_gen] 第 {d} 行：期望 `{s}`，实际 `{s}`\n", .{ self.tok.line, s, self.tok.text });
            return error.Syntax;
        }
        try self.advance();
    }

    fn expectIdent(self: *Parser) ![]const u8 {
        if (self.tok.kind != .ident) {
            std.debug.print("[proto_gen] 第 {d} 行：期望标识符，实际 `{s}`\n", .{ self.tok.line, self.tok.text });
            return error.Syntax;
        }
        const t = self.tok.text;
        try self.advance();
        return t;
    }

    fn atSym(self: *Parser, s: []const u8) bool {
        return self.tok.kind == .sym and std.mem.eql(u8, self.tok.text, s);
    }

    fn atIdent(self: *Parser, s: []const u8) bool {
        return self.tok.kind == .ident and std.mem.eql(u8, self.tok.text, s);
    }

    fn parseFile(self: *Parser) ParseError!Proto {
        var out = Proto{};
        var messages: std.ArrayListUnmanaged(Message) = .empty;
        var enums: std.ArrayListUnmanaged(EnumDef) = .empty;
        var services: std.ArrayListUnmanaged(Service) = .empty;

        while (self.tok.kind != .eof) {
            if (self.atIdent("syntax")) {
                try self.advance();
                try self.expectSym("=");
                if (self.tok.kind != .string) return self.fail("syntax 值必须是字符串");
                if (!std.mem.eql(u8, self.tok.text, "proto3")) {
                    std.debug.print("[proto_gen] 第 {d} 行：只支持 proto3\n", .{self.tok.line});
                    return error.Unsupported;
                }
                try self.advance();
                try self.expectSym(";");
            } else if (self.atIdent("package")) {
                try self.advance();
                out.package = try self.expectIdent();
                try self.expectSym(";");
            } else if (self.atIdent("message")) {
                try self.advance();
                const name = try self.expectIdent();
                try messages.append(self.a, try self.parseMessage(name));
            } else if (self.atIdent("enum")) {
                try self.advance();
                const name = try self.expectIdent();
                try enums.append(self.a, try self.parseEnum(name));
            } else if (self.atIdent("service")) {
                try self.advance();
                const name = try self.expectIdent();
                try services.append(self.a, try self.parseService(name));
            } else if (self.atIdent("import") or self.atIdent("option")) {
                std.debug.print("[proto_gen] 第 {d} 行：不支持 `{s}`\n", .{ self.tok.line, self.tok.text });
                return error.Unsupported;
            } else {
                return self.fail("顶层只支持 syntax/package/message/enum/service");
            }
        }

        out.messages = messages.items;
        out.enums = enums.items;
        out.services = services.items;
        return out;
    }

    fn parseMessage(self: *Parser, name: []const u8) ParseError!Message {
        var fields: std.ArrayListUnmanaged(Field) = .empty;
        try self.expectSym("{");
        while (!self.atSym("}")) {
            if (self.tok.kind == .eof) return self.fail("message 未闭合");
            if (self.atIdent("reserved") or self.atIdent("extend") or self.atIdent("oneof") or
                self.atIdent("optional") or self.atIdent("required") or self.atIdent("group"))
            {
                std.debug.print("[proto_gen] 第 {d} 行：不支持 `{s}`\n", .{ self.tok.line, self.tok.text });
                return error.Unsupported;
            }
            if (self.atIdent("message") or self.atIdent("enum")) {
                std.debug.print("[proto_gen] 第 {d} 行：不支持嵌套定义\n", .{self.tok.line});
                return error.Unsupported;
            }
            if (self.atIdent("map")) {
                std.debug.print("[proto_gen] 第 {d} 行：不支持 map\n", .{self.tok.line});
                return error.Unsupported;
            }

            var repeated = false;
            if (self.atIdent("repeated")) {
                repeated = true;
                try self.advance();
            }
            const type_name = try self.expectIdent();
            const fname = try self.expectIdent();
            try self.expectSym("=");
            if (self.tok.kind != .number) return self.fail("字段号必须是数字");
            const number = std.fmt.parseInt(u32, self.tok.text, 10) catch return error.InvalidNumber;
            try self.advance();

            // 跳过字段选项（如 [packed = true]、[default = x]）
            if (self.atSym("[")) {
                while (!self.atSym("]")) {
                    if (self.tok.kind == .eof) return self.fail("字段选项未闭合");
                    try self.advance();
                }
                try self.advance();
            }
            try self.expectSym(";");

            const ftype: FieldType = if (scalarFromName(type_name)) |s|
                .{ .scalar = s }
            else
                .{ .named = type_name };

            if (repeated) {
                switch (ftype) {
                    .scalar => |s| if (s == .string or s == .bytes) {
                        std.debug.print("[proto_gen] 第 {d} 行：repeated string/bytes 暂不支持\n", .{self.tok.line});
                        return error.Unsupported;
                    },
                    .named => {
                        std.debug.print("[proto_gen] 第 {d} 行：repeated message/enum 暂不支持\n", .{self.tok.line});
                        return error.Unsupported;
                    },
                }
            }

            try fields.append(self.a, .{ .name = fname, .number = number, .ftype = ftype, .repeated = repeated });
        }
        try self.expectSym("}");
        return .{ .name = name, .fields = fields.items };
    }

    fn parseEnum(self: *Parser, name: []const u8) ParseError!EnumDef {
        var values: std.ArrayListUnmanaged(EnumValue) = .empty;
        try self.expectSym("{");
        while (!self.atSym("}")) {
            if (self.tok.kind == .eof) return self.fail("enum 未闭合");
            const vname = try self.expectIdent();
            try self.expectSym("=");
            if (self.tok.kind != .number) return self.fail("枚举值必须是数字");
            const v = std.fmt.parseInt(i32, self.tok.text, 10) catch return error.InvalidNumber;
            try self.advance();
            if (self.atSym("[")) {
                while (!self.atSym("]")) try self.advance();
                try self.advance();
            }
            try self.expectSym(";");
            try values.append(self.a, .{ .name = vname, .value = v });
        }
        try self.expectSym("}");
        return .{ .name = name, .values = values.items };
    }

    fn parseService(self: *Parser, name: []const u8) ParseError!Service {
        var rpcs: std.ArrayListUnmanaged(Rpc) = .empty;
        try self.expectSym("{");
        while (!self.atSym("}")) {
            if (self.tok.kind == .eof) return self.fail("service 未闭合");
            if (!self.atIdent("rpc")) return self.fail("service 内只支持 rpc");
            try self.advance();
            const mname = try self.expectIdent();
            try self.expectSym("(");
            var client_stream = false;
            if (self.atIdent("stream")) {
                client_stream = true;
                try self.advance();
            }
            const req = try self.expectIdent();
            try self.expectSym(")");
            if (!self.atIdent("returns")) return self.fail("rpc 缺少 returns");
            try self.advance();
            try self.expectSym("(");
            var server_stream = false;
            if (self.atIdent("stream")) {
                server_stream = true;
                try self.advance();
            }
            const rsp = try self.expectIdent();
            try self.expectSym(")");

            // 跳过 rpc 选项 { ... } 或 ;
            if (self.atSym("{")) {
                var depth: usize = 0;
                while (true) {
                    if (self.tok.kind == .eof) return self.fail("rpc 选项未闭合");
                    if (self.atSym("{")) depth += 1;
                    if (self.atSym("}")) {
                        depth -= 1;
                        if (depth == 0) {
                            try self.advance();
                            break;
                        }
                    }
                    try self.advance();
                }
            } else {
                try self.expectSym(";");
            }
            try rpcs.append(self.a, .{
                .name = mname,
                .req = req,
                .rsp = rsp,
                .client_stream = client_stream,
                .server_stream = server_stream,
            });
        }
        try self.expectSym("}");
        return .{ .name = name, .rpcs = rpcs.items };
    }
};

fn scalarFromName(n: []const u8) ?Scalar {
    // Zig 0.17 的 Enum TypeInfo 是 field_names + field_values（不再是 fields）
    const names = @typeInfo(Scalar).@"enum".field_names;
    inline for (names, 0..) |name, i| {
        if (std.mem.eql(u8, n, name)) return @enumFromInt(i);
    }
    return null;
}

/// 取类型名末段（`a.b.Foo` → `Foo`）。
fn lastSegment(name: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |i| return name[i + 1 ..];
    return name;
}

// ---------------------------------------------------------------------------
// 代码生成
// ---------------------------------------------------------------------------

/// 生成字段的 Zig 类型写法（named 类型需要拼接，故用分配器）。
fn zigType(a: std.mem.Allocator, proto: Proto, f: Field) ![]const u8 {
    _ = proto; // named 类型只取末段名字，不需要查表
    if (f.repeated) {
        return switch (f.ftype) {
            .scalar => |s| switch (s) {
                .double => "[]const f64",
                .float => "[]const f32",
                .int32, .sint32, .sfixed32 => "[]const i32",
                .int64, .sint64, .sfixed64 => "[]const i64",
                .uint32, .fixed32 => "[]const u32",
                .uint64, .fixed64 => "[]const u64",
                .bool => "[]const bool",
                .string, .bytes => "[]const []const u8",
            },
            .named => "[]const []const u8",
        };
    }
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
        .named => |n| try std.fmt.allocPrint(a, "?{s}", .{lastSegment(n)}),
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
    };
}

fn writeEncodeField(w: anytype, proto: Proto, f: Field, pad: []const u8) !void {
    const fnum = f.number;
    if (f.repeated) {
        const s = f.ftype.scalar;
        try w.print("{s}if (self.{s}.len > 0) {{\n", .{ pad, f.name });
        switch (s) {
            .int32, .sint32, .sfixed32 => try w.print("{s}    try w.packedInt32({d}, self.{s});\n", .{ pad, fnum, f.name }),
            .uint32, .fixed32 => try w.print("{s}    var tmp: [16]u32 = undefined;\n", .{pad}),
            else => try w.print("{s}    try w.packedUint64({d}, self.{s});\n", .{ pad, fnum, f.name }),
        }
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
            if (isEnum(proto, n)) {
                const t = lastSegment(n);
                try w.print("{s}if (self.{s}) |v| try w.enumField({d}, v) else try w.enumField({d}, {s}.{s});\n", .{
                    pad, f.name, fnum, fnum, t, proto.enums[enumIndexOf(proto, n)].values[0].name,
                });
            } else {
                try w.print("{s}if (self.{s}) |v| try w.message({d}, try v.encode(a));\n", .{ pad, f.name, fnum });
            }
        },
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
        const s = f.ftype.scalar;
        try w.print("{s}{d} => {{\n", .{ pad, fnum });
        try w.print("{s}    const vals = try f.packedVarints(a);\n", .{pad});
        try w.print("{s}    var tmp: std.ArrayListUnmanaged({s}) = .empty;\n", .{ pad, try zigType(a, proto, .{ .name = f.name, .number = fnum, .ftype = .{ .scalar = s }, .repeated = false }) });
        try w.print("{s}    for (vals) |v| try tmp.append(a, {s});\n", .{ pad, castExpr(s) });
        try w.print("{s}    self.{s} = tmp.items;\n", .{ pad, f.name });
        try w.print("{s}}},\n", .{pad});
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
        try w.writeAll("        var r = pb.Reader{ .data = bytes };\n");
        try w.writeAll("        while (r.next()) |f| switch (f.number) {\n");
        for (m.fields) |f| try writeDecodeField(a, w, proto, f, "            ");
        try w.writeAll("            else => {},\n");
        try w.writeAll("        };\n");
        try w.writeAll("        return self;\n    }\n};\n\n");
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

    var p = try Parser.init(a, src);
    const proto = try p.parseFile();

    // 校验：named 类型必须已定义
    for (proto.messages) |m| {
        for (m.fields) |f| {
            switch (f.ftype) {
                .scalar => {},
                .named => |n| if (!isMessage(proto, n) and !isEnum(proto, n)) {
                    std.debug.print("[proto_gen] {s} 的字段 {s} 引用了未定义类型 {s}\n", .{ m.name, f.name, n });
                    return error.UnknownType;
                },
            }
        }
    }

    const code = try generate(a, proto, in_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = out_path, .data = code });
    std.debug.print("[proto_gen] 已生成 {s}（{d} 字节，{d} 个 message / {d} 个 enum / {d} 个 service）\n", .{
        out_path, code.len, proto.messages.len, proto.enums.len, proto.services.len,
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

    var p = try Parser.init(a, src);
    const proto = try p.parseFile();
    try std.testing.expectEqualStrings("demo.v1", proto.package);
    try std.testing.expectEqual(@as(usize, 2), proto.messages.len);
    try std.testing.expectEqual(@as(usize, 1), proto.enums.len);
    try std.testing.expectEqual(@as(usize, 1), proto.services.len);
    try std.testing.expectEqual(@as(usize, 5), proto.messages[1].fields.len);
    try std.testing.expect(proto.messages[1].fields[3].repeated);
    try std.testing.expect(proto.services[0].rpcs[1].client_stream);
    try std.testing.expect(proto.services[0].rpcs[1].server_stream);

    const code = try generate(a, proto, "demo.proto");
    try std.testing.expect(std.mem.indexOf(u8, code, "pub const Outer = struct {") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "pub const Color = enum(i32)") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "\"/demo.v1.Demo/Say\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "pub fn encode") != null);
    try std.testing.expect(std.mem.indexOf(u8, code, "pub fn decode") != null);
}
