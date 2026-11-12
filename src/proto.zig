//! proto3 **子集**解析器：把 `.proto` 文本解析成 Zig 语法树。
//!
//! 两个使用方：
//! 1. `tools/proto_gen.zig` —— 代码生成（.proto → Zig struct + 编解码）；
//! 2. `zgrpc.reflection` —— 把语法树序列化成 `FileDescriptorProto`，供 grpcurl / ghz 自省。
//!
//! ## 支持的语法
//!
//! - `syntax = "proto3";`、`package a.b.c;`
//! - `message Name { ... }`：标量、`string`/`bytes`、`enum` 字段、同文件 message 字段，
//!   `repeated` 支持**任意元素**（标量 / `string` / `bytes` / enum / message），
//!   `map<K, V>`（key 限整数或 `string`）
//! - `enum Name { A = 0; ... }`
//! - `service Name { rpc M(Req) returns (Res); rpc S(stream Req) returns (stream Res); }`
//!
//! ## 明确不支持（报错并指出行号，不静默忽略）
//!
//! `import`、`oneof`、`optional`、`reserved`、`extend`、`group`、嵌套 `message`/`enum` 定义。

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

pub const Scalar = enum { double, float, int32, int64, uint32, uint64, sint32, sint64, fixed32, fixed64, sfixed32, sfixed64, bool, string, bytes };

/// `map<K, V>`。key 只能是整数或 string（protobuf 规定）；value 可以是任意标量或命名类型。
pub const MapType = struct {
    key: Scalar,
    /// 指向 arena 上的值类型（用指针避开 union 自引用带来的无限大小）
    value: *const FieldType,
};

pub const FieldType = union(enum) {
    scalar: Scalar,
    /// 引用同文件内的 message / enum（按末段名字查）
    named: []const u8,
    map: MapType,
};

pub const Field = struct {
    name: []const u8,
    number: u32,
    ftype: FieldType,
    repeated: bool,
};

pub const Message = struct {
    name: []const u8,
    fields: []Field,
};

pub const EnumValue = struct { name: []const u8, value: i32 };
pub const EnumDef = struct { name: []const u8, values: []EnumValue };

pub const Rpc = struct {
    name: []const u8,
    req: []const u8,
    rsp: []const u8,
    client_stream: bool,
    server_stream: bool,
};

pub const Service = struct { name: []const u8, rpcs: []Rpc };

pub const Proto = struct {
    package: []const u8 = "",
    messages: []Message = &.{},
    enums: []EnumDef = &.{},
    services: []Service = &.{},
};

pub const ParseError = error{ Syntax, Unsupported, InvalidNumber, OutOfMemory };

// ---------------------------------------------------------------------------
// 语法分析
// ---------------------------------------------------------------------------

pub const Parser = struct {
    lex: Lexer,
    tok: Token = .{ .kind = .eof, .text = "", .line = 0 },
    a: std.mem.Allocator,

    pub fn init(a: std.mem.Allocator, src: []const u8) !Parser {
        var p = Parser{ .lex = .{ .src = src }, .a = a };
        p.tok = try p.lex.next();
        return p;
    }

    fn advance(self: *Parser) !void {
        self.tok = try self.lex.next();
    }

    fn fail(self: *Parser, comptime msg: []const u8) ParseError {
        std.debug.print("[zgrpc.proto] 第 {d} 行：{s}（遇到 `{s}`）\n", .{ self.tok.line, msg, self.tok.text });
        return error.Syntax;
    }

    fn expectSym(self: *Parser, s: []const u8) !void {
        if (self.tok.kind != .sym or !std.mem.eql(u8, self.tok.text, s)) {
            std.debug.print("[zgrpc.proto] 第 {d} 行：期望 `{s}`，实际 `{s}`\n", .{ self.tok.line, s, self.tok.text });
            return error.Syntax;
        }
        try self.advance();
    }

    fn expectIdent(self: *Parser) ![]const u8 {
        if (self.tok.kind != .ident) {
            std.debug.print("[zgrpc.proto] 第 {d} 行：期望标识符，实际 `{s}`\n", .{ self.tok.line, self.tok.text });
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

    pub fn parseFile(self: *Parser) ParseError!Proto {
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
                    std.debug.print("[zgrpc.proto] 第 {d} 行：只支持 proto3\n", .{self.tok.line});
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
                std.debug.print("[zgrpc.proto] 第 {d} 行：不支持 `{s}`\n", .{ self.tok.line, self.tok.text });
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
                std.debug.print("[zgrpc.proto] 第 {d} 行：不支持 `{s}`\n", .{ self.tok.line, self.tok.text });
                return error.Unsupported;
            }
            if (self.atIdent("message") or self.atIdent("enum")) {
                std.debug.print("[zgrpc.proto] 第 {d} 行：不支持嵌套定义\n", .{self.tok.line});
                return error.Unsupported;
            }
            if (self.atIdent("map")) {
                try fields.append(self.a, try self.parseMapField());
                continue;
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

            try fields.append(self.a, .{ .name = fname, .number = number, .ftype = ftype, .repeated = repeated });
        }
        try self.expectSym("}");
        return .{ .name = name, .fields = fields.items };
    }

    /// `map<K, V> name = N [options];`
    fn parseMapField(self: *Parser) ParseError!Field {
        try self.advance(); // 吃掉 `map`
        try self.expectSym("<");
        const kname = try self.expectIdent();
        const key = scalarFromName(kname) orelse {
            std.debug.print("[zgrpc.proto] 第 {d} 行：map 的 key 必须是标量类型（不能是 `{s}`）\n", .{ self.tok.line, kname });
            return error.Unsupported;
        };
        // protobuf 规定：key 不能是 float / double / bytes / message / enum
        switch (key) {
            .double, .float, .bytes => {
                std.debug.print("[zgrpc.proto] 第 {d} 行：map 的 key 不能是 `{s}`\n", .{ self.tok.line, kname });
                return error.Unsupported;
            },
            else => {},
        }
        try self.expectSym(",");
        const vname = try self.expectIdent();
        try self.expectSym(">");
        const fname = try self.expectIdent();
        try self.expectSym("=");
        if (self.tok.kind != .number) return self.fail("字段号必须是数字");
        const number = std.fmt.parseInt(u32, self.tok.text, 10) catch return error.InvalidNumber;
        try self.advance();
        if (self.atSym("[")) {
            while (!self.atSym("]")) {
                if (self.tok.kind == .eof) return self.fail("字段选项未闭合");
                try self.advance();
            }
            try self.advance();
        }
        try self.expectSym(";");

        const vptr = try self.a.create(FieldType);
        vptr.* = if (scalarFromName(vname)) |s| .{ .scalar = s } else .{ .named = vname };

        return .{
            .name = fname,
            .number = number,
            // 语义上 map 就是 `repeated Entry`；这里仍标 repeated，消费方按 `.map` 单独处理
            .ftype = .{ .map = .{ .key = key, .value = vptr } },
            .repeated = true,
        };
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

/// 便捷入口：解析整份 `.proto` 文本。输出在 `a` 上。
pub fn parse(a: std.mem.Allocator, src: []const u8) ParseError!Proto {
    var p = Parser.init(a, src) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.Syntax,
    };
    return p.parseFile();
}

pub fn scalarFromName(n: []const u8) ?Scalar {
    // Zig 0.17 的 Enum TypeInfo 是 field_names + field_values（不再是 fields）
    const names = @typeInfo(Scalar).@"enum".field_names;
    inline for (names, 0..) |name, i| {
        if (std.mem.eql(u8, n, name)) return @enumFromInt(i);
    }
    return null;
}

/// 取 `a.b.C` 的最后一段（`C`）。
pub fn lastSegment(name: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |i| return name[i + 1 ..];
    return name;
}

/// `foo_bar` → `FooBar`。
/// protobuf 的 map 字段要在描述符里生成嵌套类型 `<PascalCase(字段名)>Entry`，
/// 代码生成也要用它当 Zig 类型名 —— 两处共用这一份，避免命名漂移。
pub fn toPascalCase(alloc: std.mem.Allocator, s: []const u8) ![]const u8 {
    const out = try alloc.alloc(u8, s.len);
    var n: usize = 0;
    var upper = true;
    for (s) |c| {
        if (c == '_') {
            upper = true;
            continue;
        }
        out[n] = if (upper) std.ascii.toUpper(c) else c;
        n += 1;
        upper = false;
    }
    return out[0..n];
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "proto：解析 message / enum / service（含流式标记）" {
    const src =
        \\syntax = "proto3";
        \\package demo.v1;
        \\enum Kind { A = 0; B = 1; }
        \\message Req { int32 id = 1; string name = 2; repeated int32 nums = 3; Kind kind = 4; }
        \\message Res { bytes data = 1; }
        \\service Api {
        \\  rpc Unary(Req) returns (Res);
        \\  rpc Bidi(stream Req) returns (stream Res);
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const p = try parse(arena.allocator(), src);

    try testing.expectEqualStrings("demo.v1", p.package);
    try testing.expectEqual(@as(usize, 2), p.messages.len);
    try testing.expectEqual(@as(usize, 4), p.messages[0].fields.len);
    try testing.expectEqual(@as(usize, 1), p.enums.len);
    try testing.expectEqual(@as(usize, 1), p.services.len);

    const rpcs = p.services[0].rpcs;
    try testing.expectEqual(@as(usize, 2), rpcs.len);
    try testing.expectEqualStrings("Unary", rpcs[0].name);
    try testing.expect(!rpcs[0].client_stream and !rpcs[0].server_stream);
    try testing.expectEqualStrings("Bidi", rpcs[1].name);
    try testing.expect(rpcs[1].client_stream and rpcs[1].server_stream);
}

test "proto：不支持的语法报错而不静默忽略" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try testing.expectError(error.Unsupported, parse(arena.allocator(), "syntax = \"proto3\";\nimport \"x.proto\";\n"));
    try testing.expectError(error.Unsupported, parse(arena.allocator(), "syntax = \"proto3\";\nmessage M { oneof k { int32 a = 1; } }\n"));
    try testing.expectError(error.Unsupported, parse(arena.allocator(), "syntax = \"proto3\";\nmessage M { message N { int32 a = 1; } }\n"));
    // map 的 key 不能是 float / double / bytes
    try testing.expectError(error.Unsupported, parse(arena.allocator(), "syntax = \"proto3\";\nmessage M { map<double,int32> m = 1; }\n"));
    try testing.expectError(error.Unsupported, parse(arena.allocator(), "syntax = \"proto3\";\nmessage M { map<bytes,int32> m = 1; }\n"));
}

test "proto：map<K,V> 解析（含 message 值）" {
    const src =
        \\syntax = "proto3";
        \\package demo;
        \\message Item { int32 id = 1; }
        \\message Bag {
        \\  map<string, int32> counts = 1;
        \\  map<int32, Item> by_id = 2;
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const p = try parse(arena.allocator(), src);

    const fields = p.messages[1].fields;
    try testing.expectEqual(@as(usize, 2), fields.len);
    try testing.expect(fields[0].repeated); // map 语义上就是 repeated entry

    const m0 = fields[0].ftype.map;
    try testing.expectEqual(Scalar.string, m0.key);
    try testing.expectEqual(Scalar.int32, m0.value.scalar);

    const m1 = fields[1].ftype.map;
    try testing.expectEqual(Scalar.int32, m1.key);
    try testing.expectEqualStrings("Item", m1.value.named);
}

test "proto：repeated 支持 string / bytes / enum / message 元素" {
    const src =
        \\syntax = "proto3";
        \\package demo;
        \\enum Kind { A = 0; }
        \\message Item { int32 id = 1; }
        \\message Bag {
        \\  repeated string tags = 1;
        \\  repeated bytes blobs = 2;
        \\  repeated Item items = 3;
        \\  repeated Kind kinds = 4;
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const p = try parse(arena.allocator(), src);

    const fields = p.messages[1].fields;
    try testing.expectEqual(@as(usize, 4), fields.len);
    for (fields) |f| try testing.expect(f.repeated);

    try testing.expectEqual(Scalar.string, fields[0].ftype.scalar);
    try testing.expectEqual(Scalar.bytes, fields[1].ftype.scalar);
    try testing.expectEqualStrings("Item", fields[2].ftype.named);
    try testing.expectEqualStrings("Kind", fields[3].ftype.named);
}
