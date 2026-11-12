//! 加载带 `import` 的 `.proto`：递归读取、拓扑排序（被依赖的在前）、循环依赖报错。
//!
//! 导入路径按 **root 文件所在目录**解析（等价于 protoc 的单一 `-I <root 目录>`）——
//! 不是「相对引用方目录」（那是 C 语言 `#include ""` 的语义）。这样
//! `common/base.proto` 里的 `import "deep/x.proto"` 指向的仍是 root 目录下的 `deep/x.proto`。
//!
//! ## 为什么需要拓扑排序
//!
//! 反射返回 `FileDescriptorProto` 时，客户端（grpcurl / protoreflect）会按 `dependency`
//! 逐个拉依赖；把依赖排在前面能让单次 `file_containing_symbol` 请求就拿到可解析的集合。
//!
//! ## 循环依赖
//!
//! protobuf 本身不允许循环 import，这里在递归栈上做检测并打印完整链路 ——
//! 否则只会看到栈溢出或无穷递归。

const std = @import("std");
const proto = @import("proto.zig");

pub const LoadError = error{ ImportNotFound, ImportCycle, TooLarge } || proto.ParseError;

/// 单个 `.proto` 的大小上限：正常协议文件远低于此，防止误读大文件把内存吃掉。
pub const max_file_size: usize = 8 * 1024 * 1024;

pub const File = struct {
    /// 相对 root 目录的路径（与 import 语句里写的原样一致）。
    /// 反射的 `FileDescriptorProto.name` 与 `file_by_filename` 的键都用它。
    name: []const u8,
    /// 该文件所在目录（相对 root；顶层文件为空串）
    dir: []const u8,
    proto: proto.Proto,
};

pub const Loaded = struct {
    /// 依赖在前、root 在最后
    files: []File,
};

pub const Loader = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    files: std.ArrayListUnmanaged(File) = .empty,
    /// 已完成加载的 name → files 索引（去重，避免菱形依赖被加载两次）
    done: std.StringHashMapUnmanaged(usize) = .empty,
    /// 当前递归栈（循环检测 + 打印链路）
    stack: std.ArrayListUnmanaged([]const u8) = .empty,
    /// root 文件所在目录（所有 import 都相对它解析）
    root_dir: []const u8 = "",
    /// 测试用：直接从内存取内容（key 是磁盘路径），非空时不读磁盘
    memory: ?std.StringHashMapUnmanaged([]const u8) = null,

    pub fn init(alloc: std.mem.Allocator, io: std.Io) Loader {
        return .{ .alloc = alloc, .io = io };
    }

    pub fn deinit(self: *Loader) void {
        self.files.deinit(self.alloc);
        self.done.deinit(self.alloc);
        self.stack.deinit(self.alloc);
        if (self.memory) |*m| m.deinit(self.alloc);
        self.* = undefined;
    }

    /// 加载 `root_path` 及其全部依赖。
    pub fn loadRoot(self: *Loader, root_path: []const u8) LoadError!Loaded {
        self.root_dir = normalize(self.alloc, std.fs.path.dirname(root_path) orelse "") catch return error.OutOfMemory;
        const base = std.fs.path.basename(root_path);
        try self.loadOne(base);
        return .{ .files = self.files.items };
    }

    fn normalize(alloc: std.mem.Allocator, p: []const u8) ![]const u8 {
        const out = try alloc.dupe(u8, p);
        for (out) |*c| {
            if (c.* == '\\') c.* = '/';
        }
        var s = out;
        while (std.mem.startsWith(u8, s, "./")) s = s[2..];
        return s;
    }

    fn reportCycle(self: *Loader, closing: []const u8) void {
        std.debug.print("[zgrpc.proto] import 循环：", .{});
        for (self.stack.items) |s| std.debug.print("{s} → ", .{s});
        std.debug.print("{s}\n", .{closing});
    }

    fn loadOne(self: *Loader, name: []const u8) LoadError!void {
        const norm = normalize(self.alloc, name) catch return error.OutOfMemory;
        if (self.done.contains(norm)) return;

        for (self.stack.items) |s| {
            if (std.mem.eql(u8, s, norm)) {
                self.reportCycle(norm);
                return error.ImportCycle;
            }
        }

        const src = try self.readSource(norm);
        const parsed = proto.parse(self.alloc, src) catch |e| return e;

        try self.stack.append(self.alloc, norm);
        for (parsed.imports) |imp| {
            try self.loadOne(imp.path);
        }
        _ = self.stack.pop();

        try self.files.append(self.alloc, .{ .name = norm, .dir = std.fs.path.dirname(norm) orelse "", .proto = parsed });
        try self.done.put(self.alloc, norm, self.files.items.len - 1);
    }

    fn readSource(self: *Loader, name: []const u8) LoadError![]const u8 {
        // 统一成 `/` 分隔（Windows 的 join 会给 `\`，内存表与日志都以 `/` 为准）
        const disk = if (self.root_dir.len == 0) name else blk: {
            const joined = std.fs.path.join(self.alloc, &.{ self.root_dir, name }) catch return error.OutOfMemory;
            break :blk normalize(self.alloc, joined) catch return error.OutOfMemory;
        };

        if (self.memory) |m| {
            if (m.get(disk)) |s| return s;
            std.debug.print("[zgrpc.proto] 找不到 import 文件：{s}（测试内存模式）\n", .{disk});
            return error.ImportNotFound;
        }

        return std.Io.Dir.cwd().readFileAlloc(self.io, disk, self.alloc, .limited(max_file_size)) catch |e| {
            std.debug.print("[zgrpc.proto] 读 {s} 失败：{s}（import 路径按 root 所在目录解析）\n", .{ disk, @errorName(e) });
            return error.ImportNotFound;
        };
    }
};

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

/// 用内存表构造 loader（不碰磁盘），`kv` 是 文件名 → 内容。
fn testLoader(alloc: std.mem.Allocator, kv: []const struct { []const u8, []const u8 }) !Loader {
    var l = Loader.init(alloc, undefined);
    var m: std.StringHashMapUnmanaged([]const u8) = .empty;
    for (kv) |pair| try m.put(alloc, pair[0], pair[1]);
    l.memory = m;
    return l;
}

test "loader：无 import 的单文件" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var l = try testLoader(a, &.{
        .{ "a.proto", "syntax = \"proto3\";\npackage p;\nmessage A { int32 x = 1; }\n" },
    });
    const loaded = try l.loadRoot("a.proto");
    try testing.expectEqual(@as(usize, 1), loaded.files.len);
    try testing.expectEqualStrings("a.proto", loaded.files[0].name);
    try testing.expectEqualStrings("p", loaded.files[0].proto.package);
}

test "loader：import 递归加载 + 拓扑排序（依赖在前）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var l = try testLoader(a, &.{
        .{ "root.proto", "syntax = \"proto3\";\nimport \"common/base.proto\";\nmessage R { int32 x = 1; }\n" },
        .{ "common/base.proto", "syntax = \"proto3\";\nimport \"deep/x.proto\";\nmessage B { int32 y = 1; }\n" },
        .{ "deep/x.proto", "syntax = \"proto3\";\nmessage X { int32 z = 1; }\n" },
    });
    const loaded = try l.loadRoot("root.proto");

    try testing.expectEqual(@as(usize, 3), loaded.files.len);
    // 最深的依赖排最前，root 在最后
    try testing.expectEqualStrings("deep/x.proto", loaded.files[0].name);
    try testing.expectEqualStrings("common/base.proto", loaded.files[1].name);
    try testing.expectEqualStrings("root.proto", loaded.files[2].name);
}

test "loader：菱形依赖只加载一次" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var l = try testLoader(a, &.{
        .{ "root.proto", "syntax = \"proto3\";\nimport \"l.proto\";\nimport \"r.proto\";\n" },
        .{ "l.proto", "syntax = \"proto3\";\nimport \"base.proto\";\n" },
        .{ "r.proto", "syntax = \"proto3\";\nimport \"base.proto\";\n" },
        .{ "base.proto", "syntax = \"proto3\";\nmessage Base { int32 x = 1; }\n" },
    });
    const loaded = try l.loadRoot("root.proto");

    try testing.expectEqual(@as(usize, 4), loaded.files.len);
    var base_count: usize = 0;
    for (loaded.files) |f| {
        if (std.mem.eql(u8, f.name, "base.proto")) base_count += 1;
    }
    try testing.expectEqual(@as(usize, 1), base_count);
}

test "loader：循环 import 报错而不是栈溢出" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var l = try testLoader(a, &.{
        .{ "a.proto", "syntax = \"proto3\";\nimport \"b.proto\";\n" },
        .{ "b.proto", "syntax = \"proto3\";\nimport \"a.proto\";\n" },
    });
    try testing.expectError(error.ImportCycle, l.loadRoot("a.proto"));
}

test "loader：找不到 import 文件时报 ImportNotFound" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var l = try testLoader(a, &.{
        .{ "a.proto", "syntax = \"proto3\";\nimport \"missing.proto\";\n" },
    });
    try testing.expectError(error.ImportNotFound, l.loadRoot("a.proto"));
}

test "loader：子目录里的相对 import 按引用方目录解析" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var l = try testLoader(a, &.{
        .{ "svc/root.proto", "syntax = \"proto3\";\nimport \"types/t.proto\";\n" },
        .{ "svc/types/t.proto", "syntax = \"proto3\";\nmessage T { int32 x = 1; }\n" },
    });
    const loaded = try l.loadRoot("svc/root.proto");
    try testing.expectEqual(@as(usize, 2), loaded.files.len);
    // 反射里的名字是「相对 root 目录」的：与 import 语句原样一致
    try testing.expectEqualStrings("types/t.proto", loaded.files[0].name);
    try testing.expectEqualStrings("root.proto", loaded.files[1].name);
}
