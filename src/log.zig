//! 统一日志门面 —— **转发到 zzig 的日志组件**（规则对齐 DH.NCore，经 DH.RustBase 落地）。
//!
//! 为什么要有这一层：
//! - zGRPC 的日志需要**按模块区分**（`zgrpc.h2` / `zgrpc.server` / `zgrpc.client` …），
//!   直接调 `zzig.Logger.info` 会丢掉模块名，排查时只能靠关键字猜；
//! - 这里只做两件事：① 把 `Scope` 前缀作为格式参数注入；② 把级别/配置口径转发给 zzig，
//!   **不再自己实现级别判断与行格式**（那是 zzig 的职责，且必须与 DH.NCore 一致）。
//!
//! 级别与行格式全部来自 zzig（对齐 DH.NCore `LogLevel` / `Setting.LogLineFormat`）：
//! `All/Debug/Info/Warn/Error/Fatal/Off`，默认 `Info`；行默认
//! `HH:MM:SS.mmm ThreadId Kind Name Message`。
//!
//! 用法：
//! ```zig
//! const log = zgrpc.log;
//! _ = log.applySettings(.{ .LogLevel = "Debug" }); // 或 applyEnv / applyToml
//! ```
//!
//! 注意：`Debug` 级别下才看得到的都是**高频明细**（keepalive 逐连接跳过、重试、连接收尾等），
//! 默认 `Info` 只是「不刷屏」，一条都没删 —— 需要时调级别就能全部看到。

const std = @import("std");
const zzig = @import("zzig");

/// 日志级别（直接透出 zzig 的 `Level`，对齐 DH.NCore `LogLevel`）。
pub const Level = zzig.Logger.Level;

/// 行格式（直接透出 zzig 的 `LineFormat`）。
pub const LineFormat = zzig.Logger.LineFormat;

pub const setLevel = zzig.Logger.setLevel;
pub const getLevel = zzig.Logger.getLevel;
pub const enabled = zzig.Logger.enabled;
pub const setLineFormat = zzig.Logger.setLineFormat;
pub const getLineFormat = zzig.Logger.getLineFormat;

/// 解析级别名（不区分大小写，容忍 trace/warning/error/none 等别名）。
pub const parseLevel = Level.parse;

/// 日志相关配置项。
///
/// 字段名**按 DH.NCore `Setting` 的 PascalCase 命名**，以便同一份配置文件
/// （TOML/JSON）在 C# / Rust / Zig 三端通用。
pub const Settings = struct {
    /// 只输出 >= 该级别的日志。`All/Debug/Info/Warn/Error/Fatal/Off`，默认 `Info`
    LogLevel: []const u8 = "Info",
};

/// 应用配置：解析 `LogLevel` 并设置级别。
///
/// 非法值**不改变**当前级别，但会打一条 warn（含非法值与当前生效值），
/// 避免「配错了却毫无反应」这种最难查的情况。
pub fn applySettings(s: Settings) Level {
    if (Level.parse(s.LogLevel)) |lv| {
        setLevel(lv);
    } else {
        std.debug.print("[zgrpc.log] [WARN] LogLevel=\"{s}\" 非法（应为 All/Debug/Info/Warn/Error/Fatal/Off），保持 {s}\n", .{ s.LogLevel, getLevel().asStr() });
    }
    return getLevel();
}

/// 环境变量覆盖：优先 `ZGRPC_LOG`，其次沿用 zzig 的 `ZZIG_LOG`。
///
/// 环境变量通常用于「临时排障把级别调到 debug」，因此优先级高于配置文件。
pub fn applyEnv(environ_map: *const std.process.Environ.Map) Level {
    if (environ_map.get("ZGRPC_LOG")) |raw| {
        if (Level.parse(raw)) |lv| {
            setLevel(lv);
            return getLevel();
        }
    }
    return zzig.Logger.applyEnv(environ_map);
}

/// 从 TOML 配置文件读取日志配置（用 zzig 的配置组件；文件缺失时按模板创建并保持默认）。
///
/// 配置形状（与 DH.NCore 的 `Setting` 同名）：
/// ```toml
/// LogLevel = "Debug"
/// ```
pub fn applyToml(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Level {
    var file = zzig.config.FileText.open(io, allocator, path, "LogLevel = \"Info\"\n", .{}) catch |e| {
        std.debug.print("[zgrpc.log] [WARN] 打开日志配置 {s} 失败（{s}），保持当前级别 {s}\n", .{ path, @errorName(e), getLevel().asStr() });
        return e;
    };
    defer file.deinit();

    const table = zzig.config.toml.parse(allocator, file.current()) catch |e| {
        std.debug.print("[zgrpc.log] [WARN] 解析日志配置 {s} 失败（{s}），保持当前级别 {s}\n", .{ path, @errorName(e), getLevel().asStr() });
        return e;
    };
    const settings = zzig.config.toml.decode(Settings, allocator, table) catch |e| {
        std.debug.print("[zgrpc.log] [WARN] 解码日志配置 {s} 失败（{s}），保持当前级别 {s}\n", .{ path, @errorName(e), getLevel().asStr() });
        return e;
    };
    return applySettings(settings);
}

/// 按模块划分的日志作用域：统一 `[zgrpc.xxx]` 前缀，便于 `grep '\[zgrpc.server\]'` 过滤。
pub const Scope = struct {
    tag: []const u8,

    pub fn debug(self: Scope, comptime fmt: []const u8, args: anytype) void {
        zzig.Logger.debug("[{s}] " ++ fmt, .{self.tag} ++ args);
    }
    pub fn info(self: Scope, comptime fmt: []const u8, args: anytype) void {
        zzig.Logger.info("[{s}] " ++ fmt, .{self.tag} ++ args);
    }
    pub fn warn(self: Scope, comptime fmt: []const u8, args: anytype) void {
        zzig.Logger.warn("[{s}] " ++ fmt, .{self.tag} ++ args);
    }
    pub fn err(self: Scope, comptime fmt: []const u8, args: anytype) void {
        zzig.Logger.err("[{s}] " ++ fmt, .{self.tag} ++ args);
    }
    pub fn fatal(self: Scope, comptime fmt: []const u8, args: anytype) void {
        zzig.Logger.fatal("[{s}] " ++ fmt, .{self.tag} ++ args);
    }
};

pub const h2 = Scope{ .tag = "zgrpc.h2" };
pub const grpc = Scope{ .tag = "zgrpc.grpc" };
pub const server = Scope{ .tag = "zgrpc.server" };
pub const client = Scope{ .tag = "zgrpc.client" };
pub const caller = Scope{ .tag = "zgrpc.caller" };
pub const proto = Scope{ .tag = "zgrpc.proto" };
pub const socket = Scope{ .tag = "zgrpc.socket_timeout" };
pub const afd = Scope{ .tag = "zgrpc.afd" };

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "log：级别交由 zzig 判定（默认 info，debug 明细默认不可见但可开）" {
    const before = getLevel();
    defer setLevel(before);

    setLevel(.info);
    try testing.expect(!enabled(.debug));
    try testing.expect(enabled(.err));

    setLevel(.debug);
    try testing.expect(enabled(.debug)); // 想看就能看到

    setLevel(.off);
    try testing.expect(!enabled(.fatal));
}

test "log：级别名解析复用 zzig（含别名与非法值）" {
    try testing.expectEqual(Level.debug, parseLevel("TRACE").?);
    try testing.expectEqual(Level.warn, parseLevel("warning").?);
    try testing.expectEqual(Level.err, parseLevel("error").?);
    try testing.expectEqual(@as(?Level, null), parseLevel("verbose"));
}

test "log：applySettings 读 NCore 同名配置项 LogLevel；非法值不改级别" {
    const before = getLevel();
    defer setLevel(before);

    setLevel(.info);
    try testing.expectEqual(Level.debug, applySettings(.{ .LogLevel = "Debug" }));
    try testing.expectEqual(Level.debug, getLevel());

    // 非法值：级别保持，不崩
    try testing.expectEqual(Level.debug, applySettings(.{ .LogLevel = "乱写" }));
    try testing.expectEqual(Level.debug, getLevel());

    // 默认值就是 Info
    const default_settings = Settings{};
    try testing.expectEqualStrings("Info", default_settings.LogLevel);
}

test "log：applyEnv 优先 ZGRPC_LOG，其次沿用 ZZIG_LOG" {
    const before = getLevel();
    defer setLevel(before);

    var env = std.process.Environ.Map.init(testing.allocator);
    defer env.deinit();

    setLevel(.info);
    try testing.expectEqual(Level.info, applyEnv(&env)); // 都没有 → 保持

    try env.put("ZZIG_LOG", "warn");
    try testing.expectEqual(Level.warn, applyEnv(&env));

    try env.put("ZGRPC_LOG", "debug"); // zgrpc 自己的优先
    try testing.expectEqual(Level.debug, applyEnv(&env));
}
