//! 单调时钟 + 睡眠（零依赖）：给 `grpc-timeout` 的截止时间用。
//!
//! 为什么不用实时钟：deadline 必须「只向前、不被系统时间跳变影响」，否则
//! 校时（NTP）或手工改表会让超时判定忽早忽晚。Zig 0.17.1 已移除
//! `std.time.nanoTimestamp`，而 `std.Io.Clock` 需要一个 `std.Io` 实例
//! （本库是纯同步 IO，不调用点拿不到 io），因此这里直接走 **std 已声明的原生入口**：
//!
//! - Windows：`std.os.windows.ntdll.RtlQueryPerformanceCounter/Frequency`
//!   （即 std 自己在 `Io/Threaded.zig` 里用的同一组函数，单调、高精度）
//! - POSIX：`clock_gettime(CLOCK_MONOTONIC)`
//! - 其它：退回实时钟（尽力而为）
//!
//! 全程**没有任何手写 `extern`**：用到的每个系统函数都在标准库里已有声明。
//!
//! 单位统一纳秒，基准点未定义（调用方只能做差值，不能当墙钟使用）。

const std = @import("std");
const builtin = @import("builtin");

const win = std.os.windows;

/// 单调时钟当前纳秒数。只保证单调递增，基准点未定义。
pub fn monotonicNs() i128 {
    switch (builtin.os.tag) {
        .windows => return windowsMonotonicNs(),
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos, .haiku => {
            var ts: std.c.timespec = undefined;
            if (std.c.clock_gettime(.MONOTONIC, &ts) != 0) return realtimeNs();
            return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
        },
        else => return realtimeNs(),
    }
}

fn windowsMonotonicNs() i128 {
    var counter: i64 = 0;
    var freq: i64 = 0;
    if (win.ntdll.RtlQueryPerformanceCounter(&counter) == .FALSE) return realtimeNs();
    if (win.ntdll.RtlQueryPerformanceFrequency(&freq) == .FALSE or freq <= 0) return realtimeNs();
    // 先扩到 i128 再除，避免大计数器溢出
    return @divTrunc(@as(i128, counter) * std.time.ns_per_s, @as(i128, freq));
}

/// 实时钟（兜底用；不单调，仅当单调时钟不可用时退而求其次）。
pub fn realtimeNs() i128 {
    switch (builtin.os.tag) {
        .windows => {
            // `RtlGetSystemTimePrecise` 返回自 1601-01-01 起的 100ns 计数（LARGE_INTEGER）
            const ticks_100ns: i128 = win.ntdll.RtlGetSystemTimePrecise();
            const unix_epoch_100ns: i128 = 116_444_736_000_000_000; // 1601-01-01 → 1970-01-01
            return (ticks_100ns - unix_epoch_100ns) * 100;
        },
        else => {
            var ts: std.c.timespec = undefined;
            if (std.c.clock_gettime(.REALTIME, &ts) != 0) return 0;
            return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
        },
    }
}

/// 睡眠（跨平台；Zig 0.17.1 已移除 `std.Thread.sleep`）。
pub fn sleepNs(ns: u64) void {
    if (ns == 0) return;
    switch (builtin.os.tag) {
        .windows => {
            // NtDelayExecution 的间隔单位是 100ns，负数表示相对时间
            var interval_100ns: i64 = -@as(i64, @intCast(@divTrunc(ns + 99, 100)));
            _ = std.os.windows.ntdll.NtDelayExecution(@enumFromInt(1), &interval_100ns);
        },
        else => {
            var req = std.c.timespec{
                .sec = @intCast(ns / std.time.ns_per_s),
                .nsec = @intCast(ns % std.time.ns_per_s),
            };
            while (std.c.nanosleep(&req, &req) != 0) {}
        },
    }
}

test "clock：单调时钟递增" {
    const t0 = monotonicNs();
    sleepNs(2 * std.time.ns_per_ms);
    const t1 = monotonicNs();
    try std.testing.expect(t1 > t0);
    try std.testing.expect(t1 - t0 >= std.time.ns_per_ms);
}

test "clock：实时钟像样的 Unix 纳秒时间（> 2020 年）" {
    const t = realtimeNs();
    try std.testing.expect(t > @as(i128, 1_577_836_800) * std.time.ns_per_s);
}
