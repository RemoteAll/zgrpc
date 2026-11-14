//! 共享内存（免系统调用）往返延迟基准：回答「换架构能到多少」。
//!
//! 与 `tcp_floor.zig` 对照：那个测的是「走内核 socket」的地板（本机实测 avg≈59µs），
//! 这里测「两个执行体直接读写同一块内存 + 自旋握手」的地板 —— 没有任何系统调用。
//!
//! 现实对应物：Aeron 的 IPC 通道、iceoryx、Chromium Mojo 的共享内存路径、
//! io_uring 的 SQ/CQ 环。它们都把「控制面 + 数据面」放进共享内存，
//! 只在需要阻塞时才碰内核。
//!
//! 用法：`zig build shm-ring -Doptimize=ReleaseFast` → `zig-out\bin\shm_ring.exe [iterations]`
//!
//! 说明：这里用两个线程测（同进程、共享堆内存）。跨进程时**每次操作的成本相同**
//! （都是缓存一致性流量），多出来的只是 `CreateFileMapping`/`MapViewOfFile` 的一次性建图开销，
//! 所以这个数字可以直接当作跨进程 IPC 的地板量级参考。

const std = @import("std");

/// 请求/响应各一个 1 字节的信箱 + 一个序号（缓存行对齐，避免伪共享）
const Mailbox = struct {
    seq: std.atomic.Value(u32) align(64) = std.atomic.Value(u32).init(0),
    payload: u8 align(64) = 0,
};

var req: Mailbox = .{};
var rsp: Mailbox = .{};

const Iterations = 200000;

fn serverMain(iterations: u32) void {
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        const want = i + 1;
        // 自旋等新请求（真实实现里可以用短自旋 → futex/WaitOnAddress 混合）
        while (req.seq.load(.acquire) != want) std.atomic.spinLoopHint();
        const v = req.payload;
        rsp.payload = v;
        rsp.seq.store(want, .release);
    }
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    var iterations: u32 = Iterations;
    {
        var it = std.process.Args.Iterator.initAllocator(init.minimal.args, gpa) catch return;
        defer it.deinit();
        _ = it.skip();
        if (it.next()) |a| iterations = std.fmt.parseInt(u32, a, 10) catch iterations;
    }
    if (iterations == 0) iterations = Iterations;

    const io = init.io;

    const th = try std.Thread.spawn(.{}, serverMain, .{iterations + 1000});
    defer th.join();

    // 预热：让两边都进到自旋循环里，缓存行也在正确状态
    var warm: u32 = 0;
    while (warm < 1000) : (warm += 1) {
        const want = warm + 1;
        req.payload = @truncate(warm);
        req.seq.store(want, .release);
        while (rsp.seq.load(.acquire) != want) std.atomic.spinLoopHint();
    }

    var total_ns: u128 = 0;
    var min_ns: u128 = std.math.maxInt(u128);
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        const want = i + 1001;
        const t0 = std.Io.Clock.now(.boot, io);
        req.payload = @truncate(i);
        req.seq.store(want, .release);
        while (rsp.seq.load(.acquire) != want) std.atomic.spinLoopHint();
        const t1 = std.Io.Clock.now(.boot, io);
        const dt: u128 = @intCast(t1.toNanoseconds() - t0.toNanoseconds());
        total_ns += dt;
        min_ns = @min(min_ns, dt);
    }

    const n: f64 = @floatFromInt(iterations);
    std.debug.print(
        "[shm-ring] iterations={d} avg_ns={d:.1} min_ns={d:.1} (avg_us={d:.3})\n",
        .{
            iterations,
            @as(f64, @floatFromInt(total_ns)) / n,
            @as(f64, @floatFromInt(min_ns)),
            @as(f64, @floatFromInt(total_ns)) / n / 1000.0,
        },
    );
}
