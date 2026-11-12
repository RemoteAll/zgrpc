//! 客户端本地读超时：**看门狗线程 + 原生取消挂起 IO**。
//!
//! ## 为什么不是 `poll` / `select` / `SO_RCVTIMEO`（都实测过）
//!
//! Zig 0.17 在 Windows 上的网络实现走的是 **AFD**（`\Device\Afd`）设备句柄 + overlapped IO **，
//! 不是 Winsock 的 SOCKET：
//! - `WSAPoll` / `select` → `WSAENOTSOCK`（句柄不是 socket）；
//! - `setsockopt(SO_RCVTIMEO)` → 只约束 Winsock 的 `recv()`，对 AFD 的 `IOCTL_AFD.RECEIVE` 无效
//!   （实测设 200ms，读照样阻塞 10 秒以上）；
//! - `io.operateTimeout(.{ .net_read = ... })` → `netReadWindows` 同步且无 cancel 参数，
//!   直接返回 `error.ConcurrencyUnavailable`；
//! - `std.posix.poll` → Windows 上直接 `@compileError("use std.Io instead")`。
//!
//! ## AFD 模型下的原生手段
//!
//! overlapped IO 是**可取消**的：调用 **`CancelIoEx(handle, null)`** 会取消该句柄上所有挂起操作，
//! 阻塞中的读立刻以取消完成 —— 这才是 Windows 上的「原生超时」。
//! POSIX 侧则用 `shutdown(.recv)` 唤醒阻塞读（同效）。
//!
//! 于是实现为：调用开始时起一个看门狗线程，到点执行上述取消动作；调用正常结束时通知它退出。

const std = @import("std");
const builtin = @import("builtin");

pub const TimeoutGuard = struct {
    const State = struct {
        /// 调用方已收尾，看门狗可以退出
        done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        /// 看门狗确实触发了取消（= 本次失败是超时，不是对端断开）
        fired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };

    /// 分片睡眠粒度：决定「正常完成后看门狗多久退出」
    const poll_ns: u64 = 5 * std.time.ns_per_ms;

    alloc: std.mem.Allocator,
    state: *State,
    thread: std.Thread,

    /// 启动看门狗：`ns` 纳秒后若仍未 `cancel`，就取消这条连接上挂起的读。
    pub fn start(
        alloc: std.mem.Allocator,
        stream: std.Io.net.Stream,
        io: std.Io,
        ns: u64,
    ) !TimeoutGuard {
        const state = try alloc.create(State);
        state.* = .{};
        errdefer alloc.destroy(state);
        const thread = try std.Thread.spawn(.{}, run, .{ state, stream, io, ns });
        return .{ .alloc = alloc, .state = state, .thread = thread };
    }

    fn run(state: *State, stream: std.Io.net.Stream, io: std.Io, ns: u64) void {
        var waited: u64 = 0;
        while (waited < ns) {
            const slice = @min(poll_ns, ns - waited);
            @import("clock.zig").sleepNs(slice);
            waited += slice;
            if (state.done.load(.acquire)) return;
        }
        if (state.done.load(.acquire)) return;
        state.fired.store(true, .release);
        abortPendingRead(stream, io);
    }

    /// 是否由看门狗触发（= 本次失败是超时）。
    pub fn fired(self: *const TimeoutGuard) bool {
        return self.state.fired.load(.acquire);
    }

    /// 收尾：通知看门狗退出并回收（最多等一个 poll 粒度）。
    pub fn cancel(self: *TimeoutGuard) void {
        self.state.done.store(true, .release);
        self.thread.join();
        self.alloc.destroy(self.state);
    }
};

/// 取消该连接上挂起的读。
///
/// ⚠️ 不能用 `CancelIoEx`：Zig 0.17 的 `netReadWindows` 把 `STATUS_CANCELLED` 写成
/// `unreachable`（它假设调用方不会取消），一旦取消就直接 panic（已实测）。
/// 所以统一走 `shutdown`：挂起的 AFD 读会以「连接断开」完成，映射为
/// `error.ConnectionResetByPeer` 之类的普通错误，不会 panic。
fn abortPendingRead(stream: std.Io.net.Stream, io: std.Io) void {
    stream.shutdown(io, .both) catch {};
}

test "timeout_guard：启动后立刻收尾不会误报超时" {
    var gpa_state = std.heap.DebugAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    // 只验证「启动 → 立刻 cancel」这条路径不误报；
    // 真实超时由 examples/client_timeout_probe.zig 用真 socket 端到端验证。
    const fake: std.Io.net.Stream = undefined;
    var g = try TimeoutGuard.start(gpa, fake, undefined, 10 * std.time.ns_per_ms);
    try std.testing.expect(!g.fired());
    g.cancel(); // 注意：cancel 会销毁 state，之后不能再调 fired()
}
