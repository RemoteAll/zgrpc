//! **原生 socket 读超时**：Windows 走 AFD 层 `SO_RCVTIMEO`，POSIX 走 `setsockopt(SO_RCVTIMEO)`。
//!
//! ## 为什么这条路能成（读 Zig 0.17.1 源码得出的结论）
//!
//! 之前用 `ws2_32.setsockopt(SO_RCVTIMEO)` 实测「设置成功但读照旧阻塞」，原因是
//! **Zig 0.17 的 socket 是 AFD 句柄（`\Device\Afd`），不是 Winsock SOCKET**，
//! Winsock 的 `setsockopt` 根本没落到这个句柄上。
//!
//! 而 `std/Io/Threaded.zig` 里 Zig 自己是这么干的：
//!
//! ```zig
//! fn setSocketOptionAfd(socket, level, opt_name, opt_val) !void {
//!     try socketOptionAfd(socket, .set, level, opt_name, ...);   // → IOCTL_AFD_SOCKOPT
//! }
//! ```
//!
//! 并且 `netReadWindows` 在发起读时会带上 `.AfdFlags = .{ .NO_FAST_IO = true, .OVERLAPPED = true }`，
//! 其状态映射里有：
//!
//! ```zig
//! .IO_TIMEOUT => return error.ConnectionTimedOut,
//! ```
//!
//! 也就是说：**只要通过 `IOCTL_AFD_SOCKOPT` 设上 `SO_RCVTIMEO`，AFD 内核层自己就会在到点时
//! 返回 `STATUS_IO_TIMEOUT`，Zig 会把它翻译成 `error.ConnectionTimedOut`** —— 这就是原生超时，
//! 不需要读线程、不需要 poll、不需要取消 IO。
//!
//! ## 使用提示
//!
//! - `ms == 0` 表示「不限」。
//! - 超时后连接**不要复用**（读方向已不确定），重新建连。
//! - AFD 的 `SO_RCVTIMEO` 是**每次读**的上限，不是整次调用的总量；调用的整体时限仍由
//!   `Client` 的 deadline 兜。

const std = @import("std");
const builtin = @import("builtin");

pub const SetError = error{
    /// 该平台不支持在已建立的连接上设置读超时
    Unsupported,
    /// 底层返回了失败状态
    SetFailed,
    SystemResources,
};

/// 给 socket 设置**原生读超时**（毫秒）。`ms == 0` 表示不限。
pub fn setRecvTimeout(io: std.Io, stream: std.Io.net.Stream, ms: u32) SetError!void {
    if (builtin.os.tag == .windows) {
        return setRecvTimeoutAfd(io, stream, ms);
    }
    return setRecvTimeoutPosix(stream, ms);
}

/// Windows：`IOCTL_AFD_SOCKOPT` + `SO_RCVTIMEO`。
///
/// 注意用的是 `windows.ws2_32.SOL.SOCKET` / `SO.RCVTIMEO` 常量，但**调用路径是 AFD**，
/// 不是 Winsock 库函数 —— 这正是之前失败的那一步与现在成功的一步的唯一区别。
fn setRecvTimeoutAfd(io: std.Io, stream: std.Io.net.Stream, ms: u32) SetError!void {
    if (builtin.os.tag != .windows) unreachable;
    var value: u32 = ms;
    const info = std.os.windows.AFD.SOCKOPT_INFO{
        .mode = .set,
        .level = std.os.windows.ws2_32.SOL.SOCKET,
        .optname = std.os.windows.ws2_32.SO.RCVTIMEO,
        .optval = @ptrCast(&value),
        .optlen = @sizeOf(u32),
    };
    const result = io.operate(.{ .device_io_control = .{
        .file = .{ .handle = stream.socket.handle, .flags = .{ .nonblocking = true } },
        .code = std.os.windows.IOCTL.AFD.SOCKOPT,
        .in = std.mem.asBytes(&info),
    } }) catch |e| {
        std.debug.print("[zgrpc.socket_timeout] AFD SOCKOPT 调用失败：{s}\n", .{@errorName(e)});
        return error.SetFailed;
    };
    switch (result.device_io_control.u.Status) {
        .SUCCESS => return,
        .INSUFFICIENT_RESOURCES => return error.SystemResources,
        .CANCELLED => return error.SetFailed,
        else => |status| {
            std.debug.print(
                "[zgrpc.socket_timeout] 设置 SO_RCVTIMEO={d}ms 失败：status=0x{X}（AFD SOCKOPT）\n",
                .{ ms, @intFromEnum(status) },
            );
            return error.SetFailed;
        },
    }
}

/// POSIX：标准 `setsockopt(SO_RCVTIMEO)`。超时后 `read` 返回 `error.WouldBlock`。
fn setRecvTimeoutPosix(stream: std.Io.net.Stream, ms: u32) SetError!void {
    if (builtin.os.tag == .windows) unreachable;
    if (builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.Unsupported;
    const secs: i64 = @intCast(ms / 1000);
    const usecs: i64 = @intCast((ms % 1000) * 1000);
    var tv: std.posix.timeval = .{ .sec = secs, .usec = usecs };
    std.posix.setsockopt(
        stream.socket.handle,
        std.posix.SOL.SOCKET,
        std.posix.SO.RCVTIMEO,
        std.mem.asBytes(&tv),
    ) catch |e| {
        std.debug.print("[zgrpc.socket_timeout] setsockopt(SO_RCVTIMEO={d}ms) 失败：{s}\n", .{ ms, @errorName(e) });
        return error.SetFailed;
    };
}

/// 读错误是否属于「原生读超时」。
///
/// - Windows：AFD 到点返回 `STATUS_IO_TIMEOUT` → Zig 映射为 `error.ConnectionTimedOut`；
/// - POSIX：`SO_RCVTIMEO` 到点 → `EAGAIN` → Zig 映射为 `error.WouldBlock`。
pub fn isReadTimeout(err: anyerror) bool {
    return switch (err) {
        error.ConnectionTimedOut, error.WouldBlock => true,
        else => false,
    };
}

test "socket_timeout：超时错误识别" {
    try std.testing.expect(isReadTimeout(error.ConnectionTimedOut));
    try std.testing.expect(isReadTimeout(error.WouldBlock));
    try std.testing.expect(!isReadTimeout(error.ConnectionResetByPeer));
    try std.testing.expect(!isReadTimeout(error.Timeout));
}
