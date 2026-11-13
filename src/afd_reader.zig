//! **Windows AFD 原生读超时**：overlapped IO + `NtWaitForSingleObject` 带时限等待。
//!
//! ## 为什么不是「没有超时」而是「没人用对方式」
//!
//! 读 Zig 0.17.1 源码（`lib/std/Io/Threaded.zig`）可以确认三件事：
//!
//! 1. `netReadWindows` 发起读时**已经是 AFD overlapped 模式**
//!    （`.AfdFlags = .{ .NO_FAST_IO = true, .OVERLAPPED = true }`）；
//! 2. 但它调用 `deviceIoControl` 时把 **event 传成 `null`**、改用 APC 例程等待，
//!    因此在 Zig 内部**无法给这次读设时限**；
//! 3. 状态映射里已经有 `.IO_TIMEOUT => error.ConnectionTimedOut` —— 内核到点就会给这个状态。
//!
//! 结论：只要自己发起同一条 AFD 读，并**传入自己的 event**，就能用
//! `NtWaitForSingleObject(event, 0, timeout)` 做**纯内核层超时**：
//! 到点 `NtCancelIoFileEx` 取消 + 等 IRP 真正收尾，**全程零线程、零轮询**。
//!
//! 这也解释了此前 `CancelIoEx` 为什么会崩：那是 Zig 自己的 `netReadWindows` 里
//! 把 `.CANCELLED => unreachable`（它假设「没人会取消我的读」）；我们自己的路径正常处理该状态。
//!
//! ## 与旧方案的关系
//!
//! 此前用「常驻读线程 + `io.waitTimeout`」实现超时，需要额外线程与缓冲，
//! 且「对端静默」时会滞留一个阻塞线程。本模块改为纯内核层等待后，那些代价全部消失，
//! 读线程实现已删除（失败过程与实测数据仍在 `docs/决策记录.md`）。

const std = @import("std");
const log = @import("log.zig");
const builtin = @import("builtin");
const windows = std.os.windows;

pub const ReadError = error{
    /// 达到了给定的时限
    Timeout,
    ConnectionResetByPeer,
    SocketUnconnected,
    SystemResources,
} || std.Io.UnexpectedError;

/// 在 AFD 句柄上发起一次读，最多等 `timeout_ms`（`null` = 一直等）。
///
/// - 成功返回读到的字节数（`0` 表示对端已关闭）；
/// - 到点返回 `error.Timeout`，**且保证 IRP 已收尾**（不存在悬空 IO，句柄可安全复用/关闭）。
pub fn read(handle: windows.HANDLE, dest: []u8, timeout_ms: ?u32) ReadError!usize {
    if (builtin.os.tag != .windows) return error.Unsupported;
    if (dest.len == 0) return 0;

    var iovecs: [1]windows.AFD.WSABUF(.@"var") = undefined;
    iovecs[0] = .{
        .buf = dest.ptr,
        .len = @intCast(@min(dest.len, std.math.maxInt(u32))),
    };
    const recv_info = windows.AFD.RECV_INFO{
        .BufferArray = &iovecs,
        .BufferCount = 1,
        .AfdFlags = .{ .NO_FAST_IO = true, .OVERLAPPED = true },
        .TdiFlags = .{ .NORMAL = true },
    };

    // AFD 实测**不接受 event 模式**（传 event 会返回 STATUS_ACCESS_DENIED），
    // 但操作本身是异步的（改传 null 时返回 STATUS_PENDING）。
    // 因此这里用 Windows 的另一种等价等待方式：**异步句柄在所有挂起 IO 完成时会 signaling**，
    // 直接 `NtWaitForSingleObject(handle, ...)` 即可带时限等待，无需 event、无需 APC。
    var iosb: windows.IO_STATUS_BLOCK = undefined;
    switch (windows.ntdll.NtDeviceIoControlFile(
        handle,
        null, // event：AFD 不支持
        null, // APC routine：不需要（我们等句柄）
        null,
        &iosb,
        windows.IOCTL.AFD.RECEIVE,
        &recv_info,
        @sizeOf(windows.AFD.RECV_INFO),
        null,
        0,
    )) {
        .SUCCESS => return fromIosb(iosb),
        .PENDING => {},
        else => |status| return mapStatus(status),
    }

    var interval: windows.LARGE_INTEGER = undefined;
    const timeout_arg: ?*const windows.LARGE_INTEGER = if (timeout_ms) |ms| blk: {
        // 负数 = 相对时间，单位 100ns
        interval = -@as(windows.LARGE_INTEGER, @intCast(ms)) * 10_000;
        break :blk &interval;
    } else null;

    switch (windows.ntdll.NtWaitForSingleObject(handle, .FALSE, timeout_arg)) {
        .SUCCESS => return fromIosb(iosb),
        .TIMEOUT => {
            // 取消挂起的读，并**等它真正完成**：否则 iosb 指向的栈内存会悬空。
            var cancel_iosb: windows.IO_STATUS_BLOCK = undefined;
            _ = windows.ntdll.NtCancelIoFileEx(handle, &iosb, &cancel_iosb);
            _ = windows.ntdll.NtWaitForSingleObject(handle, .FALSE, null);
            return error.Timeout;
        },
        else => |status| return mapStatus(status),
    }
}

/// 诊断用：显式指定是否传 event（`null` = 走 Zig 同款「同步/APC」语义）。
///
/// 存在的意义：把「Event 传不传」这一唯一变量单独拉出来实测，
/// 避免把 `STATUS_ACCESS_DENIED` 归因错。
pub fn readDiagnostic(handle: windows.HANDLE, dest: []u8, use_event: bool) ReadError!usize {
    if (dest.len == 0) return 0;
    var iovecs: [1]windows.AFD.WSABUF(.@"var") = undefined;
    iovecs[0] = .{ .buf = dest.ptr, .len = @intCast(@min(dest.len, std.math.maxInt(u32))) };
    const recv_info = windows.AFD.RECV_INFO{
        .BufferArray = &iovecs,
        .BufferCount = 1,
        .AfdFlags = .{ .NO_FAST_IO = true, .OVERLAPPED = true },
        .TdiFlags = .{ .NORMAL = true },
    };
    var event: windows.HANDLE = undefined;
    if (use_event) {
        if (windows.ntdll.NtCreateEvent(
            &event,
            .{ .STANDARD = .{ .SYNCHRONIZE = true } },
            null,
            .Synchronization,
            .FALSE,
        ) != .SUCCESS) return error.Unexpected;
    }
    defer if (use_event) {
        _ = windows.CloseHandle(event);
    };

    var iosb: windows.IO_STATUS_BLOCK = undefined;
    const status = windows.ntdll.NtDeviceIoControlFile(
        handle,
        if (use_event) event else null,
        null,
        null,
        &iosb,
        windows.IOCTL.AFD.RECEIVE,
        &recv_info,
        @sizeOf(windows.AFD.RECV_INFO),
        null,
        0,
    );
    log.afd.debug(
        "诊断：use_event={} -> NTSTATUS 0x{X}（SUCCESS=0x0, PENDING=0x103）",
        .{ use_event, @backingInt(status) },
    );
    if (status == .SUCCESS) return iosb.Information;
    if (use_event) {
        if (status == .PENDING) {
            const r = windows.ntdll.NtWaitForSingleObject(event, .FALSE, null);
            log.afd.debug("诊断：等待完成 -> 0x{X}", .{@backingInt(r)});
            return fromIosb(iosb);
        }
        // event 已置位时也会走到这
        if (status == .TIMEOUT) return error.Timeout;
    }
    return mapStatus(status);
}
/// 读是否属于「原生超时」。
pub fn isTimeout(err: anyerror) bool {
    return err == error.Timeout or err == error.ConnectionTimedOut;
}

fn fromIosb(iosb: windows.IO_STATUS_BLOCK) ReadError!usize {
    return switch (iosb.u.Status) {
        .SUCCESS => iosb.Information,
        .IO_TIMEOUT => error.Timeout,
        .CANCELLED => error.Unexpected, // 正常情况下我们自己取消会走 TIMEOUT 分支
        .CONNECTION_RESET, .REMOTE_DISCONNECT, .LOCAL_DISCONNECT => error.ConnectionResetByPeer,
        .INSUFFICIENT_RESOURCES => error.SystemResources,
        else => |status| mapStatus(status),
    };
}

fn mapStatus(status: windows.NTSTATUS) ReadError {
    return switch (status) {
        .TIMEOUT => error.Timeout,
        .CANCELLED, .INVALID_DEVICE_REQUEST => error.Unexpected,
        .INSUFFICIENT_RESOURCES => error.SystemResources,
        .CONNECTION_RESET, .REMOTE_DISCONNECT, .LOCAL_DISCONNECT => error.ConnectionResetByPeer,
        .CONNECTION_ABORTED, .CONNECTION_DISCONNECTED => error.ConnectionResetByPeer,
        else => |s| blk: {
            log.afd.warn(
                "未预期的 AFD 读状态：NTSTATUS 0x{X}",
                .{@backingInt(s)},
            );
            break :blk error.Unexpected;
        },
    };
}

test "afd_reader：超时错误识别" {
    try std.testing.expect(isTimeout(error.Timeout));
    try std.testing.expect(!isTimeout(error.ConnectionResetByPeer));
}
