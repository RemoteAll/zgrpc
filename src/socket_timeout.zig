//! **已下沉到 zzig**（2026-11-19）：实现见 `zzig/src/net/socket_timeout.zig`。
//!
//! 本文件只做转发，保持 zgrpc 内部 `@import("socket_timeout.zig")` 的调用点不变。
//! 下沉原因：HTTP 服务端（zzig）需要同一套「原生 socket 读超时 / TCP_NODELAY」，
//! 两处各写一份违反「同一逻辑出现第二份就下沉」的约定。
//!
//! 注意（实测结论已搬过去）：Windows 上 `IOCTL_AFD_SOCKOPT + SO_RCVTIMEO` **结果不稳定**
//! （既出现过 `STATUS_NOT_SUPPORTED` 0xC00000BB，也出现过成功），调用方必须能降级；
//! POSIX（Linux）稳定生效。

const std = @import("std");
const zzig = @import("zzig");

pub const SetError = zzig.socket_timeout.SetError;
pub const setRecvTimeout = zzig.socket_timeout.setRecvTimeout;
pub const setNoDelay = zzig.socket_timeout.setNoDelay;
pub const isReadTimeout = zzig.socket_timeout.isReadTimeout;

test "socket_timeout：超时错误识别（转发后仍生效）" {
    try std.testing.expect(isReadTimeout(error.ConnectionTimedOut));
    try std.testing.expect(isReadTimeout(error.WouldBlock));
    try std.testing.expect(!isReadTimeout(error.ConnectionResetByPeer));
}
