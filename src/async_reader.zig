//! 带**超时**的 socket 读：常驻读线程 + `io.waitTimeout`（futex / `WaitOnAddress`，跨平台）。
//!
//! ## 为什么需要它（Zig 0.17 在 Windows 上没有可用的「读超时」）
//!
//! 六条路全部实测失败（详见 `docs/决策记录.md` 增补十六）：
//! `io.operateTimeout(net_read)` → `ConcurrencyUnavailable`；`std.posix.poll` → Windows 上 `@compileError`；
//! `WSAPoll` → `WSAENOTSOCK`（Zig 用 AFD 句柄）；`setsockopt(SO_RCVTIMEO)` → 对 AFD 读无效；
//! `CancelIoEx` → std 里 `STATUS_CANCELLED` 是 `unreachable`（panic）；`shutdown` → 唤不醒挂起读。
//!
//! 结论：**底层读一旦挂起就无法安全打断**，所以只能在**用户态**做超时：
//! 读交给一条常驻线程，调用方在条件变量上**带超时等待**数据到达。
//!
//! ## 语义与代价
//!
//! - 超时返回 `error.Timeout`，但底层读线程可能仍阻塞在 socket 上；
//!   调用方应随后**关闭连接**（`close` 会让读线程以错误退出）。这符合 gRPC 客户端超时的通常做法：
//!   调用已失败，连接不再复用。
//! - 正常路径零额外拷贝语义变化：数据先进内部缓冲，`readExact` 从缓冲取；`std.Io.Reader`
//!   的「必须读满」语义保持不变。

const std = @import("std");
const clock = @import("clock.zig");

pub const AsyncReader = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,

    mutex: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,

    /// 已读入但尚未被调用方消费的数据
    buf: std.ArrayListUnmanaged(u8) = .empty,
    consumed: usize = 0,
    /// 读线程结束原因：null 表示还在跑
    finished: bool = false,
    eof: bool = false,
    err: ?anyerror = null,

    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: std.Thread,

    /// 起读线程。`alloc` 用于内部缓冲，需线程安全。
    pub fn start(
        alloc: std.mem.Allocator,
        io: std.Io,
        stream: std.Io.net.Stream,
    ) !*AsyncReader {
        const self = try alloc.create(AsyncReader);
        errdefer alloc.destroy(self);
        self.* = .{ .alloc = alloc, .io = io, .stream = stream, .thread = undefined };
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        return self;
    }

    fn run(self: *AsyncReader) void {
        var tmp: [16 * 1024]u8 = undefined;
        while (!self.stop.load(.acquire)) {
            var bufs = [1][]u8{&tmp};
            const op = std.Io.Operation{
                .net_read = .{
                    .socket_handle = self.stream.socket.handle,
                    .data = &bufs,
                },
            };
            const op_res = self.io.operate(op) catch |e| {
                self.finishWith(e, false);
                return;
            };
            const res = op_res.net_read catch |e| {
                self.finishWith(e, false);
                return;
            };
            if (res.data_len == 0) {
                self.finishWith(null, true); // 对端关闭
                return;
            }
            self.push(tmp[0..res.data_len]) catch |e| {
                self.finishWith(e, false);
                return;
            };
        }
        self.finishWith(null, false);
    }

    fn push(self: *AsyncReader, data: []const u8) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        // 消费完的旧数据顺手回收，避免无限增长
        if (self.consumed > 0) {
            const rest = self.buf.items.len - self.consumed;
            std.mem.copyForwards(u8, self.buf.items[0..rest], self.buf.items[self.consumed..]);
            self.buf.shrinkRetainingCapacity(rest);
            self.consumed = 0;
        }
        try self.buf.appendSlice(self.alloc, data);
        self.cond.signal(self.io);
    }

    fn finishWith(self: *AsyncReader, err: ?anyerror, eof: bool) void {
        self.mutex.lock(self.io) catch return;
        defer self.mutex.unlock(self.io);
        self.finished = true;
        self.eof = eof;
        self.err = err;
        self.cond.signal(self.io);
    }

    /// 读满 `dest`。`deadline_ns` 为单调时钟的绝对截止时刻（null = 一直等）。
    pub fn readExact(self: *AsyncReader, dest: []u8, deadline_ns: ?i128) !void {
        var off: usize = 0;
        while (off < dest.len) {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);

            if (self.consumed < self.buf.items.len) {
                const avail = self.buf.items.len - self.consumed;
                const take = @min(avail, dest.len - off);
                @memcpy(dest[off..][0..take], self.buf.items[self.consumed..][0..take]);
                self.consumed += take;
                off += take;
                continue;
            }

            if (self.err) |e| return e;
            if (self.eof) return error.EndOfStream;
            if (self.finished) return error.EndOfStream;

            if (deadline_ns) |d| {
                const remain = d - clock.monotonicNs();
                if (remain <= 0) return error.Timeout;
                const ns: u64 = @intCast(@min(remain, @as(i128, std.math.maxInt(u64))));
                self.cond.waitTimeout(self.io, &self.mutex, .{
                    .duration = .{ .raw = .{ .nanoseconds = ns }, .clock = .awake },
                }) catch |e| switch (e) {
                    error.Timeout => return error.Timeout,
                    else => return e,
                };
            } else {
                try self.cond.wait(self.io, &self.mutex);
            }
        }
    }

    /// 读线程是否已经自行结束（遇到错误 / EOF）。
    pub fn isFinished(self: *AsyncReader) bool {
        self.mutex.lock(self.io) catch return false;
        defer self.mutex.unlock(self.io);
        return self.finished;
    }

    /// 尝试完整回收：只有读线程已自行结束才安全 join + 释放。
    /// 返回 false 表示「线程仍阻塞在底层读上」，此时不能释放（否则 use-after-free）。
    pub fn tryDestroy(self: *AsyncReader) bool {
        if (!self.isFinished()) return false;
        self.stop.store(true, .release);
        self.thread.join();
        {
            self.mutex.lock(self.io) catch {};
            self.buf.deinit(self.alloc);
            self.mutex.unlock(self.io);
        }
        const alloc = self.alloc;
        alloc.destroy(self);
        return true;
    }
};

test "async_reader：结构可构造（真实读由 examples/client_timeout_probe.zig 验证）" {
    // 这个类型依赖真实 socket，单测只保证编译期形态正确
    try std.testing.expect(@sizeOf(AsyncReader) > 0);
}
