//! HTTP/2（RFC 9113 / RFC 7540）**明文 h2c（prior-knowledge）服务端**。
//!
//! 只做 gRPC 需要的那部分，刻意不做的事（都写在「未支持」里，避免误解为完整实现）：
//! - 不做 TLS/ALPN（由前置反代终止 TLS，或以明文供服务间直连）；
//! - 不做服务端推送（`PUSH_PROMISE` 只跳过）；
//! - 不做优先级调度（`PRIORITY` 只跳过）；
//! - handler 仍是**同步串行执行**（一次跑一个流的业务），但**流的接收是多路复用的**：
//!   多个流可以同时处于半开状态（已收 HEADERS、body 未收完），互不踢掉 —— 真实 gRPC 客户端
//!   在一条连接上并发调用时不会被打回 `REFUSED_STREAM`；
//! - 发送受流控阻塞时，期间读到的业务帧进 `pending` 队列延后处理（不会丢，也不会误拒）。
//!
//! ## 发送侧：**延迟 flush**（对齐 Go `net/http2` 的 loopyWriter 批量写）
//!
//! 一个 unary 响应原本要写 3 帧（HEADERS / DATA / trailers HEADERS），如果每帧都 flush，
//! 就是 **3 次 write 系统调用/请求**。这里改为：帧只写进发送缓冲，**只在即将真正阻塞读 socket 前**
//! 才 flush 一次，于是同一批到达的 N 个请求共享 1 次 write。单连接 64 并发的实测收益见
//! `bench_pair/README.md`。
//!
//! 安全性由三点保证：
//! 1. 每次**可能阻塞**的读之前都调 `flushForRead`（否则就是对端等我们、我们等对端的死锁）；
//! 2. 发送缓冲到高水位（`flush_high_water`）立刻 flush，避免对端持续发帧导致迟迟不刷；
//! 3. 控制帧与跨线程调用（keepalive PING）走 `writeFrameFlush`，语义不变。
//!
//! 已支持：`SETTINGS`(含 ACK/并发流上限)、`HEADERS`/`CONTINUATION`（含 PADDED/PRIORITY）、
//! `DATA`（含 PADDED、流控回补）、`WINDOW_UPDATE`（连接级 + 每流）、`PING`(ACK)、`RST_STREAM`、`GOAWAY`。

const std = @import("std");
const log = @import("log.zig");
const hpack = @import("hpack.zig");

/// 客户端连接前言（RFC 9113 §3.4）。
pub const client_preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub const FrameType = enum(u8) {
    data = 0x0,
    headers = 0x1,
    priority = 0x2,
    rst_stream = 0x3,
    settings = 0x4,
    push_promise = 0x5,
    ping = 0x6,
    goaway = 0x7,
    window_update = 0x8,
    continuation = 0x9,
    _,
};

pub const Flags = struct {
    pub const end_stream: u8 = 0x1;
    /// SETTINGS 与 PING 复用同一位
    pub const ack: u8 = 0x1;
    pub const end_headers: u8 = 0x4;
    pub const padded: u8 = 0x8;
    pub const priority: u8 = 0x20;
};

pub const ErrorCode = enum(u32) {
    no_error = 0x0,
    protocol_error = 0x1,
    internal_error = 0x2,
    flow_control_error = 0x3,
    settings_timeout = 0x4,
    stream_closed = 0x5,
    frame_size_error = 0x6,
    refused_stream = 0x7,
    cancel = 0x8,
    compression_error = 0x9,
    connect_error = 0xa,
    enhance_your_calm = 0xb,
    inadequate_security = 0xc,
    http_1_1_required = 0xd,
    _,
};

pub const default_max_frame_size: usize = 16384;
pub const default_window_size: i64 = 65535;

/// 空闲流对象（含 arena 已分配容量）的复用上限。
///
/// 每条流原本都要 `create` + `ArenaAllocator.init` + 约 20 次 arena 分配，流结束时再全部释放；
/// 复用后这些分配退化为「保留容量的 bump 指针」，单请求省下十几次 malloc/free。
const max_spare_streams: usize = 256;

/// handler 嵌套深度上限（发送阻塞 → 读帧 → 又派发新请求 → 再阻塞…）。
const max_dispatch_depth: u8 = 4;

pub const FrameHeader = struct {
    length: usize,
    ftype: FrameType,
    flags: u8,
    stream_id: u32,
};

pub const Options = struct {
    /// 单个 header block（含 CONTINUATION 拼接）上限，防 HPACK 炸弹
    max_header_block: usize = 64 * 1024,
    /// 单请求体上限
    max_body: usize = 4 * 1024 * 1024,
    /// 我们自己声明的 `SETTINGS_INITIAL_WINDOW_SIZE`
    initial_window_size: u32 = @intCast(default_window_size),
    /// 我们自己声明的 `SETTINGS_MAX_CONCURRENT_STREAMS`
    max_concurrent_streams: usize = 100,
};

pub const Request = struct {
    stream_id: u32,
    method: []const u8 = "",
    path: []const u8 = "",
    scheme: []const u8 = "",
    authority: []const u8 = "",
    /// 全部请求头（含 `:method` 等伪头），已从 HPACK 解开
    headers: []const hpack.Header = &.{},
    /// 完整请求体（已跨 DATA 帧拼接）
    body: []const u8 = "",

    /// 大小写不敏感取头（h2 要求小写，这里仍兼容大小写）
    pub fn header(self: Request, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

/// 响应写入器：`begin` → `sendData`(可多次) → `end`。
pub const Responder = struct {
    conn: *Connection,
    stream: *StreamState,
    begun: bool = false,
    ended: bool = false,

    pub fn streamId(self: *const Responder) u32 {
        return self.stream.id;
    }

    /// 本条流是否走并发写路径。
    ///
    /// 判断依据是**流级的租约**（`stream.leased`）而不是连接级开关：
    /// 连接开了每流任务后，仍有一部分请求（同连接只有一个流时）由连接任务内联处理，
    /// 它们必须继续走原来的阻塞式快路径 —— 否则会白白多付锁与 flush 合并的开销，
    /// 稳态吞吐就掉了。
    fn concurrentMode(self: *const Responder) bool {
        return self.stream.leased and self.conn.concurrent_write;
    }

    /// 发送响应头块（`status` 如 "200"；`extra` 里不要含 `:status`）。
    pub fn begin(self: *Responder, status: []const u8, extra: []const hpack.Header) !void {
        if (self.begun) return error.AlreadyBegun;
        const a = self.stream.allocator();

        var list: std.ArrayListUnmanaged(hpack.Header) = .empty;
        try list.append(a, .{ .name = ":status", .value = status });
        try list.appendSlice(a, extra);

        var block: std.ArrayListUnmanaged(u8) = .empty;
        try hpack.encode(list.items, &block, a);

        try self.conn.writeHeaderBlock(self.stream.id, block.items, 0);
        self.begun = true;
    }

    /// 发送正文（自动按对端 `MAX_FRAME_SIZE` 与流控窗口切片）。
    pub fn sendData(self: *Responder, bytes: []const u8) !void {
        if (!self.begun) try self.begin("200", &.{});
        if (self.ended) return error.AlreadyEnded;
        // 并发写路径：不阻塞读，窗口不足挂 pending（详见 `sendStreamData`）
        if (self.concurrentMode()) {
            return self.conn.sendStreamData(self.stream, bytes, false);
        }

        var off: usize = 0;
        while (off < bytes.len) {
            const room = @min(self.conn.send_window, self.stream.send_window);
            if (room <= 0) {
                try self.conn.pumpForWindow(self.stream);
                continue;
            }
            const want: usize = @intCast(@min(
                @as(i64, @intCast(@min(bytes.len - off, self.conn.peer_max_frame_size))),
                room,
            ));
            try self.conn.writeFrame(.data, 0, self.stream.id, bytes[off .. off + want]);
            self.conn.send_window -= @as(i64, @intCast(want));
            self.stream.send_window -= @as(i64, @intCast(want));
            off += want;
        }
    }

    /// 结束响应：`trailers` 非空则放在尾部 HEADERS（gRPC 的 `grpc-status` 就走这里）。
    pub fn end(self: *Responder, trailers: []const hpack.Header) !void {
        if (!self.begun) try self.begin("200", &.{});
        if (self.ended) return error.AlreadyEnded;
        const a = self.stream.allocator();
        var block: std.ArrayListUnmanaged(u8) = .empty;
        try hpack.encode(trailers, &block, a);
        if (self.concurrentMode()) {
            try self.conn.finishStream(self.stream, block.items);
            self.ended = true;
            return;
        }
        try self.conn.writeHeaderBlock(self.stream.id, block.items, Flags.end_stream);
        self.ended = true;
    }

    /// 发送最后一段正文并结束流（gRPC-Web 把 trailers 放在 body 尾帧，需要这种收尾方式）。
    pub fn endData(self: *Responder, bytes: []const u8) !void {
        if (self.ended) return error.AlreadyEnded;
        if (!self.begun) try self.begin("200", &.{});
        if (self.concurrentMode()) {
            try self.conn.sendStreamData(self.stream, bytes, true);
            self.ended = true;
            return;
        }
        if (bytes.len == 0) {
            try self.conn.writeFrame(.data, Flags.end_stream, self.stream.id, &.{});
            self.ended = true;
            return;
        }
        var off: usize = 0;
        while (off < bytes.len) {
            const room = @min(self.conn.send_window, self.stream.send_window);
            if (room <= 0) {
                try self.conn.pumpForWindow(self.stream);
                continue;
            }
            const want: usize = @intCast(@min(
                @as(i64, @intCast(@min(bytes.len - off, self.conn.peer_max_frame_size))),
                room,
            ));
            const last = off + want == bytes.len;
            try self.conn.writeFrame(
                .data,
                if (last) Flags.end_stream else 0,
                self.stream.id,
                bytes[off .. off + want],
            );
            self.conn.send_window -= @as(i64, @intCast(want));
            self.stream.send_window -= @as(i64, @intCast(want));
            off += want;
        }
        self.ended = true;
    }
};

pub const HandlerFn = *const fn (
    ctx: *anyopaque,
    req: Request,
    rsp: *Responder,
    arena: std.mem.Allocator,
) anyerror!void;

/// 流式处理钩子（对应 gRPC 的客户端流 / bidi）：
/// `onHeaders` 在请求头就绪时调一次，`onData` 在**每个 DATA 帧到达时**立即调（不等 END_STREAM）。
pub const StreamHooks = struct {
    ctx: *anyopaque,
    onHeaders: *const fn (ctx: *anyopaque, req: Request, rsp: *Responder) anyerror!void,
    onData: *const fn (ctx: *anyopaque, rsp: *Responder, data: []const u8, end_stream: bool) anyerror!void,
    /// 流被对端 `RST_STREAM` 重置（可选）
    onReset: ?*const fn (ctx: *anyopaque, stream_id: u32) void = null,
};

/// 每流任务的内容种类：同一个提交钩子要能承载两条路径（整请求派发 / 流式 END_STREAM）。
pub const JobKind = enum { dispatch, stream_end };

/// 一条流的全部请求状态。**自带 arena**：流结束时整体释放，
/// 因此多流并存时不会互相污染（不用连接级 arena 的 reset 时机去赌）。
pub const StreamState = struct {
    id: u32,
    arena: std.heap.ArenaAllocator,
    header_block: std.ArrayListUnmanaged(u8) = .empty,
    headers: std.ArrayListUnmanaged(hpack.Header) = .empty,
    body: std.ArrayListUnmanaged(u8) = .empty,
    headers_done: bool = false,
    method: []const u8 = "",
    path: []const u8 = "",
    scheme: []const u8 = "",
    authority: []const u8 = "",
    /// 该流剩余发送窗口
    send_window: i64 = default_window_size,
    /// 该流待回补的接收窗口字节数（同连接级，累积到阈值再发）
    pending_stream_refund: usize = 0,
    /// 流式模式下的响应器（**必须活到流结束**，不能在栈上建：handler 会跨回调持有它）
    rsp: ?*Responder = null,
    /// 业务挂载点（流式模式：gRPC 的 Call）
    user_data: ?*anyopaque = null,
    /// 该流的 handler 已交给**其它任务**执行：连接任务在此期间不得回收此流。
    leased: bool = false,
    /// handler 任务已经跑完（写锁保护）；连接任务在 `reapStreams` 里回收。
    finished: bool = false,
    /// 对端发了 RST_STREAM：handler 不应再写，等它收尾后再回收（写锁保护）。
    aborted: bool = false,
    /// 每流任务的内容种类（提交前由连接任务写好，任务只读）。
    job_kind: JobKind = .dispatch,
    /// END_STREAM 那一帧的正文：**已复制进本流 arena**（`frame_buf` 会被复用，不能直接持有）。
    end_data: []const u8 = &.{},
    /// 提交时快照的钩子：`serveStreaming` 返回后 `stream_hooks` 会被置空，任务不能再去读它。
    end_hooks: ?StreamHooks = null,

    // --- 并发写模式（`Connection.concurrent_write`）专用 ---
    /// 窗口不足时暂存的待发正文。**只有该流自己的任务会 append**，
    /// 由连接任务在收到 `WINDOW_UPDATE` 时 drain —— 两侧都在写锁内操作。
    pending_out: std.ArrayListUnmanaged(u8) = .empty,
    /// `pending_out` 排空后是否要带 `END_STREAM`
    pending_end: bool = false,
    /// 编码好的 trailers header block（arena 持有），等 `pending_out` 排空后再发
    pending_trailers: ?[]const u8 = null,

    pub fn allocator(self: *StreamState) std.mem.Allocator {
        return self.arena.allocator();
    }
};

const PendingFrame = struct {
    hdr: FrameHeader,
    payload: []u8,
};

/// 极简自旋锁：临界区只有「写一帧 + flush」（微秒级）。
/// Zig 0.17.1 的 `std.Io.Mutex` 需要 `io` 参数且可取消，而 `writeFrame` 没有 io 上下文 ——
/// 为一把只用几微秒的锁引入 io 依赖不值得。
pub const SpinLock = struct {
    locked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn lock(self: *SpinLock) void {
        var spins: usize = 0;
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            spins += 1;
            if (spins < 64) {
                std.atomic.spinLoopHint();
            } else {
                // 临界区里可能含 `write()` 系统调用（几十微秒）。纯自旋会把 CPU
                // 烧光，把同机其它任务（包括忙等型 handler）饿死 —— 实测 1ms handler、
                // 32 路并发时，不退还的版本比退让版本慢一倍。
                std.Thread.yield() catch {};
            }
        }
    }

    /// 尝试拿锁；拿不到立刻返回 false。
    /// keepalive 用它跳过「正忙」的连 —— 有业务流量时本来就不需要 PING。
    pub fn tryLock(self: *SpinLock) bool {
        return self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) == null;
    }

    pub fn unlock(self: *SpinLock) void {
        self.locked.store(false, .release);
    }
};

/// 连接写锁。
///
/// **默认是自旋锁**（临界区只有「写一帧 + flush」，微秒级），一旦绑定了 `std.Io`
/// （协程后端，见 `enableIo`）就换成 `std.Io.Mutex`：
///
/// 在协程后端（zio/IOCP）上，**每一次 socket 写都会让出协程**。如果用自旋锁，
/// 持锁者已经挂起、其它协程还在 worker 线程上死转，迟早把 worker 全占满，
/// 就再也没有线程去把持锁者唤醒 —— 实测 100 路并发直接整条连接挂死。
/// `std.Io.Mutex` 的等待是 futex 挂起，会让出执行体重排，所以不会自锁。
pub const WriteLock = struct {
    spin: SpinLock = .{},
    io_mutex: std.Io.Mutex = .init,
    io: ?std.Io = null,

    /// 绑定 `std.Io`：此后所有加锁都走 `std.Io.Mutex`（必须在开始服务前调用）。
    pub fn enableIo(self: *WriteLock, io: std.Io) void {
        self.io = io;
    }

    pub fn lock(self: *WriteLock) void {
        if (self.io) |io| {
            // 不引入额外取消点：写路径中途被取消会留下半个帧
            self.io_mutex.lockUncancelable(io);
            return;
        }
        self.spin.lock();
    }

    pub fn tryLock(self: *WriteLock) bool {
        if (self.io != null) return self.io_mutex.tryLock();
        return self.spin.tryLock();
    }

    pub fn unlock(self: *WriteLock) void {
        if (self.io) |io| {
            self.io_mutex.unlock(io);
            return;
        }
        self.spin.unlock();
    }
};

pub const Connection = struct {
    /// 连接级长期分配（HPACK 动态表、流对象与延后帧）
    alloc: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    /// 帧写入互斥：keepalive 线程可能与业务线程同时发帧，
    /// 一帧的「头 + 体 + flush」必须原子，否则会交错出非法帧。
    write_mutex: WriteLock = .{},
    opts: Options = .{},
    decoder: hpack.Decoder,

    /// 我方剩余发送窗口（连接级）
    send_window: i64 = default_window_size,
    /// 对端 `SETTINGS_MAX_FRAME_SIZE`
    peer_max_frame_size: usize = default_max_frame_size,
    /// 对端 `SETTINGS_INITIAL_WINDOW_SIZE`（新流的发送窗口初值）
    peer_initial_window: i64 = default_window_size,
    /// 连接级待回补的接收窗口字节数（**累积到阈值再发**，见 `refundWindow`）
    pending_conn_refund: usize = 0,
    /// 窗口回补的累积阈值。
    ///
    /// 对齐 Go `net/http2` 的 `inflowMinRefresh = 4 KiB`：逐帧回补会让每个请求多出
    /// 2 个 WINDOW_UPDATE 帧（128 字节请求下 18 字节 + 2 帧纯开销）。
    /// 4 KiB 远小于初始窗口 64 KiB，**不会把对端窗口饿死**。
    refund_threshold: usize = 4 * 1024,

    /// 活跃流（多路复用：同一连接可同时存在多个半开流）
    streams: std.ArrayListUnmanaged(*StreamState) = .empty,
    /// 空闲流对象池（含 arena 已分配容量，见 `max_spare_streams`）
    spare: std.ArrayListUnmanaged(*StreamState) = .empty,
    /// 发送受流控阻塞期间读到的业务帧：延后到主循环处理（不丢、不误拒）
    pending: std.ArrayListUnmanaged(PendingFrame) = .empty,
    /// 发送缓冲累计到该字节数就强制 flush（延迟 flush 的防饿死阈值）。
    ///
    /// 取 4 KiB：远小于默认 16 KiB 发送缓冲，正常一批响应（~210 字节/条）约 20 条刷一次，
    /// 既拿到批量写，又不会让响应在缓冲里久留。
    flush_high_water: usize = 4 * 1024,
    /// 帧载荷复用缓冲（只增不减，`deinit` 释放）。
    ///
    /// 原先每帧 `alloc`+`free`，一个 unary 请求 2 帧就是 4 次分配器调用。
    frame_buf: std.ArrayListUnmanaged(u8) = .empty,
    /// 已写入发送缓冲但**尚未刷出**的字节批次计数（写锁保护）。
    ///
    /// 并发写路径用它把「一批同时完成的响应」合并成一次 flush：
    /// 抢到写锁且看到它 > 0 的人一次刷完，其余人看到 0 就直接返回。
    unflushed: usize = 0,
    /// 允许从**非连接任务**并发写响应（每流一个 handler 任务时必需）。
    ///
    /// 开启后 `Responder.sendData` / `end` / `endData` 走
    /// 「写锁内写帧 + 窗口不足挂到 per-stream pending」这条路径，
    /// **不再调用会阻塞读的 `pumpForWindow`**（只有连接任务能读 socket）。
    /// 关闭时行为与之前完全一致（阻塞式，吞吐更好）。
    concurrent_write: bool = false,
    /// 每流任务提交钩子：非 null 时 unary handler 会被提交到**其它任务**执行。
    ///
    /// 这是消除「队头阻塞」的关键：同一条连接上的慢 handler 不再挡住其它流。
    /// 实现方只需转发给 `runStreamJob`（结束租约、回收都由它负责）。
    job_submit: ?*const fn (ctx: *anyopaque, conn: *Connection, s: *StreamState) void = null,
    job_ctx: ?*anyopaque = null,
    /// 仍在执行中的每流任务数（写锁保护）。连接收尾前必须等它归零。
    active_jobs: usize = 0,

    serve_ctx: ?*anyopaque = null,
    serve_handler: ?HandlerFn = null,
    /// 非空时进入**流式模式**：DATA 到达即回调，不攒到 END_STREAM
    stream_hooks: ?StreamHooks = null,
    dispatch_depth: u8 = 0,
    /// 已收到对端 SETTINGS（收到前不能算连接就绪）
    got_peer_settings: bool = false,
    /// 收到对端 PING ACK 的单调时刻（0 = 从未收到）
    last_ping_ack_ns: i128 = 0,
    /// 已发 / 已确认的 PING 计数
    pings_sent: u64 = 0,
    pings_acked: u64 = 0,

    pub fn init(
        alloc: std.mem.Allocator,
        reader: *std.Io.Reader,
        writer: *std.Io.Writer,
        opts: Options,
    ) Connection {
        return .{
            .alloc = alloc,
            .reader = reader,
            .writer = writer,
            .opts = opts,
            .decoder = hpack.Decoder.init(alloc),
        };
    }

    pub fn deinit(self: *Connection) void {
        for (self.streams.items) |s| {
            s.arena.deinit();
            self.alloc.destroy(s);
        }
        self.streams.deinit(self.alloc);
        for (self.spare.items) |s| {
            s.arena.deinit();
            self.alloc.destroy(s);
        }
        self.spare.deinit(self.alloc);
        for (self.pending.items) |pf| self.alloc.free(pf.payload);
        self.pending.deinit(self.alloc);
        self.frame_buf.deinit(self.alloc);
        self.decoder.deinit();
    }

    /// 当前活跃流数（可观测/测试用）。
    pub fn activeStreams(self: *const Connection) usize {
        return self.streams.items.len;
    }

    fn findStream(self: *Connection, id: u32) ?*StreamState {
        for (self.streams.items) |s| {
            if (s.id == id) return s;
        }
        return null;
    }

    /// 取一个可用的流对象：优先复用空闲池，否则新建。
    /// 返回的对象**逻辑状态已归零**，但 `id` 由调用方设置。
    fn acquireStream(self: *Connection) !*StreamState {
        if (self.spare.pop()) |s| {
            resetStream(s, self.peer_initial_window);
            return s;
        }
        const s = try self.alloc.create(StreamState);
        s.* = .{
            .id = 0,
            .arena = std.heap.ArenaAllocator.init(self.alloc),
            .send_window = self.peer_initial_window,
        };
        return s;
    }

    /// 把流对象状态归零（保留 arena 已分配容量，供下一个请求复用）。
    fn resetStream(s: *StreamState, peer_initial_window: i64) void {
        // `.retain_capacity` 只保留**最近一块**缓冲并复用其容量：
        // 热路径上的 arena 分配因此退化为 bump 指针，不再触发 malloc。
        _ = s.arena.reset(.retain_capacity);
        s.header_block = .empty;
        s.headers = .empty;
        s.body = .empty;
        s.headers_done = false;
        s.method = "";
        s.path = "";
        s.scheme = "";
        s.authority = "";
        s.send_window = peer_initial_window;
        s.pending_stream_refund = 0;
        s.rsp = null;
        s.user_data = null;
        s.pending_out = .empty;
        s.pending_end = false;
        s.pending_trailers = null;
        s.leased = false;
        s.finished = false;
        s.aborted = false;
    }

    fn createStream(self: *Connection, id: u32) !*StreamState {
        const s = try self.acquireStream();
        s.id = id;
        errdefer self.releaseStream(s);
        try self.streams.append(self.alloc, s);
        return s;
    }

    /// 归还流对象：进空闲池待复用（超上限才真释放）。
    fn releaseStream(self: *Connection, s: *StreamState) void {
        if (self.spare.items.len >= max_spare_streams) {
            s.arena.deinit();
            self.alloc.destroy(s);
            return;
        }
        resetStream(s, self.peer_initial_window);
        self.spare.append(self.alloc, s) catch {
            s.arena.deinit();
            self.alloc.destroy(s);
        };
    }

    fn destroyStream(self: *Connection, s: *StreamState) void {
        for (self.streams.items, 0..) |x, i| {
            if (x == s) {
                _ = self.streams.orderedRemove(i);
                break;
            }
        }
        self.releaseStream(s);
    }

    /// 阻塞服务一条连接，直到对端 `GOAWAY`/断开。返回 `error.ConnectionClosed` 属正常收尾。
    pub fn serve(self: *Connection, ctx: *anyopaque, handler: HandlerFn) !void {
        self.serve_ctx = ctx;
        self.serve_handler = handler;
        self.stream_hooks = null;
        defer {
            self.serve_ctx = null;
            self.serve_handler = null;
        }
        return self.runLoop();
    }

    /// 流式服务： `hooks.onData` 在 DATA 到达时立即回调（支持客户端流 / bidi）。
    pub fn serveStreaming(self: *Connection, hooks: StreamHooks) !void {
        self.stream_hooks = hooks;
        self.serve_ctx = null;
        self.serve_handler = null;
        defer self.stream_hooks = null;
        return self.runLoop();
    }

    fn runLoop(self: *Connection) !void {
        var pf: [client_preface.len]u8 = undefined;
        self.reader.readSliceAll(&pf) catch |e| {
            log.h2.warn("读连接前言失败：{s}", .{@errorName(e)});
            return e;
        };
        if (!std.mem.eql(u8, &pf, client_preface)) {
            log.h2.warn("连接前言不匹配（可能是 HTTP/1.1 打到 h2 端口）", .{});
            return error.BadPreface;
        }

        // 服务端必须先发 SETTINGS（RFC 9113 §3.4）
        try self.sendSettings();

        while (true) {
            // 先消化"发送阻塞期间"暂存的业务帧
            if (self.pending.items.len > 0) {
                const p = self.pending.orderedRemove(0);
                defer self.alloc.free(p.payload);
                try self.handleFrame(p.hdr, p.payload);
                try self.flushIfFull();
                continue;
            }

            // 读之前先把挂起的输出刷出去（延迟 flush 的安全性基石）：
            // 缓冲里没有完整的 9 字节头，接下来这次读很可能真阻塞在 socket 上。
            try self.flushForRead(9);

            const hdr = self.readFrameHeader() catch |e| {
                // 对端关闭/半包 → 正常收尾；其它读错误也在这里定位
                log.h2.debug("读帧结束（{s}），连接收尾", .{@errorName(e)});
                return error.ConnectionClosed;
            };

            // 帧体也可能不在缓冲里：读它之前同样要刷
            try self.flushForRead(hdr.length);

            // 复用帧载荷缓冲：原先每帧一次 `alloc`+`free`，一个 unary 请求就是 2 帧
            // （HEADERS + DATA）＝ 4 次分配器调用；压成每连接一块只增不减的缓冲。
            const payload = try self.framePayload(hdr.length);
            self.reader.readSliceAll(payload) catch |e| {
                log.h2.warn("读帧体失败（流 {d}，{d} 字节）：{s}", .{
                    hdr.stream_id, hdr.length, @errorName(e),
                });
                return error.ConnectionClosed;
            };

            try self.handleFrame(hdr, payload);
            try self.flushIfFull();
        }
    }

    fn handleFrame(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        // 回收已跑完的租约流（每流任务模式）。放这里而不是循环末尾：
        // 帧到达即意味着该流的响应很可能已写完，回收时机刚好。
        self.reapStreams();
        switch (hdr.ftype) {
            .settings => try self.onSettings(hdr, payload),
            .ping => try self.onPing(hdr, payload),
            .window_update => try self.onWindowUpdate(hdr, payload),
            .headers => try self.onHeaders(hdr, payload),
            .continuation => try self.onContinuation(hdr, payload),
            .data => try self.onData(hdr, payload),
            .rst_stream => try self.onRstStream(hdr, payload),
            .goaway => return error.ConnectionClosed,
            // PRIORITY / PUSH_PROMISE / 未知帧：按 RFC 必须忽略
            else => {},
        }
    }

    // -----------------------------------------------------------------------
    // 帧读写
    // -----------------------------------------------------------------------

    fn readFrameHeader(self: *Connection) !FrameHeader {
        var b: [9]u8 = undefined;
        try self.reader.readSliceAll(&b);
        const len = (@as(usize, b[0]) << 16) | (@as(usize, b[1]) << 8) | @as(usize, b[2]);
        return .{
            .length = len,
            .ftype = @fromBackingInt(@intCast(b[3])),
            .flags = b[4],
            .stream_id = std.mem.readInt(u32, b[5..9], .big) & 0x7fff_ffff,
        };
    }

    /// 发送 header block，并按需分片为 `HEADERS + CONTINUATION…`。
    ///
    /// RFC 9113 §6.10：一个 header block 的 HEADERS 与它后面的 CONTINUATION 之间
    /// **不允许插入任何帧**（PING 也不行），否则对端会当协议错误。因此整段序列
    /// 必须在同一把写锁内完成 —— 这也正是 keepalive 能安全并发发 PING 的前提。
    ///
    /// `extra_flags` 只允许 `END_STREAM`（HEADERS 专属）；`END_HEADERS` 由这里控制，
    /// 而且只能出现在末帧上。未超帧长时直接一帧发出（常见路径不变）。
    fn writeHeaderBlock(
        self: *Connection,
        stream_id: u32,
        block: []const u8,
        extra_flags: u8,
    ) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try self.writeHeaderBlockLocked(stream_id, block, extra_flags, false);
    }

    /// 同 `writeHeaderBlock`，但**调用者必须已持写锁**。
    ///
    /// `ignore_extra = true` 时忽略 `extra_flags`（并发写路径用：END_STREAM 由调用点决定）。
    fn writeHeaderBlockLocked(
        self: *Connection,
        stream_id: u32,
        block: []const u8,
        extra_flags: u8,
        ignore_extra: bool,
    ) !void {
        _ = ignore_extra;
        const max = self.peer_max_frame_size;

        if (block.len <= max) {
            try self.writeFrameBody(.headers, extra_flags | Flags.end_headers, stream_id, block);
            return;
        }
        // 首片带 END_STREAM（若有），不带 END_HEADERS；其余走 CONTINUATION，末片才带 END_HEADERS
        try self.writeFrameBody(.headers, extra_flags & Flags.end_stream, stream_id, block[0..max]);
        var off: usize = max;
        while (off < block.len) {
            const n = @min(max, block.len - off);
            const last = off + n == block.len;
            try self.writeFrameBody(
                .continuation,
                if (last) Flags.end_headers else 0,
                stream_id,
                block[off .. off + n],
            );
            off += n;
        }
    }

    fn writeFrame(
        self: *Connection,
        ftype: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !void {
        // 头 + 体必须原子（keepalive 线程可能并发发 PING）。
        // **不 flush**：由读循环在阻塞前统一刷出（见 `flushForRead`）。
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        return self.writeFrameBody(ftype, flags, stream_id, payload);
    }

    /// 写一帧并**立即 flush**：控制帧、以及可能被其它线程调用的路径用它（语义同旧版）。
    fn writeFrameFlush(
        self: *Connection,
        ftype: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try self.writeFrameBody(ftype, flags, stream_id, payload);
        try self.writer.flush();
    }

    /// 把挂起的输出真正写出去。持写锁，因此可与 keepalive 线程安全并发。
    ///
    /// 空缓冲时 `flush` 是纯空转（`defaultFlush` 的循环条件为假），没有系统调用开销。
    pub fn flushOut(self: *Connection) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try self.writer.flush();
        self.unflushed = 0;
    }

    /// 取一块能装下 `need` 字节的帧载荷缓冲（**每连接复用**，只增不减）。
    ///
    /// 原先 `runLoop` 每帧 `alloc`+`free`：一个 unary 请求 2 帧就是 4 次分配器调用。
    /// 改成复用后这些分配在连接生命周期内只发生 O(log) 次。
    ///
    /// 生命周期约定与之前一致：返回的切片只在本次 `handleFrame` 期间有效
    /// （需要留存的路径 `pumpForWindow` 会自己复制一份）。
    fn framePayload(self: *Connection, need: usize) ![]u8 {
        if (need > self.frame_buf.capacity) {
            // 按需扩张：至少翻倍，避免大帧之后每次小帧都重新分配
            const want = @max(need, @max(self.frame_buf.capacity * 2, 1024));
            try self.frame_buf.ensureTotalCapacity(self.alloc, want);
        }
        self.frame_buf.items.len = need;
        return self.frame_buf.items[0..need];
    }

    // -----------------------------------------------------------------------
    // 并发写路径（`concurrent_write`）：可被任意任务调用
    // -----------------------------------------------------------------------

    /// 写一个流的响应正文，可被**任意任务**并发调用。
    ///
    /// 与旧的阻塞路径的区别：
    /// - 窗口检查、扣减、写帧**全在同一把写锁内**完成（否则多任务会算错窗口）；
    /// - 窗口不足时**不阻塞读**，把剩余字节挂到 `s.pending_out`，
    ///   等连接任务收到 `WINDOW_UPDATE` 时 `drainStreamPending` 续发。
    pub fn sendStreamData(
        self: *Connection,
        s: *StreamState,
        bytes: []const u8,
        end_stream: bool,
    ) !void {
        self.write_mutex.lock();
        // 对端已 RST：不再写（写了也是白写，还可能被回 GOAWAY）
        if (s.aborted) {
            self.write_mutex.unlock();
            return;
        }
        const r = self.writeStreamDataLocked(s, bytes, end_stream);
        if (r) |_| {
            self.unflushed += 1;
        } else |_| {}
        self.write_mutex.unlock();

        // 刷出交给“最后一个完成的 handler”（`markStreamFinished`）：
        // 这里绝不能自己刷新，否则 N 路并发就是 N 次系统调用。
        return r;
    }

    /// 调用者**必须已持有** `write_mutex`。
    fn writeStreamDataLocked(
        self: *Connection,
        s: *StreamState,
        bytes: []const u8,
        end_stream: bool,
    ) !void {
        var off: usize = 0;
        while (off < bytes.len) {
            const room = @min(self.send_window, s.send_window);
            if (room <= 0) break;
            const want: usize = @intCast(@min(
                @as(i64, @intCast(@min(bytes.len - off, self.peer_max_frame_size))),
                room,
            ));
            const last = off + want == bytes.len;
            try self.writeFrameBody(
                .data,
                if (last and end_stream) Flags.end_stream else 0,
                s.id,
                bytes[off .. off + want],
            );
            self.send_window -= @as(i64, @intCast(want));
            s.send_window -= @as(i64, @intCast(want));
            off += want;
        }
        if (off < bytes.len) {
            // 窗口不足：挂起剩余部分（这块内存属于该流自己的 arena，不跨流共享）
            if (s.pending_out.items.len + (bytes.len - off) > self.opts.max_body) {
                return error.ResponseTooLarge;
            }
            try s.pending_out.appendSlice(s.allocator(), bytes[off..]);
            if (end_stream) s.pending_end = true;
        }
    }

    /// 结束一个流的响应：写 trailers header block，可被任意任务并发调用。
    ///
    /// 若该流还有挂起的正文（`pending_out` 非空），把 trailers 也挂起来，
    /// 等正文排空后再由连接任务发（顺序不能乱）。
    pub fn finishStream(self: *Connection, s: *StreamState, trailers_block: []const u8) !void {
        self.write_mutex.lock();
        if (s.aborted) {
            self.write_mutex.unlock();
            return;
        }
        if (s.pending_out.items.len > 0) {
            s.pending_trailers = trailers_block;
            self.write_mutex.unlock();
            return;
        }
        const r = self.writeHeaderBlockLocked(s.id, trailers_block, Flags.end_stream, true);
        if (r) |_| {
            self.unflushed += 1;
        } else |_| {}
        self.write_mutex.unlock();

        // 同样交给“最后一个完成的 handler”合并刷出（见 `markStreamFinished`）
        return r;
    }

    /// 把某条流挂起的正文/trailers 尽量发出去（**只由连接任务调用**）。
    pub fn drainStreamPending(self: *Connection, s: *StreamState) !void {
        if (s.pending_out.items.len == 0 and s.pending_trailers == null) return;

        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        if (s.pending_out.items.len > 0) {
            const buf = s.pending_out.items;
            var off: usize = 0;
            while (off < buf.len) {
                const room = @min(self.send_window, s.send_window);
                if (room <= 0) break;
                const want: usize = @intCast(@min(
                    @as(i64, @intCast(@min(buf.len - off, self.peer_max_frame_size))),
                    room,
                ));
                const last = off + want == buf.len;
                try self.writeFrameBody(
                    .data,
                    if (last and s.pending_end) Flags.end_stream else 0,
                    s.id,
                    buf[off .. off + want],
                );
                self.send_window -= @as(i64, @intCast(want));
                s.send_window -= @as(i64, @intCast(want));
                off += want;
            }
            if (off > 0) {
                const rest = buf.len - off;
                std.mem.copyForwards(u8, s.pending_out.items[0..rest], s.pending_out.items[off..]);
                s.pending_out.shrinkRetainingCapacity(rest);
            }
            if (s.pending_out.items.len > 0) return; // 窗口还是不够，等下一次 WINDOW_UPDATE
        }

        if (s.pending_trailers) |block| {
            s.pending_trailers = null;
            try self.writeHeaderBlockLocked(s.id, block, Flags.end_stream, true);
        }
        try self.writer.flush();
    }

    /// 若下一次读**必须真正走 socket**，则先把挂起的输出刷出去。
    ///
    /// 这是延迟 flush 的正确性基石：不刷就阻塞读 = 对端等我们、我们等对端。
    fn flushForRead(self: *Connection, need: usize) !void {
        if (self.reader.buffered().len >= need) return;
        try self.flushOut();
    }

    /// 发送缓冲超过高水位就强制 flush。
    /// 防止对端持续发帧导致读循环一直命中缓冲、迟迟不刷出响应。
    fn flushIfFull(self: *Connection) !void {
        if (self.writer.buffered().len < self.flush_high_water) return;
        try self.flushOut();
    }

    /// 非阻塞写帧：拿不到写锁（连接正忙）就返回 false，不排队。
    fn writeFrameTry(
        self: *Connection,
        ftype: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !bool {
        if (!self.write_mutex.tryLock()) return false;
        defer self.write_mutex.unlock();
        try self.writeFrameBody(ftype, flags, stream_id, payload);
        // keepalive 在独立线程上，不能依赖读循环帮忙 flush
        try self.writer.flush();
        return true;
    }

    fn writeFrameBody(
        self: *Connection,
        ftype: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !void {
        if (payload.len > 0x00ff_ffff) return error.FrameTooLarge;

        var hdr: [9]u8 = undefined;
        hdr[0] = @intCast((payload.len >> 16) & 0xff);
        hdr[1] = @intCast((payload.len >> 8) & 0xff);
        hdr[2] = @intCast(payload.len & 0xff);
        hdr[3] = @backingInt(ftype);
        hdr[4] = flags;
        std.mem.writeInt(u32, hdr[5..9], stream_id & 0x7fff_ffff, .big);

        try self.writer.writeAll(&hdr);
        if (payload.len > 0) try self.writer.writeAll(payload);
        // 不在这里 flush：合用读循环的批量刷出（见文件头「延迟 flush」）
    }

    fn sendSettings(self: *Connection) !void {
        var payload: [12]u8 = undefined;
        // MAX_CONCURRENT_STREAMS
        std.mem.writeInt(u16, payload[0..2], 0x0003, .big);
        std.mem.writeInt(u32, payload[2..6], @intCast(self.opts.max_concurrent_streams), .big);
        // INITIAL_WINDOW_SIZE
        std.mem.writeInt(u16, payload[6..8], 0x0004, .big);
        std.mem.writeInt(u32, payload[8..12], self.opts.initial_window_size, .big);
        // 建连第一帧：必须立即刷出（客户端在等 SETTINGS 才发请求）
        try self.writeFrameFlush(.settings, 0, 0, &payload);
    }

    fn errBytes(code: ErrorCode) [4]u8 {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, @backingInt(code), .big);
        return b;
    }

    fn sendRst(self: *Connection, stream_id: u32, code: ErrorCode) !void {
        if (stream_id == 0) return;
        const payload = errBytes(code);
        try self.writeFrame(.rst_stream, 0, stream_id, &payload);
    }

    fn sendGoAway(self: *Connection, code: ErrorCode) !void {
        var payload: [8]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], 0, .big); // last_stream_id
        std.mem.writeInt(u32, payload[4..8], @backingInt(code), .big);
        // 刻意吞掉：GOAWAY 是「尽力告知」，能走到这里通常已经是对端断了/写不出去，
        // 没有后续动作可做，上层已拿到原始错误，这里再报错只会掩盖真正原因。
        self.writeFrameFlush(.goaway, 0, 0, &payload) catch {};
    }

    // -----------------------------------------------------------------------
    // 控制帧
    // -----------------------------------------------------------------------

    fn onSettings(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        if (hdr.flags & Flags.ack != 0) {
            if (payload.len != 0) return error.ProtocolError;
            self.got_peer_settings = true;
            return;
        }
        if (payload.len % 6 != 0) return error.FrameSizeError;

        var i: usize = 0;
        while (i + 6 <= payload.len) : (i += 6) {
            const id = std.mem.readInt(u16, payload[i..][0..2], .big);
            const val = std.mem.readInt(u32, payload[i + 2 ..][0..4], .big);
            switch (id) {
                0x0001 => {}, // HEADER_TABLE_SIZE：本库编码不用动态表，忽略
                0x0004 => { // INITIAL_WINDOW_SIZE：按差值调整所有现存流的发送窗口
                    const new_win: i64 = @intCast(val);
                    const delta = new_win - self.peer_initial_window;
                    for (self.streams.items) |s| s.send_window += delta;
                    self.peer_initial_window = new_win;
                },
                0x0005 => { // MAX_FRAME_SIZE
                    if (val < 16384 or val > 16_777_215) return error.ProtocolError;
                    self.peer_max_frame_size = val;
                },
                else => {},
            }
        }
        self.got_peer_settings = true;
        try self.writeFrame(.settings, Flags.ack, 0, &.{});
    }

    fn onPing(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        if (payload.len != 8) return error.FrameSizeError;
        if (hdr.flags & Flags.ack != 0) {
            // 对端回了我们的 PING：记下时间与计数，供调用方判活
            self.last_ping_ack_ns = @import("clock.zig").monotonicNs();
            self.pings_acked +|= 1;
            return;
        }
        try self.writeFrame(.ping, Flags.ack, 0, payload);
    }

    /// 主动发一个 PING（`payload` 为空则自动填时间戳字节）。
    ///
    /// 发送与业务帧共用写锁，因此可以从 keepalive 线程调用。
    pub fn sendPing(self: *Connection, payload: ?[8]u8) !void {
        const buf = pingPayload(payload);
        self.pings_sent +|= 1;
        // keepalive 可能跑在独立线程上：必须自己把字节推出去，不能等读循环
        try self.writeFrameFlush(.ping, 0, 0, &buf);
    }

    /// 非阻塞版：连接正忙（有业务帧在写）时返回 false ——
    /// 「正在传数据」本身就达到了保活目的，没必要排队等写锁。
    pub fn trySendPing(self: *Connection, payload: ?[8]u8) !bool {
        const buf = pingPayload(payload);
        const wrote = try self.writeFrameTry(.ping, 0, 0, &buf);
        if (wrote) self.pings_sent +|= 1;
        return wrote;
    }

    fn pingPayload(payload: ?[8]u8) [8]u8 {
        return payload orelse blk: {
            var b: [8]u8 = undefined;
            std.mem.writeInt(u64, &b, @bitCast(@as(i64, @truncate(@import("clock.zig").monotonicNs()))), .big);
            break :blk b;
        };
    }

    /// 收到过多少个 PING ACK（调用方可据此判活；0 表示对端从未回应）。
    pub fn pingAcks(self: *const Connection) u64 {
        return self.pings_acked;
    }

    fn onWindowUpdate(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        if (payload.len != 4) return error.FrameSizeError;
        const inc: i64 = @intCast(std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff);
        if (hdr.stream_id == 0) {
            self.send_window += inc;
            // 连接级窗口回补了：把所有还有积压的流都推一把
            for (self.streams.items) |s| {
                if (s.pending_out.items.len > 0 or s.pending_trailers != null) {
                    self.drainStreamPending(s) catch |e| {
                        log.h2.warn("续发挂起数据失败（流 {d}）：{s}", .{ s.id, @errorName(e) });
                    };
                }
            }
        } else if (self.findStream(hdr.stream_id)) |s| {
            s.send_window += inc;
            self.drainStreamPending(s) catch |e| {
                log.h2.warn("续发挂起数据失败（流 {d}）：{s}", .{ s.id, @errorName(e) });
            };
        }
    }

    fn onRstStream(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        // RFC 9113 §6.4：RST_STREAM 载荷必为 4 字节，否则是连接级 FRAME_SIZE_ERROR
        if (payload.len != 4) return error.FrameSizeError;
        if (self.findStream(hdr.stream_id)) |s| {
            log.h2.info("对端重置流 {d}，丢弃其状态", .{hdr.stream_id});
            if (self.stream_hooks) |hooks| {
                if (hooks.onReset) |cb| cb(hooks.ctx, hdr.stream_id);
            }
            self.killStream(s);
        }
    }

    /// 发送受阻时推进窗口：只立即处理控制帧；业务帧进 `pending` 延后（不丢）。
    fn pumpForWindow(self: *Connection, stream: *StreamState) !void {
        // 这里**一定**要先把已写好的字节推出去：我们正在等对端给窗口，
        // 对端很可能也在等我们的响应 —— 不刷就是互相干等（延迟 flush 的死锁点）。
        try self.flushOut();
        while (self.send_window <= 0 or stream.send_window <= 0) {
            try self.flushForRead(9);
            const hdr = self.readFrameHeader() catch |e| {
                log.h2.debug("等窗口时读帧结束（{s}）", .{@errorName(e)});
                return error.ConnectionClosed;
            };
            try self.flushForRead(hdr.length);
            const payload = try self.alloc.alloc(u8, hdr.length);
            self.reader.readSliceAll(payload) catch |e| {
                self.alloc.free(payload);
                log.h2.warn("等窗口时读帧体失败：{s}", .{@errorName(e)});
                return error.ConnectionClosed;
            };

            switch (hdr.ftype) {
                .window_update, .ping, .settings => {
                    defer self.alloc.free(payload);
                    try self.handleFrame(hdr, payload);
                },
                .rst_stream => {
                    defer self.alloc.free(payload);
                    const reset_id = hdr.stream_id;
                    try self.handleFrame(hdr, payload);
                    if (reset_id == stream.id) return error.StreamReset;
                },
                .goaway => {
                    self.alloc.free(payload);
                    return error.ConnectionClosed;
                },
                else => try self.pending.append(self.alloc, .{ .hdr = hdr, .payload = payload }),
            }
        }
    }

    // -----------------------------------------------------------------------
    // 请求帧（多路复用）
    // -----------------------------------------------------------------------

    fn onHeaders(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        // RFC 9113 §5.1.1：客户端发起的流号必须是正奇数（偶数是服务端推送专用，本库不用）
        if (hdr.stream_id == 0 or hdr.stream_id % 2 == 0) {
            log.h2.warn("非法流号 {d}（客户端发起必须为奇数）", .{hdr.stream_id});
            try self.sendGoAway(.protocol_error);
            return error.ProtocolError;
        }
        const s = self.findStream(hdr.stream_id) orelse blk: {
            if (self.streams.items.len >= self.opts.max_concurrent_streams) {
                log.h2.warn(
                    "并发流超上限 {d}，拒绝流 {d}",
                    .{ self.opts.max_concurrent_streams, hdr.stream_id },
                );
                try self.sendRst(hdr.stream_id, .refused_stream);
                return;
            }
            break :blk try self.createStream(hdr.stream_id);
        };

        if (s.headers_done) {
            // 同流二次 HEADERS（trailers）本库不支持
            try self.sendRst(hdr.stream_id, .protocol_error);
            self.killStream(s);
            return;
        }

        const a = s.allocator();
        const data = stripHeaderExtras(hdr, payload) orelse {
            try self.sendRst(hdr.stream_id, .protocol_error);
            self.killStream(s);
            return;
        };

        try s.header_block.appendSlice(a, data);
        if (s.header_block.items.len > self.opts.max_header_block) {
            log.h2.warn("header block 超上限 {d} 字节", .{self.opts.max_header_block});
            try self.sendRst(hdr.stream_id, .enhance_your_calm);
            self.killStream(s);
            return;
        }

        if (hdr.flags & Flags.end_headers != 0) {
            try self.afterHeaders(s, hdr.flags & Flags.end_stream != 0);
        }
    }

    fn onContinuation(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        const s = self.findStream(hdr.stream_id) orelse {
            // 流已不存在（比如上一条 HEADERS 已因超限被 RST/销毁）：**忽略该 CONTINUATION**。
            // 这里不能返回错误——那会把整条连接拆掉，连带弄死其它正常流（流级错误就按流处理）。
            log.h2.debug("忽略孤立的 CONTINUATION（流 {d} 不存在）", .{hdr.stream_id});
            return;
        };
        if (s.headers_done) {
            try self.sendRst(hdr.stream_id, .protocol_error);
            self.killStream(s);
            return;
        }
        const a = s.allocator();
        try s.header_block.appendSlice(a, payload);
        if (s.header_block.items.len > self.opts.max_header_block) {
            try self.sendRst(hdr.stream_id, .enhance_your_calm);
            self.killStream(s);
            return;
        }
        if (hdr.flags & Flags.end_headers != 0) {
            try self.afterHeaders(s, hdr.flags & Flags.end_stream != 0);
        }
    }

    fn skipContinuation(self: *Connection) !void {
        _ = self;
    }

    fn makeRequest(s: *StreamState) Request {
        return .{
            .stream_id = s.id,
            .method = s.method,
            .path = s.path,
            .scheme = s.scheme,
            .authority = s.authority,
            .headers = s.headers.items,
            .body = s.body.items,
        };
    }

    /// 请求头就绪：整请求模式攒到 END_STREAM 再派发；流式模式立刻回调 `onHeaders`。
    fn afterHeaders(self: *Connection, s: *StreamState, end_stream: bool) !void {
        try self.finishHeaders(s);

        if (self.stream_hooks) |hooks| {
            // Responder 必须长期存活（业务会跨多次回调持有它）→ 放在流的 arena 里
            const rsp = try s.allocator().create(Responder);
            rsp.* = .{ .conn = self, .stream = s };
            s.rsp = rsp;

            hooks.onHeaders(hooks.ctx, makeRequest(s), rsp) catch |e| {
                log.h2.err("流式 onHeaders 失败（流 {d}）：{s}", .{ s.id, @errorName(e) });
                if (!rsp.begun) rsp.begin("500", &.{}) catch {};
                if (!rsp.ended) rsp.end(&.{}) catch {};
            };

            // 无请求体的流：立刻通知"结束"
            if (end_stream) try self.deliverData(s, &.{}, true);
            return;
        }

        if (end_stream) try self.dispatch(s);
    }

    /// 流式模式：把一段请求数据交给业务；`end_stream` 为真时收尾并销毁流。
    fn deliverData(self: *Connection, s: *StreamState, data: []const u8, end_stream: bool) !void {
        const hooks = self.stream_hooks orelse return;
        const rsp = s.rsp orelse blk: {
            const r = try s.allocator().create(Responder);
            r.* = .{ .conn = self, .stream = s };
            s.rsp = r;
            break :blk r;
        };

        // 每流任务模式：把**真正干活的那一帧**（END_STREAM）挑到别的任务。
        // 这样同连接上的多个 unary/bidi 请求不再互相排队，队头阻塞消失。
        //
        // 只在**同连接确实有多条流**时才挑走：顺序客户端（一次一个请求）继续内联
        // 执行，零handoff、零锁、零额外 flush —— 这正是稳态吞吐不低于默认模式的关键。
        if (end_stream and self.streams.items.len > 1) {
            if (self.job_submit) |submit| {
                if (self.job_ctx) |jctx| {
                    // data 指向复用的 frame_buf，必须复制进本流 arena 才能跨任务持有
                    s.end_data = try s.allocator().dupe(u8, data);
                    s.end_hooks = hooks;
                    s.job_kind = .stream_end;
                    s.leased = true;
                    s.finished = false;
                    s.aborted = false;
                    // 写在写锁内：给任务一个明确的 happens-before
                    self.write_mutex.lock();
                    self.active_jobs += 1;
                    self.write_mutex.unlock();
                    submit(jctx, self, s);
                    return;
                }
            }
        }

        hooks.onData(hooks.ctx, rsp, data, end_stream) catch |e| {
            log.h2.err("流式 onData 失败（流 {d}，{d} 字节）：{s}", .{
                s.id, data.len, @errorName(e),
            });
            if (!rsp.begun) rsp.begin("500", &.{}) catch {};
            if (!rsp.ended) rsp.end(&.{}) catch {};
        };

        if (end_stream) {
            // 业务未显式收尾时补上，保证每个流都有 END_STREAM
            if (!rsp.ended) rsp.end(&.{}) catch {};
            self.destroyStream(s);
        }
    }

    fn finishHeaders(self: *Connection, s: *StreamState) !void {
        const a = s.allocator();
        self.decoder.decode(s.header_block.items, &s.headers, a) catch |e| {
            log.h2.err("HPACK 解码失败（流 {d}）：{s}", .{ s.id, @errorName(e) });
            try self.sendGoAway(.compression_error);
            return error.CompressionError;
        };
        s.headers_done = true;
        for (s.headers.items) |h| {
            if (std.mem.eql(u8, h.name, ":method")) s.method = h.value;
            if (std.mem.eql(u8, h.name, ":path")) s.path = h.value;
            if (std.mem.eql(u8, h.name, ":scheme")) s.scheme = h.value;
            if (std.mem.eql(u8, h.name, ":authority")) s.authority = h.value;
        }
    }

    fn onData(self: *Connection, hdr: FrameHeader, payload: []const u8) !void {
        const s = self.findStream(hdr.stream_id) orelse {
            // 流不存在（已结束/从未开启）：回补连接窗口即可，不必回 RST（可能已关闭）
            try self.refundWindow(hdr.stream_id, payload.len, false);
            return;
        };

        var data = payload;
        if (hdr.flags & Flags.padded != 0) {
            // pad 长度越界：只拒绝该流（不能把错误抛上去，否则一条坏帧就能拆掉整条连接、
            // 连带弄死其它正常流。RFC 9113 也要求按流处理协议错误）。
            if (payload.len == 0 or @as(usize, payload[0]) > payload.len - 1) {
                log.h2.warn("流 {d} 的 PADDED 帧 pad 长度越界（载荷 {d} 字节），拒绝该流", .{
                    hdr.stream_id, payload.len,
                });
                try self.refundWindow(hdr.stream_id, payload.len, true);
                try self.sendRst(hdr.stream_id, .protocol_error);
                self.killStream(s);
                return;
            }
            const pad = @as(usize, payload[0]);
            data = payload[1 .. payload.len - pad];
        }

        // 无论是否超限都要回补窗口，否则对端会卡住
        try self.refundWindow(hdr.stream_id, payload.len, true);

        // 流式模式：不累积 body（业务自己维护重组缓冲），DATA 到达即回调
        if (self.stream_hooks != null) {
            try self.deliverData(s, data, hdr.flags & Flags.end_stream != 0);
            return;
        }

        const a = s.allocator();
        if (s.body.items.len + data.len > self.opts.max_body) {
            log.h2.warn("请求体超上限 {d} 字节（流 {d}）", .{ self.opts.max_body, s.id });
            try self.sendRst(hdr.stream_id, .enhance_your_calm);
            self.killStream(s);
            return;
        }
        try s.body.appendSlice(a, data);

        if (hdr.flags & Flags.end_stream != 0) try self.dispatch(s);
    }

    /// 收到 DATA 后把窗口补回去（连接级 + 流级）。
    ///
    /// **累积到 `refund_threshold` 才发**（对齐 Go `net/http2` 的 `inflowMinRefresh = 4 KiB`）。
    /// 逐帧回补会让每个请求多出 2 个 WINDOW_UPDATE 帧；128 字节请求下这是 18 字节 + 2 帧的纯开销。
    /// 阈值 4 KiB 远小于初始窗口 64 KiB，因此不会让对端窗口饿死。
    fn refundWindow(self: *Connection, stream_id: u32, amount: usize, with_stream: bool) !void {
        if (amount == 0) return;
        self.pending_conn_refund += amount;
        if (with_stream) {
            if (self.findStream(stream_id)) |s| s.pending_stream_refund += amount;
        }

        if (self.pending_conn_refund >= self.refund_threshold) {
            const n = self.pending_conn_refund;
            self.pending_conn_refund = 0;
            try self.sendWindowUpdate(0, n);
        }
        if (with_stream) {
            if (self.findStream(stream_id)) |s| {
                if (s.pending_stream_refund >= self.refund_threshold) {
                    const n = s.pending_stream_refund;
                    s.pending_stream_refund = 0;
                    try self.sendWindowUpdate(stream_id, n);
                }
            }
        }
    }

    fn sendWindowUpdate(self: *Connection, stream_id: u32, amount: usize) !void {
        const inc: u32 = @intCast(@min(amount, 0x7fff_ffff));
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, inc, .big);
        try self.writeFrame(.window_update, 0, stream_id, &buf);
    }

    fn dispatch(self: *Connection, s: *StreamState) !void {
        const ctx = self.serve_ctx orelse return;
        const handler = self.serve_handler orelse return;

        if (self.dispatch_depth >= max_dispatch_depth) {
            log.h2.warn("派发嵌套过深（{d}），拒绝流 {d}", .{ self.dispatch_depth, s.id });
            try self.sendRst(s.id, .refused_stream);
            self.killStream(s);
            return;
        }

        // 每流一个任务：把 handler 挪出连接任务，消除队头阻塞。
        // 租约期间该流归 handler 任务所有，连接任务只读、不回收（见 `reapStreams`）。
        if (self.job_submit) |submit| {
            if (self.job_ctx) |jctx| {
                s.job_kind = .dispatch;
                s.leased = true;
                s.finished = false;
                s.aborted = false;
                self.write_mutex.lock();
                self.active_jobs += 1;
                self.write_mutex.unlock();
                submit(jctx, self, s);
                return;
            }
        }

        defer self.destroyStream(s);
        self.dispatch_depth += 1;
        defer self.dispatch_depth -= 1;
        try self.runInlineHandler(s, ctx, handler);
    }

    /// 在**当前任务**里同步执行 handler（默认路径：吞吐最好，但会队头阻塞）。
    fn runInlineHandler(
        self: *Connection,
        s: *StreamState,
        ctx: *anyopaque,
        handler: HandlerFn,
    ) !void {
        var rsp = Responder{ .conn = self, .stream = s };
        const req = Request{
            .stream_id = s.id,
            .method = s.method,
            .path = s.path,
            .scheme = s.scheme,
            .authority = s.authority,
            .headers = s.headers.items,
            .body = s.body.items,
        };

        handler(ctx, req, &rsp, s.allocator()) catch |e| {
            log.h2.err("处理 {s} {s} 失败（流 {d}）：{s}", .{
                req.method, req.path, s.id, @errorName(e),
            });
            if (!rsp.begun) rsp.begin("500", &.{}) catch {};
            if (!rsp.ended) rsp.end(&.{}) catch {};
            return;
        };

        // handler 没显式收尾时补上，保证每个流都有 END_STREAM
        if (!rsp.begun) try rsp.begin("200", &.{});
        if (!rsp.ended) try rsp.end(&.{});
    }

    /// 每流任务的入口：由提交钩子的执行方（线程池 / 事件循环任务）调用。
    ///
    /// 结束后自动释放租约，连接任务会在下次处理帧时回收该流。
    pub fn runStreamJob(self: *Connection, s: *StreamState) void {
        defer self.markStreamFinished(s);
        const ctx = self.serve_ctx orelse return;
        const handler = self.serve_handler orelse return;
        self.runInlineHandler(s, ctx, handler) catch |e| {
            log.h2.warn("流 {d} 处理结束但收尾失败：{s}", .{ s.id, @errorName(e) });
        };
    }

    /// 每流任务的统一入口：提交钩子的执行方只需调这一个函数。
    ///
    /// 结束后必定释放租约（`markStreamFinished`），连接任务会在下次处理帧时回收该流。
    pub fn runJob(self: *Connection, s: *StreamState) void {
        switch (s.job_kind) {
            .dispatch => self.runStreamJob(s),
            .stream_end => self.runStreamEndJob(s),
        }
    }

    /// 流式路径的每流任务：处理 END_STREAM 那一帧并收尾。
    ///
    /// 只依赖**提交时快照**的 `end_hooks` / `end_data`，不再读 `self.stream_hooks`
    /// （那会在 `serveStreaming` 返回时被置空，晚到的任务会拿到 null 而静默丢失请求）。
    fn runStreamEndJob(self: *Connection, s: *StreamState) void {
        defer self.markStreamFinished(s);
        const hooks = s.end_hooks orelse return;
        const rsp = s.rsp orelse return;

        hooks.onData(hooks.ctx, rsp, s.end_data, true) catch |e| {
            log.h2.err("流式 onData 失败（流 {d}，{d} 字节）：{s}", .{
                s.id, s.end_data.len, @errorName(e),
            });
            if (!rsp.begun) rsp.begin("500", &.{}) catch {};
            if (!rsp.ended) rsp.end(&.{}) catch {};
        };

        // 业务未显式收尾时补上，保证每个流都有 END_STREAM
        if (!rsp.ended) rsp.end(&.{}) catch {};
    }

    fn markStreamFinished(self: *Connection, s: *StreamState) void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        s.finished = true;
        self.active_jobs -= 1;
        // **合并刷出的精确判据**：只有“最后一个完成的 handler”（把 active_jobs
        // 减到 0 的那个）负责刷出。此时其他任务的写帧早已在缓冲里（它们先写后减），
        // 所以 N 路并发完成只会产生 **一次** write() 系统调用。
        // 不用时间窗、不用试探锁，也不会出现“没人刷”的窗口。
        if (self.active_jobs == 0 and self.unflushed > 0) {
            self.writer.flush() catch {};
            self.unflushed = 0;
        }
    }

    /// 等所有每流任务结束（**连接收尾前必须调**，否则 `deinit` 会撕掉仍在用的 arena）。
    pub fn waitJobs(self: *Connection, io: std.Io) void {
        while (true) {
            self.write_mutex.lock();
            const n = self.active_jobs;
            self.write_mutex.unlock();
            if (n == 0) return;
            std.Io.sleep(io, .{ .nanoseconds = 50 * std.time.ns_per_ms }, .awake) catch return;
        }
    }

    /// 回收已完成的租约流（**只由连接任务调用**）。
    ///
    /// `destroyStream` 会从 `streams` 里交换移除，所以销毁时不要自增下标。
    fn reapStreams(self: *Connection) void {
        var i: usize = 0;
        while (i < self.streams.items.len) {
            const s = self.streams.items[i];
            if (!s.leased or !s.finished) {
                i += 1;
                continue;
            }
            self.destroyStream(s);
        }
    }

    /// 安全地终止一条流：若它正被别的任务持有，只做标记，回收交给 `reapStreams`。
    ///
    /// 直接销毁会 use-after-free：handler 任务手上还有 `*StreamState`。
    fn killStream(self: *Connection, s: *StreamState) void {
        if (s.leased and !s.finished) {
            self.write_mutex.lock();
            defer self.write_mutex.unlock();
            s.aborted = true;
            return;
        }
        self.destroyStream(s);
    }
};

/// 去掉 HEADERS 帧的 PADDED / PRIORITY 前缀；非法返回 null。
fn stripHeaderExtras(hdr: FrameHeader, payload: []const u8) ?[]const u8 {
    var start: usize = 0;
    var end: usize = payload.len;
    if (hdr.flags & Flags.padded != 0) {
        if (payload.len == 0) return null;
        const pad = @as(usize, payload[0]);
        start += 1;
        if (pad > end - start) return null;
        end -= pad;
    }
    if (hdr.flags & Flags.priority != 0) {
        start += 5;
        if (start > end) return null;
    }
    return payload[start..end];
}

// ---------------------------------------------------------------------------
// 测试：明文 h2c 端到端（构造真实客户端字节流，逐帧断言响应）
// ---------------------------------------------------------------------------

const testing = std.testing;

pub fn appendFrame(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: std.mem.Allocator,
    ftype: FrameType,
    flags: u8,
    sid: u32,
    payload: []const u8,
) !void {
    try buf.append(alloc, @intCast((payload.len >> 16) & 0xff));
    try buf.append(alloc, @intCast((payload.len >> 8) & 0xff));
    try buf.append(alloc, @intCast(payload.len & 0xff));
    try buf.append(alloc, @backingInt(ftype));
    try buf.append(alloc, flags);
    var sidb: [4]u8 = undefined;
    std.mem.writeInt(u32, &sidb, sid, .big);
    try buf.appendSlice(alloc, &sidb);
    try buf.appendSlice(alloc, payload);
}

pub const ParsedFrame = struct {
    ftype: FrameType,
    flags: u8,
    stream_id: u32,
    payload: []const u8,
};

pub fn nextFrame(data: []const u8, pos: *usize) ?ParsedFrame {
    if (pos.* + 9 > data.len) return null;
    const b = data[pos.*..];
    const len = (@as(usize, b[0]) << 16) | (@as(usize, b[1]) << 8) | @as(usize, b[2]);
    if (pos.* + 9 + len > data.len) return null;
    const f = ParsedFrame{
        .ftype = @fromBackingInt(@intCast(b[3])),
        .flags = b[4],
        .stream_id = std.mem.readInt(u32, b[5..9], .big) & 0x7fff_ffff,
        .payload = b[9 .. 9 + len],
    };
    pos.* += 9 + len;
    return f;
}

fn echoHandler(
    ctx: *anyopaque,
    req: Request,
    rsp: *Responder,
    arena: std.mem.Allocator,
) anyerror!void {
    _ = ctx;
    _ = arena;
    try rsp.begin("200", &.{.{ .name = "content-type", .value = "text/plain" }});
    try rsp.sendData(req.body);
    try rsp.end(&.{.{ .name = "x-done", .value = "1" }});
}

fn encodeReqHeaders(a: std.mem.Allocator, path: []const u8) !std.ArrayListUnmanaged(u8) {
    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = path },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "text/plain" },
    }, &block, a);
    return block;
}

test "h2：明文 h2c 端到端（preface + SETTINGS + HEADERS/DATA → 响应帧齐全）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});

    const block = try encodeReqHeaders(a, "/echo");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, block.items);
    try appendFrame(&input, a, .data, Flags.end_stream, 1, "hello-h2");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    const out = aw.written();
    var pos: usize = 0;
    var saw_settings = false;
    var saw_settings_ack = false;
    var saw_headers = false;
    var saw_status_200 = false;
    var got_body: []const u8 = "";
    var saw_trailers = false;
    var saw_trailer_value = false;

    while (nextFrame(out, &pos)) |f| {
        switch (f.ftype) {
            .settings => {
                if (f.flags & Flags.ack != 0) saw_settings_ack = true else saw_settings = true;
            },
            .window_update => {},
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, ":status") and std.mem.eql(u8, h.value, "200")) {
                        saw_status_200 = true;
                    }
                    if (std.mem.eql(u8, h.name, "x-done") and std.mem.eql(u8, h.value, "1")) {
                        saw_trailer_value = true;
                    }
                }
                if (f.flags & Flags.end_stream != 0) {
                    saw_trailers = true;
                } else {
                    saw_headers = true;
                }
            },
            .data => got_body = f.payload,
            else => {},
        }
    }

    try testing.expect(saw_settings);
    try testing.expect(saw_settings_ack);
    try testing.expect(saw_headers);
    try testing.expect(saw_status_200);
    try testing.expectEqualStrings("hello-h2", got_body);
    try testing.expect(saw_trailers);
    try testing.expect(saw_trailer_value);
}

test "h2：PING 必须回 ACK（同 payload）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    try appendFrame(&input, a, .ping, 0, 0, "12345678");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var ack_payload: []const u8 = "";
    while (nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype == .ping and f.flags & Flags.ack != 0) ack_payload = f.payload;
    }
    try testing.expectEqualStrings("12345678", ack_payload);
}

test "h2：WINDOW_UPDATE 累积到阈值才发（对齐 Go inflowMinRefresh=4KiB）" {
    // 场景一：小 DATA（10 字节 < 4 KiB 阈值）→ **不应**发送 WINDOW_UPDATE，
    // 否则每个 128 字节请求都要多出 2 个帧（纯开销）。
    {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();

        var input: std.ArrayListUnmanaged(u8) = .empty;
        try input.appendSlice(a, client_preface);
        try appendFrame(&input, a, .settings, 0, 0, &.{});
        const block = try encodeReqHeaders(a, "/echo");
        try appendFrame(&input, a, .headers, Flags.end_headers, 1, block.items);
        try appendFrame(&input, a, .data, Flags.end_stream, 1, "1234567890"); // 10 字节

        var r = std.Io.Reader.fixed(input.items);
        var aw = std.Io.Writer.Allocating.init(testing.allocator);
        defer aw.deinit();

        var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
        defer conn.deinit();
        var dummy: u8 = 0;
        conn.serve(&dummy, echoHandler) catch |e| {
            if (e != error.ConnectionClosed) return e;
        };

        var pos: usize = 0;
        var updates: usize = 0;
        while (nextFrame(aw.written(), &pos)) |f| {
            if (f.ftype == .window_update) updates += 1;
        }
        try testing.expectEqual(@as(usize, 0), updates);
    }

    // 场景二：DATA 超过阈值 → 连接级与流级**各发一次**，额度等于实际接收量
    {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();

        const big = try a.alloc(u8, 5000);
        @memset(big, 'x');

        var input: std.ArrayListUnmanaged(u8) = .empty;
        try input.appendSlice(a, client_preface);
        try appendFrame(&input, a, .settings, 0, 0, &.{});
        const block = try encodeReqHeaders(a, "/echo");
        try appendFrame(&input, a, .headers, Flags.end_headers, 1, block.items);
        try appendFrame(&input, a, .data, Flags.end_stream, 1, big);

        var r = std.Io.Reader.fixed(input.items);
        var aw = std.Io.Writer.Allocating.init(testing.allocator);
        defer aw.deinit();

        var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
        defer conn.deinit();
        var dummy: u8 = 0;
        conn.serve(&dummy, echoHandler) catch |e| {
            if (e != error.ConnectionClosed) return e;
        };

        var pos: usize = 0;
        var conn_inc: u32 = 0;
        var stream_inc: u32 = 0;
        while (nextFrame(aw.written(), &pos)) |f| {
            if (f.ftype == .window_update) {
                const inc = std.mem.readInt(u32, f.payload[0..4], .big);
                if (f.stream_id == 0) conn_inc += inc else stream_inc += inc;
            }
        }
        try testing.expectEqual(@as(u32, 5000), conn_inc);
        try testing.expectEqual(@as(u32, 5000), stream_inc);
    }
}

test "h2：多路复用（两个流交错收头/收体，各自独立响应）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});

    const b1 = try encodeReqHeaders(a, "/echo/one");
    const b3 = try encodeReqHeaders(a, "/echo/three");

    // 交错：两条流都先发 HEADERS（不带 END_STREAM），再分别发 DATA
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    try appendFrame(&input, a, .headers, Flags.end_headers, 3, b3.items);
    try appendFrame(&input, a, .data, Flags.end_stream, 1, "one");
    try appendFrame(&input, a, .data, Flags.end_stream, 3, "three");

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var status_count: usize = 0;
    var body1: []const u8 = "";
    var body3: []const u8 = "";
    var end1 = false;
    var end3 = false;
    var refused = false;

    while (nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .headers => {
                if (f.stream_id == 0) continue;
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                var is_status = false;
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, ":status")) is_status = true;
                }
                if (is_status) status_count += 1;
                if (f.flags & Flags.end_stream != 0) {
                    if (f.stream_id == 1) end1 = true;
                    if (f.stream_id == 3) end3 = true;
                }
            },
            .data => {
                if (f.stream_id == 1) body1 = f.payload;
                if (f.stream_id == 3) body3 = f.payload;
            },
            .rst_stream => refused = true,
            else => {},
        }
    }

    // 两条流都必须被处理（不是被 REFUSED_STREAM 打回）
    try testing.expect(!refused);
    try testing.expectEqual(@as(usize, 2), status_count);
    try testing.expectEqualStrings("one", body1);
    try testing.expectEqualStrings("three", body3);
    try testing.expect(end1);
    try testing.expect(end3);
}
// ---------------------------------------------------------------------------
// 健壮性：畸形/超限输入必须「明确拒绝」或「干净收尾」，绝不能 panic 或越界读
// ---------------------------------------------------------------------------

/// 把一段构造好的客户端字节流喂给连接，返回服务端写入的字节（-ConnectionClosed 视为正常收尾）。
fn driveConn(a: std.mem.Allocator, input: []const u8, opts: Options) ![]const u8 {
    var r = std.Io.Reader.fixed(input);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();
    var conn = Connection.init(testing.allocator, &r, &aw.writer, opts);
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };
    return try a.dupe(u8, aw.written());
}

/// 统计输出里指定流收到的 RST_STREAM 的错误码（没有则返回 null）。
fn findRstCode(out: []const u8, sid: u32) ?u32 {
    var pos: usize = 0;
    while (nextFrame(out, &pos)) |f| {
        if (f.ftype == .rst_stream and f.stream_id == sid and f.payload.len >= 4) {
            return std.mem.readInt(u32, f.payload[0..4], .big);
        }
    }
    return null;
}

test "h2健壮性：连接前言不对 → 返回 BadPreface（不 panic）" {
    const bad = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";
    var r = std.Io.Reader.fixed(bad);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();
    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    try testing.expectError(error.BadPreface, conn.serve(&dummy, echoHandler));
}

test "h2健壮性：并发流超上限 → REFUSED_STREAM(7)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    // 两条都只发 HEADERS（不 END_STREAM）→ 第二条会超过上限 1
    const b1 = try encodeReqHeaders(a, "/one");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    const b3 = try encodeReqHeaders(a, "/three");
    try appendFrame(&input, a, .headers, Flags.end_headers, 3, b3.items);

    const out = try driveConn(a, input.items, .{ .max_concurrent_streams = 1 });
    try testing.expectEqual(@as(?u32, 7), findRstCode(out, 3)); // REFUSED_STREAM
}

test "h2健壮性：header block 超上限 → ENHANCE_YOUR_CALM(11)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b1 = try encodeReqHeaders(a, "/big-header");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);

    // 上限压到 16 字节：上面那块的 HPACK 编码必然超
    const out = try driveConn(a, input.items, .{ .max_header_block = 16 });
    try testing.expectEqual(@as(?u32, 11), findRstCode(out, 1)); // ENHANCE_YOUR_CALM
}

test "h2健壮性：请求体超上限 → ENHANCE_YOUR_CALM(11)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b1 = try encodeReqHeaders(a, "/too-big");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    try appendFrame(&input, a, .data, 0, 1, "0123456789abcdef"); // 16 字节

    const out = try driveConn(a, input.items, .{ .max_body = 8 });
    // 实现用 ENHANCE_YOUR_CALM(11)：语义是「请求体太激进」，比 REFUSED_STREAM 更准确
    try testing.expectEqual(@as(?u32, 11), findRstCode(out, 1));
}

test "h2健壮性：帧声明长度大于实际字节 → 干净收尾（不 panic）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    // 头部声明 1024 字节，但只给 3 字节
    try input.appendSlice(a, &.{ 0x00, 0x04, 0x00, @backingInt(FrameType.data), 0, 0, 0, 0, 1 });
    try input.appendSlice(a, "abc");

    _ = try driveConn(a, input.items, .{});
}

test "h2健壮性：PADDED 帧 pad 长度越界 → 拒绝该流（不越界读）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b1 = try encodeReqHeaders(a, "/padded");
    try appendFrame(&input, a, .headers, Flags.end_headers, 1, b1.items);
    // PADDED 标志 + pad length=200（远超实际载荷）
    try appendFrame(&input, a, .data, Flags.padded, 1, &.{ 200, 'x' });

    const out = try driveConn(a, input.items, .{});
    // 必须给出明确拒绝（协议错误），而不是读到缓冲区外
    try testing.expect(findRstCode(out, 1) != null);
}
test "h2健壮性：孤立 CONTINUATION（流不存在）不拆连接，后续帧仍被处理" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    // 流 3 从未建立 → 这条 CONTINUATION 是孤立的
    try appendFrame(&input, a, .continuation, Flags.end_headers, 3, "junk");
    // 同一条连接上的 PING 必须仍然被回应（说明连接没被拆）
    try appendFrame(&input, a, .ping, 0, 0, "12345678");

    const out = try driveConn(a, input.items, .{});
    var pos: usize = 0;
    var got_ping_ack = false;
    while (nextFrame(out, &pos)) |f| {
        if (f.ftype == .ping and f.flags & Flags.ack != 0) got_ping_ack = true;
    }
    try testing.expect(got_ping_ack);
}

test "h2健壮性：偶数流号（客户端非法）→ GOAWAY + ProtocolError" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    const b2 = try encodeReqHeaders(a, "/even");
    try appendFrame(&input, a, .headers, Flags.end_headers, 2, b2.items); // 偶数流号

    try testing.expectError(error.ProtocolError, driveConn(a, input.items, .{}));
}

test "h2健壮性：RST_STREAM 载荷不是 4 字节 → FRAME_SIZE_ERROR" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    try appendFrame(&input, a, .rst_stream, 0, 1, &.{ 0x00, 0x01 }); // 只给 2 字节

    try testing.expectError(error.FrameSizeError, driveConn(a, input.items, .{}));
}

test "SpinLock：tryLock 拿不到立刻返回 false（不自旋阻塞）" {
    var l: SpinLock = .{};
    try testing.expect(l.tryLock());
    // 已被占住 → 不能再拿到
    try testing.expect(!l.tryLock());
    l.unlock();
    try testing.expect(l.tryLock());
    l.unlock();
}

test "keepalive：连接正忙时 trySendPing 立刻放弃（不阻塞、不计数）" {
    var r = std.Io.Reader.fixed("");
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();

    // 空闲连接：PING 正常发出并计数
    try testing.expect(try conn.trySendPing(null));
    try testing.expectEqual(@as(u64, 1), conn.pings_sent);

    // 模拟「另一线程正在写业务帧」：写锁被占
    conn.write_mutex.lock();
    defer conn.write_mutex.unlock();
    // 必须立刻返回 false（而不是排队等锁）——这是高并发下 pingAll 不拖死别人的前提
    try testing.expect(!(try conn.trySendPing(null)));
    // 跳过的那次不计数
    try testing.expectEqual(@as(u64, 1), conn.pings_sent);
}

test "h2：延迟 flush —— 一批请求在一段输入里必须全部得到响应" {
    // 回归点：`writeFrameBody` 不再自己 flush，改由读循环在阻塞前统一刷。
    // 所以「一段输入里塞 N 个请求」时，不能因为缓冲没刷而漏发任何一条响应。
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const n_req = 4;
    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});

    const block = try encodeReqHeaders(a, "/echo");
    for (0..n_req) |i| {
        const sid: u32 = 1 + @as(u32, @intCast(i)) * 2;
        try appendFrame(&input, a, .headers, Flags.end_headers, sid, block.items);
        try appendFrame(&input, a, .data, Flags.end_stream, sid, "batch");
    }

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    // 每个流的 DATA 都必须到，且内容正确
    var got: [n_req]bool = @splat(false);
    var pos: usize = 0;
    while (nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype != .data) continue;
        if (f.stream_id == 0 or f.stream_id % 2 == 0) continue;
        const idx = (f.stream_id - 1) / 2;
        if (idx >= n_req) continue;
        try testing.expectEqualStrings("batch", f.payload);
        got[idx] = true;
    }
    for (got) |g| try testing.expect(g);

    // 流全部收尾，且对象已归还空闲池（供下一个请求复用，不再走 malloc）
    try testing.expectEqual(@as(usize, 0), conn.activeStreams());
    try testing.expect(conn.spare.items.len >= 1);
}

test "h2：流对象复用 —— 串行 N 个请求不会攒下 N 个流对象" {
    // 复用判据：串行服务多个请求时，空闲池不会随请求数线性增长
    // （每个请求用完立刻归还，下一个请求复用同一个对象与同一块 arena 容量）。
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const block = try encodeReqHeaders(a, "/echo");

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, client_preface);
    try appendFrame(&input, a, .settings, 0, 0, &.{});
    for (0..3) |i| {
        const sid: u32 = 1 + @as(u32, @intCast(i)) * 2;
        try appendFrame(&input, a, .headers, Flags.end_headers, sid, block.items);
        try appendFrame(&input, a, .data, Flags.end_stream, sid, "reuse");
    }

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.serve(&dummy, echoHandler) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    try testing.expectEqual(@as(usize, 0), conn.activeStreams());
    // 3 个请求串行完成：池里最多 1 个对象（反复复用），绝不是 3 个
    try testing.expect(conn.spare.items.len <= 1);
}

// 并发写路径（为「每流一个 handler 任务」准备的地基）。
//
// 关键契约：窗口不足时**不阻塞读**（只有连接任务能读 socket），
// 而是把剩余字节挂到该流的 pending_out，等 WINDOW_UPDATE 由连接任务续发。
test "h2 concurrent write: pending data drains after window update" {
    var r = std.Io.Reader.fixed("");
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.concurrent_write = true;

    const s = try conn.createStream(1);
    // 模拟「对端还没回补窗口」
    conn.send_window = 0;
    s.send_window = 0;

    try conn.sendStreamData(s, "hello", true);
    try testing.expectEqual(@as(usize, 0), aw.written().len); // 一个字节都没发
    try testing.expectEqual(@as(usize, 5), s.pending_out.items.len);

    // 窗口回补 → 续发，且最后一帧带 END_STREAM
    conn.send_window = 1024;
    s.send_window = 1024;
    try conn.drainStreamPending(s);

    try testing.expectEqual(@as(usize, 0), s.pending_out.items.len);
    var pos: usize = 0;
    var saw_data = false;
    while (nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype != .data) continue;
        try testing.expectEqualStrings("hello", f.payload);
        try testing.expect(f.flags & Flags.end_stream != 0);
        saw_data = true;
    }
    try testing.expect(saw_data);
}

test "h2 concurrent write: trailers deferred behind pending body" {
    var r = std.Io.Reader.fixed("");
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.concurrent_write = true;

    const s = try conn.createStream(1);
    conn.send_window = 0;
    s.send_window = 0;

    try conn.sendStreamData(s, "abc", false);
    // 还有正文挂起时收尾：trailers 不能抢先发（顺序会乱）
    const trailers_block = [_]u8{ 0x00, 0x00 };
    try conn.finishStream(s, &trailers_block);
    try testing.expect(s.pending_trailers != null);
    try testing.expectEqual(@as(usize, 0), aw.written().len);

    conn.send_window = 1024;
    s.send_window = 1024;
    try conn.drainStreamPending(s);

    // 期望顺序：DATA(abc) 然后 HEADERS(END_STREAM)
    var pos: usize = 0;
    var first_data = false;
    var headers_after = false;
    while (nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .data => {
                try testing.expectEqualStrings("abc", f.payload);
                first_data = true;
            },
            .headers => {
                try testing.expect(first_data); // 必须在 DATA 之后
                try testing.expect(f.flags & Flags.end_stream != 0);
                headers_after = true;
            },
            else => {},
        }
    }
    try testing.expect(first_data);
    try testing.expect(headers_after);
    try testing.expect(s.pending_trailers == null);
}

test "h2 concurrent write: unflushed 由最后一个完成的 handler 清零" {
    var r = std.Io.Reader.fixed("");
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.concurrent_write = true;

    const s = try conn.createStream(1);
    conn.send_window = 1024;
    s.send_window = 1024;

    // 模拟「一个任务正在跑」：租约 + 活跃计数
    s.leased = true;
    s.finished = false;
    conn.active_jobs = 1;

    try conn.sendStreamData(s, "hi", true);
    // 并发写路径自己不刷：否则 N 路并发就是 N 次系统调用
    try testing.expect(conn.unflushed > 0);

    // 最后一个完成的 handler 负责刷出，并把计数清零
    conn.markStreamFinished(s);
    try testing.expectEqual(@as(usize, 0), conn.unflushed);
    try testing.expect(s.finished);

    var pos: usize = 0;
    var saw = false;
    while (nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype != .data) continue;
        try testing.expectEqualStrings("hi", f.payload);
        try testing.expect(f.flags & Flags.end_stream != 0);
        saw = true;
    }
    try testing.expect(saw);
}
