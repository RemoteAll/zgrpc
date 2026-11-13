//! gRPC 语义层（跑在 `h2` 之上）：消息分帧、状态码、路由、unary 与服务端流式。
//!
//! 协议要点（gRPC over HTTP/2，对照 `doc/PROTOCOL-HTTP2.md`）：
//! - 请求：`POST /{service}/{method}`、`content-type: application/grpc*`、`te: trailers`；
//! - 消息分帧：`[压缩标志:1][长度:4 大端][payload]`，可在一帧内多条（流式）；
//! - 响应：`:status 200` + `content-type: application/grpc`，**状态放在 trailers 的 `grpc-status`**；
//! - 错误文本 `grpc-message` 按规范做 **percent-encoding**（非 ASCII 直接发会违反规范）；
//! - `grpc-timeout` 换算成截止时刻，超时回 `DEADLINE_EXCEEDED`（见 `Call.expired`）；
//! - 本库不做 protobuf 编解码：业务拿到/给出的是**已序列化的字节**，由调用方自带 protobuf 实现
//!   （服务间自控两端时通常是零成本；要对接现成 .proto 再接一个 codegen 即可，不必改动本层）。
//!
//! 未支持（明确写出，避免误解）：`-bin` 二进制 metadata（base64）、`grpc-status-details-bin`、
//! `application/grpc-web-text`（base64 文本流）、`grpc-timeout` 的**强制中断**（本层只判超时状态，
//! 不会打断已经阻塞在业务里的调用）、客户端流/bidi 的背压。

const std = @import("std");
const log = @import("log.zig");
const h2 = @import("h2.zig");
const hpack = @import("hpack.zig");
const gzip = @import("gzip.zig");
const rpc_status = @import("status.zig");
const clock = @import("clock.zig");

/// gRPC 状态码（https://grpc.io/docs/guides/status-codes/）。
pub const Status = enum(u32) {
    ok = 0,
    cancelled = 1,
    unknown_error = 2,
    invalid_argument = 3,
    deadline_exceeded = 4,
    not_found = 5,
    already_exists = 6,
    permission_denied = 7,
    resource_exhausted = 8,
    failed_precondition = 9,
    aborted = 10,
    out_of_range = 11,
    unimplemented = 12,
    internal = 13,
    unavailable = 14,
    data_loss = 15,
    unauthenticated = 16,
};

pub const Message = struct {
    compressed: bool,
    payload: []const u8,
};

/// 遍历一个 gRPC 请求体里的全部消息（压缩消息会**自动解压**）。
pub const MessageIterator = struct {
    data: []const u8,
    pos: usize = 0,
    /// 解压用分配器：压缩消息必须有它（为空时压缩消息返回 null，避免把压缩字节当明文给业务）
    alloc: ?std.mem.Allocator = null,

    pub fn next(self: *MessageIterator) ?Message {
        if (self.pos + 5 > self.data.len) return null;
        const compressed = self.data[self.pos] != 0;
        const len = std.mem.readInt(u32, self.data[self.pos + 1 ..][0..4], .big);
        const start = self.pos + 5;
        if (len > self.data.len - start) return null;
        const raw = self.data[start .. start + len];
        self.pos = start + len;
        if (!compressed) return .{ .compressed = false, .payload = raw };

        const a = self.alloc orelse return null;
        const plain = gzip.decompress(a, raw) catch return null;
        return .{ .compressed = true, .payload = plain };
    }
};

/// 把一条消息编码成 gRPC 帧（未压缩）追加到 `out`。
pub fn encodeMessage(
    out: *std.ArrayListUnmanaged(u8),
    alloc: std.mem.Allocator,
    payload: []const u8,
) !void {
    try out.append(alloc, 0);
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(payload.len), .big);
    try out.appendSlice(alloc, &len);
    try out.appendSlice(alloc, payload);
}

// ---------------------------------------------------------------------------
// 协议级工具：`grpc-timeout` 解析、`grpc-message` 的 percent 编解码
// ---------------------------------------------------------------------------

/// 解析 `grpc-timeout`（规范：≤ 8 位正整数 + 单位 H/M/S/m/u/n），返回**纳秒**。
/// 格式非法返回 null（调用方按「无限超时」处理，并留日志）。
pub fn parseTimeout(value: []const u8) ?i128 {
    if (value.len < 2) return null;
    const digits = value[0 .. value.len - 1];
    if (digits.len > 8) return null; // 规范上限：至多 8 位
    const unit = value[value.len - 1];
    var n: i128 = 0;
    for (digits) |c| {
        if (c < '0' or c > '9') return null;
        n = n * 10 + @as(i128, c - '0');
    }
    const per: i128 = switch (unit) {
        'H' => std.time.ns_per_hour,
        'M' => std.time.ns_per_min,
        'S' => std.time.ns_per_s,
        'm' => std.time.ns_per_ms,
        'u' => std.time.ns_per_us,
        'n' => 1,
        else => return null,
    };
    return n * per;
}

/// `grpc-message` 编码：规范只允许 `%x20-%x24` 与 `%x26-%x7E`（`%` 除外），其余一律 `%HH`。
pub fn percentEncode(alloc: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    const hex = "0123456789ABCDEF";
    for (s) |c| {
        if (c >= 0x20 and c <= 0x7e and c != '%') {
            try out.append(alloc, c);
        } else {
            try out.append(alloc, '%');
            try out.append(alloc, hex[c >> 4]);
            try out.append(alloc, hex[c & 0x0f]);
        }
    }
    return out.items;
}

/// `grpc-message` 解码：非法 `%xx` **原样保留**（规范要求解码方不得报错）。
pub fn percentDecode(alloc: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '%' and i + 2 < s.len) {
            const hi = std.fmt.charToDigit(s[i + 1], 16) catch {
                try out.append(alloc, s[i]);
                i += 1;
                continue;
            };
            const lo = std.fmt.charToDigit(s[i + 2], 16) catch {
                try out.append(alloc, s[i]);
                i += 1;
                continue;
            };
            try out.append(alloc, hi * 16 + lo);
            i += 3;
            continue;
        }
        try out.append(alloc, s[i]);
        i += 1;
    }
    return out.items;
}

// ---------------------------------------------------------------------------
// 二进制 metadata（`-bin`）：base64 编解码
// 规范：实现 MUST 接受带与不带 padding 的值，并**应当发不带 padding 的值**。
// ---------------------------------------------------------------------------

/// 编码：**不带 padding**（`-bin` metadata 用；规范要求发无 padding）。
pub fn encodeBase64NoPad(alloc: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    const enc = std.base64.standard_no_pad.Encoder;
    if (bytes.len == 0) return "";
    const buf = try alloc.alloc(u8, enc.calcSize(bytes.len));
    return enc.encode(buf, bytes);
}

/// 编码：**带 padding**（gRPC-Web 文本模式用）。
///
/// 为什么不能无 padding：多帧 base64 拼接时，无 padding 会让位流边界错位
/// （5 字节 → 7 字符留 2 bit），下一帧直接接上就解不回来了。规范允许服务端
/// 「随时 flush 并带 padding」，所以每帧独立 padding、客户端解码时忽略 `=` 即可。
pub fn encodeBase64Padded(alloc: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    const enc = std.base64.standard.Encoder;
    if (bytes.len == 0) return "";
    const buf = try alloc.alloc(u8, enc.calcSize(bytes.len));
    return enc.encode(buf, bytes);
}

/// 解码：**带/不带 padding 都接受**；两者都失败则 `error.InvalidBase64`。
pub fn decodeBase64Loose(alloc: std.mem.Allocator, text: []const u8) ![]const u8 {
    if (text.len == 0) return "";
    const padded = std.base64.standard.Decoder;
    if (padded.calcSizeForSlice(text)) |n| {
        const buf = try alloc.alloc(u8, n);
        if (padded.decode(buf, text)) |_| {
            return buf;
        } else |_| {
            // 落到无 padding 分支再试
        }
    } else |_| {}

    const no_pad = std.base64.standard_no_pad.Decoder;
    const n = no_pad.calcSizeForSlice(text) catch return error.InvalidBase64;
    const buf = try alloc.alloc(u8, n);
    no_pad.decode(buf, text) catch return error.InvalidBase64;
    return buf;
}

/// 取逗号分隔的第一段（规范：重复的二进制头可被合并成 `a,b`）。
fn firstCommaSegment(raw: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, raw, ',')) |i| return raw[0..i];
    return raw;
}

/// base64 **流式宽松解码**：忽略 `=`（in-stream padding）与空白。
///
/// 为什么需要它：gRPC-Web 文本模式下服务端可能「随时 flush 并带 padding」，
/// 所以 `=` 会出现在流中间（规范明确客户端不得假设 padding 只在消息边界），
/// 标准的 `std.base64` 解码器遇到中间 padding 会直接结束。
pub fn decodeBase64Stream(alloc: std.mem.Allocator, text: []const u8) ![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    var acc: u32 = 0;
    var bits: u5 = 0;
    for (text) |c| {
        if (c == '=') {
            // padding = 当前 base64 分组结束，丢弃不满 8 位的残余位
            acc = 0;
            bits = 0;
            continue;
        }
        if (c == '\r' or c == '\n' or c == ' ' or c == '\t') continue;
        const v: u32 = switch (c) {
            'A'...'Z' => c - 'A',
            'a'...'z' => c - 'a' + 26,
            '0'...'9' => c - '0' + 52,
            '+' => 62,
            '/' => 63,
            else => return error.InvalidBase64,
        };
        acc = (acc << 6) | v;
        if (bits + 6 >= 8) {
            const shift: u5 = @intCast(bits + 6 - 8);
            try out.append(alloc, @truncate((acc >> shift) & 0xff));
            bits = @intCast((bits + 6) % 8);
            acc &= (@as(u32, 1) << @intCast(bits)) - 1;
        } else {
            bits += 6;
        }
    }
    return out.items;
}

const grpc_content_type = [_]hpack.Header{
    .{ .name = "content-type", .value = "application/grpc" },
};

/// 识别 content-type：原生 gRPC / gRPC-Web / gRPC-Web-text（浏览器可达）。返回 false 表示不是 gRPC 流量。
fn describeContentType(req: h2.Request, call: *Call) bool {
    const ct = req.header("content-type") orelse return false;
    // 必须先判 web：`application/grpc-web` 也以 `application/grpc` 开头
    if (std.mem.startsWith(u8, ct, "application/grpc-web-text")) {
        // 文本模式（浏览器 XHR 回退）：整个 body 是 base64，响应也要 base64
        call.web = true;
        call.web_text = true;
        call.resp_content_type = "application/grpc-web-text+proto";
    } else if (std.mem.startsWith(u8, ct, "application/grpc-web")) {
        call.web = true;
        call.web_text = false;
        call.resp_content_type = "application/grpc-web+proto";
    } else if (std.mem.startsWith(u8, ct, "application/grpc")) {
        call.web = false;
        call.web_text = false;
        call.resp_content_type = "application/grpc";
    } else {
        return false;
    }

    // 客户端声明可接受的响应压缩算法（`grpc-accept-encoding: gzip, deflate`）
    if (req.header("grpc-accept-encoding")) |ae| {
        call.accept_gzip = containsIgnoreCase(ae, "gzip");
    }

    // `grpc-timeout`：换算成单调时钟上的绝对截止时刻（规范：缺省视为无限超时）
    if (req.header("grpc-timeout")) |v| {
        if (parseTimeout(v)) |ns| {
            call.timeout_ns = ns;
            call.deadline_ns = clock.monotonicNs() + ns;
        } else {
            log.grpc.warn("忽略非法的 grpc-timeout：{s}", .{v});
        }
    }

    // `te: trailers` 是规范要求的请求头（用于探测不兼容代理）。缺失时宽容处理但留痕，
    // 因为多数能跑到这里的客户端（包括我们的 Rust 对拍工具）都会带。
    const te_ok = if (req.header("te")) |v| containsIgnoreCase(v, "trailers") else false;
    if (!te_ok) {
        log.grpc.debug("请求缺少 te: trailers（{s}）", .{req.path});
    }
    return true;
}

/// 大小写不敏感的子串查找（头值匹配）。
fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

/// 一次调用：业务通过它读请求消息、写响应消息、设状态。
pub const Call = struct {
    req: h2.Request,
    rsp: *h2.Responder,
    alloc: std.mem.Allocator,
    status: Status = .ok,
    /// 失败时回给客户端的文本（放在 trailers 的 `grpc-message`）
    error_message: []const u8 = "",
    sent_any: bool = false,
    finished: bool = false,

    /// gRPC-Web（浏览器可达）：响应 `content-type: application/grpc-web+proto`，
    /// **trailers 走 body 尾帧**而不是 HTTP trailers。
    web: bool = false,
    /// gRPC-Web **文本模式**（`application/grpc-web-text`）：请求与响应都是 base64，
    /// 响应 `content-type: application/grpc-web-text+proto`。
    web_text: bool = false,
    /// 响应 content-type（web / 原生）
    resp_content_type: []const u8 = "application/grpc",
    /// 客户端声明接受 gzip（`grpc-accept-encoding`）→ 响应消息压缩
    accept_gzip: bool = false,
    /// 响应消息是否真压过（决定是否发 `grpc-encoding: gzip` 头）
    resp_gzip: bool = false,

    // --- 截止时间（`grpc-timeout`）---
    /// 客户端给的超时纳秒数（null = 未指定，视为无限）
    timeout_ns: ?i128 = null,
    /// 换算出的绝对截止时刻（单调时钟纳秒）；null = 无限
    deadline_ns: ?i128 = null,

    // --- 流式模式状态（由 `Server.streamHooks` 驱动）---
    /// 跨 DATA 帧的累积缓冲（整请求路由存原始帧，流式路由存待切分帧）
    accum: std.ArrayListUnmanaged(u8) = .empty,
    stream_route: ?*const StreamRoute = null,
    unary_route: ?Route = null,
    route_ctx: *anyopaque = undefined,

    /// 要随**响应头**一起发的自定义 metadata（须在首条 `sendMessage` 之前 append）
    resp_headers: std.ArrayListUnmanaged(hpack.Header) = .empty,
    /// 要随 **trailers** 一起发的自定义 metadata（可在 finish 前任意时刻 append）
    resp_trailers: std.ArrayListUnmanaged(hpack.Header) = .empty,

    /// unary 调用取第一条消息。
    pub fn firstMessage(self: *Call) ?Message {
        var it = MessageIterator{ .data = self.req.body, .alloc = self.alloc };
        return it.next();
    }

    pub fn messages(self: *Call) MessageIterator {
        return .{ .data = self.req.body, .alloc = self.alloc };
    }

    // -----------------------------------------------------------------------
    // Custom-Metadata（规范：`-bin` 结尾的用 base64，重复名可逗号合并）
    // -----------------------------------------------------------------------

    /// 读一个 ASCII metadata（同名多次出现时返回第一个）。
    pub fn requestMetadata(self: *Call, name: []const u8) ?[]const u8 {
        return self.req.header(name);
    }

    /// 读一个**二进制** metadata（名字以 `-bin` 结尾）：按规范先按 `,` 切分再 base64 解码。
    /// 同名多段时返回第一段；需要全部请用 `requestBinaryMetadataAll`。
    pub fn requestBinaryMetadata(self: *Call, arena: std.mem.Allocator, name: []const u8) !?[]const u8 {
        const raw = self.req.header(name) orelse return null;
        const first = firstCommaSegment(raw);
        return try decodeBase64Loose(arena, std.mem.trim(u8, first, " \t"));
    }

    /// 读一个二进制 metadata 的**全部**段（规范：重复名或逗号合并都可能出现）。
    pub fn requestBinaryMetadataAll(
        self: *Call,
        arena: std.mem.Allocator,
        name: []const u8,
    ) ![]const []const u8 {
        const raw = self.req.header(name) orelse return &.{};
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        var it = std.mem.splitScalar(u8, raw, ',');
        while (it.next()) |seg| {
            const t = std.mem.trim(u8, seg, " \t");
            if (t.len == 0) continue;
            try out.append(arena, try decodeBase64Loose(arena, t));
        }
        return out.items;
    }

    /// 加一条**响应头** metadata（须在首条 `sendMessage` 之前调用，否则已发出）。
    /// `name` 不得以 `-bin` 结尾（二进制请用 `setBinaryHeader`），否则头部值会不合规范。
    pub fn setHeader(self: *Call, name: []const u8, value: []const u8) !void {
        if (std.mem.endsWith(u8, name, "-bin")) return error.UseSetBinaryHeader;
        try self.resp_headers.append(self.alloc, .{ .name = name, .value = value });
    }

    /// 加一条**二进制响应头**：值自动 base64（**不带 padding**，规范：应发无 padding）。
    pub fn setBinaryHeader(self: *Call, name: []const u8, value: []const u8) !void {
        if (!std.mem.endsWith(u8, name, "-bin")) return error.BinaryNameMustEndWithBin;
        const enc = try encodeBase64NoPad(self.alloc, value);
        try self.resp_headers.append(self.alloc, .{ .name = name, .value = enc });
    }

    /// 加一条 **trailers** metadata（`grpc-status-details-bin` 等标准字段也走这里）。
    pub fn setTrailer(self: *Call, name: []const u8, value: []const u8) !void {
        if (std.mem.endsWith(u8, name, "-bin")) return error.UseSetBinaryTrailer;
        try self.resp_trailers.append(self.alloc, .{ .name = name, .value = value });
    }

    /// 加一条**二进制 trailers**（如 `grpc-status-details-bin`）：值自动 base64（无 padding）。
    pub fn setBinaryTrailer(self: *Call, name: []const u8, value: []const u8) !void {
        if (!std.mem.endsWith(u8, name, "-bin")) return error.BinaryNameMustEndWithBin;
        const enc = try encodeBase64NoPad(self.alloc, value);
        try self.resp_trailers.append(self.alloc, .{ .name = name, .value = enc });
    }

    /// 距截止时刻还有多少纳秒（未设超时返回 null；已过期则为负数）。
    /// 设置**富错误详情**：把 `google.rpc.Status` 序列化后写入 `grpc-status-details-bin` trailer。
    ///
    /// 规范里 `grpc-status` 只有一个数字码；业务要表达「哪个字段错了 / 缺什么权限 / 多久后重试」
    /// 就靠这条 trailer。`code` 建议与随后 `setStatus` 的错误码一致。
    pub fn setStatusDetails(self: *Call, arena: std.mem.Allocator, st: rpc_status.Status) !void {
        const raw = try rpc_status.encode(arena, st);
        try self.setBinaryTrailer("grpc-status-details-bin", raw);
    }
    pub fn remainingNs(self: *const Call) ?i128 {
        const d = self.deadline_ns orelse return null;
        return d - clock.monotonicNs();
    }

    /// 是否已过截止时刻。
    pub fn expired(self: *const Call) bool {
        const r = self.remainingNs() orelse return false;
        return r <= 0;
    }

    /// 业务在耗时循环（大钡循环 / 多阶段处理）里主动检查：过期就 `error.DeadlineExceeded`。
    /// 说明：本层不做强制中断（Zig 无抢占式取消），长时间阻塞的 handler 要靠业务自己插检查点。
    pub fn checkDeadline(self: *const Call) !void {
        if (self.expired()) return error.DeadlineExceeded;
    }

    /// 发送一条响应消息（服务端流式可多次调用）。
    ///
    /// 客户端在 `grpc-accept-encoding` 里声明 gzip 且消息够大时，自动压缩并置帧标志位。
    pub fn sendMessage(self: *Call, payload: []const u8) !void {
        if (self.finished) return error.AlreadyFinished;
        if (!self.sent_any) {
            const a = self.alloc;
            const should_gzip = self.accept_gzip and payload.len >= compress_min_size;
            // 顺序按规范 ABNF：HTTP-Status [Message-Encoding] [Message-Accept-Encoding] Content-Type *Custom-Metadata
            var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
            if (should_gzip) {
                try hs.append(a, .{ .name = "grpc-encoding", .value = "gzip" });
                self.resp_gzip = true;
            }
            try hs.append(a, .{ .name = "grpc-accept-encoding", .value = "gzip" });
            try hs.append(a, .{ .name = "content-type", .value = self.resp_content_type });
            // 业务自定义响应 metadata 放在 Call-Definition 之后（规范：Call-Definition 在前）
            try hs.appendSlice(a, self.resp_headers.items);
            try self.rsp.begin("200", hs.items);
            self.sent_any = true;
        }

        var buf: std.ArrayListUnmanaged(u8) = .empty;
        if (self.resp_gzip and payload.len >= compress_min_size) {
            const gz = try gzip.compress(self.alloc, payload);
            try buf.append(self.alloc, 1); // 压缩标志位
            var len: [4]u8 = undefined;
            std.mem.writeInt(u32, &len, @intCast(gz.len), .big);
            try buf.appendSlice(self.alloc, &len);
            try buf.appendSlice(self.alloc, gz);
        } else {
            try encodeMessage(&buf, self.alloc, payload);
        }
        if (self.web_text) {
            // 文本模式：每帧单独 base64（**带 padding**）后拼接，客户端解码时忽略 `=`
            const enc = try encodeBase64Padded(self.alloc, buf.items);
            return self.rsp.sendData(enc);
        }
        try self.rsp.sendData(buf.items);
    }

    pub fn setStatus(self: *Call, status: Status, message: []const u8) void {
        self.status = status;
        self.error_message = message;
    }

    /// 收尾：把 `grpc-status` 写进 trailers 并结束流。幂等。
    pub fn finish(self: *Call) !void {
        if (self.finished) return;
        self.finished = true;

        const a = self.alloc;
        var code_buf: [8]u8 = undefined;
        const code = try std.fmt.bufPrint(&code_buf, "{d}", .{@backingInt(self.status)});

        // gRPC-Web：trailers 做成 body 尾帧（0x80 | 4B 长度 | "k: v\r\n"...）
        if (self.web) {
            var block: std.ArrayListUnmanaged(u8) = .empty;
            try block.append(a, 0x80);
            const len_pos = block.items.len;
            try block.appendSlice(a, "\x00\x00\x00\x00");
            const start = block.items.len;
            try block.appendSlice(a, "grpc-status: ");
            try block.appendSlice(a, code);
            try block.appendSlice(a, "\r\n");
            if (self.error_message.len > 0) {
                // 规范：Status-Message 概念上是 UTF-8 文本，物理上必须 percent-encoding
                const enc = try percentEncode(a, self.error_message);
                try block.appendSlice(a, "grpc-message: ");
                try block.appendSlice(a, enc);
                try block.appendSlice(a, "\r\n");
            }
            // 自定义 trailers（含 `grpc-status-details-bin`）也入尾帧，键名保持小写
            for (self.resp_trailers.items) |h| {
                try block.appendSlice(a, h.name);
                try block.appendSlice(a, ": ");
                try block.appendSlice(a, h.value);
                try block.appendSlice(a, "\r\n");
            }
            std.mem.writeInt(u32, block.items[len_pos..][0..4], @intCast(block.items.len - start), .big);

            if (!self.sent_any) {
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try hs.append(a, .{ .name = "grpc-accept-encoding", .value = "gzip" });
                try hs.append(a, .{ .name = "content-type", .value = self.resp_content_type });
                try hs.appendSlice(a, self.resp_headers.items);
                try self.rsp.begin("200", hs.items);
                self.sent_any = true;
            }
            if (self.web_text) {
                const enc = try encodeBase64Padded(a, block.items);
                return self.rsp.endData(enc);
            }
            return self.rsp.endData(block.items);
        }

        var trailers: std.ArrayListUnmanaged(hpack.Header) = .empty;
        try trailers.append(a, .{ .name = "grpc-status", .value = code });
        if (self.error_message.len > 0) {
            // 规范：非 ASCII 的错误文本必须 percent-encoding 后才能进 header
            const enc = try percentEncode(a, self.error_message);
            try trailers.append(a, .{ .name = "grpc-message", .value = enc });
        }
        try trailers.appendSlice(a, self.resp_trailers.items);

        if (!self.sent_any) {
            var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
            try hs.append(a, .{ .name = "grpc-accept-encoding", .value = "gzip" });
            try hs.append(a, .{ .name = "content-type", .value = self.resp_content_type });
            try hs.appendSlice(a, self.resp_headers.items);
            try self.rsp.begin("200", hs.items);
            self.sent_any = true;
        }
        try self.rsp.end(trailers.items);
    }
};

/// 非 gRPC 流量（非 POST / content-type 不符）按 HTTP 状态码回，而不是伪装成 `:status 200` 的 gRPC 响应。
/// 规范：Content-Type 不以 `application/grpc` 开头时服务端 SHOULD 返回 415，
/// 目的正是「不让其它 HTTP/2 客户端把错误当成成功的 gRPC 响应」。
fn httpError(rsp: *h2.Responder, status: []const u8, text: []const u8) !void {
    const hs = [_]hpack.Header{.{ .name = "content-type", .value = "text/plain; charset=utf-8" }};
    try rsp.begin(status, &hs);
    return rsp.endData(text);
}

pub const HandlerFn = *const fn (
    ctx: *anyopaque,
    call: *Call,
    arena: std.mem.Allocator,
) anyerror!void;

pub const Route = struct {
    /// 完整方法路径，如 `/echo.Echo/Say`
    path: []const u8,
    handler: HandlerFn,
    /// 该路由专用上下文；为空则用 `Server` 的 ctx
    ctx: ?*anyopaque = null,
};

/// 流式方法（客户端流 / bidi）：消息逐条回调，可在回调里随时 `call.sendMessage`。
pub const StreamRoute = struct {
    path: []const u8,
    /// 收到一条**完整** gRPC 消息（跨 DATA 帧的重组由本层负责）
    on_message: *const fn (ctx: *anyopaque, call: *Call, msg: []const u8) anyerror!void,
    /// 请求流结束（收到 END_STREAM）
    on_end: ?*const fn (ctx: *anyopaque, call: *Call) anyerror!void = null,
    ctx: ?*anyopaque = null,
};

/// 单条消息上限（流式重组时防炸）。
const max_stream_message: usize = 4 * 1024 * 1024;

/// 小于该体积不压缩（小报文压完反而变大，与 zzig/DH.RustBase 同口径）。
const compress_min_size: usize = 256;

/// gRPC 路由表；把它作为 `h2.Connection.serve` 的 handler 使用：
/// ```zig
/// var server = zgrpc.grpc.Server{ .routes = &routes };
/// try conn.serve(server.context(), server.handler());
/// ```
pub const Server = struct {
    /// 整请求路由（unary / 服务端流式）
    routes: []const Route = &.{},
    /// 流式路由（客户端流 / bidi）
    stream_routes: []const StreamRoute = &.{},
    /// 路由未指定 `ctx` 时使用
    outer_ctx: *anyopaque,

    /// keepalive 间隔（毫秒）。`null` = 不发 PING（默认，避免无意义周期劳作）。
    ///
    /// 只配它**不会自动发**：还需要 `spawnKeepalive()` 或由调用方周期调 `pingAll()`。
    /// 典型场景：长连接要穿过 NAT / 四层 LB（它们会掐掉「空闲」连接，通常 5~60 分钟）。
    keepalive_interval_ms: ?u32 = null,

    /// 活跃连接（keepalive 线程遍历用）；由 `serveConn` 自动登记/注销。
    ///
    /// **固定用 `page_allocator`**：这个表会被连接线程（并发 append）和 keepalive 线程
    /// 同时访问，而调用方传进来的 allocator 可能是 arena（无锁、不可并发）—— 那时候
    /// 会以极难定位的方式崩掉。keepalive 是服务端基础设施，用系统分配器最稳。
    conns: std.ArrayListUnmanaged(*h2.Connection) = .empty,
    conns_lock: h2.SpinLock = .{},
    /// keepalive 线程的停止标志（`spawnKeepalive` 用）
    keepalive_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),
    /// 因「连接正忙」而跳过的 PING 次数（聚合计数）。
    ///
    /// 有意**不逐次打日志**：10ms 一轮心跳时每轮都可能跳过上百个连接，
    /// 逐次输出会把日志自身变成性能瓶颈（也把真正有价值的错误冲掉）。
    pings_skipped: u64 = 0,
    /// PING 写失败的次数（通常是连接刚关闭、`unregisterConn` 还没跑）。同样只计数不报日志。
    pings_failed: u64 = 0,

    pub fn context(self: *Server) *anyopaque {
        return @ptrCast(self);
    }

    /// 当前活跃连接数（可观测）。
    pub fn connCount(self: *Server) usize {
        self.conns_lock.lock();
        defer self.conns_lock.unlock();
        return self.conns.items.len;
    }

    fn registerConn(self: *Server, c: *h2.Connection) void {
        self.conns_lock.lock();
        defer self.conns_lock.unlock();
        self.conns.append(std.heap.page_allocator, c) catch |e| {
            log.grpc.warn("登记连接失败（keepalive 对该连接不可用）：{s}", .{@errorName(e)});
        };
    }

    fn unregisterConn(self: *Server, c: *h2.Connection) void {
        self.conns_lock.lock();
        defer self.conns_lock.unlock();
        for (self.conns.items, 0..) |item, i| {
            if (item == c) {
                _ = self.conns.swapRemove(i);
                return;
            }
        }
    }

    /// 向所有活跃连接发一次 PING（keepalive 的核心动作）。返回**实际发出**的条数。
    ///
    /// 关键点：对正忙（写锁被占）的连接**跳过**而不是排队等 ——
    /// 1. 正在传数据本身已经保活了，不需要 PING；
    /// 2. 若在这里阻塞，一个慢连接（TCP 背压）就会拖住整轮，
    ///    而且一直持有 `conns_lock` 会让新连接登记不进来（高并发下是致命的）。
    ///
    /// 不主动断开无响应的连接 —— 那属于传输层/上层策略，这里只保证「链路上有流量」。
    pub fn pingAll(self: *Server) usize {
        self.conns_lock.lock();
        defer self.conns_lock.unlock();
        var sent: usize = 0;
        var skipped: usize = 0;
        var failed: usize = 0;
        for (self.conns.items) |c| {
            const wrote = c.trySendPing(null) catch |e| {
                failed += 1;
                // 明细走 debug：写失败通常是「连接刚关闭、还没 unregister」，属正常竞态，
                // 默认 info 级别下不刷屏；排查时 `log.setLevel(.debug)` 逐条可见。
                log.server.debug("keepalive PING 发送失败（连接可能刚关闭）：{s}", .{@errorName(e)});
                continue;
            };
            if (wrote) sent += 1 else skipped += 1;
        }
        self.pings_failed +|= failed;
        if (skipped > 0) {
            self.pings_skipped +|= skipped;
            // 同上：逐轮明细降到 debug，计数（`pings_skipped`）始终可查。
            log.server.debug("keepalive：{d} 个连接正忙已跳过（有业务流量时无需 PING）", .{skipped});
        }
        return sent;
    }

    /// 起一个后台线程，每 `keepalive_interval_ms` 调一次 `pingAll()`。
    ///
    /// **生命周期要求**：`Server` 必须比该线程活得久 —— 退出前先 `stopKeepalive()`
    /// 再 join，最后才 `deinit()`。
    pub fn spawnKeepalive(self: *Server) !std.Thread {
        const interval_ms = self.keepalive_interval_ms orelse return error.KeepaliveDisabled;
        self.keepalive_stop.store(false, .release);
        return std.Thread.spawn(.{ .stack_size = conn_stack_size }, keepaliveLoop, .{ self, interval_ms });
    }

    /// 让 keepalive 线程退出并等待它结束（join 由调用方用返回的句柄完成）。
    pub fn stopKeepalive(self: *Server) void {
        self.keepalive_stop.store(true, .release);
    }

    /// 释放服务端内部状态（`conns` 表）。
    ///
    /// 调用时机：**keepalive 已停止、所有连接线程已结束**。
    /// 重复调用是安全的（表会被清空）。
    pub fn deinit(self: *Server) void {
        self.stopKeepalive();
        self.conns_lock.lock();
        defer self.conns_lock.unlock();
        self.conns.deinit(std.heap.page_allocator);
        self.conns = .empty;
    }

    fn keepaliveLoop(self: *Server, interval_ms: u32) void {
        const interval_ns: u64 = @as(u64, interval_ms) *| std.time.ns_per_ms;
        while (!self.keepalive_stop.load(.acquire)) {
            // Zig 0.17.1 移除了 `std.Thread.sleep`；`clock.sleepNs` 是跨平台实现
            @import("clock.zig").sleepNs(interval_ns);
            if (self.keepalive_stop.load(.acquire)) break;
            _ = self.pingAll();
        }
    }

    /// 整请求模式（也是 `serve` 的旧入口）。
    pub fn handler(self: *Server) h2.HandlerFn {
        _ = self;
        return handle;
    }

    /// 流式模式：DATA 到达即回调，支持客户端流 / bidi；同时兼容整请求路由。
    pub fn streamHooks(self: *Server) h2.StreamHooks {
        return .{
            .ctx = @ptrCast(self),
            .onHeaders = streamOnHeaders,
            .onData = streamOnData,
            .onReset = streamOnReset,
        };
    }

    /// 单连接服务线程的**推荐栈大小**。
    ///
    /// `std.Thread.SpawnConfig.default_stack_size` 是 **16 MiB**。h2 连接只需要两块 16 KiB 缓冲加少量调用栈，
    /// 用 16 MiB 意味着每条连接多占 16 MiB 虚拟地址；实测（`bench --thread-cost`）线程创建+销毁成本
    /// 也从 **147.3 us** 降到 **86.1 us**。
    ///
    /// ```zig
    /// const th = try std.Thread.spawn(.{ .stack_size = zgrpc.grpc.conn_stack_size },
    ///     zgrpc.grpc.Server.serveConnThread, .{ &server, allocator, io, stream });
    /// th.detach();
    /// ```
    pub const conn_stack_size: usize = 128 * 1024;

    /// 服务一条**已接受**的连接，返回时连接已关闭（配合「每连接一线程」模型）。
    ///
    /// 这里把三件容易漏掉的事一起做了：
    /// 1. 关闭 Nagle（`TCP_NODELAY`）—— 否则小包往返会撞上 Nagle × 延迟 ACK，出现随机 10~200ms 尖峰；
    /// 2. 用 `serveStreaming` 同时兼容 unary 与流式路由；
    /// 3. 连接级错误只记日志，不静默吞掉。
    pub fn serveConn(
        self: *Server,
        allocator: std.mem.Allocator,
        io: std.Io,
        stream: std.Io.net.Stream,
    ) void {
        defer stream.close(io);
        @import("socket_timeout.zig").setNoDelay(io, stream, true) catch |e| {
            log.grpc.warn("关闭 Nagle 失败（延迟可能偏高）：{s}", .{@errorName(e)});
        };

        var recv_buf: [16 * 1024]u8 = undefined;
        var send_buf: [16 * 1024]u8 = undefined;
        var sr = stream.reader(io, &recv_buf);
        var sw = stream.writer(io, &send_buf);

        var conn = h2.Connection.init(allocator, &sr.interface, &sw.interface, .{});
        defer conn.deinit();

        // keepalive 需要能从其它线程发帧 → 把连接登记进服务端
        self.registerConn(&conn);
        defer self.unregisterConn(&conn);

        conn.serveStreaming(self.streamHooks()) catch |e| {
            if (e != error.ConnectionClosed) {
                log.grpc.warn("连接异常结束：{s}", .{@errorName(e)});
            }
        };
    }

    fn findStreamRoute(self: *Server, path: []const u8) ?*const StreamRoute {
        for (self.stream_routes) |*r| {
            if (std.mem.eql(u8, r.path, path)) return r;
        }
        return null;
    }

    fn find(self: *Server, path: []const u8) ?Route {
        for (self.routes) |r| {
            if (std.mem.eql(u8, r.path, path)) return r;
        }
        return null;
    }

    fn handle(server_ctx: *anyopaque, req: h2.Request, rsp: *h2.Responder, arena: std.mem.Allocator) anyerror!void {
        const self: *Server = @ptrCast(@alignCast(server_ctx));
        var call = Call{ .req = req, .rsp = rsp, .alloc = arena };

        if (!std.mem.eql(u8, req.method, "POST")) {
            log.grpc.info("非 POST 请求 {s} {s} → 405", .{ req.method, req.path });
            return httpError(rsp, "405", "gRPC requires POST");
        }
        if (!describeContentType(req, &call)) {
            log.grpc.info("不支持的 content-type（{s}）→ 415", .{req.header("content-type") orelse "缺失"});
            return httpError(rsp, "415", "content-type must be application/grpc[+proto] or application/grpc-web[+proto]");
        }

        // gRPC-Web 文本模式：整个请求体是 base64，先解码再交给业务
        if (call.web_text) {
            call.req.body = decodeBase64Stream(arena, call.req.body) catch {
                call.setStatus(.invalid_argument, "grpc-web-text 请求体不是合法 base64");
                return call.finish();
            };
        }

        const route = self.find(req.path) orelse {
            log.grpc.warn("未注册的方法：{s}", .{req.path});
            call.setStatus(.unimplemented, "未注册的方法");
            return call.finish();
        };

        const route_ctx = route.ctx orelse self.outer_ctx;
        route.handler(route_ctx, &call, arena) catch |e| {
            if (call.expired()) {
                // 过期优先：即便 handler 报的是别的错，对客户端而言语义就是「超了」
                call.setStatus(.deadline_exceeded, "超过 grpc-timeout");
                log.grpc.warn("处理 {s} 超过了 grpc-timeout（{s}）", .{ req.path, @errorName(e) });
            } else {
                // 业务未显式设过状态时兜底为 internal，并留下可定位日志
                if (call.status == .ok) call.setStatus(.internal, @errorName(e));
                log.grpc.err("处理 {s} 失败：{s}", .{ req.path, @errorName(e) });
            }
        };

        // handler 正常返回但已经超过 `grpc-timeout`：语义上仍是超时，不能眼睁睁回 OK
        if (call.status == .ok and call.expired()) {
            call.setStatus(.deadline_exceeded, "超过 grpc-timeout");
            log.grpc.info("处理 {s} 正常返回但已超时 → DEADLINE_EXCEEDED", .{req.path});
        }
        return call.finish();
    }

    // -----------------------------------------------------------------------
    // 流式模式（客户端流 / bidi）：DATA 到达即回调
    // -----------------------------------------------------------------------

    fn streamOnHeaders(server_ctx: *anyopaque, req: h2.Request, rsp: *h2.Responder) anyerror!void {
        const self: *Server = @ptrCast(@alignCast(server_ctx));
        const a = rsp.stream.allocator();
        const call = try a.create(Call);
        call.* = .{ .req = req, .rsp = rsp, .alloc = a };
        rsp.stream.user_data = call;

        if (!std.mem.eql(u8, req.method, "POST")) {
            log.grpc.info("非 POST 请求 {s} {s} → 405", .{ req.method, req.path });
            return httpError(rsp, "405", "gRPC requires POST");
        }
        if (!describeContentType(req, call)) {
            log.grpc.info("不支持的 content-type（{s}）→ 415", .{req.header("content-type") orelse "缺失"});
            return httpError(rsp, "415", "content-type must be application/grpc[+proto] or application/grpc-web[+proto]");
        }

        if (self.findStreamRoute(req.path)) |sr| {
            if (call.web_text) {
                // 规范：gRPC-Web 不支持 bidi / 客户端流；文本模式更不可能逐帧解码
                call.setStatus(.unimplemented, "grpc-web-text 不支持客户端流/bidi");
                return call.finish();
            }
            call.stream_route = sr;
            call.route_ctx = sr.ctx orelse self.outer_ctx;
            return;
        }
        if (self.find(req.path)) |r| {
            // 整请求路由也走流式驱动：先攒满 body，END_STREAM 时调用
            call.unary_route = r;
            call.route_ctx = r.ctx orelse self.outer_ctx;
            return;
        }

        log.grpc.warn("未注册的方法：{s}", .{req.path});
        call.setStatus(.unimplemented, "未注册的方法");
        return call.finish();
    }

    fn streamOnData(server_ctx: *anyopaque, rsp: *h2.Responder, data: []const u8, end_stream: bool) anyerror!void {
        _ = server_ctx;
        const p = rsp.stream.user_data orelse return;
        const call: *Call = @ptrCast(@alignCast(p));
        if (call.finished) return;

        // 超过 `grpc-timeout`：不再把请求交给业务，直接以 DEADLINE_EXCEEDED 收尾
        if (call.expired()) {
            call.setStatus(.deadline_exceeded, "超过 grpc-timeout");
            return call.finish();
        }

        // ① 整请求路由：原样累积（含 gRPC 长度前缀），END_STREAM 时一次交付
        if (call.unary_route) |route| {
            try call.accum.appendSlice(call.alloc, data);
            if (call.accum.items.len > max_stream_message) return error.BodyTooLarge;
            if (end_stream) {
                // 文本模式：累积的是 base64 文本，到这里整体解码
                if (call.web_text) {
                    call.req.body = decodeBase64Stream(call.alloc, call.accum.items) catch {
                        call.setStatus(.invalid_argument, "grpc-web-text 请求体不是合法 base64");
                        return call.finish();
                    };
                } else {
                    call.req.body = call.accum.items;
                }
                route.handler(call.route_ctx, call, call.alloc) catch |e| {
                    if (call.expired()) {
                        call.setStatus(.deadline_exceeded, "超过 grpc-timeout");
                        log.grpc.warn("处理 {s} 超过了 grpc-timeout（{s}）", .{ call.req.path, @errorName(e) });
                    } else {
                        if (call.status == .ok) call.setStatus(.internal, @errorName(e));
                        log.grpc.err("处理 {s} 失败：{s}", .{ call.req.path, @errorName(e) });
                    }
                };
                // 慢 handler 正常返回但已超时：仍然按超时告知客户端
                if (call.status == .ok and call.expired()) {
                    call.setStatus(.deadline_exceeded, "超过 grpc-timeout");
                }
                return call.finish();
            }
            return;
        }

        // ② 流式路由：跨 DATA 帧重组，逐条完整消息回调
        const sr = call.stream_route orelse return;
        try call.accum.appendSlice(call.alloc, data);
        if (call.accum.items.len > max_stream_message) return error.BodyTooLarge;

        var pos: usize = 0;
        while (true) {
            const avail = call.accum.items.len - pos;
            if (avail < 5) break;
            const len = std.mem.readInt(u32, call.accum.items[pos + 1 ..][0..4], .big);
            if (avail - 5 < len) break;
            const msg = call.accum.items[pos + 5 .. pos + 5 + len];
            sr.on_message(call.route_ctx, call, msg) catch |e| {
                if (call.expired()) {
                    call.setStatus(.deadline_exceeded, "超过 grpc-timeout");
                    log.grpc.warn("流式处理 {s} 超过了 grpc-timeout（{s}）", .{ call.req.path, @errorName(e) });
                } else {
                    if (call.status == .ok) call.setStatus(.internal, @errorName(e));
                    log.grpc.err("流式处理 {s} 失败：{s}", .{ call.req.path, @errorName(e) });
                }
                return call.finish();
            };
            pos += 5 + len;
        }
        if (pos > 0) {
            const rest = call.accum.items.len - pos;
            std.mem.copyForwards(u8, call.accum.items[0..rest], call.accum.items[pos..]);
            call.accum.shrinkRetainingCapacity(rest);
        }

        if (end_stream) {
            if (sr.on_end) |f| {
                f(call.route_ctx, call) catch |e| {
                    if (call.status == .ok) call.setStatus(.internal, @errorName(e));
                    log.grpc.err("流式 on_end 失败：{s}", .{@errorName(e)});
                };
            }
            return call.finish();
        }
    }

    fn streamOnReset(ctx: *anyopaque, stream_id: u32) void {
        _ = ctx;
        log.grpc.debug("客户端取消了流 {d}", .{stream_id});
    }
};

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

test "grpc：消息编码与迭代往返" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&buf, a, "hello");
    try encodeMessage(&buf, a, "world");

    // 首字节是压缩标志，随后 4 字节是大端长度
    try testing.expectEqual(@as(u8, 0), buf.items[0]);
    try testing.expectEqual(@as(u32, 5), std.mem.readInt(u32, buf.items[1..5], .big));

    var it = MessageIterator{ .data = buf.items };
    try testing.expectEqualStrings("hello", it.next().?.payload);
    try testing.expectEqualStrings("world", it.next().?.payload);
    try testing.expect(it.next() == null);
}

test "grpc：截断的消息体不会被误判为完整消息" {
    var it = MessageIterator{ .data = "\x00\x00\x00\x00\x09abc" };
    try testing.expect(it.next() == null);
}

// ---------------------------------------------------------------------------
// 端到端：h2 + gRPC 完整链路（真实客户端字节流 → 断言 DATA 与 trailers）
// ---------------------------------------------------------------------------

fn pingHandler(ctx: *anyopaque, call: *Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    if (msg.compressed) {
        call.setStatus(.unimplemented, "不支持压缩");
        return;
    }
    try call.sendMessage(if (std.mem.eql(u8, msg.payload, "ping")) "pong" else "?");
}

test "grpc：h2 上的 unary 端到端（grpc-status 走 trailers）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Echo/Ping" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "te", .value = "trailers" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&body, a, "ping");
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, body.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Ping", .handler = pingHandler }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var content_type_ok = false;
    var status_200 = false;
    var resp_body: []const u8 = "";
    var grpc_status: []const u8 = "";
    var saw_end_stream = false;

    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, ":status") and std.mem.eql(u8, h.value, "200")) status_200 = true;
                    if (std.mem.eql(u8, h.name, "content-type") and std.mem.eql(u8, h.value, "application/grpc")) content_type_ok = true;
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
                if (f.flags & h2.Flags.end_stream != 0) saw_end_stream = true;
            },
            .data => resp_body = f.payload,
            else => {},
        }
    }

    try testing.expect(status_200);
    try testing.expect(content_type_ok);
    try testing.expectEqualStrings("0", grpc_status); // OK
    try testing.expect(saw_end_stream);

    var it = MessageIterator{ .data = resp_body };
    try testing.expectEqualStrings("pong", it.next().?.payload);
    try testing.expect(it.next() == null);
}

test "grpc：未注册方法返回 UNIMPLEMENTED(12)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/nope.Unknown/Call" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers | h2.Flags.end_stream, 1, block.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Ping", .handler = pingHandler }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var grpc_status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype != .headers) continue;
        var dec = hpack.Decoder.init(testing.allocator);
        defer dec.deinit();
        var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
        try dec.decode(f.payload, &hs, a);
        for (hs.items) |h| {
            if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
        }
    }
    try testing.expectEqualStrings("12", grpc_status);
}
// ---------------------------------------------------------------------------
// 端到端：流式（bidi / 客户端流）—— DATA 到达即回调，消息跨帧重组
// ---------------------------------------------------------------------------

fn chatOnMessage(ctx: *anyopaque, call: *Call, msg: []const u8) anyerror!void {
    _ = ctx;
    var buf: [512]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "echo:{s}", .{msg});
    try call.sendMessage(out);
}

test "grpc：bidi 流式（每条消息即时响应 + 跨 DATA 帧重组）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Stream/Chat" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, &block, a);
    // 注意：HEADERS 不带 END_STREAM —— 客户端流
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var msg1: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&msg1, a, "hello");
    var msg2: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&msg2, a, "world");

    // 第 1 条完整；第 2 条被切成两帧（验证跨帧重组）
    try h2.appendFrame(&input, a, .data, 0, 1, msg1.items);
    const cut = msg2.items.len / 2;
    try h2.appendFrame(&input, a, .data, 0, 1, msg2.items[0..cut]);
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, msg2.items[cut..]);

    var routes = [_]Route{};
    var stream_routes = [_]StreamRoute{
        .{ .path = "/test.Stream/Chat", .on_message = chatOnMessage },
    };
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .stream_routes = &stream_routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serveStreaming(server.streamHooks()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    // 汇总响应：DATA 拼接 + trailers
    var resp: std.ArrayListUnmanaged(u8) = .empty;
    var pos: usize = 0;
    var grpc_status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .data => try resp.appendSlice(a, f.payload),
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
            },
            else => {},
        }
    }

    var it = MessageIterator{ .data = resp.items };
    try testing.expectEqualStrings("echo:hello", it.next().?.payload);
    try testing.expectEqualStrings("echo:world", it.next().?.payload);
    try testing.expect(it.next() == null);
    try testing.expectEqualStrings("0", grpc_status);
}

test "grpc：流式模式下整请求路由仍可用（累积到 END_STREAM）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Echo/Ping" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&body, a, "ping");
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, body.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Ping", .handler = pingHandler }};
    var stream_routes = [_]StreamRoute{};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .stream_routes = &stream_routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serveStreaming(server.streamHooks()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var resp: std.ArrayListUnmanaged(u8) = .empty;
    var pos: usize = 0;
    var grpc_status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .data => try resp.appendSlice(a, f.payload),
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
            },
            else => {},
        }
    }

    var it = MessageIterator{ .data = resp.items };
    try testing.expectEqualStrings("pong", it.next().?.payload);
    try testing.expectEqualStrings("0", grpc_status);
}

test "grpc：gRPC-Web（trailers 走 body 尾帧，不发 trailer HEADERS）" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Echo/Ping" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc-web+proto" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&body, a, "ping");
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, body.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Ping", .handler = pingHandler }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var resp: std.ArrayListUnmanaged(u8) = .empty;
    var pos: usize = 0;
    var resp_ct: []const u8 = "";
    var saw_trailer_headers = false;
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .data => try resp.appendSlice(a, f.payload),
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "content-type")) resp_ct = h.value;
                    if (std.mem.eql(u8, h.name, "grpc-status")) saw_trailer_headers = true;
                }
            },
            else => {},
        }
    }

    try testing.expectEqualStrings("application/grpc-web+proto", resp_ct);
    // web 模式下 trailers 必须在 body 里，不能是 trailer HEADERS
    try testing.expect(!saw_trailer_headers);

    // body = [5 字节头 + 消息] + [0x80 尾帧]
    var it = MessageIterator{ .data = resp.items };
    const first = it.next() orelse return error.MissingMessage;
    try testing.expectEqualStrings("pong", first.payload);

    const msg_frame_len = 5 + first.payload.len;
    try testing.expect(resp.items.len > msg_frame_len);
    try testing.expectEqual(@as(u8, 0x80), resp.items[msg_frame_len]);
    const tlen = std.mem.readInt(u32, resp.items[msg_frame_len + 1 ..][0..4], .big);
    const tbytes = resp.items[msg_frame_len + 5 .. msg_frame_len + 5 + tlen];
    try testing.expect(std.mem.indexOf(u8, tbytes, "grpc-status: 0") != null);
}
// ---------------------------------------------------------------------------
// gRPC 消息压缩（grpc-encoding: gzip）
// ---------------------------------------------------------------------------

fn echoHandler2(ctx: *anyopaque, call: *Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    try call.sendMessage(msg.payload);
}

test "grpc：Server.deinit 可重入，pingAll 无连接时返 0（不依赖用户 allocator）" {
    var dummy: u8 = 0;
    var server = Server{ .routes = &.{}, .outer_ctx = &dummy };

    // 无连接时 pingAll 不应该报错或阻塞
    try testing.expectEqual(@as(usize, 0), server.pingAll());
    try testing.expectEqual(@as(usize, 0), server.connCount());

    // 登记后卸载：conns 由 page_allocator 管理，不依赖调用方传的 allocator
    var r = std.Io.Reader.fixed("");
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();
    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();

    server.registerConn(&conn);
    try testing.expectEqual(@as(usize, 1), server.connCount());
    // 空闲连接应能收到 PING
    try testing.expectEqual(@as(usize, 1), server.pingAll());
    server.unregisterConn(&conn);
    try testing.expectEqual(@as(usize, 0), server.connCount());

    // 重复 deinit 安全（表已空）
    server.deinit();
    server.deinit();
    try testing.expectEqual(@as(usize, 0), server.connCount());
}

test "grpc：压缩请求自动解压 + 大响应按 grpc-accept-encoding 压缩" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // 1 KB 可压缩正文
    const base = "compressible-payload-0123456789-";
    var plain_buf: [base.len * 32]u8 = undefined;
    for (0..32) |i| @memcpy(plain_buf[i * base.len ..][0..base.len], base);
    const plain: []const u8 = &plain_buf;

    // 请求体：压缩帧（flag=1 + 长度 + gzip 数据）
    const gz_req = try gzip.compress(a, plain);
    var body: std.ArrayListUnmanaged(u8) = .empty;
    try body.append(a, 1);
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(gz_req.len), .big);
    try body.appendSlice(a, &len);
    try body.appendSlice(a, gz_req);

    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/test.Echo/Big" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "grpc-encoding", .value = "gzip" },
        .{ .name = "grpc-accept-encoding", .value = "gzip, deflate" },
    }, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, body.items);

    var routes = [_]Route{.{ .path = "/test.Echo/Big", .handler = echoHandler2 }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };

    var r = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &r, &aw.writer, .{});
    defer conn.deinit();
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var resp: std.ArrayListUnmanaged(u8) = .empty;
    var saw_grpc_encoding = false;
    var grpc_status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .data => try resp.appendSlice(a, f.payload),
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &hs, a);
                for (hs.items) |h| {
                    if (std.mem.eql(u8, h.name, "grpc-encoding") and std.mem.eql(u8, h.value, "gzip")) saw_grpc_encoding = true;
                    if (std.mem.eql(u8, h.name, "grpc-status")) grpc_status = h.value;
                }
            },
            else => {},
        }
    }

    // 响应帧：flag=1 表示压缩
    try testing.expect(resp.items.len > 5);
    try testing.expectEqual(@as(u8, 1), resp.items[0]);
    const rlen = std.mem.readInt(u32, resp.items[1..5], .big);
    try testing.expectEqual(@as(usize, rlen), resp.items.len - 5);
    const back = try gzip.decompress(a, resp.items[5..]);
    try testing.expectEqualStrings(plain, back);
    try testing.expect(saw_grpc_encoding);
    try testing.expectEqualStrings("0", grpc_status);
}
// ---------------------------------------------------------------------------
// 协议一致性（对照 doc/PROTOCOL-HTTP2.md）
// ---------------------------------------------------------------------------

test "grpc：grpc-timeout 解析（各单位 / 8 位上限 / 非法值）" {
    try testing.expectEqual(@as(?i128, 1 * std.time.ns_per_hour), parseTimeout("1H"));
    try testing.expectEqual(@as(?i128, 90 * std.time.ns_per_min), parseTimeout("90M"));
    try testing.expectEqual(@as(?i128, 5 * std.time.ns_per_s), parseTimeout("5S"));
    try testing.expectEqual(@as(?i128, 500 * std.time.ns_per_ms), parseTimeout("500m"));
    try testing.expectEqual(@as(?i128, 100 * std.time.ns_per_us), parseTimeout("100u"));
    try testing.expectEqual(@as(?i128, 7), parseTimeout("7n"));
    // 规范：TimeoutValue 至多 8 位数字
    try testing.expect(parseTimeout("123456789m") == null);
    // 非法：无单位 / 单位未知 / 空 / 非数字
    try testing.expect(parseTimeout("5") == null);
    try testing.expect(parseTimeout("5x") == null);
    try testing.expect(parseTimeout("") == null);
    try testing.expect(parseTimeout("abcS") == null);
}

test "grpc：grpc-message percent 编解码（中文必须编码，非法 %xx 原样保留）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // ASCII 可见字符（除 %）原样保留
    try testing.expectEqualStrings("not found", try percentEncode(a, "not found"));
    // % 本身必须编码，否则解码会歧义
    try testing.expectEqualStrings("100%25", try percentEncode(a, "100%"));
    // 中文 → UTF-8 逐字节 %HH（"中" = E4 B8 AD）
    try testing.expectEqualStrings("%E4%B8%AD", try percentEncode(a, "中"));

    // 往返
    const msg = "错误：目标不存在（code=5%）";
    const enc = try percentEncode(a, msg);
    try testing.expectEqualStrings(msg, try percentDecode(a, enc));

    // 规范：解码遇到非法 %xx 必须不报错（原样保留）
    try testing.expectEqualStrings("100%", try percentDecode(a, "100%"));
    try testing.expectEqualStrings("%zz", try percentDecode(a, "%zz"));
}
// ---------------------------------------------------------------------------
// 405 / 415：非 gRPC 流量必须用 HTTP 状态码回（规范 SHOULD 415，只看 POST）
// ---------------------------------------------------------------------------

fn okHandlerForTest(ctx: *anyopaque, call: *Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    try call.sendMessage("ok");
}

fn encodeHeadersForTest(
    a: std.mem.Allocator,
    method: []const u8,
    path: []const u8,
    content_type: []const u8,
) !std.ArrayListUnmanaged(u8) {
    var block: std.ArrayListUnmanaged(u8) = .empty;
    try hpack.encode(&.{
        .{ .name = ":method", .value = method },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = path },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = content_type },
        .{ .name = "te", .value = "trailers" },
    }, &block, a);
    return block;
}

/// 走完整 h2 帧（preface → SETTINGS → HEADERS → 空 DATA/END_STREAM），返回响应的 `:status`。
fn roundTripStatus(
    a: std.mem.Allocator,
    method: []const u8,
    content_type: []const u8,
) ![]const u8 {
    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});
    const block = try encodeHeadersForTest(a, method, "/echo.Echo/Say", content_type);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, &.{});

    var reader = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &reader, &aw.writer, .{});
    defer conn.deinit();

    var routes = [_]Route{.{ .path = "/echo.Echo/Say", .handler = okHandlerForTest }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var pos: usize = 0;
    var status: []const u8 = "";
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        if (f.ftype != .headers) continue;
        var dec = hpack.Decoder.init(testing.allocator);
        defer dec.deinit();
        var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
        try dec.decode(f.payload, &hs, a);
        for (hs.items) |h| {
            if (std.mem.eql(u8, h.name, ":status")) status = h.value;
        }
    }
    return status;
}

test "grpc：非 POST 回 HTTP 405（不伪装成 :status 200）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings("405", try roundTripStatus(a, "GET", "application/grpc"));
}

test "grpc：content-type 不符回 HTTP 415（规范指定）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings("415", try roundTripStatus(a, "POST", "text/plain"));
    // 正例：application/grpc 才走 gRPC 语义（200）
    try testing.expectEqualStrings("200", try roundTripStatus(a, "POST", "application/grpc"));
}
// ---------------------------------------------------------------------------
// Custom-Metadata：`-bin` 的 base64（规范 MUST：接受带/不带 padding，应发不带 padding）
// ---------------------------------------------------------------------------

test "metadata：-bin base64 编码无 padding、解码容忍两种形式" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // "hello" → "aGVsbG8"（无 padding）；带 padding 形式为 "aGVsbG8="
    try testing.expectEqualStrings("aGVsbG8", try encodeBase64NoPad(a, "hello"));
    try testing.expectEqualStrings("hello", try decodeBase64Loose(a, "aGVsbG8"));
    try testing.expectEqualStrings("hello", try decodeBase64Loose(a, "aGVsbG8="));
    // 空值：发出去是空串，解回来也是空串
    try testing.expectEqualStrings("", try encodeBase64NoPad(a, ""));
    try testing.expectEqualStrings("", try decodeBase64Loose(a, ""));
    // 非法 base64 必须报错，而不是悄悄返回垃圾
    try testing.expectError(error.InvalidBase64, decodeBase64Loose(a, "!!!not-base64"));
}

fn metadataHandler(ctx: *anyopaque, call: *Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    const token = (try call.requestBinaryMetadata(arena, "x-token-bin")) orelse {
        call.setStatus(.unauthenticated, "缺少 x-token-bin");
        return;
    };
    try call.setBinaryHeader("x-echo-bin", token);
    try call.setTrailer("x-note", "trailer-ok");
    try call.setBinaryTrailer("x-trail-bin", "world");
    try call.sendMessage("ok");
}

const TripDetail = struct {
    status: []const u8 = "",
    headers: std.ArrayListUnmanaged(hpack.Header) = .empty,
    trailers: std.ArrayListUnmanaged(hpack.Header) = .empty,
    body: []const u8 = "",

    fn headerValue(self: TripDetail, name: []const u8) ?[]const u8 {
        for (self.headers.items) |h| {
            if (std.mem.eql(u8, h.name, name)) return h.value;
        }
        return null;
    }

    fn trailerValue(self: TripDetail, name: []const u8) ?[]const u8 {
        for (self.trailers.items) |h| {
            if (std.mem.eql(u8, h.name, name)) return h.value;
        }
        return null;
    }
};

/// 走完整 h2 帧发一次请求（带额外请求头），收集响应头 / trailers / body。
fn roundTripDetail(
    a: std.mem.Allocator,
    extra: []const hpack.Header,
    handler: HandlerFn,
) !TripDetail {
    var input: std.ArrayListUnmanaged(u8) = .empty;
    try input.appendSlice(a, h2.client_preface);
    try h2.appendFrame(&input, a, .settings, 0, 0, &.{});

    var block: std.ArrayListUnmanaged(u8) = .empty;
    var hs: std.ArrayListUnmanaged(hpack.Header) = .empty;
    try hs.appendSlice(a, &.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/echo.Echo/Say" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "te", .value = "trailers" },
    });
    try hs.appendSlice(a, extra);
    try hpack.encode(hs.items, &block, a);
    try h2.appendFrame(&input, a, .headers, h2.Flags.end_headers, 1, block.items);

    var body: std.ArrayListUnmanaged(u8) = .empty;
    try encodeMessage(&body, a, "ping");
    try h2.appendFrame(&input, a, .data, h2.Flags.end_stream, 1, body.items);

    var reader = std.Io.Reader.fixed(input.items);
    var aw = std.Io.Writer.Allocating.init(testing.allocator);
    defer aw.deinit();

    var conn = h2.Connection.init(testing.allocator, &reader, &aw.writer, .{});
    defer conn.deinit();

    var routes = [_]Route{.{ .path = "/echo.Echo/Say", .handler = handler }};
    var dummy: u8 = 0;
    var server = Server{ .routes = &routes, .outer_ctx = &dummy };
    conn.serve(server.context(), server.handler()) catch |e| {
        if (e != error.ConnectionClosed) return e;
    };

    var out = TripDetail{};
    var pos: usize = 0;
    while (h2.nextFrame(aw.written(), &pos)) |f| {
        switch (f.ftype) {
            .headers => {
                var dec = hpack.Decoder.init(testing.allocator);
                defer dec.deinit();
                var list: std.ArrayListUnmanaged(hpack.Header) = .empty;
                try dec.decode(f.payload, &list, a);
                for (list.items) |h| {
                    if (std.mem.eql(u8, h.name, ":status")) {
                        out.status = h.value;
                    } else if (f.flags & h2.Flags.end_stream != 0) {
                        try out.trailers.append(a, h);
                    } else {
                        try out.headers.append(a, h);
                    }
                }
            },
            .data => out.body = f.payload,
            else => {},
        }
    }
    return out;
}

test "metadata：请求 -bin 可读、响应头与 trailers 可写（端到端）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const trip = try roundTripDetail(a, &.{
        .{ .name = "x-token-bin", .value = "aGVsbG8" }, // "hello"
    }, metadataHandler);

    try testing.expectEqualStrings("200", trip.status);
    // 响应头里的二进制 metadata 应是**无 padding** 的 base64
    try testing.expectEqualStrings("aGVsbG8", trip.headerValue("x-echo-bin").?);
    // 自定义 ASCII trailer 与二进制 trailer 都要出现
    try testing.expectEqualStrings("trailer-ok", trip.trailerValue("x-note").?);
    try testing.expectEqualStrings("d29ybGQ", trip.trailerValue("x-trail-bin").?); // "world"
    // grpc-status 仍在 trailers
    try testing.expectEqualStrings("0", trip.trailerValue("grpc-status").?);
    // 请求体与响应体（5 字节帧 + "ok"）
    try testing.expect(trip.body.len >= 5);
}

test "metadata：逗号合并的多段 -bin 逐段解码" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // 规范：重复的二进制头可能被合并成 "aGVsbG8,d29ybGQ"
    const trip = try roundTripDetail(a, &.{
        .{ .name = "x-token-bin", .value = "aGVsbG8,d29ybGQ" }, // "hello","world"
    }, metadataHandler);
    // requestBinaryMetadata 取第一段
    try testing.expectEqualStrings("aGVsbG8", trip.headerValue("x-echo-bin").?);
}
test "base64：流式宽松解码（容忍 in-stream padding，对应 grpc-web-text）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expectEqualStrings("hello", try decodeBase64Stream(a, "aGVsbG8"));
    // 中间出现 padding（服务端分块 flush 的典型形态）
    try testing.expectEqualStrings("hellohello", try decodeBase64Stream(a, "aGVsbG8=aGVsbG8="));
    // 空白与换行也忽略
    try testing.expectEqualStrings("hello", try decodeBase64Stream(a, "aGVs\r\nbG8="));
    try testing.expectError(error.InvalidBase64, decodeBase64Stream(a, "aGVsbG8!"));
}
