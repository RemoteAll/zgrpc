//! HPACK —— HTTP/2 头部压缩（RFC 7541）。
//!
//! 为什么自己实现：Zig 0.17 标准库只有 HTTP/1.1（`std.http` 下无 h2、无 hpack），
//! gRPC 又强依赖 HTTP/2，所以这一层只能自建。
//!
//! 口径（有意的不对称）：
//! - **解码完整实现**：静态表 + 动态表 + Huffman。所有主流 h2 客户端（nghttp2 / h2 / hyper-h2 /
//!   grpc-*）都会对头部做 Huffman 与索引编码，服务端**必须**解得开；
//! - **编码刻意简化**：静态表精确匹配走 indexed，其余走 literal-without-indexing + 明文串。
//!   服务端每次响应只有几个头，省下的几十字节不值得再维护一套 Huffman 编码表与动态表。
//!
//! 内存：动态表条目用 `alloc` 长期持有（跨请求存活）；解码输出复制到调用方给的 arena
//! （请求级，随请求释放）。

const std = @import("std");
const huff = @import("hpack_huffman_table.zig");

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Error = error{
    Truncated,
    InvalidIndex,
    InvalidHuffman,
    IntegerOverflow,
    InvalidTableSizeUpdate,
    OutOfMemory,
};

/// RFC 9113 §6.5.2 默认 `SETTINGS_HEADER_TABLE_SIZE`。
pub const default_table_size: usize = 4096;

/// RFC 7541 附录 A 静态表（索引 1..=61，内部按 0 基存放）。
pub const static_table = [_]Header{
    .{ .name = ":authority", .value = "" },
    .{ .name = ":method", .value = "GET" },
    .{ .name = ":method", .value = "POST" },
    .{ .name = ":path", .value = "/" },
    .{ .name = ":path", .value = "/index.html" },
    .{ .name = ":scheme", .value = "http" },
    .{ .name = ":scheme", .value = "https" },
    .{ .name = ":status", .value = "200" },
    .{ .name = ":status", .value = "204" },
    .{ .name = ":status", .value = "206" },
    .{ .name = ":status", .value = "304" },
    .{ .name = ":status", .value = "400" },
    .{ .name = ":status", .value = "404" },
    .{ .name = ":status", .value = "500" },
    .{ .name = "accept-charset", .value = "" },
    .{ .name = "accept-encoding", .value = "gzip, deflate" },
    .{ .name = "accept-language", .value = "" },
    .{ .name = "accept-ranges", .value = "" },
    .{ .name = "accept", .value = "" },
    .{ .name = "access-control-allow-origin", .value = "" },
    .{ .name = "age", .value = "" },
    .{ .name = "allow", .value = "" },
    .{ .name = "authorization", .value = "" },
    .{ .name = "cache-control", .value = "" },
    .{ .name = "content-disposition", .value = "" },
    .{ .name = "content-encoding", .value = "" },
    .{ .name = "content-language", .value = "" },
    .{ .name = "content-length", .value = "" },
    .{ .name = "content-location", .value = "" },
    .{ .name = "content-range", .value = "" },
    .{ .name = "content-type", .value = "" },
    .{ .name = "cookie", .value = "" },
    .{ .name = "date", .value = "" },
    .{ .name = "etag", .value = "" },
    .{ .name = "expect", .value = "" },
    .{ .name = "expires", .value = "" },
    .{ .name = "from", .value = "" },
    .{ .name = "host", .value = "" },
    .{ .name = "if-match", .value = "" },
    .{ .name = "if-modified-since", .value = "" },
    .{ .name = "if-none-match", .value = "" },
    .{ .name = "if-range", .value = "" },
    .{ .name = "if-unmodified-since", .value = "" },
    .{ .name = "last-modified", .value = "" },
    .{ .name = "link", .value = "" },
    .{ .name = "location", .value = "" },
    .{ .name = "max-forwards", .value = "" },
    .{ .name = "proxy-authenticate", .value = "" },
    .{ .name = "proxy-authorization", .value = "" },
    .{ .name = "range", .value = "" },
    .{ .name = "referer", .value = "" },
    .{ .name = "refresh", .value = "" },
    .{ .name = "retry-after", .value = "" },
    .{ .name = "server", .value = "" },
    .{ .name = "set-cookie", .value = "" },
    .{ .name = "strict-transport-security", .value = "" },
    .{ .name = "transfer-encoding", .value = "" },
    .{ .name = "user-agent", .value = "" },
    .{ .name = "vary", .value = "" },
    .{ .name = "via", .value = "" },
    .{ .name = "www-authenticate", .value = "" },
};

// ---------------------------------------------------------------------------
// Huffman 解码（RFC 7541 §5.2 / 附录 B）
// ---------------------------------------------------------------------------
//
// 用编译期构建的前缀树解码：`child[node][bit]` 非负是节点下标，负数编码叶子（`-1 - 符号`）。
// 256 个码全是叶子，内部节点最多 255 个，512 项足够。
// 不用「12 位快表」是为了可读性——头部只有几百字节，逐位下降完全够快。

const Child = [2]i32;

const empty_tree: [512]Child = blk: {
    var t: [512]Child = undefined;
    for (&t) |*e| e.* = .{ -1, -1 };
    break :blk t;
};

const huff_child: [512]Child = blk: {
    @setEvalBranchQuota(4_000_000);
    var t: [512]Child = empty_tree;
    var next: i32 = 1;
    for (huff.codes, huff.lengths, 0..) |code, len, sym| {
        // 符号 256 是 EOS，不允许出现在编码流中：不写入树，命中即非法。
        if (sym == 256) continue;
        var cur: i32 = 0;
        var i: u8 = 0;
        while (i < len) : (i += 1) {
            const bit: usize = @intCast((code >> @intCast(len - 1 - i)) & 1);
            if (i + 1 == len) {
                // 叶子编码为 `-2 - sym`：与「空槽 = -1」区分开（sym 0 时 `-1 - sym` 会撞车）
                t[@intCast(cur)][bit] = -2 - @as(i32, @intCast(sym));
            } else {
                if (t[@intCast(cur)][bit] == -1) {
                    t[@intCast(cur)][bit] = next;
                    next += 1;
                }
                cur = t[@intCast(cur)][bit];
            }
        }
    }
    break :blk t;
};

/// Huffman 解码。尾部允许 1~7 个全 1 的填充位（EOS 前缀），其余一律判非法。
pub fn huffmanDecode(alloc: std.mem.Allocator, input: []const u8) Error![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.ensureTotalCapacity(alloc, input.len);

    var node: i32 = 0;
    var dangling: u32 = 0;
    var dangling_len: u5 = 0;

    for (input) |byte| {
        var i: u3 = 7;
        while (true) {
            const bit: usize = (byte >> i) & 1;
            const nx = huff_child[@intCast(node)][bit];
            if (nx == -1) return error.InvalidHuffman; // 空槽：不存在该码
            if (nx < -1) {
                const sym = -2 - nx;
                if (sym > 255) return error.InvalidHuffman; // EOS 不得出现在流中
                try out.append(alloc, @intCast(sym));
                node = 0;
                dangling = 0;
                dangling_len = 0;
            } else {
                node = nx;
                dangling = (dangling << 1) | @as(u32, @intCast(bit));
                if (dangling_len < 31) dangling_len += 1;
            }
            if (i == 0) break;
            i -= 1;
        }
    }

    // 结束时残留的位只能是 EOS 的前缀：≤ 7 位且全为 1。
    if (dangling_len > 7) return error.InvalidHuffman;
    if (dangling_len > 0 and dangling != (@as(u32, 1) << dangling_len) - 1) return error.InvalidHuffman;

    return out.toOwnedSlice(alloc);
}

// ---------------------------------------------------------------------------
// 整数 / 字符串原语（RFC 7541 §5.1 §5.2）
// ---------------------------------------------------------------------------

fn decodeInt(prefix_bits: u3, data: []const u8, pos: *usize, first: u8) Error!usize {
    const mask8: u8 = @intCast((@as(u16, 1) << prefix_bits) - 1);
    const mask: usize = mask8;
    var value: usize = @as(usize, first & mask8);
    pos.* += 1;
    if (value < mask) return value;

    var shift: u6 = 0;
    while (true) {
        if (pos.* >= data.len) return error.Truncated;
        const b = data[pos.*];
        pos.* += 1;
        const add: usize = b & 0x7f;
        if (shift >= @bitSizeOf(usize)) return error.IntegerOverflow;
        if (add > (@as(usize, std.math.maxInt(usize)) >> shift)) return error.IntegerOverflow;
        value = std.math.add(usize, value, add << shift) catch return error.IntegerOverflow;
        if (b & 0x80 == 0) break;
        shift += 7;
    }
    return value;
}

fn decodeString(data: []const u8, pos: *usize, scratch: std.mem.Allocator) Error![]const u8 {
    if (pos.* >= data.len) return error.Truncated;
    const first = data[pos.*];
    const is_huffman = (first & 0x80) != 0;
    const len = try decodeInt(7, data, pos, first);
    if (len > data.len - pos.*) return error.Truncated;
    const raw = data[pos.* .. pos.* + len];
    pos.* += len;
    if (!is_huffman) return raw;
    return huffmanDecode(scratch, raw);
}

fn appendInt(
    out: *std.ArrayListUnmanaged(u8),
    alloc: std.mem.Allocator,
    first_bits: u8,
    prefix_bits: u3,
    value: usize,
) !void {
    const mask: usize = (@as(usize, 1) << prefix_bits) - 1;
    if (value < mask) {
        try out.append(alloc, first_bits | @as(u8, @intCast(value)));
        return;
    }
    try out.append(alloc, first_bits | @as(u8, @intCast(mask)));
    var v = value - mask;
    while (v >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(v & 0x7f)) | 0x80);
        v >>= 7;
    }
    try out.append(alloc, @intCast(v));
}

fn appendString(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, s: []const u8) !void {
    try appendInt(out, alloc, 0, 7, s.len);
    try out.appendSlice(alloc, s);
}

// ---------------------------------------------------------------------------
// 静态表查询
// ---------------------------------------------------------------------------

/// 精确匹配 (name, value)；返回 1 基索引。
fn staticExact(name: []const u8, value: []const u8) ?usize {
    for (static_table, 1..) |e, idx| {
        if (std.mem.eql(u8, e.name, name) and std.mem.eql(u8, e.value, value)) return idx;
    }
    return null;
}

/// 只匹配 name（value 无关）；返回 1 基索引。
fn staticName(name: []const u8) ?usize {
    for (static_table, 1..) |e, idx| {
        if (std.mem.eql(u8, e.name, name)) return idx;
    }
    return null;
}

// ---------------------------------------------------------------------------
// 解码器
// ---------------------------------------------------------------------------

pub const Decoder = struct {
    /// 动态表条目的长期分配器（跨请求存活，连接结束由 `deinit` 释放）
    alloc: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Header) = .empty,
    /// 当前动态表占用（每条 = name + value + 32）
    size: usize = 0,
    /// 当前生效上限；可被块内 Table Size Update 下调
    max_size: usize = default_table_size,
    /// 我们允许的上限（对端不允许要求更高）
    limit: usize = default_table_size,

    pub fn init(alloc: std.mem.Allocator) Decoder {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Decoder) void {
        self.clear();
        self.entries.deinit(self.alloc);
    }

    pub fn clear(self: *Decoder) void {
        for (self.entries.items) |e| {
            self.alloc.free(e.name);
            self.alloc.free(e.value);
        }
        self.entries.clearRetainingCapacity();
        self.size = 0;
    }

    /// 对端 SETTINGS 变更 `HEADER_TABLE_SIZE` 后调用（只能调小，超出部分立即淘汰）。
    pub fn setMaxSize(self: *Decoder, new_max: usize) void {
        self.max_size = @min(new_max, self.limit);
        self.evict();
    }

    /// 按 1 基索引取条目（1..=61 静态，62.. 动态，越靠前越新）。
    fn at(self: *const Decoder, idx: usize) ?Header {
        if (idx == 0) return null;
        if (idx <= static_table.len) return static_table[idx - 1];
        const dyn_idx = idx - static_table.len - 1;
        if (dyn_idx >= self.entries.items.len) return null;
        return self.entries.items[self.entries.items.len - 1 - dyn_idx];
    }

    fn insert(self: *Decoder, name: []const u8, value: []const u8) !void {
        const sz = name.len + value.len + 32;
        if (sz > self.max_size) {
            // RFC 7541 §4.4：任何超出表尺寸的插入都会清空动态表
            self.clear();
            return;
        }
        const n = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(n);
        const v = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(v);
        try self.entries.append(self.alloc, .{ .name = n, .value = v });
        self.size += sz;
        self.evict();
    }

    fn evict(self: *Decoder) void {
        var i: usize = 0;
        while (i < self.entries.items.len and self.size > self.max_size) {
            const e = self.entries.items[i];
            self.size -= e.name.len + e.value.len + 32;
            self.alloc.free(e.name);
            self.alloc.free(e.value);
            i += 1;
        }
        if (i > 0) self.entries.replaceRangeAssumeCapacity(0, i, &.{});
    }

    /// 解码一个完整的 header block（可能跨多个 HEADERS/CONTINUATION 帧拼成）。
    /// 输出 header 全部复制到 `out_alloc`（请求级 arena，随请求释放）。
    pub fn decode(
        self: *Decoder,
        block: []const u8,
        out: *std.ArrayListUnmanaged(Header),
        out_alloc: std.mem.Allocator,
    ) Error!void {
        var pos: usize = 0;
        var saw_field = false;

        while (pos < block.len) {
            const b = block[pos];
            if (b & 0x80 != 0) {
                // 1xxxxxxx 索引头
                const idx = try decodeInt(7, block, &pos, b);
                const e = self.at(idx) orelse return error.InvalidIndex;
                try out.append(out_alloc, .{
                    .name = try out_alloc.dupe(u8, e.name),
                    .value = try out_alloc.dupe(u8, e.value),
                });
                saw_field = true;
            } else if (b & 0x40 != 0) {
                // 01xxxxxx 带增量索引的字面量（写入动态表）
                const idx = try decodeInt(6, block, &pos, b);
                const name = if (idx == 0)
                    try decodeString(block, &pos, out_alloc)
                else
                    (self.at(idx) orelse return error.InvalidIndex).name;
                const value = try decodeString(block, &pos, out_alloc);
                try out.append(out_alloc, .{
                    .name = try out_alloc.dupe(u8, name),
                    .value = try out_alloc.dupe(u8, value),
                });
                try self.insert(name, value);
                saw_field = true;
            } else if (b & 0x20 != 0) {
                // 001xxxxx 动态表尺寸更新（必须出现在任何 header 之前）
                if (saw_field) return error.InvalidTableSizeUpdate;
                const new_size = try decodeInt(5, block, &pos, b);
                if (new_size > self.limit) return error.InvalidTableSizeUpdate;
                self.max_size = new_size;
                self.evict();
            } else {
                // 0000xxxx 不索引 / 0001xxxx 永不索引：都不写入动态表
                const idx = try decodeInt(4, block, &pos, b);
                const name = if (idx == 0)
                    try decodeString(block, &pos, out_alloc)
                else
                    (self.at(idx) orelse return error.InvalidIndex).name;
                const value = try decodeString(block, &pos, out_alloc);
                try out.append(out_alloc, .{
                    .name = try out_alloc.dupe(u8, name),
                    .value = try out_alloc.dupe(u8, value),
                });
                saw_field = true;
            }
        }
    }
};

// ---------------------------------------------------------------------------
// 编码器（简化：静态表命中走索引，其余明文 literal 不索引）
// ---------------------------------------------------------------------------

/// 编码一组响应头。输出追加到 `out`（用 `alloc` 扩容）。
pub fn encode(
    headers: []const Header,
    out: *std.ArrayListUnmanaged(u8),
    alloc: std.mem.Allocator,
) !void {
    for (headers) |h| {
        if (staticExact(h.name, h.value)) |idx| {
            try appendInt(out, alloc, 0x80, 7, idx);
            continue;
        }
        if (staticName(h.name)) |idx| {
            try appendInt(out, alloc, 0x00, 4, idx);
        } else {
            try out.append(alloc, 0x00);
            try appendString(out, alloc, h.name);
        }
        try appendString(out, alloc, h.value);
    }
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

const testing = std.testing;

fn expectHeaders(actual: []const Header, expected: []const Header) !void {
    try testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| {
        try testing.expectEqualStrings(e.name, a.name);
        try testing.expectEqualStrings(e.value, a.value);
    }
}

test "hpack：Huffman 解码 RFC 7541 C.4.1 请求" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // 8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff
    const block = "\x82\x86\x84\x41\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff";
    var dec = Decoder.init(testing.allocator);
    defer dec.deinit();
    var out: std.ArrayListUnmanaged(Header) = .empty;
    try dec.decode(block, &out, a);
    try expectHeaders(out.items, &.{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "www.example.com" },
    });
}

test "hpack：动态表 + Huffman RFC 7541 C.6.1 响应" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var dec = Decoder.init(testing.allocator);
    defer dec.deinit();

    // 4882 6402 5885 aec3 771a 4b 61 96 d0 7a be 94 10 54 d4 44 a8 20 05 95 04 0b 81 66 e0 82 a6 2d 1b ff（首块）
    const block1 = "\x48\x82\x64\x02\x58\x85\xae\xc3\x77\x1a\x4b" ++
        "\x61\x96\xd0\x7a\xbe\x94\x10\x54\xd4\x44\xa8\x20\x05\x95\x04\x0b\x81\x66\xe0\x82\xa6\x2d\x1b\xff";
    var out1: std.ArrayListUnmanaged(Header) = .empty;
    try dec.decode(block1, &out1, a);
    try expectHeaders(out1.items, &.{
        .{ .name = ":status", .value = "302" },
        .{ .name = "cache-control", .value = "private" },
        .{ .name = "date", .value = "Mon, 21 Oct 2013 20:13:21 GMT" },
    });
    // 3 条写入动态表
    try testing.expectEqual(@as(usize, 3), dec.entries.items.len);
}

test "hpack：编码后可被自身解码（往返）" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const headers = [_]Header{
        .{ .name = ":status", .value = "200" }, // 静态表精确命中
        .{ .name = "content-type", .value = "application/grpc" }, // 静态表名命中
        .{ .name = "grpc-status", .value = "0" }, // 完全自定义
    };

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try encode(&headers, &buf, a);

    var dec = Decoder.init(testing.allocator);
    defer dec.deinit();
    var out: std.ArrayListUnmanaged(Header) = .empty;
    try dec.decode(buf.items, &out, a);
    try expectHeaders(out.items, &headers);

    // 静态精确命中应只占 1 字节
    try testing.expectEqual(@as(u8, 0x88), buf.items[0]);
}

test "hpack：动态表尺寸更新与淘汰" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var dec = Decoder.init(testing.allocator);
    defer dec.deinit();

    // 001 前缀 + 尺寸 0 → 清空动态表
    try dec.insert("x-a", "1");
    try testing.expectEqual(@as(usize, 1), dec.entries.items.len);
    const block = "\x20"; // 0x20 = 001 00000，尺寸更新为 0
    var out: std.ArrayListUnmanaged(Header) = .empty;
    try dec.decode(block, &out, a);
    try testing.expectEqual(@as(usize, 0), dec.entries.items.len);
    try testing.expectEqual(@as(usize, 0), dec.max_size);
}

test "hpack：非法 Huffman 尾部填充被拒" {
    // 全 1 → 解出 EOS 符号，EOS 不允许出现在编码流中
    try testing.expectError(error.InvalidHuffman, huffmanDecode(testing.allocator, "\xff\xff\xff\xff"));
    // 0x00 = 0000 0000：先解出 '0'(00000)，尾部残留 000 不是全 1 的 EOS 前缀
    try testing.expectError(error.InvalidHuffman, huffmanDecode(testing.allocator, "\x00"));
}
