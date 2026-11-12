//! 带 `import` 的 gRPC 服务端示例：从磁盘加载 `shop.proto`（它 import 了 `common/types.proto`），
//! 并把两份文件都登记进服务反射 —— grpcurl 无需本地 `.proto` 即可自省跨文件类型。
//!
//! 运行：`zig build import-server`（工作目录为仓库根）
//! 验证：`grpcurl -plaintext 127.0.0.1:PORT list` / `describe shop.Order`

const std = @import("std");
const zgrpc = @import("zgrpc");

const default_port: u16 = 18052;

const Args = struct { port: u16, proto: []const u8 };

fn parseArgs(init: std.process.Init) Args {
    var out = Args{ .port = default_port, .proto = "examples/proto_import/shop.proto" };
    var it = std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa) catch return out;
    defer it.deinit();
    _ = it.skip();
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            if (it.next()) |v| out.port = std.fmt.parseInt(u16, v, 10) catch out.port;
        } else if (std.mem.eql(u8, arg, "--proto")) {
            if (it.next()) |v| out.proto = v;
        }
    }
    return out;
}

/// `/shop.Shop/Get`：把请求原样回显（示例只需要证明路由与反射可用）
fn getHandler(ctx: *anyopaque, call: *zgrpc.grpc.Call, arena: std.mem.Allocator) anyerror!void {
    _ = ctx;
    _ = arena;
    const msg = call.firstMessage() orelse {
        call.setStatus(.invalid_argument, "缺少请求消息");
        return;
    };
    std.debug.print("[zgrpc] /shop.Shop/Get 收到 {d} 字节\n", .{msg.payload.len});
    try call.sendMessage(msg.payload);
}

fn handleConn(allocator: std.mem.Allocator, io: std.Io, stream: std.Io.net.Stream, server: *zgrpc.grpc.Server) void {
    server.serveConn(allocator, io, stream);
}

pub fn main(init: std.process.Init) !void {
    const cfg = parseArgs(init);

    const allocator = std.heap.page_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // 1) 加载 root 及其 import（拓扑排序：被依赖的在前）
    var loader = zgrpc.proto_loader.Loader.init(allocator, io);
    defer loader.deinit();
    const loaded = loader.loadRoot(cfg.proto) catch |e| {
        std.debug.print("[zgrpc] 加载 {s} 失败：{s}\n", .{ cfg.proto, @errorName(e) });
        return e;
    };
    std.debug.print("[zgrpc] 已加载 {d} 个 .proto：", .{loaded.files.len});
    for (loaded.files) |f| std.debug.print("{s} ", .{f.name});
    std.debug.print("\n", .{});

    // 2) 反射：每个文件一份 FileDescriptorProto（含 dependency）
    var refl = try zgrpc.reflection.Service.fromLoaded(allocator, loaded);
    defer refl.deinit();

    var routes = [_]zgrpc.grpc.Route{
        .{ .path = "/shop.Shop/Get", .handler = getHandler },
    };
    var stream_routes = [_]zgrpc.grpc.StreamRoute{
        refl.streamRoute(),
        refl.streamRouteV1Alpha(),
    };
    var dummy: u8 = 0;
    var server = zgrpc.grpc.Server{
        .routes = &routes,
        .stream_routes = &stream_routes,
        .outer_ctx = &dummy,
    };

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", cfg.port);
    var listener = try addr.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);

    std.debug.print("[zgrpc] import 示例服务监听 127.0.0.1:{d}，方法 /shop.Shop/Get\n", .{cfg.port});

    while (true) {
        const stream = listener.accept(io) catch |e| {
            std.debug.print("[zgrpc] accept 失败：{s}\n", .{@errorName(e)});
            continue;
        };
        const th = std.Thread.spawn(.{}, handleConn, .{ allocator, io, stream, &server }) catch |e| {
            std.debug.print("[zgrpc] 创建连接线程失败：{s}\n", .{@errorName(e)});
            stream.close(io);
            continue;
        };
        th.detach();
    }
}
