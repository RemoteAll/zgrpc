const std = @import("std");
const builtin = @import("builtin");

const min_zig_string = "0.17.1";

comptime {
    const min_zig = std.SemanticVersion.parse(min_zig_string) catch unreachable;
    if (builtin.zig_version.order(min_zig) == .lt) {
        @compileError(std.fmt.comptimePrint(
            "zgrpc requires Zig >= v{s}, found v{}",
            .{ min_zig_string, builtin.zig_version },
        ));
    }
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // 公共工具库（日志 / 配置）：日志级别与行格式、配置读取统一走它，
    // 规则对齐 DH.NCore（经 DH.RustBase 落地）。
    const zzig = b.dependency("zzig", .{ .target = target, .optimize = optimize });

    const zgrpc = b.addModule("zgrpc", .{
        .root_source_file = b.path("src/zgrpc.zig"),
        .target = target,
        .optimize = optimize,
    });
    zgrpc.addImport("zzig", zzig.module("zzig"));

    // 单元测试（`zig build test`）
    const unit_tests = b.addTest(.{ .root_module = zgrpc });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "运行全部单元测试");
    test_step.dependOn(&run_unit_tests.step);

    // 同时把测试二进制装到固定路径：回归脚本要按 `All N tests passed` 解析测试数，
    // 而 `zig build test` 只在失败时输出细节、拿不到数量。
    // 这样也方便人工直接跑 `zig-out/bin/zgrpc-tests.exe`。
    const install_tests = b.addInstallArtifact(unit_tests, .{
        .dest_sub_path = "zgrpc-tests.exe",
    });
    test_step.dependOn(&install_tests.step);

    // 代码生成产物测试（`examples/echo.pb.zig` 由 tools/proto_gen.zig 生成）：
    // 把「生成代码能用」也纳入一键回归，而不是只靠肉眼。
    const pb_gen_mod = b.createModule(.{
        .root_source_file = b.path("examples/echo_pb_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    pb_gen_mod.addImport("zgrpc", zgrpc);
    const pb_gen_tests = b.addTest(.{ .root_module = pb_gen_mod });
    test_step.dependOn(&b.addRunArtifact(pb_gen_tests).step);
    // 带 import 的生成代码测试（`shop.pb.zig` 通过 @import 引用 `common/types.pb.zig`）
    const shop_mod = b.createModule(.{
        .root_source_file = b.path("examples/shop_pb_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    shop_mod.addImport("zgrpc", zgrpc);
    const shop_tests = b.addTest(.{ .root_module = shop_mod });
    test_step.dependOn(&b.addRunArtifact(shop_tests).step);

    // 代码生成：`zig build codegen`（重新生成 examples/echo.pb.zig）
    const gen_mod = b.createModule(.{
        .root_source_file = b.path("tools/proto_gen.zig"),
        .target = target,
        .optimize = optimize,
    });
    gen_mod.addImport("zgrpc", zgrpc);
    const gen_exe = b.addExecutable(.{ .name = "proto_gen", .root_module = gen_mod });
    const gen_run = b.addRunArtifact(gen_exe);
    gen_run.addArgs(&.{ "examples/echo.proto", "examples/echo.pb.zig" });
    // 基准：`zig build bench -Doptimize=ReleaseFast -- --iterations 5000`
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("tools/bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_mod.addImport("zgrpc", zgrpc);
    // bench 里要直接用 zzig 的弹性线程池做 `--pool` 对比
    bench_mod.addImport("zzig", zzig.module("zzig"));
    const bench_exe = b.addExecutable(.{ .name = "bench", .root_module = bench_mod });
    b.installArtifact(bench_exe);
    // 透传命令行参数（Zig 0.17：用 b.args() 取）`n    if (comptime @hasDecl(std.Build, "args")) {}
    b.step("bench", "编译 unary 基准（跑 zig-out\\bin\\bench.exe，可加 --iterations N）").dependOn(&b.addInstallArtifact(bench_exe, .{}).step);

    // 裸 TCP 往返地板基准：`zig build tcp-floor -Doptimize=ReleaseFast`
    // 不依赖 zgrpc，纯粹量「这台机器上任何 socket RPC 的下限」。
    const floor_mod = b.createModule(.{
        .root_source_file = b.path("tools/tcp_floor.zig"),
        .target = target,
        .optimize = optimize,
    });
    const floor_exe = b.addExecutable(.{ .name = "tcp_floor", .root_module = floor_mod });
    b.installArtifact(floor_exe);
    b.step("tcp-floor", "编译裸 TCP ping-pong 地板基准（zig-out\\bin\\tcp_floor.exe）").dependOn(&b.addInstallArtifact(floor_exe, .{}).step);
    // 带 import 的示例：一次生成 root + 它的依赖（各自一个 .pb.zig）
    const gen_run2 = b.addRunArtifact(gen_exe);
    gen_run2.addArgs(&.{ "examples/proto_import/shop.proto", "examples/proto_import/shop.pb.zig" });
    const codegen_step = b.step("codegen", "从 examples/echo.proto 与 examples/proto_import/shop.proto 重新生成 .pb.zig");
    codegen_step.dependOn(&gen_run.step);
    codegen_step.dependOn(&gen_run2.step);

    // 示例：gRPC echo 服务端 / 客户端（`zig build echo-server` / `zig build echo-client`）
    addExample(b, zgrpc, target, optimize, "echo-server", "grpc_echo_server.zig", "运行 gRPC echo 服务端示例");
    addExample(b, zgrpc, target, optimize, "import-server", "grpc_import_server.zig", "带 import 的 gRPC 服务端示例（反射含 dependency）");
    addExample(b, zgrpc, target, optimize, "echo-client", "grpc_echo_client.zig", "运行 gRPC echo 客户端示例");
    addExample(b, zgrpc, target, optimize, "timeout-probe", "client_timeout_probe.zig", "验证客户端本地读超时（服务端不响应时按时退出）");
    addExample(b, zgrpc, target, optimize, "afd-probe", "afd_timeout_probe.zig", "验证 Windows AFD 原生 SO_RCVTIMEO 读超时（无线程）");
    addExample(b, zgrpc, target, optimize, "keepalive-probe", "keepalive_probe.zig", "验证服务端 keepalive 会主动发 PING");
    addExample(b, zgrpc, target, optimize, "caller-probe", "caller_probe.zig", "验证拦截器 / 重试 / 负载均衡");
}

fn addExample(
    b: *std.Build,
    mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    step_name: []const u8,
    file_name: []const u8,
    description: []const u8,
) void {
    const exe_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ "examples", file_name })),
        .target = target,
        .optimize = optimize,
    });
    exe_mod.addImport("zgrpc", mod);

    const exe = b.addExecutable(.{
        .name = step_name,
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    const step = b.step(step_name, description);
    step.dependOn(&run.step);
}
