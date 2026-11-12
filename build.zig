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

    const zgrpc = b.addModule("zgrpc", .{
        .root_source_file = b.path("src/zgrpc.zig"),
        .target = target,
        .optimize = optimize,
    });

    // 单元测试（`zig build test`）
    const unit_tests = b.addTest(.{ .root_module = zgrpc });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "运行全部单元测试");
    test_step.dependOn(&run_unit_tests.step);

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

    // 代码生成：`zig build codegen`（重新生成 examples/echo.pb.zig）
    const gen_mod = b.createModule(.{
        .root_source_file = b.path("tools/proto_gen.zig"),
        .target = target,
        .optimize = optimize,
    });
    const gen_exe = b.addExecutable(.{ .name = "proto_gen", .root_module = gen_mod });
    const gen_run = b.addRunArtifact(gen_exe);
    gen_run.addArgs(&.{ "examples/echo.proto", "examples/echo.pb.zig" });
    b.step("codegen", "从 examples/echo.proto 重新生成 examples/echo.pb.zig").dependOn(&gen_run.step);

    // 示例：gRPC echo 服务端 / 客户端（`zig build echo-server` / `zig build echo-client`）
    addExample(b, zgrpc, target, optimize, "echo-server", "grpc_echo_server.zig", "运行 gRPC echo 服务端示例");
    addExample(b, zgrpc, target, optimize, "echo-client", "grpc_echo_client.zig", "运行 gRPC echo 客户端示例");
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
