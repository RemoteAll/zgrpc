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
