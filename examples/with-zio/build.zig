const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zio = b.dependency("zio", .{ .target = target, .optimize = optimize });
    const zgrpc = b.dependency("zgrpc", .{ .target = target, .optimize = optimize });

    const mod = b.createModule(.{
        .root_source_file = b.path("main.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("zio", zio.module("zio"));
    mod.addImport("zgrpc", zgrpc.module("zgrpc"));

    const exe = b.addExecutable(.{ .name = "zio-bench", .root_module = mod });
    b.installArtifact(exe);

    // 跑：zig-out\bin\zio-bench.exe [iterations]
    b.step("run", "编译 zio 后端的 zgrpc 基准").dependOn(&b.addInstallArtifact(exe, .{}).step);
}
