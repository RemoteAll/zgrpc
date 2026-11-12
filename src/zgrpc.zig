//! zgrpc —— 纯 Zig、**零依赖**的 gRPC / HTTP/2 协议栈。
//!
//! 为什么独立成库（而不是塞进 zzig）：
//! 1. **体量与受众不同**：h2 + HPACK + gRPC 语义是一整套协议栈（数千行、演进快），
//!    只有需要 gRPC 的项目才应付出这个编译与维护成本；
//! 2. **依赖方向干净**：本库只依赖 Zig 标准库，不依赖 zzig；zzig 将来若要集成，
//!    以「可选依赖 + 薄适配」的方式引入，避免两个通用库互相绑死；
//! 3. **可替换**：生态里已有 `http2.zig` / `zig-libs` / `grpc-lite` 等实现，
//!    独立成库后便于做对照测试，也便于将来整体替换而不是深挖耦合。
//!
//! 现状（按 RFC 逐层实现，每层都可独立测试）：
//! - `hpack`：头部压缩（解码完整含 Huffman；编码为简化版，见该文件说明）；
//! - `h2`：HTTP/2 帧层与连接状态机（明文 h2c，prior-knowledge）；
//! - `grpc`：gRPC 消息分帧、状态码、unary 与服务端流式；`grpc-timeout` / `grpc-message` 按规范处理；
//! - `gzip`：消息压缩（`grpc-encoding: gzip`）；
//! - `health`：标准 `grpc.health.v1.Health`（Check / Watch）；
//! - `clock`：单调时钟与睡眠（零依赖，服务 `grpc-timeout` 用）；
//! - `pb`：最小 protobuf 读写原语（varint/zigzag/tag/packed），业务手写 message 编解码用。

const std = @import("std");

pub const hpack = @import("hpack.zig");
pub const h2 = @import("h2.zig");
pub const grpc = @import("grpc.zig");
pub const client = @import("client.zig");
pub const gzip = @import("gzip.zig");
pub const health = @import("health.zig");
pub const clock = @import("clock.zig");
pub const pb = @import("pb.zig");

/// 库版本（语义化）。
pub const version = "0.2.0";

test {
    std.testing.refAllDecls(@This());
}
