# 变更记录

本库遵循 [语义化版本](https://semver.org/lang/zh-CN/)。版本号在 `build.zig.zon` 中维护，
运行时可通过 `zgrpc.version` 取到（也会作为默认 `user-agent` 的一部分发出）。

## 0.2.1 — 2026-11-13

### 修复
- **README 内容丢失**：0.2.0 发版提交误把 `README.md` 截成两行（6364 → 197 字节），本版按上一完整版本恢复，并清掉正文里残留的字面 `\n`。
- **CHANGELOG 编码损坏**：同一次发版把本文件写成了乱码，本版重建。

### 新增
- **客户端本地读超时（内核层，全平台含 Windows）**：`Client.Options.timeout_ms` 从「只管建连」升级为**整次调用**的时限，`client.enableTimeout(stream, io)` 启用。
  - **Windows**：在 AFD 句柄上自己发起 overlapped `IOCTL_AFD_RECEIVE`，用 `NtWaitForSingleObject(句柄)` 带时限等待，到点 `NtCancelIoFileEx` 取消并等 IRP 收尾（`src/afd_reader.zig`）。
  - **POSIX**：`setsockopt(SO_RCVTIMEO)`（`src/socket_timeout.zig`）。
  - **零线程、零额外内存**：不需要读线程，也就不存在线程滞留问题。
  - 实测：`timeout_ms = 200` → **201~211ms** 返回 `error.Timeout`。
  - 此前用「读线程 + `io.waitTimeout`」的方案已删除（`src/async_reader.zig`）。
- 新增探针 `examples/afd_timeout_probe.zig`（`zig build afd-probe`），逐步验证「Zig 原生读可用 → 我方 IOCTL 超时生效 → 耗时在区间内」。

### 修复
- **README 内容丢失**：0.2.0 发版提交误把 `README.md` 截成两行（6364 → 197 字节），本版按上一完整版本恢复，并清掉正文里残留的字面 `\n`。
- **CHANGELOG 编码损坏**：同一次发版把本文件写成了乱码，本版重建。
- **`user_agent` 版本号**：`grpc-zgrpc/0.2.0` 未随版本更新，本版一并修正。
### 测试
- 单元 / 端到端测试 **51** 个（原 50，新增超时相关断言）；回归脚本新增 timeout-probe 段。

## 0.2.0 — 2026-11-12

首个「功能完整、可上线」的版本。相对 0.1.0（只有 hpack + h2 + grpc 服务端骨架），
补齐了从「协议正确性」到「生产可用性」的整条链路。

### 新增

**客户端**
- `client.Client`：握手、`unary`、`callMany`（同连接并发多流）、`clientStream`（客户端流 / bidi）
- `Options.content_type` 三档：原生 gRPC / `grpc-web` / `grpc-web-text`
- 自动 SETTINGS ACK、PING ACK、窗口回补；响应按 stream_id 归位
- 发送 `grpc-timeout`、`user-agent`、`grpc-accept-encoding`、自定义 metadata
- 按规范**合成** Status/Status-Message（对端非 200 或缺 `grpc-status` 时）
- gRPC-Web 尾帧状态解析；`DATA` 带 `END_STREAM` 时正确关闭

**gRPC 语义层**
- 服务端流式 / 客户端流 / bidi（`Server.stream_routes` + `streamHooks`）
- gRPC-Web（trailers 走 body 尾帧）与**文本模式**（`application/grpc-web-text`，base64）
- gzip 消息压缩（`grpc-encoding: gzip`，阈值 256 字节，与 zzig / DH.RustBase 同口径）
- `grpc-timeout`：解析为单调时钟截止时刻，超时回 `DEADLINE_EXCEEDED(4)`；
  `Call.expired()` / `checkDeadline()` 供业务埋检查点
- **Custom-Metadata**：`-bin` base64（编码无 padding、解码双容忍、逗号合并逐段解码），
  `setHeader` / `setBinaryHeader` / `setTrailer` / `setBinaryTrailer`
- `grpc-message` percent-encoding（发送编码 / 客户端解码 / 非法 `%xx` 原样保留）
- 非 POST 或 **HTTP 405**；content-type 不符 → **HTTP 415**
- 响应头按 ABNF 顺序：`[grpc-encoding] [grpc-accept-encoding] content-type` + 自定义 metadata

**标准服务**
- `health`：`grpc.health.v1.Health` 的 `Check` / `Watch`（零依赖手写 protobuf）

**protobuf**
- `pb`：varint / zigzag / tag / length-delimited / fixed / packed 读写原语；
  未知字段自动跳过、截断数据返回 `null` 而不 panic
- `tools/proto_gen.zig`：proto3 **子集**代码生成（`.proto` → Zig struct + `encode`/`decode` + 路由常量）；
  超范围语法（`import` / `map` / `oneof` / `optional` / 嵌套定义等）**报错带行号**

**基础设施**
- `clock`：单调时钟与睡眠（Zig 0.17 已移除 `std.time.nanoTimestamp` / `std.Thread.sleep`）
- `gzip`：消息压缩

### 健壮性 / 安全

- h2 层上限：`max_concurrent_streams`（默认 100）、`max_header_block`（默认 64 KiB）、
  `max_body`（默认 4 MiB），超限有明确的 `RST_STREAM` 拒绝码
- **流级错误不再拆整条连接**：PADDED 的 pad 越界、孤立 CONTINUATION 均按流处理
  （RFC 9113 要求），并有多组回归测试值守
- 客户端流号必须为正奇数；`RST_STREAM` 载荷必须 4 字节
- 9 个 h2 健壮性测试（坏前言 / 并发超限 / header 超限 / body 超限 / 截断帧 / PADDED 越界 /
  孤立 CONTINUATION / 偶数流号 / RST 长度）

### 测试与验证

- 单元 / 端到端测试 **50** 个（`zig test src/zgrpc.zig`）；生成代码测试 3 个（`zig build test`）
- 一键回归 `tools/regression.ps1`：构建 → 单测 → 代码生成 → 真实 socket 冒烟 → **四向 Rust 对拍**
  （zgrpc client → Rust server；Rust client 原生 / gRPC-Web / gRPC-Web-text → zgrpc server）
- 冒烟覆盖：并发双流、bidi 3/3、健康检查 SERVING、`grpc-timeout → 4`、`-bin` metadata 往返、
  grpc-web-text 往返、pb 结构化往返

### 明确不做（附原因）

| 项 | 原因 |
|---|---|
| 内置 TLS | Zig 0.17 标准库只有 tls **client** 且无 ALPN；服务端 TLS 应交给反代（见 `docs/部署.md`） |
| `grpc-timeout` 强制中断 | Zig 无抢占式取消，同步 handler 无法被外部打断；改为业务侧 `checkDeadline()` |
| well-known types / 服务反射 | 需要完整 protobuf descriptor 体系，超出传输层定位 |
| 完整的 `.proto` 支持 | 同上；当前覆盖服务间自控两端的常见写法 |

## 0.1.0 — 初始版本

- `hpack`（RFC 7541，解码含 Huffman）、`h2`（明文 h2c）、`grpc` 语义层骨架
