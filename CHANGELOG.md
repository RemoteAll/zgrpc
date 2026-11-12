# 变更记录

本库遵循 [语义化版本](https://semver.org/lang/zh-CN/)。版本号在 `build.zig.zon` 中维护，
运行时可通过 `zgrpc.version` 取到（也会作为默认 `user-agent` 的一部分发出）。

## 未发布

### 新增
- **`import` 跨文件引用（加载 + 反射）**：新增 `zgrpc.proto_loader`（递归加载、拓扑排序、循环依赖报错）
  与 `zgrpc.reflection.Service.fromLoaded`（每个文件一份 `FileDescriptorProto`，带 `dependency`）。
  路径按 **root 文件所在目录**解析（等价单一 `-I`，与 protoc 一致）。
  - 跨包类型（`common.Region`）的 enum/message 判定靠 `encodeFileWith(..., all)` 的多文件上下文；
    漏了它会被当成 message 编码，protoreflect 直接报错。
  - `describe shop.Order` 实测渲染出 `.common.Money` / `.common.Region`，且 `describe common.Money`
    （依赖文件里的类型）也能查；无 `.proto` 调用可正确往返 enum 与嵌套 message。
  - 示例：`zig build import-server`（`examples/proto_import/`）；**codegen 仍只支持单文件**（遇 `import` 会报错）。
- **嵌套 `message` / `enum` 定义**：生成 Zig 嵌套声明（引用写 `Outer.Inner`），反射侧递归收集符号
  并用相对全名解析 `type_name`；`grpcurl describe echo.Container.Nested` 可直接查。
- **`optional`（proto3 显式 presence）**：生成 `?T`，**设为默认值也会写盘**（否则无法区分「设成 0」与「未设置」）。
  描述符侧按 protoc 的做法合成 `_<字段名>` oneof 并打 `FieldDescriptorProto.proto3_optional = 17`；
  `grpcurl describe` 实测渲染为 `optional int32 opt_value = 15;`（合成 oneof 被正确隐藏）。
- **`oneof` 支持**：生成 Zig 的 tagged union（语义天然对应「同时只能有一个」），
  `DescriptorProto.oneof_decl = 8` + `FieldDescriptorProto.oneof_index = 9`；`grpcurl describe` 实测
  渲染为 `oneof payload { string text = 13; .echo.BumpResponse rich = 14; }`。
  oneof 成员**即使取默认值也会写入**（用于区分「未设置」），与普通字段的「零值不写」规则不同。
- **`map<K, V>` 支持**（代码生成 + 服务反射两侧）：key 限整数或 `string`（protobuf 规定），
  value 可以是标量 / enum / message。（见下方详细说明）
- **`repeated` 支持任意元素类型**（此前只允许数字标量）：`repeated string` / `bytes` / enum / message
  现在都能生成与编解码。生成侧按元素类型选正确 wire 编码：
  - 数字/bool → packed（`packedVarint` / `packedSint` / `packedFixed32` / `packedFixed64`）
  - `string`/`bytes`/enum/message → 每条一个 tag + 长度
  - 解码侧**同时接受 packed 与非 packed**（对端写 `[packed=false]` 也能解），同号字段重复出现会累积。
- `pb.Writer` 新增泛型 packed 写入口：`packedVarint`（任意宽度整数/bool）、`packedSint`（zigzag）、
  `packedFixed32`（float/fixed32/sfixed32）、`packedFixed64`（double/fixed64/sfixed64）；
  `pb.Field` 新增 `packedFixed32s` / `packedFixed64s`。此前 `repeated float/double/fixed*` 会被当成
  varint 编码（**错误**），现在按定长小端处理。
- **`map<K, V>`（详细）**：
  - 生成侧：`[]const XxxEntry` + `pub fn get<Field>(k) ?V` 线性查找辅助，零依赖、不引入 hash 容器；
    条目类型名与描述符里的嵌套类型同名，便于双向对照。
  - 反射侧：按官方 `descriptor.proto` 生成嵌套 `XxxEntry`（`DescriptorProto.nested_type = 3`）
    并打 `options.map_entry = true`（`MessageOptions.map_entry = 7`）；`grpcurl describe` 实测渲染为
    `map<string, int32> counts = 11;`，说明 protoreflect 认出了 map_entry。
- **服务反射（`grpc.reflection.v1` + `v1alpha`）**：`zgrpc.reflection.Service` 让 `grpcurl` / `ghz` / Postman
  **不需要 `.proto`** 就能 `list` / `describe` / 直接调用。登记时给 `.proto` 源码即可（示例用 `@embedFile`）。
  - `src/proto.zig`：proto3 子集解析器（从 `tools/proto_gen.zig` 下沉，代码生成与反射共用一份）。
  - `src/descriptor.zig`：语法树 → `google.protobuf.FileDescriptorProto`，字段号严格按官方定义。
  - `src/reflection.zig`：`ServerReflectionInfo` 双向流；按 `file_by_filename` / `file_containing_symbol` /
    `list_services` 三个请求分支响应；`v1` 与 `v1alpha` 双注册（载荷相同）。
  - 数据用内部 `ArenaAllocator` 长期持有，`deinit` 一次释放，请求期只查表不重复解析。
- `grpc.StreamRoute.ctx` 的 per-route 用法正式启用：反射与业务路由可共存，互不抢占 `Server.outer_ctx`。
- `echo-server` 支持 `--port N`（回归脚本用它做端口隔离）。

### 修复
- **enum 字段为 null 时曾写入默认值**：生成器旧逻辑是 `if (v) |x| ... else try w.enumField(N, 首个枚举值)`，
  导致空 message 也会输出 2 字节。proto3 的默认值（0）本就不该序列化，改为 `if (@intFromEnum(v) != 0)`。
- **`FieldDescriptorProto.type` 与 `type_name` 不一致**：enum 字段曾被一律编成 `TYPE_MESSAGE(11)`，
  导致 `protoreflect` 报 `resolved "echo.Status", but it is not an message`，`describe` / 调用全部失败。
  现在按本文件 enum 表判定：enum→`14`、message→`11`。

### 回归
- 新增「反射」段：`grpcurl list` + `describe`（服务与消息）+ 无 `.proto` 调用 `Say`，实测通过。
- 各段改用**随机空闲端口**：Windows 上 `Stop-Process -Force`（= `TerminateProcess`）会让监听 socket
  变成 zombie `LISTEN`（`netstat` 里 `OwningProcess=0`），固定端口会让下一次回归全部 SKIP。
- `Get-NetTCPConnection` 判定补 `-State Listen`，不再把 `TIME_WAIT` 误判为端口占用。
- 测试下限 61 → **64**（现 64/64）。

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
