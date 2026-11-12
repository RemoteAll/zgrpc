# zgrpc Copilot 协作指令

纯 Zig、**零依赖**的 gRPC / HTTP/2 协议栈（Zig 0.17.1+）。本文件适用于本仓库全部代码。

## 1. 定位与边界

- **零依赖**：只用 Zig 标准库。不得引入第三方包（含 C 库绑定）——这是本库存在的理由。
- **不依赖 zzig**：两边互不绑死。zzig 若要集成，以「可选依赖 + 薄适配」方式。
- **逐协议实现、每层可独立测试**：`hpack`（RFC 7541）→ `h2`（RFC 9113）→ `grpc`（gRPC over h2）。
- **如实标注未做项**：README / 决策记录里必须写明未支持的能力，禁止让人误以为"完整实现"。

## 2. 目录

| 路径 | 内容 |
|---|---|
| `src/zgrpc.zig` | 模块根（统一在此导出） |
| `src/hpack.zig` | HPACK：解码完整（静态表/动态表/Huffman），编码简化 |
| `src/hpack_huffman_table.zig` | Huffman 规范码表（**生成文件，勿手改**） |
| `src/h2.zig` | HTTP/2 h2c 服务端帧层 / 流控 / 多路复用 + 端到端测试辅助 |
| `src/grpc.zig` | gRPC 语义层（消息分帧、状态码、路由、unary + 服务端流式） |
| `examples/` | echo server / client（客户端故意并发双流，用于真机验证多路复用） |
| `docs/决策记录.md` | 每次推进的原因、取舍、实测数据（含用户原话） |

## 3. 常用命令

```powershell
zig build                # 构建库 + 示例
zig build test           # 全部单测（含 h2 端到端、gRPC 端到端）
zig build echo-server    # 监听 127.0.0.1:18051
zig build echo-client    # 同一条 TCP 连接上并发两个 unary 调用
```

改协议层后**必须**跑 `zig build test`，并尽量补真机（socket）用例 —— 内存管道测试证明不了对端互通。

## 4. Zig 0.17 实测坑（本仓库已踩过，改代码前先看）

- `std.heap.GeneralPurposeAllocator` **已移除**；命令行要 `pub fn main(init: std.process.Init)`，
  `init.gpa` / `init.io` / `init.minimal.args`；参数迭代用 `std.process.Args.Iterator.initAllocator(...)`。
- `std.Io.Dir` 的方法基本都需要 `io` 参数（`openDir(io, ...)`、`createFile(io, ...)`、`close(io)`）。
- `Reader.readSliceAll` 在 EOF 报 `error.EndOfStream`（不是 `ReadFailed`）；连接收尾要按"任意读错误即收尾"处理并打印原因。
- `@embedFile` **不能跨包路径**（`embed of file outside package path`）；需要嵌入外部目录时先把文件复制到同目录。
- `**` 数组重复运算符已移除，用 `@splat`。

## 5. 禁止清单

- 引入第三方依赖 / C 库绑定；
- 手改 `src/hpack_huffman_table.zig`（它是生成文件）；
- 用"吞错误"掩盖协议错误（每个 `catch` 必须有可定位日志或明确的状态码）；
- 声称支持某能力但无对应用例；
- 未实测就宣称"可用"（用户偏好：不能猜）。
