//! gRPC 明文（h2c）回显服务 —— **Rust 对照实现**。
//!
//! 与本仓库其它对照实现（`goserver/`、zgrpc 的 `bench --serve-only`）行为**完全一致**：
//! 收到请求体（gRPC 分帧后的字节）后原样回显，不做字段级编解码，
//! 因此比较的是 h2 + gRPC 框架本身的开销。
//!
//! 为什么直接用 `h2` 而不是 `tonic`：本机 cargo 缓存里没有 tonic/prost，
//! 而 `h2` + `tokio` 正是 tonic 底下的协议与运行时层。
//! 所以这个数字应理解为「Rust 异步栈在该协议层的水平」，
//! 换成 tonic 会再多一层 codec / interceptor 开销。

use bytes::Bytes;
use h2::server;
use http::{HeaderMap, HeaderValue, Response};
use tokio::net::TcpListener;

const DEFAULT_ADDR: &str = "127.0.0.1:18055";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.to_string());

    let listener = TcpListener::bind(&addr).await?;
    println!("rust-h2 echo 已启动：{addr}");

    loop {
        let (socket, _peer) = listener.accept().await?;
        tokio::spawn(async move {
            // 关闭 Nagle：gRPC 是小包交互，Nagle + 延迟 ACK 会带来随机延迟尖峰
            let _ = socket.set_nodelay(true);
            let mut conn = match server::handshake(socket).await {
                Ok(c) => c,
                Err(_) => return,
            };

            // h2 的连接驱动：每个流并行处理（这正是异步栈的优势所在）
            while let Some(result) = conn.accept().await {
                let (request, respond) = match result {
                    Ok(v) => v,
                    Err(_) => return,
                };
                tokio::spawn(async move {
                    let _ = handle(request, respond).await;
                });
            }
        });
    }
}

type Respond = server::SendResponse<Bytes>;

async fn handle(
    request: http::Request<h2::RecvStream>,
    mut respond: Respond,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 读完整请求体（gRPC 一条消息 = 5 字节前缀 + 载荷，原样回显即可）
    let mut body = request.into_body();
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        body.flow_control().release_capacity(chunk.len())?;
        buf.extend_from_slice(&chunk);
    }

    // 响应头：gRPC 的 Call-Definition 与其它实现保持一致
    let response = Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .header("grpc-accept-encoding", "gzip")
        .body(())?;

    let mut send = respond.send_response(response, false)?;
    if !buf.is_empty() {
        send.send_data(Bytes::from(buf), false)?;
    }

    // 状态放 trailers（原生 gRPC 规范）
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    send.send_trailers(trailers)?;

    Ok(())
}
