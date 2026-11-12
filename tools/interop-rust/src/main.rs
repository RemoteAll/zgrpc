//! zgrpc 互操作对拍工具（Rust · hyper 1.x · h2c 明文 HTTP/2）。
//!
//! 为什么需要它：只用自己的客户端测自己的服务端，是"自己错自己对"。
//! 这里用成熟实现（hyper/h2）当裁判：
//! - `server` 子命令：Rust gRPC 服务端 → 用来测 **zgrpc 的客户端**；
//! - `client` 子命令：Rust gRPC 客户端 → 用来测 **zgrpc 的服务端**；
//! - 两者都支持 `grpc-web`（trailers 走 body 尾帧），覆盖浏览器可达路径。
//!
//! 用法：
//! ```text
//! zgrpc-interop server --port 18061
//! zgrpc-interop client --port 18051          # 原生 gRPC，调 zgrpc 服务端
//! zgrpc-interop client --port 18051 --web    # gRPC-Web，调 zgrpc 服务端
//! ```
//!
//! 不依赖 protobuf：payload 就是原始字节（zgrpc 的设计也是"业务自带序列化"）。

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::{TcpListener, TcpStream};

const GRPC_CT: &str = "application/grpc";
const GRPC_WEB_CT: &str = "application/grpc-web+proto";

/// 先把一段 data 发出去，再发 trailers（hyper 1.x 用 `Frame::trailers`）。
struct GrpcBody {
    data: Option<Bytes>,
    trailers: Option<hyper::HeaderMap>,
}

impl Body for GrpcBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if let Some(d) = this.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(d))));
        }
        if let Some(t) = this.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(t))));
        }
        Poll::Ready(None)
    }
}

/// gRPC 消息帧：`[压缩位:0][长度:4B 大端][payload]`
fn grpc_frame(msg: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(5 + msg.len());
    v.push(0u8);
    v.extend_from_slice(&(msg.len() as u32).to_be_bytes());
    v.extend_from_slice(msg);
    v
}

/// gRPC-Web 的 trailers 尾帧：`0x80 | 长度:4B | "grpc-status: 0\r\n"`
fn web_trailer_block(status: &str) -> Vec<u8> {
    let text = format!("grpc-status: {status}\r\n");
    let mut v = Vec::with_capacity(5 + text.len());
    v.push(0x80u8);
    v.extend_from_slice(&(text.len() as u32).to_be_bytes());
    v.extend_from_slice(text.as_bytes());
    v
}

/// 从字节流里切出所有 gRPC 消息（忽略尾帧）。
fn parse_messages(mut buf: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while buf.len() >= 5 {
        if buf[0] & 0x80 != 0 {
            break; // trailers 尾帧
        }
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if buf.len() < 5 + len {
            break;
        }
        out.push(buf[5..5 + len].to_vec());
        buf = &buf[5 + len..];
    }
    out
}

async fn handle(req: Request<Incoming>) -> Result<Response<GrpcBody>, Infallible> {
    let ct = req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let is_web = ct.starts_with("application/grpc-web");
    let path = req.uri().path().to_string();

    let whole = match req.into_body().collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            eprintln!("[interop] 读请求体失败：{e}");
            return Ok(empty_response());
        }
    };
    let msgs = parse_messages(&whole);
    eprintln!(
        "[interop] {path} 收到 {} 条消息（{} 字节，content-type={ct}）",
        msgs.len(),
        whole.len()
    );

    let mut out: Vec<u8> = Vec::new();
    if path.ends_with("/Chat") {
        // bidi：每条请求即时回一条
        for m in &msgs {
            let echoed = format!("chat:{}", String::from_utf8_lossy(m));
            out.extend_from_slice(&grpc_frame(echoed.as_bytes()));
        }
    } else {
        // unary / 其它：回显第一条
        let payload = msgs.first().map(|v| v.as_slice()).unwrap_or(b"");
        out.extend_from_slice(&grpc_frame(payload));
    }

    let mut resp = Response::builder().status(StatusCode::OK);
    if is_web {
        out.extend_from_slice(&web_trailer_block("0"));
        resp = resp.header(hyper::header::CONTENT_TYPE, GRPC_WEB_CT);
        let body = GrpcBody {
            data: Some(Bytes::from(out)),
            trailers: None,
        };
        return Ok(resp.body(body).unwrap());
    }

    let mut trailers = hyper::HeaderMap::new();
    trailers.insert("grpc-status", hyper::header::HeaderValue::from_static("0"));
    resp = resp.header(hyper::header::CONTENT_TYPE, GRPC_CT);
    let body = GrpcBody {
        data: Some(Bytes::from(out)),
        trailers: Some(trailers),
    };
    Ok(resp.body(body).unwrap())
}

fn empty_response() -> Response<GrpcBody> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(GrpcBody {
            data: None,
            trailers: None,
        })
        .unwrap()
}

async fn run_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("[interop] Rust gRPC 服务端（h2c）监听 127.0.0.1:{port}");
    println!("  方法：/echo.Echo/Say（unary 回显）、/echo.Echo/Chat（bidi，回 chat:<msg>）");
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(handle);
            if let Err(e) = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                eprintln!("[interop] 连接结束：{e}");
            }
        });
    }
}

async fn run_client(port: u16, web: bool) -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let (ct, mode) = if web {
        (GRPC_WEB_CT, "gRPC-Web")
    } else {
        (GRPC_CT, "gRPC")
    };
    println!("[interop] 用 Rust 客户端（{mode}）连 127.0.0.1:{port}");

    // ① unary
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{port}/echo.Echo/Say"))
        .header(hyper::header::CONTENT_TYPE, ct)
        .header("te", "trailers")
        .body(Full::new(Bytes::from(grpc_frame(b"hello-from-rust"))))?;
    let resp = sender.send_request(req).await?;
    let status = resp.status();
    let mut body = resp.into_body();
    let mut data: Vec<u8> = Vec::new();
    let mut trailer_status: Option<String> = None;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(d) = frame.data_ref() {
            data.extend_from_slice(d);
        }
        if let Some(t) = frame.trailers_ref() {
            if let Some(v) = t.get("grpc-status") {
                trailer_status = Some(v.to_str().unwrap_or("").to_string());
            }
        }
    }

    // web 模式：trailers 在 body 尾帧里
    if web {
        let msgs = parse_messages(&data);
        let mut pos = 0usize;
        for m in &msgs {
            pos += 5 + m.len();
        }
        if data.len() > pos && data[pos] & 0x80 != 0 {
            let len = u32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                as usize;
            let text = String::from_utf8_lossy(&data[pos + 5..pos + 5 + len]).to_string();
            if let Some(rest) = text.split("grpc-status:").nth(1) {
                trailer_status = Some(rest.trim().to_string());
            }
        }
        let msgs = parse_messages(&data);
        println!(
            "  unary：http={status} 消息={} grpc-status={:?}",
            msgs.len(),
            trailer_status
        );
        if let Some(m) = msgs.first() {
            println!("  响应内容={}", String::from_utf8_lossy(m));
        }
        if trailer_status.as_deref() != Some("0") {
            return Err("gRPC-Web unary 未能拿到 grpc-status=0".into());
        }
    } else {
        let msgs = parse_messages(&data);
        println!(
            "  unary：http={status} 消息={} grpc-status={:?}",
            msgs.len(),
            trailer_status
        );
        if let Some(m) = msgs.first() {
            println!("  响应内容={}", String::from_utf8_lossy(m));
        }
        if trailer_status.as_deref() != Some("0") {
            return Err("gRPC unary 未能从 trailers 拿到 grpc-status=0".into());
        }
    }

    // ② bidi（多条消息同一流）
    let mut payload: Vec<u8> = Vec::new();
    for m in ["a", "b", "c"] {
        payload.extend_from_slice(&grpc_frame(m.as_bytes()));
    }
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{port}/echo.Echo/Chat"))
        .header(hyper::header::CONTENT_TYPE, ct)
        .header("te", "trailers")
        .body(Full::new(Bytes::from(payload)))?;
    let resp = sender.send_request(req).await?;
    let mut body = resp.into_body();
    let mut data: Vec<u8> = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(d) = frame.data_ref() {
            data.extend_from_slice(d);
        }
    }
    let msgs = parse_messages(&data);
    println!("  bidi：收到 {} 条响应", msgs.len());
    for (i, m) in msgs.iter().enumerate() {
        println!("    [{i}] {}", String::from_utf8_lossy(m));
    }
    if msgs.len() != 3 {
        return Err(format!("bidi 期望 3 条响应，实际 {}", msgs.len()).into());
    }

    println!("[interop] 对拍通过");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let sub = args.get(1).map(|s| s.as_str()).unwrap_or("");
    let mut port: u16 = if sub == "server" { 18061 } else { 18051 };
    let mut web = false;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                port = args.get(i).ok_or("--port 缺参数")?.parse()?;
            }
            "--web" => web = true,
            other => {
                eprintln!("未知参数：{other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    match sub {
        "server" => run_server(port).await,
        "client" => run_client(port, web).await,
        _ => {
            eprintln!("用法：");
            eprintln!("  zgrpc-interop server --port 18061");
            eprintln!("  zgrpc-interop client --port 18051 [--web]");
            std::process::exit(2);
        }
    }
}
