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
use std::time::Duration;
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

/// gRPC-Web 的 trailers 尾帧：`0x80` + 长度:4B + `"grpc-status: 0\r\n"` 等
fn web_trailer_block(status: &str, message: Option<&str>, extra: &[(String, String)]) -> Vec<u8> {
    let mut text = format!("grpc-status: {status}\r\n");
    if let Some(m) = message {
        text.push_str(&format!("grpc-message: {m}\r\n"));
    }
    for (k, v) in extra {
        text.push_str(&format!("{k}: {v}\r\n"));
    }
    let mut v = Vec::with_capacity(5 + text.len());
    v.push(0x80u8);
    v.extend_from_slice(&(text.len() as u32).to_be_bytes());
    v.extend_from_slice(text.as_bytes());
    v
}

// ---------------------------------------------------------------------------
// base64（零新依赖）：`-bin` metadata 用；编码不带 padding，解码宽松（忽略 `=`）
// ---------------------------------------------------------------------------

const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_encode_nopad(data: &[u8]) -> String {
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = ((chunk[0] as u32) << 16) | (b1 << 8) | b2;
        let idx = [(n >> 18) & 63, (n >> 12) & 63, (n >> 6) & 63, n & 63];
        for i in 0..=chunk.len() {
            out.push(B64[idx[i] as usize] as char);
        }
    }
    out
}

fn b64_decode_loose(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for c in s.bytes() {
        if c == b'=' {
            // padding = 当前分组结束，丢弃不满 8 位的残余位（否则多帧拼接会错位）
            acc = 0;
            bits = 0;
            continue;
        }
        if matches!(c, b'\r' | b'\n' | b' ' | b'\t') {
            continue;
        }
        let v = B64.iter().position(|x| *x == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xff) as u8);
        }
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// 协议辅助：protobuf varint、`grpc.health.v1` 最小解码、`grpc-timeout` 解析
// ---------------------------------------------------------------------------

fn read_varint(buf: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut shift = 0u32;
    let mut v = 0u64;
    while pos < buf.len() && shift < 64 {
        let b = buf[pos];
        pos += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((v, pos));
        }
        shift += 7;
    }
    None
}

// --- 最小 protobuf 写/解（用于 /echo.Echo/Bump，验证跨实现编码一致性）---

fn pb_write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn pb_uint(out: &mut Vec<u8>, field: u32, v: u64) {
    pb_write_varint(out, ((field as u64) << 3) | 0);
    pb_write_varint(out, v);
}

fn pb_bytes(out: &mut Vec<u8>, field: u32, data: &[u8]) {
    pb_write_varint(out, ((field as u64) << 3) | 2);
    pb_write_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

/// 解 `BumpRequest{ int32 value = 1; string tag = 2; }`
fn parse_bump(bytes: &[u8]) -> (i64, Vec<u8>) {
    let mut value: i64 = 0;
    let mut tag: Vec<u8> = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let (key, next) = match read_varint(bytes, pos) {
            Some(v) => v,
            None => break,
        };
        pos = next;
        let field = key >> 3;
        match key & 0x7 {
            0 => {
                let (v, next) = match read_varint(bytes, pos) {
                    Some(x) => x,
                    None => break,
                };
                pos = next;
                if field == 1 {
                    value = v as i64;
                }
            }
            2 => {
                let (len, next) = match read_varint(bytes, pos) {
                    Some(x) => x,
                    None => break,
                };
                pos = next;
                let len = len as usize;
                if pos + len > bytes.len() {
                    break;
                }
                if field == 2 {
                    tag = bytes[pos..pos + len].to_vec();
                }
                pos += len;
            }
            1 => pos += 8,
            5 => pos += 4,
            _ => break,
        }
    }
    (value, tag)
}

/// 解 `HealthCheckRequest{ string service = 1; }` 的字段 1（缺失/非法返回 None）。
fn health_service(bytes: &[u8]) -> Option<String> {
    let mut pos = 0usize;
    while pos < bytes.len() {
        let (key, next) = read_varint(bytes, pos)?;
        pos = next;
        let field = key >> 3;
        match key & 0x7 {
            0 => pos = read_varint(bytes, pos)?.1,
            1 => pos += 8,
            5 => pos += 4,
            2 => {
                let (len, next) = read_varint(bytes, pos)?;
                pos = next;
                let len = len as usize;
                if pos + len > bytes.len() {
                    return None;
                }
                if field == 1 {
                    return Some(String::from_utf8_lossy(&bytes[pos..pos + len]).to_string());
                }
                pos += len;
            }
            _ => return None,
        }
    }
    None
}

/// 解析 `grpc-timeout`（规范：≤ 8 位正整数 + H/M/S/m/u/n）。
fn parse_grpc_timeout(v: &str) -> Option<Duration> {
    if v.len() < 2 {
        return None;
    }
    let (num, unit) = v.split_at(v.len() - 1);
    if num.len() > 8 || !num.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = num.parse().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(n * 3600)),
        "M" => Some(Duration::from_secs(n * 60)),
        "S" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_millis(n)),
        "u" => Some(Duration::from_micros(n)),
        "n" => Some(Duration::from_nanos(n)),
        _ => None,
    }
}

/// 规范：`grpc-message` 概念上是 UTF-8 文本，物理上必须 percent-encoding。
fn percent_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.as_bytes() {
        if (0x20..=0x7e).contains(b) && *b != b'%' {
            out.push(*b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
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
    let is_web_text = ct.starts_with("application/grpc-web-text");
    let is_web = is_web_text || ct.starts_with("application/grpc-web");
    let path = req.uri().path().to_string();
    let timeout_hdr = req
        .headers()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let token_hdr = req
        .headers()
        .get("x-token-bin")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let whole = match req.into_body().collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            eprintln!("[interop] 读请求体失败：{e}");
            return Ok(empty_response());
        }
    };
    // 文本模式：请求体整体是 base64，先解码
    let whole = if is_web_text {
        match b64_decode_loose(&String::from_utf8_lossy(&whole)) {
            Some(v) => Bytes::from(v),
            None => {
                eprintln!("[interop] web-text 请求体不是合法 base64");
                return Ok(empty_response());
            }
        }
    } else {
        whole
    };
    let msgs = parse_messages(&whole);
    eprintln!(
        "[interop] {path} 收到 {} 条消息（{} 字节，content-type={ct}）",
        msgs.len(),
        whole.len()
    );

    let mut out: Vec<u8> = Vec::new();
    let mut grpc_status = "0".to_string();
    let mut grpc_message: Option<String> = None;
    // `-bin` metadata：解码后重新编码回显（证明真是按规范在处理，而不是原样透传）
    let mut extra_headers: Vec<(String, String)> = Vec::new();
    let mut extra_trailers: Vec<(String, String)> = Vec::new();
    if let Some(t) = &token_hdr {
        match b64_decode_loose(t) {
            Some(raw) => {
                extra_headers.push(("x-echo-bin".to_string(), b64_encode_nopad(&raw)));
                extra_trailers.push(("x-note".to_string(), "metadata-ok".to_string()));
            }
            None => eprintln!("[interop] x-token-bin 不是合法 base64：{t}"),
        }
    }

    if path == "/grpc.health.v1.Health/Check" {
        // 标准健康检查：空 service 或 "echo.Echo" → SERVING；其它 → NOT_FOUND(5)
        let service = msgs
            .first()
            .and_then(|m| health_service(m))
            .unwrap_or_default();
        if service.is_empty() || service == "echo.Echo" {
            out.extend_from_slice(&grpc_frame(&[0x08, 0x01])); // ServingStatus.SERVING
            eprintln!("[interop] health Check(service=\"{service}\") → SERVING");
        } else {
            grpc_status = "5".to_string();
            grpc_message = Some(percent_encode(&format!("未注册的服务：{service}")));
            eprintln!("[interop] health Check(service=\"{service}\") → NOT_FOUND");
        }
    } else if path == "/echo.Echo/Bump" {
        // 结构化 message：解 BumpRequest{value,tag}，回 value+1 与 tag
        let (value, tag) = parse_bump(msgs.first().map(|m| m.as_slice()).unwrap_or(b""));
        let mut w: Vec<u8> = Vec::new();
        pb_uint(&mut w, 1, (value + 1) as u64);
        pb_bytes(&mut w, 2, &tag);
        pb_uint(&mut w, 3, 1);
        out.extend_from_slice(&grpc_frame(&w));
        eprintln!("[interop] /echo.Echo/Bump value={value} → {}", value + 1);
    } else if path == "/echo.Echo/Slow" {
        // 故意慢（300ms）：客户端带了更短的 grpc-timeout 就应拿到 DEADLINE_EXCEEDED(4)
        let sleep = tokio::time::sleep(Duration::from_millis(300));
        match timeout_hdr.as_deref().and_then(parse_grpc_timeout) {
            Some(limit) => {
                if tokio::time::timeout(limit, sleep).await.is_err() {
                    grpc_status = "4".to_string();
                    grpc_message = Some(percent_encode("超过 grpc-timeout（Rust 对拍服务端）"));
                    eprintln!("[interop] /echo.Echo/Slow 超时 → DEADLINE_EXCEEDED");
                } else {
                    out.extend_from_slice(&grpc_frame(b"slow done"));
                }
            }
            None => {
                sleep.await;
                out.extend_from_slice(&grpc_frame(b"slow done"));
            }
        }
    } else if path.ends_with("/Chat") {
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
        out.extend_from_slice(&web_trailer_block(
            &grpc_status,
            grpc_message.as_deref(),
            &extra_trailers,
        ));
        // 文本模式：整段（消息帧 + 尾帧）base64 后发出
        let payload: Vec<u8> = if is_web_text {
            b64_encode_nopad(&out).into_bytes()
        } else {
            out
        };
        let ct_out = if is_web_text {
            "application/grpc-web-text+proto"
        } else {
            GRPC_WEB_CT
        };
        resp = resp.header(hyper::header::CONTENT_TYPE, ct_out);
        for (k, v) in &extra_headers {
            resp = resp.header(k.as_str(), v.as_str());
        }
        let body = GrpcBody {
            data: Some(Bytes::from(payload)),
            trailers: None,
        };
        return Ok(resp.body(body).unwrap());
    }

    let mut trailers = hyper::HeaderMap::new();
    trailers.insert(
        "grpc-status",
        hyper::header::HeaderValue::from_str(&grpc_status).unwrap(),
    );
    if let Some(m) = grpc_message {
        trailers.insert("grpc-message", hyper::header::HeaderValue::from_str(&m).unwrap());
    }
    for (k, v) in &extra_trailers {
        if let (Ok(name), Ok(val)) = (
            hyper::header::HeaderName::from_bytes(k.as_bytes()),
            hyper::header::HeaderValue::from_str(v),
        ) {
            trailers.insert(name, val);
        }
    }
    resp = resp.header(hyper::header::CONTENT_TYPE, GRPC_CT);
    for (k, v) in &extra_headers {
        resp = resp.header(k.as_str(), v.as_str());
    }
    let body = GrpcBody {
        data: Some(Bytes::from(out)),
        trailers: Some(trailers),
    };
    Ok(resp.body(body).unwrap())
}

/// 从 body 尾部帧里抽 `grpc-status`（gRPC-Web 专用）。
fn web_trailer_status(data: &[u8]) -> Option<String> {
    let msgs = parse_messages(data);
    let pos: usize = msgs.iter().map(|m| 5 + m.len()).sum();
    if data.len() < pos + 5 || data[pos] & 0x80 == 0 {
        return None;
    }
    let len = u32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]) as usize;
    let end = (pos + 5 + len).min(data.len());
    let text = String::from_utf8_lossy(&data[pos + 5..end]).to_string();
    for line in text.split("\r\n") {
        if let Some(rest) = line.strip_prefix("grpc-status:") {
            return Some(rest.trim().to_string());
        }
    }
    None
}

/// 文本模式：gRPC 帧整体 base64 后发出（服务端会先解码）。
fn request_body(msg: &[u8], web_text: bool) -> Vec<u8> {
    let frame = grpc_frame(msg);
    if web_text {
        b64_encode_nopad(&frame).into_bytes()
    } else {
        frame
    }
}

/// 文本模式响应：整个 body 是 base64（含尾帧），先解成二进制帧。
fn untangle_body(data: Vec<u8>, web_text: bool) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if !web_text {
        return Ok(data);
    }
    let text = String::from_utf8_lossy(&data).to_string();
    b64_decode_loose(&text).ok_or_else(|| "web-text 响应不是合法 base64".into())
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
    println!("        /echo.Echo/Slow（睡 300ms，带 grpc-timeout 则超时回 4）");
    println!("        /grpc.health.v1.Health/Check（标准健康检查）");
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

async fn run_client(port: u16, web: bool, web_text: bool) -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // 尾帧解析对 web 系列同样适用
    let is_web = web || web_text;
    let (ct, mode) = if web_text {
        ("application/grpc-web-text+proto", "gRPC-Web-text")
    } else if web {
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
        .body(Full::new(Bytes::from(request_body(b"hello-from-rust", web_text))))?;
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

    let data = untangle_body(data, web_text)?;
    // web 模式：trailers 在 body 尾帧里
    if is_web {
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

    // ② bidi（多条消息同一流）—— 规范：gRPC-Web 不支持 bidi，文本模式跳过
    if web_text {
        println!("  bidi：web-text 模式跳过（规范不支持 bidi）");
    } else {
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
        let data = untangle_body(data, web_text)?;
        let msgs = parse_messages(&data);
        println!("  bidi：收到 {} 条响应", msgs.len());
        for (i, m) in msgs.iter().enumerate() {
            println!("    [{i}] {}", String::from_utf8_lossy(m));
        }
        if msgs.len() != 3 {
            return Err(format!("bidi 期望 3 条响应，实际 {}", msgs.len()).into());
        }
    }

    // ③ 标准健康检查：grpc.health.v1.Health/Check（空 service = 整体健康）
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{port}/grpc.health.v1.Health/Check"))
        .header(hyper::header::CONTENT_TYPE, ct)
        .header("te", "trailers")
        .body(Full::new(Bytes::from(request_body(b"", web_text))))?;
    let resp = sender.send_request(req).await?;
    let mut body = resp.into_body();
    let mut data: Vec<u8> = Vec::new();
    let mut gs: Option<String> = None;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(d) = frame.data_ref() {
            data.extend_from_slice(d);
        }
        if let Some(t) = frame.trailers_ref() {
            if let Some(v) = t.get("grpc-status") {
                gs = Some(v.to_str().unwrap_or("").to_string());
            }
        }
    }
    let data = untangle_body(data, web_text)?;
    if is_web {
        if let Some(s) = web_trailer_status(&data) {
            gs = Some(s);
        }
    }
    let hmsgs = parse_messages(&data);
    println!("  health：grpc-status={gs:?} 响应字节数={}", hmsgs.first().map_or(0, |m| m.len()));
    if gs.as_deref() != Some("0") {
        return Err("health Check 未返回 grpc-status=0".into());
    }
    if hmsgs.first().map(|m| m.as_slice()) != Some([0x08u8, 0x01].as_slice()) {
        return Err("health Check 响应不是 ServingStatus=SERVING（期望字节 08 01）".into());
    }

    // ④ grpc-timeout：80ms 调一个睡 300ms 的方法，应得到 DEADLINE_EXCEEDED(4)
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{port}/echo.Echo/Slow"))
        .header(hyper::header::CONTENT_TYPE, ct)
        .header("te", "trailers")
        .header("grpc-timeout", "80m")
        .body(Full::new(Bytes::from(request_body(b"", web_text))))?;
    let resp = sender.send_request(req).await?;
    let mut body = resp.into_body();
    let mut data: Vec<u8> = Vec::new();
    let mut gs: Option<String> = None;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(d) = frame.data_ref() {
            data.extend_from_slice(d);
        }
        if let Some(t) = frame.trailers_ref() {
            if let Some(v) = t.get("grpc-status") {
                gs = Some(v.to_str().unwrap_or("").to_string());
            }
        }
    }
    let data = untangle_body(data, web_text)?;
    if is_web {
        if let Some(s) = web_trailer_status(&data) {
            gs = Some(s);
        }
    }
    println!("  grpc-timeout：grpc-status={gs:?}（期望 4）");
    let _ = &data;
    if gs.as_deref() != Some("4") {
        return Err(format!("grpc-timeout 期望 grpc-status=4，实际 {gs:?}").into());
    }

    // ⑤ 自定义 `-bin` metadata：发出 base64，服务端应解码后重新编码回显
    let token = b64_encode_nopad(b"hello-interop");
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{port}/echo.Echo/Say"))
        .header(hyper::header::CONTENT_TYPE, ct)
        .header("te", "trailers")
        .header("x-token-bin", &token)
        .body(Full::new(Bytes::from(request_body(b"md", web_text))))?;
    let resp = sender.send_request(req).await?;
    let echoed = resp
        .headers()
        .get("x-echo-bin")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let mut body = resp.into_body();
    let mut data: Vec<u8> = Vec::new();
    let mut note: Option<String> = None;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(d) = frame.data_ref() {
            data.extend_from_slice(d);
        }
        if let Some(t) = frame.trailers_ref() {
            if let Some(v) = t.get("x-note") {
                note = Some(v.to_str().unwrap_or("").to_string());
            }
        }
    }
    let data = untangle_body(data, web_text)?;
    if is_web && note.is_none() {
        // gRPC-Web：trailer 在 body 尾帧里
        let msgs = parse_messages(&data);
        let pos: usize = msgs.iter().map(|m| 5 + m.len()).sum();
        if data.len() >= pos + 5 && data[pos] & 0x80 != 0 {
            let len = u32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]) as usize;
            let end = (pos + 5 + len).min(data.len());
            let text = String::from_utf8_lossy(&data[pos + 5..end]).to_string();
            for line in text.split("\r\n") {
                if let Some(rest) = line.strip_prefix("x-note:") {
                    note = Some(rest.trim().to_string());
                }
            }
        }
    }
    println!("  -bin metadata：x-echo-bin={echoed:?} x-note={note:?}");
    if echoed.as_deref() != Some(token.as_str()) {
        return Err(format!(
            "x-echo-bin 期望 {token:?}（=“hello-interop”），实际 {echoed:?}"
        )
        .into());
    }
    if note.as_deref() != Some("metadata-ok") {
        return Err(format!("x-note 期望 metadata-ok，实际 {note:?}").into());
    }

    // ⑥ 结构化 message（protobuf）：BumpRequest{int32 value=1; string tag=2;}
    let mut bump_req: Vec<u8> = Vec::new();
    pb_uint(&mut bump_req, 1, 41);
    pb_bytes(&mut bump_req, 2, b"pb-interop");
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{port}/echo.Echo/Bump"))
        .header(hyper::header::CONTENT_TYPE, ct)
        .header("te", "trailers")
        .body(Full::new(Bytes::from(request_body(&bump_req, web_text))))?;
    let resp = sender.send_request(req).await?;
    let mut body = resp.into_body();
    let mut data: Vec<u8> = Vec::new();
    let mut gs: Option<String> = None;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(d) = frame.data_ref() {
            data.extend_from_slice(d);
        }
        if let Some(t) = frame.trailers_ref() {
            if let Some(v) = t.get("grpc-status") {
                gs = Some(v.to_str().unwrap_or("").to_string());
            }
        }
    }
    let data = untangle_body(data, web_text)?;
    if is_web {
        if let Some(s) = web_trailer_status(&data) {
            gs = Some(s);
        }
    }
    let bmsgs = parse_messages(&data);
    let (got_value, got_tag) = parse_bump(bmsgs.first().map(|m| m.as_slice()).unwrap_or(b""));
    println!("  pb Bump：grpc-status={gs:?} value={got_value} tag={:?}", String::from_utf8_lossy(&got_tag));
    if gs.as_deref() != Some("0") {
        return Err(format!("Bump 期望 grpc-status=0，实际 {gs:?}").into());
    }
    if got_value != 42 || got_tag != b"pb-interop" {
        return Err(format!(
            "Bump 响应不符：value={got_value}（期望 42）tag={:?}",
            String::from_utf8_lossy(&got_tag)
        )
        .into());
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
    let mut web_text = false;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                port = args.get(i).ok_or("--port 缺参数")?.parse()?;
            }
            "--web" => web = true,
            "--web-text" => web_text = true,
            other => {
                eprintln!("未知参数：{other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    match sub {
        "server" => run_server(port).await,
        "client" => run_client(port, web, web_text).await,
        _ => {
            eprintln!("用法：");
            eprintln!("  zgrpc-interop server --port 18061");
            eprintln!("  zgrpc-interop client --port 18051 [--web | --web-text]");
            std::process::exit(2);
        }
    }
}
