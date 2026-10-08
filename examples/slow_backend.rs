//! # Sandbox upstream: a deliberately slow, observable backend
//!
//! Coalix is a proxy, so demos and load tests need something boring to sit
//! behind it. This example is that something: it answers on the zero-config
//! default origin (`127.0.0.1:3000`), sleeps before **every** response, logs
//! one line per call that actually reaches it, and echoes a global `seq`
//! counter in the body — three independent places to confirm that a herd of
//! clients collapsed into a single upstream visit.
//!
//! ```bash
//! cargo run --example slow_backend      # then read "Sandbox" in the README
//! ```
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `SLOW_BACKEND_ADDR` | `127.0.0.1:3000` | bind address (a bare port, e.g. `3000`, works too) |
//! | `SLOW_BACKEND_DELAY_MS` | `500` | artificial latency before each response |
//!
//! `GET /fail` returns **500** after the same delay — a convenient handle for
//! driving Coalix's circuit breaker and fallback from a load generator.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// Path that answers 500 after the usual delay (breaker / fallback demos).
const FAIL_PATH: &str = "/fail";

/// Count of requests that have actually reached this process.
type HitCounter = Arc<AtomicU64>;

#[tokio::main]
async fn main() -> Result<()> {
    let raw_addr =
        std::env::var("SLOW_BACKEND_ADDR").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let addr = parse_addr(&raw_addr)?;
    let raw_delay = std::env::var("SLOW_BACKEND_DELAY_MS").unwrap_or_else(|_| String::from("500"));
    let delay_ms: u64 = raw_delay
        .parse()
        .context("SLOW_BACKEND_DELAY_MS must be a non-negative integer (milliseconds)")?;
    let delay = Duration::from_millis(delay_ms);

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("slow_backend: cannot bind {addr}"))?;
    eprintln!(
        "slow_backend: serving http://{addr} with {delay:?} latency per call \
         (override via SLOW_BACKEND_ADDR / SLOW_BACKEND_DELAY_MS); {FAIL_PATH} answers 500"
    );

    let hits: HitCounter = Arc::new(AtomicU64::new(0));
    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("slow_backend: accept failed")?;
        let hits = hits.clone();
        tokio::spawn(async move {
            if let Err(err) = serve(stream, hits, delay).await {
                // A client hanging up mid-response is routine; anything else
                // deserves one line so the demo terminal stays honest.
                eprintln!("slow_backend: connection from {peer} ended: {err}");
            }
        });
    }
}

/// Accepts `ip:port`, but also a bare port (`3000`) for brevity.
fn parse_addr(raw: &str) -> Result<SocketAddr> {
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }
    if let Ok(port) = raw.parse::<u16>() {
        return Ok(SocketAddr::from(([127, 0, 0, 1], port)));
    }
    bail!("SLOW_BACKEND_ADDR must look like '127.0.0.1:3000' (or just '3000'), got {raw:?}");
}

/// Serves one keep-alive connection of the sandbox upstream.
async fn serve(
    stream: tokio::net::TcpStream,
    hits: HitCounter,
    delay: Duration,
) -> Result<(), hyper::Error> {
    hyper::server::conn::http1::Builder::new()
        .serve_connection(
            TokioIo::new(stream),
            service_fn(move |request: Request<hyper::body::Incoming>| {
                let hits = hits.clone();
                async move { Ok::<_, Infallible>(respond(request, hits, delay).await) }
            }),
        )
        .await
}

/// Sleeps, assigns the global call number, and answers with a body that
/// carries it — so upstream-side and client-side observations can be
/// compared byte for byte.
async fn respond(
    request: Request<hyper::body::Incoming>,
    hits: HitCounter,
    delay: Duration,
) -> Response<Full<Bytes>> {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let target = match request.uri().query() {
        Some(query) => format!("{path}?{query}"),
        None => path.clone(),
    };
    let _drained = request.into_body().collect().await;

    let seq = hits.fetch_add(1, Ordering::SeqCst) + 1;
    if delay > Duration::ZERO {
        tokio::time::sleep(delay).await;
    }

    let status = if path == FAIL_PATH {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::OK
    };
    let served_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |epoch| epoch.as_millis());

    // One line per call that really happened: during a coalesced herd this
    // terminal visibly stays quiet.
    eprintln!(
        "slow_backend: {method} {target} -> {} (call #{seq})",
        status.as_u16()
    );

    let payload = format!(
        "upstream=coalix-slow-backend\n\
         method={method}\n\
         path={target}\n\
         status={}\n\
         seq={seq}\n\
         delay_ms={}\n\
         served_at_ms={served_at_ms}\n",
        status.as_u16(),
        delay.as_millis()
    );

    let mut response = Response::new(Full::new(Bytes::from(payload)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}
