//! Rate-limit end-to-end smoke: real upstream, real proxy, real sockets —
//! every request arrives from 127.0.0.1, so one shared bucket observes the
//! whole loop.
//!
//! Verifies the admission contract: the burst goes through untouched, the
//! excess is answered 429 with a numeric `Retry-After` *before* any upstream
//! dial (upstream hits equal admitted count), the counter families mirror
//! the wire truth, `/metrics` stays exempt while throttling, and a disabled
//! limiter is a no-op.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::HeaderMap;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use coalix::config::{Config, UpstreamConfig};
use coalix::proxy::serve_with_listener;

type TestClient = Client<HttpConnector, Full<Bytes>>;
type HitCounter = Arc<AtomicUsize>;

/// Counting backend: answers 200 immediately with a `path|seq` payload so
/// re-fetches stay distinguishable from replays.
struct Upstream {
    addr: SocketAddr,
    gets: HitCounter,
    task: JoinHandle<()>,
}

async fn spawn_upstream() -> Upstream {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    let gets: HitCounter = Arc::new(AtomicUsize::new(0));
    let gets_task = gets.clone();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(_) => return,
            };
            let gets = gets_task.clone();
            tokio::spawn(async move {
                let _connection = http1_serve(stream, gets).await;
            });
        }
    });

    Upstream { addr, gets, task }
}

/// Serves one keep-alive connection of the mock upstream.
async fn http1_serve(stream: tokio::net::TcpStream, gets: HitCounter) -> Result<(), hyper::Error> {
    hyper::server::conn::http1::Builder::new()
        .serve_connection(
            TokioIo::new(stream),
            service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let gets = gets.clone();
                async move {
                    let path = request.uri().path().to_owned();
                    let _drained = request.into_body().collect().await;
                    let seq = gets.fetch_add(1, Ordering::SeqCst) + 1;
                    let payload = format!("{path}|{seq}");
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "text/plain; charset=utf-8")
                            .body(Full::new(Bytes::from(payload)))
                            .expect("valid response"),
                    )
                }
            }),
        )
        .await
}

/// Boots the proxy on an ephemeral port in front of `upstream`; `tweak`
/// mutates the zero-config default before validation, so each scenario owns
/// its own policy knobs without duplicating the harness.
async fn spawn_proxy(
    upstream: &Upstream,
    tweak: impl FnOnce(&mut Config),
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().expect("proxy addr");
    let mut config = Config {
        upstream: UpstreamConfig {
            base_url: format!("http://{}", upstream.addr),
            ..UpstreamConfig::default()
        },
        ..Config::default()
    };
    tweak(&mut config);
    config.validate().expect("test config must validate");
    let task = tokio::spawn(async move {
        let _ = serve_with_listener(listener, Arc::new(config)).await;
    });
    (addr, task)
}

fn client() -> TestClient {
    Client::builder(TokioExecutor::new()).build(HttpConnector::new())
}

/// One bodyless GET through the proxy; returns status, response headers
/// (for `Retry-After`), and the fully-read body.
async fn send(client: &TestClient, url: &str) -> (StatusCode, HeaderMap, Bytes) {
    let request = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Full::new(Bytes::new()))
        .expect("request");
    let response = client.request(request).await.expect("proxy answered");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    (status, headers, body)
}

/// Scrapes `/metrics` — which must answer 200 even mid-throttle, since the
/// reserved path outranks the limiter.
async fn scrape(client: &TestClient, proxy: SocketAddr) -> String {
    let (status, _, body) = send(client, &format!("http://{proxy}/metrics")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metrics scrapes must stay exempt while throttling"
    );
    String::from_utf8(body.to_vec()).expect("utf-8 exposition")
}

/// Value of a single-series sample line (`family value`), 0 when absent.
fn sample(text: &str, family: &str) -> u64 {
    text.lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == family)
                .then(|| value.trim().parse::<u64>().ok())
                .flatten()
        })
        .next()
        .unwrap_or(0)
}

#[tokio::test]
async fn token_bucket_admits_the_burst_then_429s_before_any_dial() {
    let upstream = spawn_upstream().await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, |config| {
        // Dial-per-admission accounting: no caching in front of the bucket.
        config.cache.enabled = false;
        config.rate_limit.enabled = true;
        config.rate_limit.requests_per_second = 5;
        config.rate_limit.burst = 5;
    })
    .await;
    let client = client();

    let started = std::time::Instant::now();
    let mut admitted = 0u32;
    let mut throttled = 0u32;
    for index in 0..20 {
        // Unique query per attempt: distinct coalescing keys, so every
        // admission is its own leader dial (no dedup-window replays).
        let url = format!("http://{proxy}/api/items?n={index}");
        let (status, headers, body) = send(&client, &url).await;
        match status {
            StatusCode::OK => admitted += 1,
            StatusCode::TOO_MANY_REQUESTS => {
                throttled += 1;
                let retry_after = headers
                    .get(hyper::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u32>().ok())
                    .expect("429 must carry a numeric Retry-After");
                assert!(
                    retry_after >= 1,
                    "Retry-After rounds up to at least 1s, got {retry_after}"
                );
                assert_eq!(
                    body, "coalix: rate limit exceeded",
                    "denials use the shared fallback-style body"
                );
            }
            other => panic!("only 200 or 429 may reach the client, got {other}"),
        }
    }
    let elapsed = started.elapsed();

    // Mathematical ceiling: burst + tokens refilled over the loop, + rounds.
    let refill = (elapsed.as_micros() * 5) / 1_000_000;
    let ceiling = 5 + u32::try_from(refill).expect("refill fits u32") + 2;
    assert!(
        admitted >= 5,
        "the configured burst must admit on the wire, got {admitted}"
    );
    assert!(
        admitted <= ceiling,
        "{admitted} admissions exceed burst + refill ceiling {ceiling}"
    );
    assert!(throttled >= 1, "the excess must be throttled");
    assert_eq!(admitted + throttled, 20, "every request gets a verdict");

    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        admitted as usize,
        "denied requests must never dial the upstream"
    );

    let exposition = scrape(&client, proxy).await;
    assert_eq!(
        sample(&exposition, "coalix_rate_limited_total"),
        u64::from(throttled),
        "one counter increment per 429"
    );
    assert_eq!(
        sample(&exposition, "coalix_upstream_requests_total"),
        u64::from(admitted),
        "dial counter mirrors admissions only"
    );

    upstream.task.abort();
    proxy_task.abort();
}

#[tokio::test]
async fn disabled_limiter_is_a_zero_interference_noop() {
    let upstream = spawn_upstream().await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, |config| {
        config.cache.enabled = false;
        // rate_limit silently keeps its zero-config default: disabled.
    })
    .await;
    let client = client();

    for index in 0..30 {
        // Unique query per attempt: one leader dial per request, so the
        // upstream counter is an exact admission oracle.
        let url = format!("http://{proxy}/api/items?n={index}");
        let (status, _, _) = send(&client, &url).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "no throttling while rate_limit.enabled is false"
        );
    }
    assert_eq!(upstream.gets.load(Ordering::SeqCst), 30);

    let exposition = scrape(&client, proxy).await;
    assert_eq!(sample(&exposition, "coalix_rate_limited_total"), 0);

    upstream.task.abort();
    proxy_task.abort();
}
