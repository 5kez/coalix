//! Phase 4 herd simulation: scale, caching, and failure behaviour of the
//! coalescing proxy against a real upstream, observed through the reserved
//! `/metrics` endpoint.
//!
//! Four scenarios: a hundred GETs collapse to one backend call; a second
//! wave is answered entirely from the micro-cache; a failing upstream trips
//! the breaker, which then fails fast; and the metrics endpoint itself is
//! reserved and contract-complete.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
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

/// Configurable backend: GETs sleep `delay` first (leaving a wide flight
/// window), then answer `status` with a `path|seq` payload, so replayed vs.
/// freshly-fetched replies stay distinguishable.
struct Upstream {
    addr: SocketAddr,
    gets: HitCounter,
    task: JoinHandle<()>,
}

async fn spawn_upstream(delay: Duration, status: StatusCode) -> Upstream {
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
                let _connection = http1_serve(stream, gets, status, delay).await;
            });
        }
    });

    Upstream { addr, gets, task }
}

/// Serves one keep-alive connection of the mock upstream.
async fn http1_serve(
    stream: tokio::net::TcpStream,
    gets: HitCounter,
    status: StatusCode,
    delay: Duration,
) -> Result<(), hyper::Error> {
    hyper::server::conn::http1::Builder::new()
        .serve_connection(
            TokioIo::new(stream),
            service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let gets = gets.clone();
                async move {
                    let path = request.uri().path().to_owned();
                    let _drained = request.into_body().collect().await;
                    if delay > Duration::ZERO {
                        tokio::time::sleep(delay).await;
                    }
                    let seq = gets.fetch_add(1, Ordering::SeqCst) + 1;
                    let payload = format!("{path}|{seq}");
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
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
/// mutates the zero-config default before validation, so tests can flip
/// cache, breaker, or routing knobs without duplicating the harness.
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

/// One bodyless GET through the proxy; returns status + full body.
async fn get(client: &TestClient, url: String) -> (StatusCode, Bytes) {
    let request = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Full::new(Bytes::new()))
        .expect("request");
    let response = client.request(request).await.expect("proxy answered");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    (status, body)
}

/// Scrapes `/metrics`; any non-200 fails the test right here.
async fn scrape(client: &TestClient, proxy: SocketAddr) -> String {
    let (status, body) = get(client, format!("http://{proxy}/metrics")).await;
    assert_eq!(status, StatusCode::OK, "scrape must succeed");
    String::from_utf8(body.to_vec()).expect("exposition is UTF-8")
}

/// Reads one exact sample line: `prefix + " " + value`. Families with no
/// matching line yield 0, which every assertion treats as "absent".
fn sample(text: &str, prefix: &str) -> u64 {
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(prefix) {
            if let Some(value) = rest.strip_prefix(' ') {
                if let Ok(parsed) = value.parse::<u64>() {
                    return parsed;
                }
            }
        }
    }
    0
}

/// Fires `count` concurrent GETs and collects every body; any non-200 fails.
async fn herd_of(client: &TestClient, url: &str, count: usize) -> Vec<Bytes> {
    let mut tasks = Vec::with_capacity(count);
    for _ in 0..count {
        let client = client.clone();
        let url = url.to_owned();
        tasks.push(tokio::spawn(async move { get(&client, url).await }));
    }
    let mut bodies = Vec::with_capacity(count);
    for task in tasks {
        let (status, body) = task.await.expect("herd member");
        assert_eq!(status, StatusCode::OK, "every member must be answered");
        bodies.push(body);
    }
    bodies
}

#[tokio::test]
async fn herd_of_one_hundred_gets_collapses_to_a_single_upstream_call() {
    let upstream = spawn_upstream(Duration::from_millis(250), StatusCode::OK).await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, |_| {}).await;
    let client = client();
    let url = format!("http://{proxy}/api/products/42");

    let bodies = herd_of(&client, &url, 100).await;
    let first = &bodies[0];
    for body in &bodies {
        assert_eq!(body, first, "the herd must share one identical reply");
    }
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        1,
        "the backend must have been called exactly once"
    );

    // The exposition tells the same story with exact numbers.
    let text = scrape(&client, proxy).await;
    assert_eq!(
        sample(
            &text,
            "coalix_requests_total{route=\"default\",method=\"GET\",coalesced=\"true\"}"
        ),
        100
    );
    assert_eq!(sample(&text, "coalix_saved_requests_total"), 99);
    assert_eq!(sample(&text, "coalix_upstream_requests_total"), 1);
    assert_eq!(sample(&text, "coalix_wait_seconds_count"), 99);
    assert_eq!(sample(&text, "coalix_waiters"), 0, "parking must drain");
    assert!(text.contains("# TYPE coalix_flights_active gauge\n"));
    assert!(text.contains("# TYPE coalix_upstream_seconds histogram\n"));

    upstream.task.abort();
    proxy_task.abort();
}

#[tokio::test]
async fn second_wave_is_served_entirely_from_the_micro_cache() {
    let upstream = spawn_upstream(Duration::from_millis(250), StatusCode::OK).await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, |_| {}).await;
    let client = client();
    let url = format!("http://{proxy}/api/products/7?size=xl");

    // Wave one: misses all the way — one flight, one store.
    let first_wave = herd_of(&client, &url, 50).await;
    assert_eq!(upstream.gets.load(Ordering::SeqCst), 1);

    // Wave two, inside the 2 s TTL: no backend, no flight — cache only.
    let second_wave = herd_of(&client, &url, 50).await;
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        1,
        "wave two must not reach the backend"
    );
    let first = &first_wave[0];
    for body in &second_wave {
        assert_eq!(body, first, "the cache replays the very same bytes");
    }

    let text = scrape(&client, proxy).await;
    assert_eq!(
        sample(
            &text,
            "coalix_requests_total{route=\"default\",method=\"GET\",coalesced=\"true\"}"
        ),
        100
    );
    assert_eq!(sample(&text, "coalix_cache_stores_total"), 1);
    let hits = sample(&text, "coalix_cache_hits_total");
    let misses = sample(&text, "coalix_cache_misses_total");
    assert!(hits >= 50, "wave two must be pure hits, got {hits}");
    assert_eq!(
        hits + misses,
        100,
        "every request probes the cache exactly once"
    );

    upstream.task.abort();
    proxy_task.abort();
}

#[tokio::test]
async fn failing_upstream_trips_the_breaker_which_then_fails_fast() {
    // Distinct keys per attempt: one flight each, so every request really
    // dials (a shared failed flight would absorb retries inside its dedup
    // window) and the rolling window sees all five failures.
    let upstream =
        spawn_upstream(Duration::from_millis(5), StatusCode::INTERNAL_SERVER_ERROR).await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, |config| {
        config.cache.enabled = false;
    })
    .await;
    let client = client();

    for i in 1..=5 {
        let (status, body) = get(&client, format!("http://{proxy}/api/io/{i}")).await;
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "upstream answers"
        );
        assert!(
            body.starts_with(b"/api/io/"),
            "leader must forward the backend reply"
        );
    }
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        5,
        "five failures dial five times"
    );

    // Threshold reached: the open circuit answers the configured fallback
    // without touching a socket.
    let (status, body) = get(&client, format!("http://{proxy}/api/io/6")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body.starts_with(b"upstream unavailable"),
        "configured fallback body"
    );
    let (status, _) = get(&client, format!("http://{proxy}/api/io/7")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        5,
        "an open circuit never dials again"
    );

    let text = scrape(&client, proxy).await;
    assert_eq!(
        sample(&text, "coalix_breaker_state"),
        2,
        "0 closed, 1 half-open, 2 open"
    );
    assert_eq!(sample(&text, "coalix_upstream_requests_total"), 5);

    upstream.task.abort();
    proxy_task.abort();
}

#[tokio::test]
async fn metrics_endpoint_is_reserved_and_contract_complete() {
    let upstream = spawn_upstream(Duration::from_millis(1), StatusCode::OK).await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, |_| {}).await;
    let client = client();

    let (status, body) = get(&client, format!("http://{proxy}/metrics")).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).expect("utf-8 exposition");
    for family in [
        "coalix_requests_total",
        "coalix_upstream_requests_total",
        "coalix_saved_requests_total",
        "coalix_flights_active",
        "coalix_waiters",
        "coalix_wait_seconds",
        "coalix_upstream_seconds",
        "coalix_cache_hits_total",
        "coalix_cache_misses_total",
        "coalix_breaker_state",
    ] {
        assert!(
            text.contains(&format!("# TYPE {family} ")),
            "missing TYPE for {family}"
        );
    }
    // The reservation itself: nothing dialled, nothing parked, nothing
    // counted — a scrape is invisible to every other metric.
    assert_eq!(sample(&text, "coalix_upstream_requests_total"), 0);
    assert_eq!(sample(&text, "coalix_waiters"), 0);
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        0,
        "a scrape must not dial the upstream"
    );
    assert!(
        !text.contains("coalix_requests_total{"),
        "a scrape must not count itself"
    );

    // Non-GET methods on the reserved path are rejected outright.
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy}/metrics"))
        .body(Full::new(Bytes::new()))
        .expect("request");
    let response = client.request(request).await.expect("proxy answered");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(upstream.gets.load(Ordering::SeqCst), 0, "still no dial");

    upstream.task.abort();
    proxy_task.abort();
}
