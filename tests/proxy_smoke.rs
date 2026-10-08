//! Phase 2+3 end-to-end smoke: a real upstream, a real proxy, real sockets.
//!
//! Verifies the structural promises of the coalescing proxy:
//! a herd of identical concurrent GETs reaches the backend exactly once —
//! and the landed flight is then *cached*, so a repeat read inside the TTL
//! reaches neither backend nor flight map again;
//! everything that must NOT share a flight (mutations, different queries,
//! different key headers) still gets its own upstream call.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{ACCEPT_LANGUAGE, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use coalix::config::{CoalescingConfig, Config, UpstreamConfig};
use coalix::proxy::serve_with_listener;

type TestClient = Client<HttpConnector, Full<Bytes>>;
type HitCounter = Arc<AtomicUsize>;

/// Per-test backend: echoes `path|lang|seq` where `seq` is its own hit
/// number, so replayed vs freshly-fetched responses are distinguishable.
/// GETs sleep first (leaving a wide flight window), POSTs answer at once.
struct Upstream {
    addr: SocketAddr,
    gets: HitCounter,
    posts: HitCounter,
    task: JoinHandle<()>,
}

async fn spawn_upstream() -> Upstream {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    let gets: HitCounter = Arc::new(AtomicUsize::new(0));
    let posts: HitCounter = Arc::new(AtomicUsize::new(0));
    let gets_task = gets.clone();
    let posts_task = posts.clone();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(_) => return,
            };
            let gets = gets_task.clone();
            let posts = posts_task.clone();
            tokio::spawn(async move {
                let connection = http1_serve(stream, gets, posts);
                let _ = connection.await;
            });
        }
    });

    Upstream {
        addr,
        gets,
        posts,
        task,
    }
}

/// Serves one keep-alive connection of the mock upstream.
async fn http1_serve(
    stream: tokio::net::TcpStream,
    gets: HitCounter,
    posts: HitCounter,
) -> Result<(), hyper::Error> {
    hyper::server::conn::http1::Builder::new()
        .serve_connection(
            TokioIo::new(stream),
            service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let gets = gets.clone();
                let posts = posts.clone();
                async move {
                    let method = request.method().clone();
                    let path = request
                        .uri()
                        .path_and_query()
                        .map(|target| target.as_str().to_owned())
                        .unwrap_or_default();
                    let lang = request
                        .headers()
                        .get(ACCEPT_LANGUAGE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("-")
                        .to_owned();
                    let _echoed_request_body = request
                        .into_body()
                        .collect()
                        .await
                        .map(|collected| collected.to_bytes());

                    if method == Method::GET {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                    let counter = if method == Method::POST {
                        &posts
                    } else {
                        &gets
                    };
                    let seq = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    let status = if method == Method::POST {
                        StatusCode::CREATED
                    } else {
                        StatusCode::OK
                    };
                    let payload = format!("{path}|{lang}|{seq}");
                    let response = Response::builder()
                        .status(status)
                        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                        .body(Full::new(Bytes::from(payload)))
                        .expect("valid response");
                    Ok::<_, Infallible>(response)
                }
            }),
        )
        .await
}

/// Boots the proxy on an ephemeral port in front of `upstream`.
async fn spawn_proxy(
    upstream: &Upstream,
    key_headers: Vec<String>,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().expect("proxy addr");
    let config = Arc::new(Config {
        upstream: UpstreamConfig {
            base_url: format!("http://{}", upstream.addr),
            ..UpstreamConfig::default()
        },
        coalescing: CoalescingConfig {
            key_headers,
            ..CoalescingConfig::default()
        },
        ..Config::default()
    });
    let task = tokio::spawn(async move {
        let _ = serve_with_listener(listener, config).await;
    });
    (addr, task)
}

fn client() -> TestClient {
    Client::builder(TokioExecutor::new()).build(HttpConnector::new())
}

/// One request through the proxy; returns status + fully-read body.
async fn send(
    client: &TestClient,
    method: Method,
    url: String,
    lang: Option<&str>,
) -> (StatusCode, Bytes) {
    let mut builder = Request::builder().method(method).uri(url);
    if let Some(lang) = lang {
        builder = builder.header(ACCEPT_LANGUAGE, lang);
    }
    let request = builder
        .body(Full::new(Bytes::from_static(b"payload")))
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

#[tokio::test]
async fn herd_of_identical_gets_hits_upstream_once() {
    let upstream = spawn_upstream().await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, Vec::new()).await;
    let client = client();
    let url = format!("http://{proxy}/api/report?range=7d");

    let mut herd = Vec::new();
    for _ in 0..8 {
        let client = client.clone();
        let url = url.clone();
        herd.push(tokio::spawn(async move {
            send(&client, Method::GET, url, None).await
        }));
    }

    let mut bodies = Vec::new();
    for task in herd {
        let (status, body) = task.await.expect("herd member");
        assert_eq!(status, StatusCode::OK);
        bodies.push(body);
    }
    let first = &bodies[0];
    for body in &bodies {
        assert_eq!(
            body, first,
            "every herd member must receive the identical flight"
        );
    }
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        1,
        "the backend must have been called exactly once"
    );
    assert!(
        first.ends_with(b"|1"),
        "payload should be the single backend answer, got: {}",
        String::from_utf8_lossy(first)
    );

    // Phase 3: the landed flight was buffered *and* cached — a repeat read
    // inside the default 2s TTL answers from the micro-cache, byte-identical
    // to the flight, with the backend counter untouched.
    let (status, body) = send(&client, Method::GET, url, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body, first, "cache hit must replay the very same bytes");
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        1,
        "the repeat read must be served from the cache, not the backend"
    );

    upstream.task.abort();
    proxy_task.abort();
}

#[tokio::test]
async fn distinct_queries_languages_and_mutations_bypass_coalescing() {
    let upstream = spawn_upstream().await;
    let (proxy, proxy_task) = spawn_proxy(&upstream, vec!["accept-language".to_owned()]).await;
    let client = client();

    // Three structurally different GETs: different query, different
    // query again, and equal query with a different key header.
    let reads: Vec<(&str, &str)> = vec![
        ("/api/report?range=7d", "en"),
        ("/api/report?range=30d", "en"),
        ("/api/report?range=7d", "th"),
    ];
    let mut tasks = Vec::new();
    for (path, lang) in &reads {
        let client = client.clone();
        let url = format!("http://{proxy}{path}");
        let lang = (*lang).to_owned();
        tasks.push((
            (*path, lang.clone()),
            tokio::spawn(async move { send(&client, Method::GET, url, Some(&lang)).await }),
        ));
    }
    for ((path, lang), task) in tasks {
        let (status, body) = task.await.expect("read task");
        assert_eq!(status, StatusCode::OK);
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.starts_with(&format!("{path}|{lang}|")),
            "distinct keys must keep their own upstream answers, got: {text}"
        );
    }
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        3,
        "three distinct flights = three backend calls"
    );

    // Mutations never share flights: three POSTs, three backend hits.
    let mut writes = Vec::new();
    for i in 0..3 {
        let client = client.clone();
        let url = format!("http://{proxy}/api/items/{i}");
        writes.push(tokio::spawn(async move {
            send(&client, Method::POST, url, None).await
        }));
    }
    for task in writes {
        let (status, _) = task.await.expect("write task");
        assert_eq!(status, StatusCode::CREATED);
    }
    assert_eq!(
        upstream.posts.load(Ordering::SeqCst),
        3,
        "POST must never be coalesced"
    );
    assert_eq!(
        upstream.gets.load(Ordering::SeqCst),
        3,
        "POST must not disturb GET flights"
    );

    upstream.task.abort();
    proxy_task.abort();
}
