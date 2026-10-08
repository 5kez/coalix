//! Phase 4: Criterion micro-benchmarks for the paths every request touches —
//! flight-key derivation, the micro-cache round trip, and metrics recording.
//!
//! All three benches are synchronous by design: the engine's async work is
//! I/O-bound, while these are the CPU costs paid per request on the hot path.
//! Run with `cargo bench`; compile-only coverage comes from
//! `cargo clippy --all-targets`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};

use coalix::cache::Cache;
use coalix::coalescer::{FlightKey, SharedResponse};
use coalix::config::{CacheConfig, RateLimitConfig};
use coalix::metrics::{LiveGauges, Metrics};
use coalix::ratelimit::RateLimiter;
use coalix::resilience::BreakerPhase;

/// Key derivation: hashing method + path + query + spilled key headers —
/// the very first cost of every coalescable request.
fn bench_flight_key(c: &mut Criterion) {
    let method = Method::GET;
    let uri: Uri = "/api/products/42?region=eu&currency=thb"
        .parse()
        .expect("valid URI");
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("accept-language"),
        HeaderValue::from_static("en-GB,en;q=0.9,th;q=0.8"),
    );
    let key_headers = vec!["accept-language".to_owned()];
    c.bench_function("flight_key_build", |b| {
        b.iter(|| {
            FlightKey::build(
                black_box(&method),
                black_box(&uri),
                black_box(&headers),
                black_box(&key_headers),
            )
        })
    });
}

/// Store-then-probe: the slide a leader takes when landing a flight, and the
/// slide every request takes on arrival.
fn bench_cache_roundtrip(c: &mut Criterion) {
    // An hour-long TTL keeps every lookup on the fresh-hit path so the
    // benchmark measures map cost, not window arithmetic.
    let cache = Cache::new(&CacheConfig {
        ttl_ms: 3_600_000,
        stale_while_revalidate_ms: 3_600_000,
        ..CacheConfig::default()
    });
    let shared = Arc::new(SharedResponse {
        status: hyper::StatusCode::OK,
        headers: Vec::new(),
        body: Bytes::from_static(b"benchmark payload"),
    });
    let method = Method::GET;
    let uri: Uri = "/api/bench".parse().expect("valid URI");
    let no_key_headers: &[String] = &[];
    let key = FlightKey::build(&method, &uri, &HeaderMap::new(), no_key_headers);
    c.bench_function("cache_store_and_lookup", |b| {
        b.iter(|| {
            cache.store(black_box(&key), shared.clone());
            black_box(cache.lookup(black_box(&key)));
        })
    });
}

/// Bookkeeping: one labelled counter, one dial counter, one observation —
/// per-request metrics cost; then a full render as `/metrics` sees it.
fn bench_metrics_record_and_render(c: &mut Criterion) {
    let metrics = Metrics::default();
    let live = LiveGauges {
        flights_active: 7,
        entries: 3,
        cache: Default::default(),
        breaker: BreakerPhase::Closed,
    };
    c.bench_function("metrics_record_and_observe", |b| {
        b.iter(|| {
            metrics.record_request(black_box("api"), black_box("GET"), black_box(true));
            metrics.record_upstream_call();
            metrics.observe_upstream(Duration::from_micros(42));
        })
    });
    c.bench_function("metrics_render_exposition", |b| {
        b.iter(|| black_box(metrics.render(black_box(&live))))
    });
}

/// Edge admission: one bucket check per request — the only new cost on the
/// hot path while rate limiting is enabled (disabled costs one `None`
/// branch, which this bench also exercises through the always-admitting
/// configuration below).
fn bench_rate_limit_check(c: &mut Criterion) {
    let limiter = RateLimiter::new(&RateLimitConfig {
        enabled: true,
        // A deep bucket at a huge rate keeps every iteration on the
        // admit path, so the bench measures lookup + refill arithmetic
        // rather than denial handling.
        requests_per_second: 1_000_000,
        burst: 1_000_000,
        ..RateLimitConfig::default()
    });
    let client: std::net::IpAddr = "198.51.100.7".parse().expect("bench ip");
    c.bench_function("ratelimit_check_admit", |b| {
        b.iter(|| black_box(limiter.check(black_box(client))))
    });
}

criterion_group!(
    benches,
    bench_flight_key,
    bench_cache_roundtrip,
    bench_metrics_record_and_render,
    bench_rate_limit_check
);
criterion_main!(benches);
