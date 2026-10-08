//! # Coalix
//!
//! **Zero-config, blazing-fast Request Coalescing Reverse Proxy — forged in
//! Rust** that defuses the *thundering herd* problem by *coalescing*
//! identical in-flight requests.
//!
//! One upstream **leader** executes the backend call while every other
//! caller waits as a **waiter**; the response is fanned out to all
//! subscribers through a `tokio::sync::broadcast` channel with a shared,
//! ref-counted body (`bytes::Bytes`) — no per-waiter copies, no duplicated
//! backend work.
//!
//! ## Hard rules
//!
//! * Only methods listed in `config.coalescing.coalesce_methods`
//!   (default: `GET`, `HEAD`) may coalesce. Mutations bypass the engine
//!   structurally — correctness never depends on remembering a check.
//! * The coalescing key is `method + path + query + configured key headers`.
//! * `/metrics` is reserved: the Prometheus exposition is answered there
//!   before routing, never by the route table.
//!
//! ## Crate layout
//!
//! | Module        | Phase | Responsibility                                     |
//! |---------------|-------|----------------------------------------------------|
//! | [`config`]    | 1     | YAML/env model, validation, first-match routing    |
//! | `proxy`       | 2     | hyper reverse proxy + upstream client pool         |
//! | `coalescer`   | 2     | single-flight engine (`DashMap` + `broadcast`)     |
//! | `cache`       | 3     | micro-cache with stale-while-revalidate            |
//! | `resilience`  | 3     | timeouts, circuit breaker, fallback response       |
//! | `metrics`     | 4     | lock-free `AtomicU64` registry + `/metrics`        |
//! | `ratelimit`   | 5     | per-IP token bucket at the edge (429/Retry-After)   |
//! | `tests/`      | 4     | herd-simulation harness + integration tests        |
//! | `benches/`    | 4     | Criterion micro-benchmarks                         |
//! | `.github/`    | 4     | CI pipeline (fmt, clippy, test, MSRV)              |
//!
//! ## Entry points
//!
//! * [`Config::load`] — zero-config defaults, optional YAML file, `COALIX_*`
//!   environment overrides, then semantic validation.
//! * [`VERSION`] — build version printed by `coalix --version`.

/// Configuration model, validation, and first-match route lookup.
pub mod config;

/// Single-flight engine: `DashMap` flight map + `tokio` broadcast fan-out.
pub mod coalescer;

/// Micro-cache with stale-while-revalidate, in front of the engine.
pub mod cache;

/// Circuit breaker and fallback policy shielding the upstream.
pub mod resilience;

/// Lock-free metrics registry and the reserved `/metrics` exposition.
pub mod metrics;

/// Per-IP token-bucket admission control at the proxy edge.
pub mod ratelimit;

/// Hyper reverse proxy: accept loop, upstream pool, request routing.
pub mod proxy;

pub use config::{Config, ConfigError};

/// Build-time version string, surfaced by `coalix --version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
