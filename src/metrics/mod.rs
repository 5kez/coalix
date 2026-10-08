//! # Metrics
//!
//! A dependency-free, lock-free registry: `AtomicU64` counters, fixed-bucket
//! latency histograms, and a labelled request counter, rendered straight into
//! the Prometheus text exposition format (version 0.0.4) — no client library,
//! no background collection, no allocation on the hot path except the one-time
//! composite key of a newly-seen label series.
//!
//! Design notes:
//!
//! * **Counters are `Relaxed`** — single-process, monotonic tallies; no
//!   ordering with other memory is required, so the fast path never
//!   serializes.
//! * **Gauges are pulled, not pushed** — [`LiveGauges`] carries what only the
//!   owning subsystem knows (active flights, cache rows, breaker phase) at
//!   scrape time; the registry never touches those structures itself.
//! * **The endpoint is reserved** — the proxy answers
//!   `observability.metrics_path` before routing, buffering, caching, or
//!   coalescing, so a scrape is invisible to every other metric.
//!
//! Family semantics (names frozen by the README contract):
//!
//! | Family | Meaning |
//! |--------|---------|
//! | `coalix_requests_total{route,method,coalesced}` | one per handled request; `coalesced` is the *routing decision* |
//! | `coalix_upstream_requests_total` | dials that actually left the proxy (leader, bypass, revalidation) |
//! | `coalix_saved_requests_total` | herd members replayed from another request's flight |
//! | `coalix_wait_seconds` / `coalix_upstream_seconds` | parked time / dial latency histograms |
//! | gauges | flights tracked, waiters parked, cache rows + counters, breaker phase (0 closed, 1 half-open, 2 open) |

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;

use crate::cache::CacheStats;
use crate::resilience::BreakerPhase;

/// Media type of a successful scrape body (Prometheus-compatible text).
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Bucket upper bounds in microseconds: 0.5 ms … 5 s, roughly 2.2–2.5x
/// apart, so twelve buckets cover four orders of magnitude with bounded
/// worst-case quantile error.
const BOUNDS_MICROS: [u64; 12] = [
    500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
    5_000_000,
];

/// One latency family: per-bucket tallies (stored non-cumulative), a running
/// sum in microseconds, and a total count. Rendering folds buckets
/// cumulatively, so observations beyond the last finite bound appear only in
/// the `+Inf` bucket — exactly what the exposition format expects.
#[derive(Debug)]
pub struct Histogram {
    counts: Box<[AtomicU64]>,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        let counts = (0..BOUNDS_MICROS.len())
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            counts,
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    /// Records one observation. Atomics wrap rather than panic; reaching
    /// `u64` microseconds of cumulative latency would take ~584 thousand
    /// years, so saturation handling is deliberately omitted.
    pub fn observe(&self, value: Duration) {
        let micros = u64::try_from(value.as_micros()).unwrap_or(u64::MAX);
        if let Some(index) = BOUNDS_MICROS.iter().position(|bound| micros <= *bound) {
            self.counts[index].fetch_add(1, Ordering::Relaxed);
        }
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of observations — cheap assertions without rendering.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Emits `# HELP`, `# TYPE`, cumulative `_bucket` lines, `_sum`, `_count`.
    fn render(&self, out: &mut String, name: &str, help: &str) {
        out.push_str(&format!("# HELP {name} {help}\n"));
        out.push_str(&format!("# TYPE {name} histogram\n"));
        let mut cumulative = 0u64;
        for (index, bound) in BOUNDS_MICROS.iter().enumerate() {
            cumulative += self.counts[index].load(Ordering::Relaxed);
            out.push_str(&format!(
                "{name}_bucket{{le=\"{}\"}} {cumulative}\n",
                *bound as f64 / 1_000_000.0
            ));
        }
        out.push_str(&format!(
            "{name}_bucket{{le=\"+Inf\"}} {}\n",
            self.count.load(Ordering::Relaxed)
        ));
        let sum_seconds = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        out.push_str(&format!("{name}_sum {sum_seconds}\n"));
        out.push_str(&format!(
            "{name}_count {}\n",
            self.count.load(Ordering::Relaxed)
        ));
    }
}

/// Live, pull-time gauges owned by other subsystems, gathered at scrape time.
#[derive(Debug, Clone, Copy)]
pub struct LiveGauges {
    /// Flights the engine tracks right now (airborne + dedup-window tail).
    pub flights_active: usize,
    /// Rows currently resident in the micro-cache.
    pub entries: usize,
    /// Cache activity since start-up.
    pub cache: CacheStats,
    /// Breaker phase at scrape time.
    pub breaker: BreakerPhase,
}

/// RAII delegate for one parked waiter: holds `coalix_waiters` up while
/// alive, records into `coalix_wait_seconds`, and releases the gauge on drop
/// — early returns, panics, and cancelled connections included.
pub struct WaitGuard<'a> {
    metrics: &'a Metrics,
    entered_at: std::time::Instant,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.metrics.waiters.fetch_sub(1, Ordering::Relaxed);
        self.metrics.wait_seconds.observe(self.entered_at.elapsed());
    }
}

/// The registry every proxy connection shares: counters advance atomically
/// on request paths, gauges are assembled only when `/metrics` is scraped.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Composite series key: `route \x1f method \x1f coalesced` → tally.
    requests: DashMap<String, AtomicU64>,
    upstream_requests: AtomicU64,
    saved_requests: AtomicU64,
    waiters: AtomicU64,
    wait_seconds: Histogram,
    upstream_seconds: Histogram,
}

impl Metrics {
    /// Increments the labelled request family once per handled request.
    /// `coalesced` carries the *routing decision* (engine/cache own the
    /// request's fate), never the outcome — one increment site, no drift.
    pub fn record_request(&self, route: &str, method: &str, coalesced: bool) {
        let series = format!("{route}\u{1f}{method}\u{1f}{coalesced}");
        let slot = self
            .requests
            .entry(series)
            .or_insert_with(|| AtomicU64::new(0));
        slot.fetch_add(1, Ordering::Relaxed);
    }

    /// One dial left the proxy (caller records *after* breaker admission).
    pub fn record_upstream_call(&self) {
        self.upstream_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// One herd member was replayed from another request's flight.
    pub fn record_saved(&self) {
        self.saved_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Latency of one upstream dial (leader, bypass, or revalidation).
    pub fn observe_upstream(&self, elapsed: Duration) {
        self.upstream_seconds.observe(elapsed);
    }

    /// Parks one waiter: raises the gauge, returns the guard that lowers it
    /// and books the parked duration when it drops.
    pub fn enter_wait(&self) -> WaitGuard<'_> {
        self.waiters.fetch_add(1, Ordering::Relaxed);
        WaitGuard {
            metrics: self,
            entered_at: std::time::Instant::now(),
        }
    }

    /// Clients parked right now.
    pub fn waiter_count(&self) -> u64 {
        self.waiters.load(Ordering::Relaxed)
    }

    /// Renders the complete exposition text — every family carries HELP and
    /// TYPE lines even with zero series, so dashboards stay linkable.
    pub fn render(&self, live: &LiveGauges) -> String {
        let mut out = String::with_capacity(4 * 1024);

        out.push_str("# HELP coalix_requests_total Handled requests labelled by route, method, and the coalescing routing decision.\n");
        out.push_str("# TYPE coalix_requests_total counter\n");
        let mut series: Vec<(String, u64)> = self
            .requests
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();
        series.sort_by(|left, right| left.0.cmp(&right.0));
        for (key, value) in series {
            let mut parts = key.split('\u{1f}');
            let route = parts.next().unwrap_or("default");
            let method = parts.next().unwrap_or("-");
            let coalesced = parts.next().unwrap_or("false");
            out.push_str(&format!(
                "coalix_requests_total{{route=\"{}\",method=\"{}\",coalesced=\"{}\"}} {value}\n",
                escape_label(route),
                escape_label(method),
                escape_label(coalesced),
            ));
        }

        emit_counter(
            &mut out,
            "coalix_upstream_requests_total",
            "Upstream dials that actually left the proxy.",
            self.upstream_requests.load(Ordering::Relaxed),
        );
        emit_counter(
            &mut out,
            "coalix_saved_requests_total",
            "Herd members answered from another request's flight.",
            self.saved_requests.load(Ordering::Relaxed),
        );
        emit_gauge(
            &mut out,
            "coalix_flights_active",
            "Flights the engine tracks (airborne plus dedup window).",
            live.flights_active as u64,
        );
        emit_gauge(
            &mut out,
            "coalix_waiters",
            "Clients parked on a flight right now.",
            self.waiter_count(),
        );

        let hits = live.cache.hits.saturating_add(live.cache.stale_hits);
        emit_counter(
            &mut out,
            "coalix_cache_hits_total",
            "Micro-cache probes answered without a fresh dial (fresh plus stale).",
            hits,
        );
        emit_counter(
            &mut out,
            "coalix_cache_stale_hits_total",
            "Rows served stale while a revalidation ran.",
            live.cache.stale_hits,
        );
        emit_counter(
            &mut out,
            "coalix_cache_misses_total",
            "Micro-cache probes that found nothing servable.",
            live.cache.misses,
        );
        emit_counter(
            &mut out,
            "coalix_cache_stores_total",
            "Responses accepted into the cache.",
            live.cache.stores,
        );
        emit_counter(
            &mut out,
            "coalix_cache_revalidations_total",
            "Background revalidations actually claimed.",
            live.cache.revalidations,
        );
        emit_counter(
            &mut out,
            "coalix_cache_evictions_total",
            "Rows dropped to honour max_entries.",
            live.cache.evictions,
        );
        emit_gauge(
            &mut out,
            "coalix_cache_entries",
            "Rows currently resident in the cache.",
            live.entries as u64,
        );
        emit_gauge(
            &mut out,
            "coalix_breaker_state",
            "Circuit breaker phase: 0 closed, 1 half-open, 2 open.",
            breaker_wire(live.breaker),
        );

        self.wait_seconds.render(
            &mut out,
            "coalix_wait_seconds",
            "Seconds a waiter stayed parked before replay or release.",
        );
        self.upstream_seconds.render(
            &mut out,
            "coalix_upstream_seconds",
            "Seconds spent dialing the upstream (leader, bypass, revalidation).",
        );
        out
    }
}

/// Wire values documented for dashboards: 0 closed, 1 half-open, 2 open.
pub fn breaker_wire(phase: BreakerPhase) -> u64 {
    match phase {
        BreakerPhase::Closed => 0,
        BreakerPhase::HalfOpen => 1,
        BreakerPhase::Open => 2,
    }
}

/// One `HELP`/`TYPE`/sample triple for an untimed counter.
fn emit_counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} counter\n"));
    out.push_str(&format!("{name} {value}\n"));
}

/// One `HELP`/`TYPE`/sample triple for an instantaneous gauge.
fn emit_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} gauge\n"));
    out.push_str(&format!("{name} {value}\n"));
}

/// Prometheus label-value escaping: backslash, double quote, newline.
fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed live-gauge fixture so every assertion reads like a sentence.
    fn live() -> LiveGauges {
        LiveGauges {
            flights_active: 2,
            entries: 1,
            cache: CacheStats {
                hits: 9,
                stale_hits: 3,
                misses: 4,
                stores: 7,
                revalidations: 1,
                evictions: 0,
            },
            breaker: BreakerPhase::Closed,
        }
    }

    /// Families frozen by the README contract — every one must carry
    /// `# HELP` and `# TYPE` headers on every scrape, zero series or not.
    #[test]
    fn every_readme_family_is_declared_even_with_zero_series() {
        let text = Metrics::default().render(&live());
        let families = [
            "coalix_requests_total",
            "coalix_upstream_requests_total",
            "coalix_saved_requests_total",
            "coalix_flights_active",
            "coalix_waiters",
            "coalix_wait_seconds",
            "coalix_upstream_seconds",
            "coalix_cache_hits_total",
            "coalix_cache_misses_total",
            "coalix_cache_stale_hits_total",
            "coalix_cache_stores_total",
            "coalix_cache_revalidations_total",
            "coalix_cache_evictions_total",
            "coalix_cache_entries",
            "coalix_breaker_state",
        ];
        for family in families {
            assert!(
                text.contains(&format!("# HELP {family} ")),
                "missing HELP for {family}"
            );
            assert!(
                text.contains(&format!("# TYPE {family} ")),
                "missing TYPE for {family}"
            );
        }
    }

    #[test]
    fn request_series_accumulate_per_label_set_and_sort_stably() {
        let metrics = Metrics::default();
        metrics.record_request("api", "GET", true);
        metrics.record_request("api", "GET", true);
        metrics.record_request("api", "POST", false);
        metrics.record_request("default", "GET", true);
        let text = metrics.render(&live());
        assert!(text.contains(
            "coalix_requests_total{route=\"api\",method=\"GET\",coalesced=\"true\"} 2\n"
        ));
        assert!(text.contains(
            "coalix_requests_total{route=\"api\",method=\"POST\",coalesced=\"false\"} 1\n"
        ));
        assert!(text.contains(
            "coalix_requests_total{route=\"default\",method=\"GET\",coalesced=\"true\"} 1\n"
        ));
        let api = text
            .find("coalix_requests_total{route=\"api\",method=\"GET\"")
            .expect("api GET series");
        let default = text
            .find("coalix_requests_total{route=\"default\"")
            .expect("default series");
        assert!(api < default, "series must render in sorted order");
    }

    #[test]
    fn label_values_are_escaped_for_the_text_format() {
        let metrics = Metrics::default();
        metrics.record_request("a\"b\\c", "GET", true);
        let text = metrics.render(&live());
        assert!(
            text.contains("route=\"a\\\"b\\\\c\""),
            "quotes and backslashes must be escaped, got:\n{text}"
        );
    }

    #[test]
    fn histogram_buckets_are_cumulative_with_inf_sum_and_count() {
        let histogram = Histogram::default();
        histogram.observe(Duration::from_micros(400));
        histogram.observe(Duration::from_micros(700));
        histogram.observe(Duration::from_secs(60));
        assert_eq!(histogram.count(), 3);

        let mut out = String::new();
        histogram.render(&mut out, "coalix_demo_seconds", "fixture");
        assert!(out.contains("# TYPE coalix_demo_seconds histogram\n"));
        assert!(out.contains("coalix_demo_seconds_bucket{le=\"0.0005\"} 1\n"));
        assert!(out.contains("coalix_demo_seconds_bucket{le=\"0.001\"} 2\n"));
        // Only the +Inf bucket absorbs the out-of-range observation.
        assert!(out.contains("coalix_demo_seconds_bucket{le=\"+Inf\"} 3\n"));
        assert!(out.contains("coalix_demo_seconds_sum 60.0011\n"));
        assert!(out.contains("coalix_demo_seconds_count 3\n"));
    }

    #[test]
    fn wait_guard_tracks_live_parkers_and_books_the_park() {
        let metrics = Metrics::default();
        assert_eq!(metrics.waiter_count(), 0);
        let guard = metrics.enter_wait();
        assert_eq!(metrics.waiter_count(), 1);
        drop(guard);
        assert_eq!(metrics.waiter_count(), 0);
        assert!(metrics
            .render(&live())
            .contains("coalix_wait_seconds_count 1\n"));
    }

    #[test]
    fn upstream_and_saved_counters_reach_the_exposition() {
        let metrics = Metrics::default();
        metrics.record_upstream_call();
        metrics.record_upstream_call();
        metrics.record_saved();
        let text = metrics.render(&live());
        assert!(text.contains("coalix_upstream_requests_total 2\n"));
        assert!(text.contains("coalix_saved_requests_total 1\n"));
    }

    #[test]
    fn breaker_phase_maps_to_the_documented_wire_values() {
        assert_eq!(breaker_wire(BreakerPhase::Closed), 0);
        assert_eq!(breaker_wire(BreakerPhase::HalfOpen), 1);
        assert_eq!(breaker_wire(BreakerPhase::Open), 2);
    }

    #[test]
    fn live_gauges_render_exact_sample_lines() {
        let text = Metrics::default().render(&live());
        assert!(text.contains("coalix_flights_active 2\n"));
        assert!(text.contains("coalix_waiters 0\n"));
        assert!(text.contains("coalix_cache_entries 1\n"));
        assert!(text.contains("coalix_breaker_state 0\n"));
        // hits_total folds fresh plus stale rows.
        assert!(text.contains("coalix_cache_hits_total 12\n"));
        assert!(text.contains("coalix_cache_stale_hits_total 3\n"));
        assert!(text.contains("coalix_cache_misses_total 4\n"));
        assert!(text.contains("coalix_cache_stores_total 7\n"));
        assert!(text.contains("coalix_cache_revalidations_total 1\n"));
        assert!(text.contains("coalix_cache_evictions_total 0\n"));

        let open = LiveGauges {
            breaker: BreakerPhase::Open,
            ..live()
        };
        assert!(Metrics::default()
            .render(&open)
            .contains("coalix_breaker_state 2\n"));
        let half_open = LiveGauges {
            breaker: BreakerPhase::HalfOpen,
            ..live()
        };
        assert!(Metrics::default()
            .render(&half_open)
            .contains("coalix_breaker_state 1\n"));
    }
}
