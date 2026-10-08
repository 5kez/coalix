//! # Micro-cache with stale-while-revalidate
//!
//! A tiny, policy-driven response cache sitting **in front of** the
//! coalescing engine:
//!
//! * [`Cache::lookup`] runs before `coalescer.join`, so a thundering herd
//!   arriving on a fresh row never reaches the flight map or the upstream —
//!   the cache alone dissolves the herd;
//! * a [`Lookup::Fresh`] row is served as-is (zero copies beyond `Bytes`
//!   reference counting); a [`Lookup::Stale`] row is served **immediately**
//!   while exactly one background revalidation is kicked — the SWR promise:
//!   *response now, refresh later*;
//! * when the SWR window closes, the row turns into [`Lookup::Miss`] and is
//!   dropped from the map.
//!
//! ## Policy
//!
//! * only `GET`/`HEAD` replies with status `200` are stored, and only while
//!   `cache.enabled` is on;
//! * `Cache-Control: no-store | no-cache | private` never stores;
//! * freshness is `min(origin s-maxage / max-age, cache.ttl_ms)` — the
//!   configured TTL is a hard ceiling (micro-cache semantics); when the
//!   origin sends no directive, `cache.ttl_ms` applies as-is;
//! * the stale window is `min(origin stale-while-revalidate,
//!   cache.stale_while_revalidate_ms)`;
//! * capacity `cache.max_entries`: fully expired rows are swept first, then
//!   the oldest stored row evicts — oldest-first per the config contract.
//!
//! Clock: [`tokio::time::Instant`], so tests can pause and advance time.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use hyper::header::{HeaderName, HeaderValue, CACHE_CONTROL};
use hyper::StatusCode;
use tokio::time::Instant;

use crate::coalescer::{FlightKey, SharedResponse};
use crate::config::CacheConfig;

/// Outcome of one cache probe.
#[derive(Debug, Clone)]
pub enum Lookup {
    /// Inside the freshness window — serve without touching the upstream.
    Fresh(Arc<SharedResponse>),
    /// Past freshness but inside the SWR window — serve now; kick a
    /// revalidation through [`Cache::claim_revalidation`].
    Stale(Arc<SharedResponse>),
    /// Nothing usable — the caller performs the upstream call itself.
    Miss,
}

/// One cached origin response plus its freshness schedule.
#[derive(Debug)]
struct Entry {
    /// Shared reply — `Bytes` clones are reference-counted.
    shared: Arc<SharedResponse>,
    /// Past this instant the row must not be served as fresh.
    fresh_until: Instant,
    /// Past this instant the row is dropped from the map.
    stale_until: Instant,
    /// When the row was written — the oldest row evicts under pressure.
    stored_at: Instant,
    /// CAS gate: exactly one background revalidation per entry.
    revalidating: AtomicBool,
}

/// Snapshot of cache activity. Ready for the phase-4 metrics registry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Rows served inside their freshness window.
    pub hits: u64,
    /// Rows served stale while a revalidation is being kicked.
    pub stale_hits: u64,
    /// Probes that found nothing servable.
    pub misses: u64,
    /// Responses accepted and written into the cache.
    pub stores: u64,
    /// Background revalidations actually claimed (CAS wins).
    pub revalidations: u64,
    /// Rows dropped to honour `max_entries` (expired sweep excluded).
    pub evictions: u64,
}

/// Mutable counters behind [`CacheStats`].
#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    stale_hits: AtomicU64,
    misses: AtomicU64,
    stores: AtomicU64,
    revalidations: AtomicU64,
    evictions: AtomicU64,
}

/// The zero-copy micro-cache: a bounded `DashMap` of scheduled responses.
///
/// Every method takes `&self` — the map is lock-free and the counters are
/// atomic; entries are written once (last writer wins on the rare
/// revalidation/stored race, which is benign).
#[derive(Debug)]
pub struct Cache {
    entries: DashMap<FlightKey, Arc<Entry>>,
    /// Configured TTL — also the hard ceiling over origin directives.
    ttl: Duration,
    /// Default stale-while-revalidate window (capped the same way).
    swr: Duration,
    /// Capacity; at or over it, storage evicts oldest-first.
    max_entries: usize,
    /// Master switch from `cache.enabled`.
    enabled: bool,
    counters: Counters,
}

impl Cache {
    /// Builds the cache from `cache.*` configuration.
    pub fn new(config: &CacheConfig) -> Self {
        Self {
            entries: DashMap::new(),
            ttl: Duration::from_millis(config.ttl_ms),
            swr: Duration::from_millis(config.stale_while_revalidate_ms),
            max_entries: config.max_entries.max(1),
            enabled: config.enabled,
            counters: Counters::default(),
        }
    }

    /// True for methods that may read *or* write the cache (master switch,
    /// then `GET`/`HEAD`). Every mutation skips the cache by construction.
    pub fn is_cacheable(&self, method: &hyper::Method) -> bool {
        self.enabled && (method == hyper::Method::GET || method == hyper::Method::HEAD)
    }

    /// Probes `key`. A row past its SWR window is removed on the way out,
    /// so expired garbage never occupies capacity.
    pub fn lookup(&self, key: &FlightKey) -> Lookup {
        let outcome = match self.entries.get(key) {
            None => Lookup::Miss,
            Some(entry) => {
                let now = Instant::now();
                if now < entry.fresh_until {
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    return Lookup::Fresh(entry.shared.clone());
                } else if now < entry.stale_until {
                    self.counters.stale_hits.fetch_add(1, Ordering::Relaxed);
                    return Lookup::Stale(entry.shared.clone());
                } else {
                    drop(entry);
                    self.entries.remove(key);
                    Lookup::Miss
                }
            }
        };
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        outcome
    }

    /// Wins the single-revalidation CAS for `key` — the caller must follow
    /// up with [`Cache::finish_revalidation`] once the background fetch
    /// finishes (success *or* failure), otherwise that entry stops
    /// revalidating until it expires anyway.
    pub fn claim_revalidation(&self, key: &FlightKey) -> bool {
        if !self.enabled {
            return false;
        }
        let Some(entry) = self.entries.get(key) else {
            return false;
        };
        let claimed = entry
            .revalidating
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if claimed {
            self.counters.revalidations.fetch_add(1, Ordering::Relaxed);
        }
        claimed
    }

    /// Releases the revalidation claim taken by [`Cache::claim_revalidation`].
    pub fn finish_revalidation(&self, key: &FlightKey) {
        if let Some(entry) = self.entries.get(key) {
            entry.revalidating.store(false, Ordering::Release);
        }
    }

    /// Writes `shared` under `key` if policy allows it. Returns `true` when
    /// the row landed. TTL/SWR windows come from origin directives capped by
    /// configuration; status and `Cache-Control` gates run first.
    pub fn store(&self, key: &FlightKey, shared: Arc<SharedResponse>) -> bool {
        if !self.enabled || shared.status != StatusCode::OK {
            return false;
        }
        let directives = parse_directives(shared.headers.iter().map(|(n, v)| (n, v)));
        if directives.no_store {
            return false;
        }
        // Origin directives are seconds; configured windows are ceilings.
        let ttl = directives
            .ttl_seconds
            .map(Duration::from_secs)
            .unwrap_or(self.ttl)
            .min(self.ttl);
        if ttl.is_zero() {
            return false; // `max-age=0` means exactly that: do not store.
        }
        let swr = directives
            .swr_seconds
            .map(Duration::from_secs)
            .unwrap_or(self.swr)
            .min(self.swr);

        self.make_room();
        let now = Instant::now();
        self.entries.insert(
            key.clone(),
            Arc::new(Entry {
                shared,
                fresh_until: now + ttl,
                stale_until: now + ttl + swr,
                stored_at: now,
                revalidating: AtomicBool::new(false),
            }),
        );
        self.counters.stores.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Creates headroom at capacity: first sweep rows whose stale window
    /// has closed, then evict oldest-first until one slot is free.
    fn make_room(&self) {
        let now = Instant::now();
        // Sweep: gather rows past their stale window, then drop them — all
        // shared borrows, since `retain` would demand `&mut self`.
        let expired: Vec<FlightKey> = self
            .entries
            .iter()
            .filter(|entry| now >= entry.stale_until)
            .map(|entry| entry.key().clone())
            .collect();
        for key in expired {
            self.entries.remove(&key);
        }
        while self.entries.len() >= self.max_entries {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|entry| entry.stored_at)
                .map(|entry| entry.key().clone());
            match oldest {
                Some(key) if self.entries.remove(&key).is_some() => {
                    self.counters.evictions.fetch_add(1, Ordering::Relaxed);
                }
                _ => break,
            }
        }
    }

    /// Live row count — surfaced for tests and phase-4 metrics.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no row is held.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Atomic snapshot of every counter.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            stale_hits: self.counters.stale_hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            stores: self.counters.stores.load(Ordering::Relaxed),
            revalidations: self.counters.revalidations.load(Ordering::Relaxed),
            evictions: self.counters.evictions.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use hyper::header::HeaderValue;
    use hyper::Method;

    use super::*;

    fn config() -> CacheConfig {
        CacheConfig {
            enabled: true,
            ttl_ms: 1_000,                    // fresh for 1s
            stale_while_revalidate_ms: 2_000, // + 2s of stale serving
            max_entries: 4,
        }
    }

    fn key(path: &str) -> FlightKey {
        FlightKey::build(
            &Method::GET,
            &path.parse::<hyper::Uri>().expect("test uri"),
            &hyper::HeaderMap::new(),
            &[],
        )
    }

    fn response(
        status: StatusCode,
        headers: Vec<(HeaderName, HeaderValue)>,
    ) -> Arc<SharedResponse> {
        Arc::new(SharedResponse {
            status,
            headers,
            body: Bytes::from_static(b"sample"),
        })
    }

    fn ok() -> Arc<SharedResponse> {
        response(StatusCode::OK, Vec::new())
    }

    fn cc(value: &'static str) -> Vec<(HeaderName, HeaderValue)> {
        vec![(CACHE_CONTROL, HeaderValue::from_static(value))]
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_stale_miss_lifecycle() {
        let cache = Cache::new(&config());
        let id = key("/api/data?x=1");
        assert!(matches!(cache.lookup(&id), Lookup::Miss));

        assert!(cache.store(&id, ok()));
        assert!(matches!(cache.lookup(&id), Lookup::Fresh(_)));

        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert!(matches!(cache.lookup(&id), Lookup::Stale(_)));
        assert_eq!(cache.len(), 1, "stale row still occupies capacity");

        tokio::time::advance(Duration::from_millis(2_000)).await;
        assert!(matches!(cache.lookup(&id), Lookup::Miss));
        assert!(cache.is_empty(), "expired row is dropped, not lingered");
    }

    #[tokio::test(start_paused = true)]
    async fn revalidation_claim_is_single_and_releasable() {
        let cache = Cache::new(&config());
        let id = key("/api/data");
        cache.store(&id, ok());
        tokio::time::advance(Duration::from_millis(1_500)).await;

        assert!(matches!(cache.lookup(&id), Lookup::Stale(_)));
        assert!(cache.claim_revalidation(&id), "first claim wins");
        assert!(
            !cache.claim_revalidation(&id),
            "the herd may kick exactly one revalidation"
        );
        cache.finish_revalidation(&id);
        assert!(cache.claim_revalidation(&id), "claim reusable after finish");
        assert_eq!(cache.stats().revalidations, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn origin_max_age_is_capped_by_config_ttl() {
        let cache = Cache::new(&config());
        let id = key("/api/slow");
        // The origin asks for an hour; the micro-cache caps at ttl_ms.
        assert!(cache.store(&id, response(StatusCode::OK, cc("max-age=3600"))));
        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert!(
            !matches!(cache.lookup(&id), Lookup::Fresh(_)),
            "config ttl is a hard ceiling"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn s_maxage_wins_and_zero_refuses_store() {
        let cache = Cache::new(&config());
        let id = key("/api/shared");
        // Shared caches honour s-maxage over max-age; zero means: revalidate.
        let headers = cc("max-age=3600, s-maxage=0");
        assert!(
            !cache.store(&id, response(StatusCode::OK, headers)),
            "s-maxage=0 must not store"
        );
        assert!(matches!(cache.lookup(&id), Lookup::Miss));
    }

    #[tokio::test(start_paused = true)]
    async fn bypass_directives_never_store() {
        let cache = Cache::new(&config());
        let id = key("/api/private");
        for directive in ["no-store", "no-cache", "private"] {
            let stored = cache.store(&id, response(StatusCode::OK, cc(directive)));
            assert!(!stored, "{directive} must prevent storing");
        }
        assert!(matches!(cache.lookup(&id), Lookup::Miss));
        assert_eq!(cache.stats().stores, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn only_get_and_head_streams_are_cacheable() {
        let cache = Cache::new(&config());
        assert!(cache.is_cacheable(&Method::GET));
        assert!(cache.is_cacheable(&Method::HEAD));
        assert!(!cache.is_cacheable(&Method::POST));
        assert!(!cache.is_cacheable(&Method::DELETE));

        let not_ok = response(StatusCode::NOT_FOUND, Vec::new());
        let id = key("/api/missing");
        assert!(
            !cache.store(&id, not_ok),
            "only 200 responses are cacheable"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_cache_is_a_permanent_miss() {
        let off = CacheConfig {
            enabled: false,
            ..config()
        };
        let cache = Cache::new(&off);
        assert!(!cache.is_cacheable(&Method::GET));
        assert!(!cache.store(&key("/api/x"), ok()));
        assert!(matches!(cache.lookup(&key("/api/x")), Lookup::Miss));
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn origin_swr_directive_shrinks_the_stale_window() {
        let cache = Cache::new(&config());
        let id = key("/api/bounce");
        // Config allows 2s of stale serving; the origin allows none.
        let headers = cc("stale-while-revalidate=0");
        assert!(cache.store(&id, response(StatusCode::OK, headers)));
        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert!(matches!(cache.lookup(&id), Lookup::Miss));
        assert!(cache.is_empty(), "no stale window means no lingering row");
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_pressure_evicts_oldest_first() {
        let cache = Cache::new(&config()); // max_entries = 4
        for index in 0..4u32 {
            let id = key(&format!("/api/burst/{index}"));
            assert!(cache.store(&id, ok()));
            tokio::time::advance(Duration::from_millis(50)).await;
        }
        let newest = key("/api/burst/4");
        assert!(cache.store(&newest, ok()), "insert within capacity");
        assert_eq!(cache.len(), 4, "old row stepped aside for the new one");
        assert_eq!(cache.stats().evictions, 1);
        assert!(
            matches!(cache.lookup(&key("/api/burst/0")), Lookup::Miss),
            "the oldest row was the one evicted"
        );
        assert!(matches!(cache.lookup(&newest), Lookup::Fresh(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn stats_track_the_whole_journey() {
        let cache = Cache::new(&config());
        let id = key("/api/journey");

        assert!(matches!(cache.lookup(&id), Lookup::Miss));
        assert!(cache.store(&id, ok()));
        assert!(matches!(cache.lookup(&id), Lookup::Fresh(_)));
        tokio::time::advance(Duration::from_millis(1_200)).await;
        assert!(matches!(cache.lookup(&id), Lookup::Stale(_)));

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.stores, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.stale_hits, 1);
    }

    #[test]
    fn directives_fold_every_cache_control_line() {
        let headers = [
            (
                CACHE_CONTROL,
                HeaderValue::from_static("max-age=5, private"),
            ),
            (
                CACHE_CONTROL,
                HeaderValue::from_static("s-maxage=9, stale-while-revalidate=1"),
            ),
        ];
        let directives = parse_directives(headers.iter().map(|(n, v)| (n, v)));
        assert!(directives.no_store, "private counts as bypass");
        assert_eq!(
            directives.ttl_seconds,
            Some(9),
            "s-maxage beats max-age even across lines"
        );
        assert_eq!(directives.swr_seconds, Some(1));
    }
}

/// Parsed subset of `Cache-Control` this cache cares about.
#[derive(Debug, Default, PartialEq, Eq)]
struct Directives {
    /// `no-store` / `no-cache` / `private` seen anywhere in the header.
    no_store: bool,
    /// Effective freshness in SECONDS (`s-maxage` wins over `max-age`).
    ttl_seconds: Option<u64>,
    /// `stale-while-revalidate` in SECONDS.
    swr_seconds: Option<u64>,
}

/// Folds every `Cache-Control` line of a header list into [`Directives`].
fn parse_directives<'a>(
    headers: impl Iterator<Item = (&'a HeaderName, &'a HeaderValue)>,
) -> Directives {
    let mut out = Directives::default();
    let mut max_age = None;
    let mut s_max_age = None;
    for (name, value) in headers {
        if name != CACHE_CONTROL {
            continue;
        }
        let Ok(text) = value.to_str() else {
            continue;
        };
        for raw in text.split(',') {
            let token = raw.trim().to_ascii_lowercase();
            if matches!(token.as_str(), "no-store" | "no-cache" | "private") {
                out.no_store = true;
                continue;
            }
            let Some((key, value)) = token.split_once('=') else {
                continue;
            };
            let digits = value.trim().trim_matches('"').trim();
            let Ok(seconds) = digits.parse::<u64>() else {
                continue;
            };
            match key.trim() {
                "max-age" => max_age = Some(seconds),
                "s-maxage" => s_max_age = Some(seconds),
                "stale-while-revalidate" => out.swr_seconds = Some(seconds),
                _ => {}
            }
        }
    }
    out.ttl_seconds = s_max_age.or(max_age);
    out
}
