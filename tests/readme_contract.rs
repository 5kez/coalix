//! Documentation contract: the README is an interface. Every metrics
//! family, every `COALIX_*` override, and the edge-feature configuration
//! sections must stay documented there, so prose cannot silently drift
//! from the code.

const README: &str = include_str!("../README.md");

/// Every family the metrics registry renders — mirrors the family list in
/// `src/metrics/mod.rs` tests.
const FAMILIES: [&str; 16] = [
    "coalix_requests_total",
    "coalix_upstream_requests_total",
    "coalix_saved_requests_total",
    "coalix_rate_limited_total",
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

/// Every environment override `src/config.rs` parses.
const OVERRIDES: [&str; 11] = [
    "COALIX_LISTEN",
    "COALIX_UPSTREAM_BASE_URL",
    "COALIX_COALESCING_ENABLED",
    "COALIX_CACHE_ENABLED",
    "COALIX_LOG_LEVEL",
    "COALIX_LOG_FORMAT",
    "COALIX_ACCESS_LOG_ENABLED",
    "COALIX_ACCESS_LOG_FORMAT",
    "COALIX_RATE_LIMIT_ENABLED",
    "COALIX_RATE_LIMIT_RPS",
    "COALIX_RATE_LIMIT_BURST",
];

#[test]
fn every_metrics_family_is_documented() {
    for family in FAMILIES {
        assert!(
            README.contains(family),
            "README does not document the metric family {family}"
        );
    }
}

#[test]
fn every_env_override_is_documented() {
    for variable in OVERRIDES {
        assert!(
            README.contains(variable),
            "README does not document {variable}"
        );
    }
}

#[test]
fn edge_features_keep_their_configuration_and_ops_guidance() {
    for needle in [
        "rate_limit:",
        "access_log:",
        "requests_per_second",
        "max_tracked_clients",
        "Retry-After",
        "429",
        "Production Deployment Guide",
        "coalix.example.yaml",
    ] {
        assert!(
            README.contains(needle),
            "README is missing {needle:?} — edge features must stay documented"
        );
    }
}
