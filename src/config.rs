//! Configuration model for Coalix.
//!
//! Coalix is **zero-config**: [`Config::default`] is a complete, working
//! configuration that already coalesces read-only traffic toward a local
//! upstream. A YAML document may override any subset of fields, and a small
//! set of `COALIX_*` environment variables may override the file again.
//!
//! Loading order (see [`Config::load`]):
//!
//! 1. built-in defaults *or* the parsed YAML document,
//! 2. `COALIX_*` environment overrides,
//! 3. semantic validation ([`Config::validate`]).
//!
//! ## Validation rules
//!
//! * positive counters and timeouts (`>0`),
//! * parsable `http`/`https` base URL that carries a host,
//! * non-empty method whitelist limited to known HTTP verbs,
//! * strictly lowercase, non-empty `key_headers`,
//! * absolute route paths with no duplicate `(matcher, path)` pair,
//! * `log_level` in `trace|debug|info|warn|error`,
//! * `metrics_path` starting with `/`, fallback status within `100..=599`.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use http::Uri;
use serde::{Deserialize, Serialize};

/// Errors produced while loading, parsing, or validating configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configuration file could not be read from disk.
    #[error("failed to read config file {path:?}: {source}")]
    Io {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// The document is not valid YAML, or it does not match the schema.
    #[error("failed to parse config: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// A value parses, but is rejected by semantic validation.
    #[error("invalid configuration: {0}")]
    Invalid(String),
    /// A `COALIX_*` environment override was rejected.
    #[error("invalid environment override {key}={value}: {reason}")]
    Env {
        /// Name of the offending environment variable.
        key: String,
        /// Raw value that was rejected.
        value: String,
        /// Human-readable rejection reason.
        reason: String,
    },
}

/// Top-level Coalix configuration.
///
/// Every section is optional in YAML; missing fields fall back to the
/// zero-config defaults implemented by [`Default`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Listener settings.
    pub server: ServerConfig,
    /// Upstream (backend) settings.
    pub upstream: UpstreamConfig,
    /// Global coalescing behaviour.
    pub coalescing: CoalescingConfig,
    /// Ordered rules; first match wins, otherwise `default_coalesce` applies.
    pub routes: Vec<RouteConfig>,
    /// Micro-cache with stale-while-revalidate.
    pub cache: CacheConfig,
    /// Timeouts, circuit breaker, and fallback response.
    pub resilience: ResilienceConfig,
    /// Metrics endpoint and logging.
    pub observability: ObservabilityConfig,
}

/// Listener settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Socket address Coalix binds to.
    pub listen: SocketAddr,
    /// Concurrent-connection cap for the accept loop.
    pub max_connections: usize,
    /// Enables `TCP_NODELAY` on accepted sockets (low-latency proxying).
    pub tcp_nodelay: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 8080)),
            max_connections: 65_536,
            tcp_nodelay: true,
        }
    }
}

/// Upstream (backend) settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UpstreamConfig {
    /// Base URL of the backend origin (e.g. `http://127.0.0.1:3000`).
    pub base_url: String,
    /// TCP connect timeout budget, in milliseconds.
    pub connect_timeout_ms: u64,
    /// Full request timeout budget for the leader, in milliseconds.
    pub request_timeout_ms: u64,
    /// Maximum idle (keep-alive) connections per upstream host.
    pub max_idle_per_host: usize,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:3000".to_owned(),
            connect_timeout_ms: 1_000,
            request_timeout_ms: 5_000,
            max_idle_per_host: 128,
        }
    }
}

/// Global coalescing behaviour.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CoalescingConfig {
    /// Master switch for the coalescing engine.
    pub enabled: bool,
    /// Whether requests that match *no* route coalesce by default.
    pub default_coalesce: bool,
    /// Upper bound of waiters parked per in-flight group.
    pub max_inflight_waiters: usize,
    /// Maximum time a waiter may stay parked, in milliseconds.
    pub max_wait_ms: u64,
    /// Re-arm delay after a flight lands, in milliseconds (0 = immediate). A
    /// fresh arrival inside the window joins the tail of the previous flight.
    pub dedup_window_ms: u64,
    /// The ONLY HTTP methods allowed to coalesce (default GET, HEAD).
    /// Mutating verbs (POST, PUT, PATCH, DELETE) are deliberately absent:
    /// a shared slot must never replay or reorder writes.
    pub coalesce_methods: Vec<String>,
    /// Lowercase header names folded into the coalescing key so responses
    /// differing only by these headers never cross (e.g. accept-language);
    /// a header missing from a request collapses to an empty string.
    pub key_headers: Vec<String>,
}

impl Default for CoalescingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_coalesce: true,
            max_inflight_waiters: 4096,
            max_wait_ms: 2000,
            dedup_window_ms: 50,
            coalesce_methods: vec!["GET".to_owned(), "HEAD".to_owned()],
            key_headers: Vec::new(),
        }
    }
}

/// How a route pattern is compared against the request path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PathMatch {
    /// The request path equals the pattern byte for byte.
    #[default]
    Exact,
    /// The request path starts with the pattern (directory-style).
    Prefix,
}

/// One entry of routes; rules run top to bottom and the FIRST match wins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RouteConfig {
    /// Human-readable name echoed in logs and metrics labels.
    pub name: String,
    /// Absolute path that activates this rule.
    pub path: String,
    /// How path is compared against the request URI.
    pub matcher: PathMatch,
    /// Overrides coalescing.default_coalesce for matching requests.
    pub coalesce: bool,
}

impl Default for RouteConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            path: "/".to_owned(),
            matcher: PathMatch::Exact,
            coalesce: true,
        }
    }
}

/// Micro-cache: a tiny TTL in front of the origin (engine lands in Phase 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Master switch for the micro-cache.
    pub enabled: bool,
    /// Freshness window in milliseconds.
    pub ttl_ms: u64,
    /// How long a stale entry may be served while a refresh runs in the
    /// background (stale-while-revalidate), in milliseconds.
    pub stale_while_revalidate_ms: u64,
    /// Upper bound of cached responses; oldest entries are evicted first.
    pub max_entries: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_ms: 2000,
            stale_while_revalidate_ms: 30_000,
            max_entries: 10_000,
        }
    }
}

/// Resilience: time budgets, circuit breaker, and fallback response.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResilienceConfig {
    /// Rolling-failure detection in front of the upstream.
    pub circuit_breaker: CircuitBreakerConfig,
    /// Served when the breaker is open or the upstream times out.
    pub fallback: FallbackConfig,
}

/// Circuit breaker: failures inside the rolling window open the circuit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    /// Master switch (false = always closed, breaker sits transparent).
    pub enabled: bool,
    /// Failures inside sampling_window_ms that trip the breaker.
    pub failure_threshold: u32,
    /// Rolling failure-observation window, in milliseconds.
    pub sampling_window_ms: u64,
    /// Cooldown after opening before a single probe is admitted, ms.
    pub open_timeout_ms: u64,
    /// Concurrent probes admitted while half-open; a success closes early,
    /// any failure reopens for another full cooldown.
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 5,
            sampling_window_ms: 10_000,
            open_timeout_ms: 30_000,
            half_open_max_calls: 3,
        }
    }
}

/// The canned response clients get instead of an outage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FallbackConfig {
    /// Master switch; with it off, timeouts surface as gateway errors.
    pub enabled: bool,
    /// Status code to return (100..=599; usually 503).
    pub status_code: u16,
    /// Body returned with status_code.
    pub body: String,
}

impl Default for FallbackConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            status_code: 503,
            body: "upstream unavailable - coalix fallback".to_owned(),
        }
    }
}

/// Metrics endpoint, logging verbosity, and log encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Prom-compatible scrape endpoint, reserved ahead of routing.
    pub metrics_path: String,
    /// Minimum severity that reaches the log stream.
    pub log_level: String,
    /// Encoding of emitted events.
    pub log_format: LogFormat,
    /// Structured access log: one line per finished request.
    pub access_log: AccessLogConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_path: "/metrics".to_owned(),
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            access_log: AccessLogConfig::default(),
        }
    }
}

/// How tracing events are rendered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable single-line records.
    #[default]
    Text,
    /// Structured JSON (one object per line) for log shippers.
    Json,
}

/// Structured access log: one line per finished request, written straight to
/// stdout.
///
/// The access log is independent of `log_level`: when enabled, every finished
/// request produces exactly one line regardless of tracing verbosity. The
/// reserved metrics path is never recorded — a scrape must stay invisible to
/// every observable, logs included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccessLogConfig {
    /// Master switch (one stdout line per request when true).
    pub enabled: bool,
    /// `auto` follows `observability.log_format`, `json` forces one JSON
    /// object per line, `clf` writes NCSA Common Log Format with UTC
    /// timestamps for offline log analyzers (GoAccess, AWStats, …).
    pub format: AccessLogFormat,
}

impl Default for AccessLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            format: AccessLogFormat::Auto,
        }
    }
}

/// Encoding of access-log lines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccessLogFormat {
    /// Follow `observability.log_format`: text line or JSON object.
    #[default]
    Auto,
    /// One JSON object per line, always.
    Json,
    /// NCSA Common Log Format, UTC (`+0000`) timestamps.
    Clf,
}

/// Parses a truthy/falsey COALIX_* override value.
fn parse_bool(key: &str, value: &str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(ConfigError::Env {
            key: key.to_owned(),
            value: value.to_owned(),
            reason: format!("{} is not a boolean (use true/false)", other),
        }),
    }
}

/// Lifecycle: defaults, optional file, environment, then validation.
impl Config {
    /// Loads configuration from an optional YAML file plus COALIX_* env
    /// overrides, then validates the merged result.
    ///
    /// With path = None the zero-config defaults are used; they are complete
    /// and valid on their own: bind 0.0.0.0:8080, forward to
    /// http://127.0.0.1:3000, every read-only GET/HEAD coalesces.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut config = match path {
            Some(path) => {
                let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
                Self::from_yaml(&raw)?
            }
            None => Self::default(),
        };
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// Parses a YAML document; an absent or comment-only document resolves
    /// to the zero-config defaults.
    pub fn from_yaml(raw: &str) -> Result<Self, ConfigError> {
        let meaningful = raw.lines().any(|line| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with('#')
        });
        if !meaningful {
            return Ok(Self::default());
        }
        Ok(serde_yaml::from_str(raw)?)
    }

    /// Renders the effective configuration back to YAML (--print-config).
    pub fn to_yaml(&self) -> Result<String, ConfigError> {
        Ok(serde_yaml::to_string(self)?)
    }

    /// Applies COALIX_* environment overrides on top of the loaded config.
    /// Precedence: defaults first, then file, then environment.
    pub fn apply_env_overrides(&mut self) -> Result<(), ConfigError> {
        self.apply_env_overrides_with(|key| std::env::var(key).ok())
    }

    /// The lookup layer split out for tests: every override flows through the
    /// injected resolver, so precedence and parsing stay unit-testable without
    /// mutating process state.
    fn apply_env_overrides_with<F>(&mut self, resolver: F) -> Result<(), ConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(value) = resolver("COALIX_LISTEN") {
            let addr = value
                .parse::<SocketAddr>()
                .map_err(|reason| ConfigError::Env {
                    key: "COALIX_LISTEN".to_owned(),
                    value: value.clone(),
                    reason: reason.to_string(),
                })?;
            self.server.listen = addr;
        }
        if let Some(value) = resolver("COALIX_UPSTREAM_BASE_URL") {
            self.upstream.base_url = value;
        }
        if let Some(value) = resolver("COALIX_COALESCING_ENABLED") {
            self.coalescing.enabled = parse_bool("COALIX_COALESCING_ENABLED", &value)?;
        }
        if let Some(value) = resolver("COALIX_CACHE_ENABLED") {
            self.cache.enabled = parse_bool("COALIX_CACHE_ENABLED", &value)?;
        }
        if let Some(value) = resolver("COALIX_LOG_LEVEL") {
            self.observability.log_level = value;
        }
        if let Some(value) = resolver("COALIX_LOG_FORMAT") {
            let parsed = match value.as_str() {
                "text" => Some(LogFormat::Text),
                "json" => Some(LogFormat::Json),
                _ => None,
            };
            if let Some(log_format) = parsed {
                self.observability.log_format = log_format;
            } else {
                return Err(ConfigError::Env {
                    key: "COALIX_LOG_FORMAT".to_owned(),
                    value,
                    reason: "expected a log format: text or json".to_owned(),
                });
            }
        }
        if let Some(value) = resolver("COALIX_ACCESS_LOG_ENABLED") {
            self.observability.access_log.enabled =
                parse_bool("COALIX_ACCESS_LOG_ENABLED", &value)?;
        }
        if let Some(value) = resolver("COALIX_ACCESS_LOG_FORMAT") {
            let parsed = match value.as_str() {
                "auto" => Some(AccessLogFormat::Auto),
                "json" => Some(AccessLogFormat::Json),
                "clf" => Some(AccessLogFormat::Clf),
                _ => None,
            };
            if let Some(format) = parsed {
                self.observability.access_log.format = format;
            } else {
                return Err(ConfigError::Env {
                    key: "COALIX_ACCESS_LOG_FORMAT".to_owned(),
                    value,
                    reason: "expected an access log format: auto, json or clf".to_owned(),
                });
            }
        }
        Ok(())
    }
}

impl Config {
    /// Semantic validation beyond serde shape. Guarantees:
    /// * every counter and timeout is greater than 0
    /// * upstream.base_url parses and carries an http or https host
    /// * coalesce_methods is non-empty and lists known HTTP verbs only
    /// * key_headers are non-empty lowercase header tokens
    /// * route paths are absolute, and no matcher plus path pair repeats
    /// * log_level is one of trace, debug, info, warn, error
    /// * metrics_path is absolute; fallback status is within 100..=599
    pub fn validate(&self) -> Result<(), ConfigError> {
        fn positive(value: u64, field: &str) -> Result<(), ConfigError> {
            if value == 0 {
                Err(ConfigError::Invalid(format!(
                    "{} must be greater than 0",
                    field
                )))
            } else {
                Ok(())
            }
        }

        positive(self.server.max_connections as u64, "server.max_connections")?;
        positive(
            self.coalescing.max_inflight_waiters as u64,
            "coalescing.max_inflight_waiters",
        )?;
        positive(self.coalescing.max_wait_ms, "coalescing.max_wait_ms")?;
        positive(
            self.upstream.connect_timeout_ms,
            "upstream.connect_timeout_ms",
        )?;
        positive(
            self.upstream.request_timeout_ms,
            "upstream.request_timeout_ms",
        )?;
        positive(
            self.upstream.max_idle_per_host as u64,
            "upstream.max_idle_per_host",
        )?;
        if self.cache.enabled {
            positive(self.cache.ttl_ms, "cache.ttl_ms")?;
            positive(self.cache.max_entries as u64, "cache.max_entries")?;
        }

        let base = self.upstream.base_url.trim();
        if base.is_empty() {
            return Err(ConfigError::Invalid(
                "upstream.base_url must not be empty".to_owned(),
            ));
        }
        let uri: Uri = base.parse().map_err(|reason| {
            ConfigError::Invalid(format!("upstream.base_url is not a valid URI: {}", reason))
        })?;
        match uri.scheme_str() {
            Some("http") | Some("https") => {}
            scheme => {
                return Err(ConfigError::Invalid(format!(
                    "upstream.base_url must use http or https, found {:?}",
                    scheme
                )));
            }
        }
        if uri.host().is_none() {
            return Err(ConfigError::Invalid(
                "upstream.base_url must include a host".to_owned(),
            ));
        }

        if self.coalescing.coalesce_methods.is_empty() {
            return Err(ConfigError::Invalid(
                "coalescing.coalesce_methods must not be empty".to_owned(),
            ));
        }
        const KNOWN_VERBS: [&str; 9] = [
            "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT",
        ];
        for method in &self.coalescing.coalesce_methods {
            if !KNOWN_VERBS.contains(&method.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "coalescing.coalesce_methods lists unknown HTTP verb {:?}",
                    method
                )));
            }
        }

        for header in &self.coalescing.key_headers {
            let token_ok = !header.is_empty()
                && header
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'-')
                && !header.starts_with('-')
                && !header.ends_with('-');
            if !token_ok {
                return Err(ConfigError::Invalid(format!(
                    "coalescing.key_headers entry {:?} must be a lowercase header token",
                    header
                )));
            }
        }

        let mut seen = HashSet::with_capacity(self.routes.len());
        for (index, route) in self.routes.iter().enumerate() {
            if !route.path.starts_with('/') {
                return Err(ConfigError::Invalid(format!(
                    "routes[{}] path must be absolute: {:?}",
                    index, route.path
                )));
            }
            let key = (format!("{:?}", route.matcher), route.path.clone());
            if !seen.insert(key) {
                return Err(ConfigError::Invalid(format!(
                    "routes[{}] repeats an earlier matcher and path: {:?}",
                    index, route.path
                )));
            }
        }

        match self.observability.log_level.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {}
            level => {
                return Err(ConfigError::Invalid(format!(
                    "observability.log_level must be trace, debug, info, warn or error; found {:?}",
                    level
                )));
            }
        }
        if !self.observability.metrics_path.starts_with('/') {
            return Err(ConfigError::Invalid(format!(
                "observability.metrics_path must be absolute: {:?}",
                self.observability.metrics_path
            )));
        }
        if !(100..=599).contains(&self.resilience.fallback.status_code) {
            return Err(ConfigError::Invalid(format!(
                "resilience.fallback.status_code must be within 100..=599, found {}",
                self.resilience.fallback.status_code
            )));
        }
        Ok(())
    }
}

impl Config {
    /// True when coalescing is globally enabled and the method is whitelisted.
    /// Matching is case-insensitive, so GET and get are equivalent.
    pub fn is_method_coalescable(&self, method: &str) -> bool {
        self.coalescing.enabled
            && self
                .coalescing
                .coalesce_methods
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(method))
    }

    /// The first matching route in file order, or None when no rule matches.
    pub fn find_route(&self, path: &str) -> Option<&RouteConfig> {
        self.routes.iter().find(|route| route.matches(path))
    }

    /// Whether a request with this method and path enters the coalescer.
    /// Evaluates in order: global switch, method whitelist, then the first
    /// matching route, falling back to coalescing.default_coalesce.
    pub fn should_coalesce(&self, method: &str, path: &str) -> bool {
        if !self.is_method_coalescable(method) {
            return false;
        }
        match self.find_route(path) {
            Some(route) => route.coalesce,
            None => self.coalescing.default_coalesce,
        }
    }
}

impl RouteConfig {
    /// Path matching with a segment boundary: an exact pattern compares the
    /// whole path, a prefix pattern matches at a boundary only, so /api covers
    /// /api and /api/x but neither /apiary nor /foo/api.
    pub fn matches(&self, path: &str) -> bool {
        match self.matcher {
            PathMatch::Exact => path == self.path,
            PathMatch::Prefix => path.strip_prefix(self.path.as_str()).is_some_and(|rest| {
                self.path.ends_with('/') || rest.starts_with('/') || rest.is_empty()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses a YAML fragment and validates it; panics on any rejection.
    fn parsed(source: &str) -> Config {
        let config = Config::from_yaml(source).expect("yaml must parse");
        config.validate().expect("config must validate");
        config
    }

    #[test]
    fn default_config_is_valid_and_zero_config() {
        let config = Config::default();
        config.validate().expect("defaults must validate");
        assert_eq!(config.server.listen.port(), 8080);
        assert_eq!(config.upstream.base_url, "http://127.0.0.1:3000");
        assert!(config.coalescing.enabled);
        assert!(config.coalescing.default_coalesce);
        assert!(config.is_method_coalescable("GET"));
        assert!(config.is_method_coalescable("head"));
        assert!(!config.is_method_coalescable("POST"));
        assert!(config.observability.access_log.enabled);
        assert_eq!(
            config.observability.access_log.format,
            AccessLogFormat::Auto
        );
    }

    #[test]
    fn example_yaml_parses_and_validates() {
        let example = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join("coalix.example.yaml");
        let raw = std::fs::read_to_string(&example).expect("example yaml must exist");
        let config = Config::from_yaml(&raw).expect("example yaml must parse");
        config.validate().expect("example yaml must validate");
        assert_eq!(config.routes.len(), 3);
        assert!(config.should_coalesce("GET", "/api/products/42"));
        assert!(!config.should_coalesce("GET", "/api/checkout"));
        assert!(!config.should_coalesce("POST", "/api/products/42"));

        let empty = Config::from_yaml("").expect("empty document");
        assert_eq!(empty, Config::default());
    }

    #[test]
    fn file_overrides_only_the_named_fields() {
        let config = parsed("server: {listen: 127.0.0.1:9999}");
        assert_eq!(config.server.listen.port(), 9999);
        assert_eq!(
            config.server.max_connections,
            ServerConfig::default().max_connections
        );
        assert_eq!(config.upstream, UpstreamConfig::default());
    }

    #[test]
    fn method_whitelist_is_get_head_by_default() {
        let mut config = Config::default();
        assert!(config.is_method_coalescable("GET"));
        assert!(config.is_method_coalescable("HEAD"));
        assert!(!config.is_method_coalescable("PATCH"));

        config.coalescing.coalesce_methods = vec!["GET".to_owned()];
        assert!(config.is_method_coalescable("GET"));
        assert!(!config.is_method_coalescable("HEAD"));
    }

    #[test]
    fn routes_are_first_match_wins() {
        let config = parsed(
            "routes: [{name: search, path: /api/search, matcher: prefix, coalesce: false}, {name: api, path: /api, matcher: prefix, coalesce: true}]",
        );
        let search = config.find_route("/api/search/items");
        assert!(search.is_some(), "earlier prefix must win");
        assert_eq!(search.map(|route| route.name.as_str()), Some("search"));
        assert!(!config.should_coalesce("GET", "/api/search/items"));
        assert!(config.should_coalesce("GET", "/api/orders"));
        assert!(config.should_coalesce("GET", "/checkout"));
        assert_eq!(config.find_route("/checkout"), None);
    }

    #[test]
    fn prefix_match_requires_a_segment_boundary() {
        let config = parsed(
            "routes: [{name: api, path: /api, matcher: prefix, coalesce: true}, {name: exact, path: /v1, matcher: exact, coalesce: true}]",
        );
        let prefix = config.find_route("/api").expect("bare prefix matches");
        assert_eq!(prefix.name, "api");
        assert!(config.find_route("/api/orders/9").is_some());
        assert!(config.find_route("/apiary").is_none());
        assert!(config.find_route("/x/api").is_none());
        assert!(config.find_route("/v1/extra").is_none());
        assert!(config.find_route("/v1").is_some());
    }

    #[test]
    fn env_overrides_are_applied() {
        let mut config = Config::default();
        config
            .apply_env_overrides_with(|key| match key {
                "COALIX_LISTEN" => Some("127.0.0.1:9090".to_owned()),
                "COALIX_UPSTREAM_BASE_URL" => Some("http://backend.internal:3001".to_owned()),
                "COALIX_COALESCING_ENABLED" => Some("false".to_owned()),
                "COALIX_LOG_LEVEL" => Some("debug".to_owned()),
                _ => None,
            })
            .expect("overrides must parse");

        assert_eq!(
            config.server.listen,
            "127.0.0.1:9090".parse::<SocketAddr>().expect("valid addr")
        );
        assert_eq!(config.upstream.base_url, "http://backend.internal:3001");
        assert!(!config.coalescing.enabled);
        assert_eq!(config.observability.log_level, "debug");
        config.validate().expect("overridden config must validate");
    }

    #[test]
    fn access_log_env_overrides_are_parsed() {
        let mut config = Config::default();
        config
            .apply_env_overrides_with(|key| match key {
                "COALIX_ACCESS_LOG_ENABLED" => Some("false".to_owned()),
                "COALIX_ACCESS_LOG_FORMAT" => Some("clf".to_owned()),
                _ => None,
            })
            .expect("overrides must parse");
        assert!(!config.observability.access_log.enabled);
        assert_eq!(config.observability.access_log.format, AccessLogFormat::Clf);

        let mut config = Config::default();
        let error = config
            .apply_env_overrides_with(|key| match key {
                "COALIX_ACCESS_LOG_FORMAT" => Some("combined".to_owned()),
                _ => None,
            })
            .expect_err("combined is not an access log format");
        match error {
            ConfigError::Env { key, .. } => assert_eq!(key, "COALIX_ACCESS_LOG_FORMAT"),
            other => panic!("expected the Env variant, found {other:?}"),
        }
    }

    #[test]
    fn invalid_env_override_is_rejected() {
        let mut config = Config::default();
        let error = config
            .apply_env_overrides_with(|key| match key {
                "COALIX_COALESCING_ENABLED" => Some("maybe".to_owned()),
                _ => None,
            })
            .expect_err("maybe is not a boolean");
        match error {
            ConfigError::Env { key, value, .. } => {
                assert_eq!(key, "COALIX_COALESCING_ENABLED");
                assert_eq!(value, "maybe");
            }
            other => panic!("expected the Env variant, found {:?}", other),
        }
    }

    #[test]
    fn invalid_values_fail_validation() {
        let mut config = Config::default();
        config.coalescing.max_wait_ms = 0;
        let message = config
            .validate()
            .expect_err("zero max_wait_ms is invalid")
            .to_string();
        assert!(message.contains("max_wait_ms"));

        let mut config = Config::default();
        config.observability.log_level = "verbose".to_owned();
        let message = config
            .validate()
            .expect_err("unknown log level")
            .to_string();
        assert!(message.contains("log_level"));

        let mut config = Config::default();
        config.resilience.fallback.status_code = 99;
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config.upstream.base_url = "ftp://files.internal".to_owned();
        assert!(config.validate().is_err());

        let config = Config {
            routes: vec![
                RouteConfig {
                    name: "first".to_owned(),
                    path: "/x".to_owned(),
                    matcher: PathMatch::Exact,
                    coalesce: true,
                },
                RouteConfig {
                    name: "second".to_owned(),
                    path: "/x".to_owned(),
                    matcher: PathMatch::Exact,
                    coalesce: true,
                },
            ],
            ..Default::default()
        };
        assert!(
            config.validate().is_err(),
            "duplicate matcher and path pairs must be rejected"
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(Config::from_yaml("serverz: {}").is_err());
        assert!(Config::from_yaml("server: {nodelay: true}").is_err());
        assert!(Config::from_yaml("coalescing: {enabledz: true}").is_err());
    }

    #[test]
    fn config_round_trips_through_yaml() {
        let original = Config::default();
        let yaml = original.to_yaml().expect("serialization");
        let reparsed = Config::from_yaml(&yaml).expect("deserialization");
        assert_eq!(original, reparsed);
        reparsed.validate().expect("round trip must stay valid");
    }
}
