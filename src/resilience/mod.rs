//! # Resilience: circuit breaker and fallback logic
//!
//! Two cooperating defences in front of a fragile upstream:
//!
//! * [`CircuitBreaker`] — a rolling-window breaker wired to the config's
//!   exact contract ([`CircuitBreakerConfig`]): `failure_threshold`
//!   failures inside `sampling_window_ms` trip it (phase
//!   [`BreakerPhase::Open`]); after `open_timeout_ms` a bounded number of
//!   [`Permit`]s (`half_open_max_calls`) probe the origin — one success
//!   closes early, any failure re-opens for another full cooldown. While
//!   denied, the handler short-circuits to the fallback without dialing;
//! * [`fallback_response`] — the canned reply for every unavailable path
//!   (breaker denied, transport failure, budget exhausted), built from
//!   [`FallbackConfig`]: configured status and body, or a plain `502` when
//!   the configured fallback is switched off — see [`crate::proxy`].
//!
//! Every granted `admit` hands back a [`Permit`]; recording happens
//! exactly once — explicitly, or on `Drop` when the attempt is abandoned
//! (cancelled future, panic), so half-open slots can never leak.
//!
//! A disabled breaker (`resilience.circuit_breaker.enabled = false`) makes
//! every admit succeed and every record a no-op: the engine sits
//! transparent in the request path.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use hyper::Response;
use thiserror::Error;
use tokio::time::Instant;

use crate::config::{CircuitBreakerConfig, ResilienceConfig};
use crate::proxy::{failure_response, OutBody};

/// Why one call did not reach the upstream: the circuit is open or its
/// probe slots are saturated.
#[derive(Debug, Clone, Copy, Error)]
#[error("circuit denied the call ({remaining_ms} ms until a probe is admitted)")]
pub struct Denied {
    /// Cooldown left before the next probe, or `0` while half-open slots
    /// are saturated.
    pub remaining_ms: u64,
}

/// Snapshot of the breaker's state machine — for logs, tests, metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerPhase {
    /// Passing traffic; failures accumulate in the rolling window.
    Closed,
    /// Tripped: denied until the cooldown expires.
    Open,
    /// Cooldown over: up to `half_open_max_calls` probes ride through.
    HalfOpen,
}

/// Internal machine. All transitions run under one mutex shared by
/// `admit`, the record paths, and the reporters.
#[derive(Debug)]
enum State {
    /// Passing traffic; failure timestamps inside the sampling window.
    Closed { failures: VecDeque<Instant> },
    /// Tripped at `until`; admission is refused until then.
    Open { until: Instant },
    /// Cooldown expired; `in_flight` probes currently ride.
    HalfOpen { in_flight: u32 },
}

/// The rolling-window circuit breaker in front of the upstream.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<State>,
}

impl CircuitBreaker {
    /// Builds the breaker from `resilience.circuit_breaker.*`.
    pub fn new(config: &CircuitBreakerConfig) -> Self {
        Self {
            config: config.clone(),
            state: Mutex::new(State::Closed {
                failures: VecDeque::new(),
            }),
        }
    }

    /// A poisoned mutex only signals a panic inside an earlier critical
    /// section; the guard itself is still valid, so recover it rather than
    /// poisoning every caller's path.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Admission point for upstream calls. On success the caller owns a
    /// [`Permit`] and owes exactly one terminal record; on denial nothing
    /// was allotted and the caller must serve the fallback.
    pub fn admit(&self) -> Result<Permit<'_>, Denied> {
        if !self.config.enabled {
            return Ok(Permit {
                breaker: self,
                settled: false,
            });
        }
        let mut state = self.lock();
        match &mut *state {
            State::Closed { failures } => {
                Self::prune(failures, Instant::now(), self.window());
                Ok(Permit {
                    breaker: self,
                    settled: false,
                })
            }
            State::Open { until } => {
                let now = Instant::now();
                if now >= *until {
                    *state = State::HalfOpen { in_flight: 1 };
                    Ok(Permit {
                        breaker: self,
                        settled: false,
                    })
                } else {
                    Err(Denied {
                        remaining_ms: until.duration_since(now).as_millis() as u64,
                    })
                }
            }
            State::HalfOpen { in_flight } => {
                if *in_flight >= self.probe_slots() {
                    Err(Denied { remaining_ms: 0 })
                } else {
                    *in_flight += 1;
                    Ok(Permit {
                        breaker: self,
                        settled: false,
                    })
                }
            }
        }
    }

    /// Readable probe-slot bound, clamped so `half_open_max_calls = 0`
    /// still admits one probe (otherwise the breaker could never heal).
    fn probe_slots(&self) -> u32 {
        self.config.half_open_max_calls.max(1)
    }

    /// Trip threshold; `0` would mean "open instantly", so clamp it up.
    fn threshold(&self) -> u64 {
        u64::from(self.config.failure_threshold.max(1))
    }

    /// Rolling window length.
    fn window(&self) -> Duration {
        Duration::from_millis(self.config.sampling_window_ms)
    }

    /// Cooldown duration after a trip.
    fn open_timeout(&self) -> Duration {
        Duration::from_millis(self.config.open_timeout_ms)
    }

    /// Drops timestamps whose window has passed. A zero window prunes
    /// everything, which keeps the breaker permanently closed — the config
    /// equivalent of "never trip".
    fn prune(failures: &mut VecDeque<Instant>, now: Instant, window: Duration) {
        while let Some(front) = failures.front() {
            if now.duration_since(*front) >= window {
                failures.pop_front();
            } else {
                break;
            }
        }
    }

    /// Current phase of the state machine — for logs, tests, and metrics.
    pub fn state_name(&self) -> BreakerPhase {
        match &*self.lock() {
            State::Closed { .. } => BreakerPhase::Closed,
            State::Open { .. } => BreakerPhase::Open,
            State::HalfOpen { .. } => BreakerPhase::HalfOpen,
        }
    }

    /// True while the master switch is on.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

impl CircuitBreaker {
    /// Records a successful upstream attempt. In [`BreakerPhase::Closed`]
    /// it clears the rolling window (a success resets the streak); in
    /// [`BreakerPhase::HalfOpen`] it closes the breaker immediately — the
    /// recovery signal. A straggler's success while already open never
    /// closes it: that transition was decided by a probe failure.
    pub fn record_success(&self) {
        if !self.config.enabled {
            return;
        }
        let mut state = self.lock();
        if matches!(&*state, State::Open { .. }) {
            return;
        }
        if matches!(&*state, State::HalfOpen { .. }) {
            *state = State::Closed {
                failures: VecDeque::new(),
            };
        } else if let State::Closed { failures } = &mut *state {
            failures.clear();
        }
    }

    /// Records a failed upstream attempt: in [`BreakerPhase::Closed`] a
    /// timestamp joins the rolling window and trips the breaker once it
    /// holds `failure_threshold` entries; in [`BreakerPhase::HalfOpen`]
    /// the probe failed and the circuit re-opens for a full cooldown.
    /// Already-open calls are ignored — the cooldown is not extended.
    pub fn record_failure(&self) {
        if !self.config.enabled {
            return;
        }
        let now = Instant::now();
        let open_for = self.open_timeout();
        let mut state = self.lock();
        if matches!(&*state, State::Closed { .. }) {
            let tripped = if let State::Closed { failures } = &mut *state {
                Self::prune(failures, now, self.window());
                failures.push_back(now);
                u64::from(failures.len() as u32) >= self.threshold()
            } else {
                false
            };
            if tripped {
                *state = State::Open {
                    until: now + open_for,
                };
            }
        } else if matches!(&*state, State::HalfOpen { .. }) {
            *state = State::Open {
                until: now + open_for,
            };
        }
    }
}

/// Proof that one upstream call was admitted. Exactly one terminal record
/// runs: whatever method the caller invokes, or — if the permit is dropped
/// unsettled (cancelled future, panic, early return) — `record_failure`,
/// so half-open slots always return to circulation.
pub struct Permit<'a> {
    breaker: &'a CircuitBreaker,
    settled: bool,
}

impl Permit<'_> {
    /// The upstream answered sanely (status below 500).
    pub fn record_success(mut self) {
        self.settled = true;
        self.breaker.record_success();
    }

    /// The upstream failed its attempt (transport error, timeout, 5xx).
    pub fn record_failure(mut self) {
        self.settled = true;
        self.breaker.record_failure();
    }

    /// Terminal accounting keyed on the upstream status: server errors and
    /// transport losses count as failures, everything below counts as
    /// healthy. Consumes the permit.
    pub fn record_status(self, status: hyper::StatusCode) {
        if status.is_server_error() {
            self.record_failure();
        } else {
            self.record_success();
        }
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.breaker.record_failure();
        }
    }
}

impl std::fmt::Debug for Permit<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit")
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

/// The reply used whenever the upstream path is unavailable — breaker
/// denied, transport failure, or budget exhaustion. Delegates to the
/// proxy's `failure_response` shaping: configured status and body when the
/// fallback is enabled, plain `502` otherwise.
pub fn fallback_response(config: &ResilienceConfig) -> Response<OutBody> {
    failure_response(
        config.fallback.status_code,
        &config.fallback.body,
        config.fallback.enabled,
    )
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use hyper::StatusCode;

    use super::*;
    use crate::config::FallbackConfig;

    fn config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 3,
            sampling_window_ms: 1_000,
            open_timeout_ms: 2_000,
            half_open_max_calls: 1,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn closed_until_the_threshold_trips_it() {
        let breaker = CircuitBreaker::new(&config());
        breaker.admit().expect("closed").record_failure();
        breaker.admit().expect("closed").record_failure();
        assert_eq!(breaker.state_name(), BreakerPhase::Closed);

        breaker.admit().expect("closed").record_failure();
        assert_eq!(breaker.state_name(), BreakerPhase::Open);
        let denied = breaker.admit().expect_err("open must deny");
        assert_eq!(denied.remaining_ms, 2_000, "full cooldown left");
        assert!(denied.to_string().contains("circuit denied"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_success_resets_the_rolling_streak() {
        let breaker = CircuitBreaker::new(&config());
        for _ in 0..2 {
            breaker.admit().expect("closed").record_failure();
        }
        breaker.admit().expect("closed").record_success();
        breaker.admit().expect("closed").record_failure();
        assert_eq!(
            breaker.state_name(),
            BreakerPhase::Closed,
            "one success wiped the streak"
        );
        // Threshold is three: two more failures are needed to trip again.
        breaker.admit().expect("closed").record_failure();
        breaker.admit().expect("closed").record_failure();
        assert_eq!(breaker.state_name(), BreakerPhase::Open);
    }

    #[tokio::test(start_paused = true)]
    async fn the_window_forgets_ancient_failures() {
        let breaker = CircuitBreaker::new(&config());
        breaker.admit().expect("closed").record_failure();
        // The sampling window is 1s: this jumps past the first timestamp.
        tokio::time::advance(Duration::from_millis(1_100)).await;

        breaker.admit().expect("closed").record_failure();
        assert_eq!(
            breaker.state_name(),
            BreakerPhase::Closed,
            "the aged-out failure no longer counts"
        );
        breaker.admit().expect("closed").record_failure();
        breaker.admit().expect("closed").record_failure();
        assert_eq!(breaker.state_name(), BreakerPhase::Open);
    }

    #[tokio::test(start_paused = true)]
    async fn cooldown_ends_in_one_bounded_probe() {
        let breaker = CircuitBreaker::new(&config()); // half_open_max_calls = 1
        for _ in 0..3 {
            breaker.admit().expect("closed").record_failure();
        }
        assert_eq!(breaker.state_name(), BreakerPhase::Open);

        tokio::time::advance(Duration::from_millis(2_000)).await;
        let probe = breaker.admit().expect("cooldown grants one probe");
        assert_eq!(breaker.state_name(), BreakerPhase::HalfOpen);
        let denied = breaker.admit().expect_err("only one probe at a time");
        assert_eq!(denied.remaining_ms, 0, "probe slots saturated");

        probe.record_success();
        assert_eq!(
            breaker.state_name(),
            BreakerPhase::Closed,
            "the first healthy probe closes the circuit"
        );
        breaker
            .admit()
            .expect("traffic flows again")
            .record_success();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_probe_reopens_a_full_cooldown() {
        let breaker = CircuitBreaker::new(&config());
        for _ in 0..3 {
            breaker.admit().expect("closed").record_failure();
        }
        tokio::time::advance(Duration::from_millis(2_000)).await;
        let probe = breaker.admit().expect("probe admitted");
        tokio::time::advance(Duration::from_millis(100)).await;

        probe.record_failure();
        assert_eq!(breaker.state_name(), BreakerPhase::Open);
        let denied = breaker.admit().expect_err("a failed probe reopens");
        assert!(
            denied.remaining_ms >= 1_900,
            "a fresh full cooldown, not the residual one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_abandoned_permit_counts_as_a_failure() {
        let breaker = CircuitBreaker::new(&config());
        for _ in 0..2 {
            drop(breaker.admit().expect("closed"));
        }
        assert_eq!(breaker.state_name(), BreakerPhase::Closed);

        // Same call as a cancelled or panicking attempt dropping its permit.
        drop(breaker.admit().expect("closed"));
        assert_eq!(
            breaker.state_name(),
            BreakerPhase::Open,
            "unsettled permits record failure on drop"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn probe_slots_respect_half_open_max_calls() {
        let cfg = CircuitBreakerConfig {
            half_open_max_calls: 2,
            ..config()
        };
        let breaker = CircuitBreaker::new(&cfg);
        for _ in 0..3 {
            breaker.admit().expect("closed").record_failure();
        }
        tokio::time::advance(Duration::from_millis(2_000)).await;

        let first = breaker.admit().expect("slot one");
        let second = breaker.admit().expect("slot two");
        assert!(breaker.admit().is_err(), "the third probe waits");

        second.record_success(); // first success closes the circuit early
        assert_eq!(breaker.state_name(), BreakerPhase::Closed);
        first.record_success(); // a straggler settle stays harmless
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_breaker_passes_everything_through() {
        let cfg = CircuitBreakerConfig {
            enabled: false,
            ..config()
        };
        let breaker = CircuitBreaker::new(&cfg);
        assert!(!breaker.is_enabled());
        for _ in 0..10 {
            breaker.admit().expect("transparent").record_failure();
        }
        assert_eq!(breaker.state_name(), BreakerPhase::Closed);
        breaker.admit().expect("still transparent").record_success();
    }

    #[tokio::test(start_paused = true)]
    async fn status_recording_keys_on_server_errors() {
        let breaker = CircuitBreaker::new(&config());
        for _ in 0..3 {
            breaker
                .admit()
                .expect("closed")
                .record_status(StatusCode::BAD_GATEWAY);
        }
        assert_eq!(breaker.state_name(), BreakerPhase::Open);

        let healthy = CircuitBreaker::new(&config());
        for _ in 0..3 {
            healthy
                .admit()
                .expect("closed")
                .record_status(StatusCode::NOT_FOUND);
        }
        assert_eq!(
            healthy.state_name(),
            BreakerPhase::Closed,
            "404 is an application answer, not an upstream failure"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_response_follows_resilience_config() {
        let resilience = ResilienceConfig {
            fallback: FallbackConfig {
                enabled: true,
                status_code: 503,
                body: "custom cooling-off".to_owned(),
            },
            ..ResilienceConfig::default()
        };
        let response = fallback_response(&resilience);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collectable body")
            .to_bytes();
        assert_eq!(&body[..], b"custom cooling-off");

        let plain = fallback_response(&ResilienceConfig {
            fallback: FallbackConfig {
                enabled: false,
                ..FallbackConfig::default()
            },
            ..ResilienceConfig::default()
        });
        assert_eq!(plain.status(), StatusCode::BAD_GATEWAY);
    }
}
