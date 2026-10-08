//! # Single-flight coalescer
//!
//! The heart of Coalix: one `DashMap<FlightKey, Arc<Flight>>` records every
//! airborne upstream call. The first arrival becomes the **leader** and must
//! publish exactly one terminal outcome through its [`Leader`] handle;
//! everyone else parks as a [`Waiter`] on a `tokio::sync::broadcast`
//! receiver and is woken — all at once, no polling — when the flight lands.
//!
//! Guarantees:
//!
//! * **No stranded waiter** — dropping a leader broadcasts a failure, so
//!   parked requests are always released (they then serve the fallback or
//!   fetch on their own).
//! * **No missed broadcast** — fixture/state transitions happen under one
//!   flight mutex shared with `join`, so subscribe and settle are atomic
//!   with respect to each other.
//! * **No unbounded parking** — each waiter carries a `max_wait_ms`
//!   deadline; each flight caps parked waiters at `max_inflight_waiters`
//!   (overflow requests bypass and execute directly — coalescing degrades,
//!   it never blocks).
//! * **Tail joining** — inside `dedup_window_ms` after landing, a fresh
//!   arrival replays the just-landed body (or, on failure, absorbs the same
//!   error instead of stampeding a recovering upstream).

pub mod key;

pub use key::FlightKey;

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use hyper::{
    header::{HeaderName, HeaderValue},
    StatusCode,
};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::time::Instant;

use crate::config::CoalescingConfig;

/// Capacity of each flight's broadcast channel: one terminal event plus
/// headroom. A subscriber only ever awaits the terminal event, so it can
/// never lag in practice; the slack keeps `Lagged` as a defensive path.
const BROADCAST_CAPACITY: usize = 4;

/// The response every parked waiter is replayed after a successful landing.
#[derive(Debug, Clone)]
pub struct SharedResponse {
    /// Status of the upstream reply.
    pub status: StatusCode,
    /// End-to-end headers as captured from the upstream. The proxy strips
    /// hop-by-hop headers before the flight lands, so replays are already
    /// legal to serialize.
    pub headers: Vec<(HeaderName, HeaderValue)>,
    /// Complete body. `Bytes` clones are reference-counted, so fanning out
    /// to thousands of waiters never copies payload bytes.
    pub body: Bytes,
}

/// The one message a flight ever publishes. `Clone` is required by
/// `broadcast`; payload clones are cheap (`Arc` / ref-counted bytes).
#[derive(Debug, Clone)]
enum FlightEvent {
    /// Leader landed; every waiter may answer its client.
    Complete(Arc<SharedResponse>),
    /// Leader's upstream call failed; waiters must not open more calls.
    Failed(String),
}

/// Flight lifecycle. Every transition runs under the flight mutex, so
/// `join` can either observe the outcome or subscribe — never a gap in
/// between where an event would be missed.
enum State {
    /// Airborne: exactly one terminal event will be published.
    Pending {
        tx: broadcast::Sender<FlightEvent>,
        parked: usize,
    },
    /// Landed within the dedup window: the tail of this flight is joinable.
    Done {
        response: Arc<SharedResponse>,
        at: Instant,
    },
    /// Failed within the dedup window: tail joiners absorb the same failure.
    Failed { message: String, at: Instant },
}

/// One in-flight upstream call, shared by its leader and every waiter.
pub struct Flight {
    state: Mutex<State>,
    #[allow(dead_code)] // retained for diagnostics/metrics labelling
    born: Instant,
}

impl Flight {
    fn new() -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            state: Mutex::new(State::Pending { tx, parked: 0 }),
            born: Instant::now(),
        }
    }

    /// Locks the flight state, recovering cleanly from poisoning so one
    /// panicking task can never wedge the flight map. Critical sections
    /// only move already-constructed values (broadcast sends ignore
    /// receiver-free errors), so they cannot re-panic.
    fn lock(&self) -> MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// Everything the `Leader` handle needs to land its flight: the map slot,
/// the flight itself, and the shared engine for deferred eviction.
struct Handle {
    engine: Arc<Engine>,
    key: FlightKey,
    flight: Arc<Flight>,
}

/// Engine-shared internals: the flight map plus tuned policy.
struct Engine {
    flights: DashMap<FlightKey, Arc<Flight>>,
    max_parked: usize,
    max_wait: Duration,
    dedup: Duration,
}

/// The single-flight coordinator — a cheap, clonable handle onto the
/// shared flight map.
#[derive(Clone)]
pub struct Coalescer {
    engine: Arc<Engine>,
}

/// What `join` decided for one arriving request.
pub enum Join {
    /// First arrival for this key. The caller **must** publish an outcome
    /// with [`Leader::complete`] / [`Leader::fail`]; dropping the handle
    /// broadcasts a failure so waiters are always released.
    Leader(Leader),
    /// Parked behind the leader until a terminal event or `max_wait_ms`.
    Waiter(Waiter),
    /// Dedup-window tail-join on a landed flight: reply with this body now.
    Ready(Arc<SharedResponse>),
    /// Dedup-window tail-join on a failed flight: serve the same failure
    /// rather than re-storming an unhealthy upstream.
    Failed(String),
    /// `max_inflight_waiters` reached (or the landed state aged out):
    /// execute this request directly — coalescing degrades to plain
    /// proxying and never blocks a client.
    Bypass,
}

/// Why a parked waiter did not receive a [`SharedResponse`].
#[derive(Debug, Error)]
pub enum WaitError {
    /// Parked longer than `max_wait_ms`: the caller should fetch on its own.
    #[error("waiter exceeded max_wait_ms")]
    Timeout,
    /// The leader reported an upstream failure — do not retry; serve the
    /// configured fallback instead of opening another upstream call.
    #[error("leader failed upstream: {0}")]
    Failed(String),
    /// Broadcast receiver fell behind (defensive path; a subscriber only
    /// awaits a single terminal event against a capacity of 4).
    #[error("broadcast channel lagged by {0} event(s)")]
    Lagged(u64),
    /// The flight closed without publishing an outcome.
    #[error("flight closed before an outcome was published")]
    Closed,
}

impl WaitError {
    /// True when the upstream itself failed and the caller should serve the
    /// configured fallback instead of issuing a fresh upstream call.
    pub fn is_upstream_failure(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
}

impl Coalescer {
    /// Builds a coordinator from the `coalescing` configuration block.
    pub fn new(config: &CoalescingConfig) -> Self {
        Self {
            engine: Arc::new(Engine {
                flights: DashMap::new(),
                max_parked: config.max_inflight_waiters,
                max_wait: Duration::from_millis(config.max_wait_ms),
                dedup: Duration::from_millis(config.dedup_window_ms),
            }),
        }
    }

    /// Admits one request to the flight for `key`.
    ///
    /// Phase 1 takes (or opens) the map slot; the shard guard is released
    /// before the flight lock is taken, giving one global order
    /// (slot-then-flight) that eviction also obeys — no lock cycles.
    pub fn join(&self, key: FlightKey) -> Join {
        let flight = match self.engine.flights.entry(key.clone()) {
            Entry::Vacant(vacant) => {
                let flight = Arc::new(Flight::new());
                vacant.insert(flight.clone());
                return Join::Leader(Leader {
                    handle: Some(Handle {
                        engine: self.engine.clone(),
                        key,
                        flight,
                    }),
                });
            }
            Entry::Occupied(occupied) => occupied.get().clone(),
        };

        // Phase 2: decide under the flight lock so a terminal event can
        // never slip between this decision and the broadcast subscription.
        let mut state = flight.lock();
        match &mut *state {
            State::Pending { tx, parked } => {
                if *parked >= self.engine.max_parked {
                    return Join::Bypass;
                }
                let rx = tx.subscribe();
                *parked += 1;
                let deadline = Instant::now() + self.engine.max_wait;
                drop(state);
                Join::Waiter(Waiter { rx, deadline })
            }
            State::Done { response, at } => {
                if at.elapsed() < self.engine.dedup {
                    Join::Ready(response.clone())
                } else {
                    drop(state);
                    Join::Bypass
                }
            }
            State::Failed { message, at } => {
                if at.elapsed() < self.engine.dedup {
                    Join::Failed(message.clone())
                } else {
                    drop(state);
                    Join::Bypass
                }
            }
        }
    }

    /// Number of flights currently tracked — airborne plus those still
    /// inside their dedup window. Surfaced for tests and metrics (phase 4).
    pub fn flight_count(&self) -> usize {
        self.engine.flights.len()
    }

    /// Publishes a terminal outcome: broadcast first (waiters subscribed
    /// while `Pending` receive it), then flip state, then schedule removal.
    /// All of it under the flight lock, which `join` also takes — a late
    /// joiner therefore either subscribes before the send or reads the
    /// landed state; it can never observe a silent pending flight.
    fn settle(handle: &Handle, state: State, event: FlightEvent) {
        let mut guard = handle.flight.lock();
        let tx = match &*guard {
            State::Pending { tx, .. } => tx.clone(),
            // Only a pending flight may land; duplicate settlements are no-ops.
            State::Done { .. } | State::Failed { .. } => return,
        };
        let _ = tx.send(event);
        *guard = state;
        drop(guard);
        Self::schedule_eviction(
            handle.engine.clone(),
            handle.key.clone(),
            handle.flight.clone(),
        );
    }

    /// Drops the flight map slot once the dedup window has passed, so the
    /// next arrival starts a fresh flight. Eviction never touches a flight
    /// that is airborne again (identity-checked, plus a `Pending` guard).
    fn schedule_eviction(engine: Arc<Engine>, key: FlightKey, flight: Arc<Flight>) {
        let delay = engine.dedup;
        let eviction = move || {
            engine.flights.remove_if(&key, |_, existing| {
                Arc::ptr_eq(existing, &flight) && !matches!(*existing.lock(), State::Pending { .. })
            });
        };
        if delay.is_zero() {
            eviction();
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    tokio::time::sleep(delay).await;
                    eviction();
                });
            }
            // Without a runtime (sync tooling/tests) fall back to an
            // immediate slot drop: correctness never relies on the window.
            Err(_) => eviction(),
        }
    }
}

/// Handle held by the first arrival of a flight.
///
/// Exactly one of [`complete`](Leader::complete) /
/// [`fail`](Leader::fail) should be called — both consume the handle. If
/// the handle is dropped first (client vanished, task cancelled), `Drop`
/// broadcasts a failure so no waiter is ever stranded.
pub struct Leader {
    handle: Option<Handle>,
}

impl Leader {
    /// Lands the flight with the upstream outcome and returns the shared
    /// response, letting the leader's own client be served from exactly
    /// the same value its waiters receive.
    pub fn complete(
        mut self,
        status: StatusCode,
        headers: Vec<(HeaderName, HeaderValue)>,
        body: Bytes,
    ) -> Arc<SharedResponse> {
        let response = Arc::new(SharedResponse {
            status,
            headers,
            body,
        });
        if let Some(handle) = self.handle.take() {
            Coalescer::settle(
                &handle,
                State::Done {
                    response: response.clone(),
                    at: Instant::now(),
                },
                FlightEvent::Complete(response.clone()),
            );
        }
        response
    }

    /// Lands the flight as failed; every waiter observes `message` as a
    /// [`WaitError::Failed`] and serves the fallback.
    pub fn fail(mut self, message: impl Into<String>) {
        let message = message.into();
        if let Some(handle) = self.handle.take() {
            Coalescer::settle(
                &handle,
                State::Failed {
                    message: message.clone(),
                    at: Instant::now(),
                },
                FlightEvent::Failed(message),
            );
        }
    }
}

impl Drop for Leader {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let message = "leader dropped before completion".to_owned();
            Coalescer::settle(
                &handle,
                State::Failed {
                    message: message.clone(),
                    at: Instant::now(),
                },
                FlightEvent::Failed(message),
            );
        }
    }
}

/// A request parked behind its flight's leader.
pub struct Waiter {
    rx: Receiver<FlightEvent>,
    deadline: Instant,
}

impl Waiter {
    /// Resolves when the flight lands, fails, or the park budget expires.
    ///
    /// Timeout and channel errors are **not** upstream failures: the caller
    /// then performs its own upstream call — self-healing, never worse than
    /// a proxy with coalescing switched off.
    pub async fn wait(mut self) -> Result<Arc<SharedResponse>, WaitError> {
        match tokio::time::timeout_at(self.deadline, self.rx.recv()).await {
            Ok(Ok(FlightEvent::Complete(response))) => Ok(response),
            Ok(Ok(FlightEvent::Failed(message))) => Err(WaitError::Failed(message)),
            Ok(Err(RecvError::Lagged(n))) => Err(WaitError::Lagged(n)),
            Ok(Err(RecvError::Closed)) => Err(WaitError::Closed),
            Err(_) => Err(WaitError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use hyper::Method;

    use super::*;

    /// Small, fast policy: 30 ms dedup tail, 500 ms park budget.
    fn config() -> CoalescingConfig {
        CoalescingConfig {
            dedup_window_ms: 30,
            max_wait_ms: 500,
            max_inflight_waiters: 8,
            ..CoalescingConfig::default()
        }
    }

    fn key() -> FlightKey {
        FlightKey::build(
            &Method::GET,
            &"/api/x".parse().expect("uri"),
            &hyper::HeaderMap::new(),
            &[],
        )
    }

    fn lead(c: &Coalescer) -> Leader {
        match c.join(key()) {
            Join::Leader(leader) => leader,
            _ => panic!("first arrival must become the leader"),
        }
    }

    fn park(c: &Coalescer) -> Waiter {
        match c.join(key()) {
            Join::Waiter(waiter) => waiter,
            _ => panic!("second arrival must park as a waiter"),
        }
    }

    fn land(leader: Leader) -> Arc<SharedResponse> {
        leader.complete(StatusCode::OK, Vec::new(), Bytes::from_static(b"payload"))
    }

    #[tokio::test]
    async fn waiter_receives_the_leader_landing() {
        let c = Coalescer::new(&config());
        let (leader, waiter) = park_setup(&c);
        let shared = land(leader);
        let got = waiter.wait().await.expect("waiter resolves");
        assert!(Arc::ptr_eq(&shared, &got));
        assert_eq!(got.body, Bytes::from_static(b"payload"));
        assert_eq!(got.status, StatusCode::OK);
    }

    #[tokio::test]
    async fn failed_flight_reaches_parked_waiters() {
        let c = Coalescer::new(&config());
        let waiter = park_setup(&c);
        waiter.0.fail("upstream exploded");
        let err = waiter.1.wait().await.expect_err("must fail");
        assert!(err.is_upstream_failure());
        assert!(err.to_string().contains("exploded"));
    }

    /// Leader and waiter in one helper so the leader is parked before the
    /// waiter joins (mirrors real request concurrency).
    fn park_setup(c: &Coalescer) -> (Leader, Waiter) {
        let leader = lead(c);
        let waiter = park(c);
        (leader, waiter)
    }

    #[tokio::test]
    async fn dropped_leader_unblocks_parked_waiters() {
        let c = Coalescer::new(&config());
        let (leader, waiter) = park_setup(&c);
        drop(leader);
        let err = waiter.wait().await.expect_err("must fail");
        assert!(
            err.to_string().contains("dropped before completion"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn tail_join_replays_within_dedup_window() {
        let c = Coalescer::new(&config());
        let shared = land(lead(&c));
        match c.join(key()) {
            Join::Ready(got) => assert!(Arc::ptr_eq(&shared, &got)),
            _ => panic!("fresh arrival inside the window must tail-join"),
        }
        assert_eq!(c.flight_count(), 1);
    }

    #[tokio::test]
    async fn failed_tail_join_absorbs_the_failure() {
        let c = Coalescer::new(&config());
        lead(&c).fail("upstream 502");
        match c.join(key()) {
            Join::Failed(message) => assert_eq!(message, "upstream 502"),
            _ => panic!("failed flight must replay its failure inside the window"),
        }
    }

    #[tokio::test]
    async fn flight_rearms_after_the_dedup_window() {
        let c = Coalescer::new(&config());
        land(lead(&c));
        tokio::time::sleep(Duration::from_millis(90)).await;
        match c.join(key()) {
            Join::Leader(_leader) => {}
            _ => panic!("expired flight must open a fresh leader"),
        }
    }

    #[tokio::test]
    async fn waiter_budget_expires_with_timeout() {
        let cfg = CoalescingConfig {
            max_wait_ms: 30,
            ..config()
        };
        let c = Coalescer::new(&cfg);
        let _leader = lead(&c); // stays airborne: nobody ever lands it
        let waiter = park(&c);
        let err = waiter.wait().await.expect_err("must time out");
        assert!(matches!(err, WaitError::Timeout));
        assert!(!err.is_upstream_failure());
    }

    #[tokio::test]
    async fn parked_cap_forces_bypass() {
        let cfg = CoalescingConfig {
            max_inflight_waiters: 1,
            ..config()
        };
        let c = Coalescer::new(&cfg);
        let _leader = lead(&c);
        assert!(matches!(c.join(key()), Join::Waiter(_)));
        assert!(
            matches!(c.join(key()), Join::Bypass),
            "over the cap the request must bypass, never block"
        );
    }

    #[tokio::test]
    async fn map_drains_after_dedup_window() {
        let c = Coalescer::new(&config());
        land(lead(&c));
        assert_eq!(c.flight_count(), 1);
        tokio::time::sleep(Duration::from_millis(90)).await;
        assert_eq!(c.flight_count(), 0, "evicted after the window");
    }

    #[tokio::test]
    async fn distinct_keys_open_distinct_flights() {
        let c = Coalescer::new(&config());
        let a = FlightKey::build(
            &Method::GET,
            &"/api/x?a=1".parse().expect("uri"),
            &hyper::HeaderMap::new(),
            &[],
        );
        let b = FlightKey::build(
            &Method::GET,
            &"/api/x?a=2".parse().expect("uri"),
            &hyper::HeaderMap::new(),
            &[],
        );
        assert!(matches!(c.join(a), Join::Leader(_)));
        assert!(matches!(c.join(b), Join::Leader(_)));
        assert_eq!(c.flight_count(), 2);
    }
}
