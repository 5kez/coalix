//! Per-client-IP token bucket at the pipeline edge.
//!
//! Admission is checked immediately after the reserved metrics shortcut and
//! before any body buffering, routing, or upstream dial: a denied request
//! costs a map probe and an atomic increment, never a socket.
//!
//! ## Algorithm
//!
//! A plain token bucket in micro-token units: each bucket rests at
//! `burst * 1_000_000` micro-tokens and spends `1_000_000` per admitted
//! request, refilling at `requests_per_second` tokens per second — integer
//! math (`u128` intermediates, saturation at the cap), no new crates, no
//! clock dependency. Fractional tokens persist between checks, so a slow
//! trickle is never rounded away.
//!
//! ## Bounded memory
//!
//! Inserts sweep buckets idle longer than twice their full refill (floor
//! 100 ms — an idle bucket would be full anyway); a full table answers
//! unknown IPs with a one-second backoff, so spoofed load cannot grow the
//! map. Concurrent inserts at the cap can overshoot by the racing threads.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use crate::config::RateLimitConfig;

/// One whole request in micro-token units — fractional tokens accumulate
/// between checks, so refill visibility never waits for a whole second.
const TOKEN: u64 = 1_000_000;

/// Fixed backoff handed out while the table is full (unknown client).
const FULL_TABLE_BACKOFF: Duration = Duration::from_secs(1);

/// Refill state of one client IP.
#[derive(Debug)]
struct Bucket {
    /// Micro-tokens available, capped at `burst * TOKEN`.
    tokens: u64,
    /// Last refill; doubles as the idle marker for sweeps.
    refreshed: Instant,
}

/// Per-client-IP admission decisions over a bounded map of buckets; one
/// instance is shared by every connection task through the `Handler`.
#[derive(Debug)]
pub struct RateLimiter {
    buckets: DashMap<IpAddr, Bucket>,
    /// Whole tokens per second (clamped ≥1): refill rate and retry math.
    requests_per_second: u64,
    /// `burst * TOKEN`, saturated — bucket ceiling in micro-tokens.
    capacity: u64,
    /// Table cap, approximate under concurrent inserts.
    max_tracked_clients: usize,
    /// Idle age past which a bucket is droppable during a sweep.
    evict_after: Duration,
}

impl RateLimiter {
    /// Builds the limiter from configuration (`requests_per_second` and
    /// `burst` are validated > 0 whenever the limiter is enabled; the `.max`
    /// clamps keep direct construction in tests total).
    pub fn new(config: &RateLimitConfig) -> Self {
        let requests_per_second = config.requests_per_second.max(1);
        let capacity = config.burst.saturating_mul(TOKEN);
        let full_refill_micros = capacity / requests_per_second;
        let evict_after = Duration::from_micros(full_refill_micros.saturating_mul(2))
            .max(Duration::from_millis(100))
            .min(Duration::from_secs(60));
        Self {
            buckets: DashMap::new(),
            requests_per_second,
            capacity,
            max_tracked_clients: config.max_tracked_clients.max(1),
            evict_after,
        }
    }

    /// Admission verdict for `client`: `Ok` consumes one token, `Err`
    /// carries how long until the next token exists — a plain source for
    /// the `Retry-After` response header.
    pub fn check(&self, client: IpAddr) -> Result<(), Duration> {
        let now = Instant::now();
        if let Some(mut bucket) = self.buckets.get_mut(&client) {
            return self.consume(&mut bucket, now);
        }
        // Slow path: free a slot when at cap, then insert. The entry API
        // re-checks a racing inserter before deciding.
        if self.buckets.len() >= self.max_tracked_clients {
            self.sweep_idle(now);
            if self.buckets.len() >= self.max_tracked_clients {
                return Err(FULL_TABLE_BACKOFF);
            }
        }
        match self.buckets.entry(client) {
            Entry::Occupied(mut occupied) => {
                let bucket = occupied.get_mut();
                self.consume(bucket, now)
            }
            Entry::Vacant(vacant) => {
                // A fresh bucket starts full minus the request it admits.
                vacant.insert(Bucket {
                    tokens: self.capacity.saturating_sub(TOKEN),
                    refreshed: now,
                });
                Ok(())
            }
        }
    }

    /// Clients currently tracked (tests, and a future gauge if wanted).
    pub fn tracked_clients(&self) -> usize {
        self.buckets.len()
    }

    /// Refills against `now`, spends one token when available, else reports
    /// the wait for the next one.
    fn consume(&self, bucket: &mut Bucket, now: Instant) -> Result<(), Duration> {
        let elapsed = now.saturating_duration_since(bucket.refreshed);
        if !elapsed.is_zero() {
            bucket.refreshed = now;
            let gained = elapsed.as_micros() * u128::from(self.requests_per_second);
            let total = u128::from(bucket.tokens).saturating_add(gained);
            bucket.tokens =
                u64::try_from(total.min(u128::from(self.capacity))).unwrap_or(self.capacity);
        }
        if bucket.tokens >= TOKEN {
            bucket.tokens -= TOKEN;
            return Ok(());
        }
        // Micro-tokens until the next admission, rounded up so the client
        // never re-requests into an empty bucket.
        let needed = TOKEN - bucket.tokens;
        let micros = needed.div_ceil(self.requests_per_second).max(1);
        Err(Duration::from_micros(micros))
    }

    /// Drops buckets idle past `evict_after` — one that old would have
    /// refilled completely anyway, so eviction costs the client nothing.
    fn sweep_idle(&self, now: Instant) {
        self.buckets
            .retain(|_, bucket| now.saturating_duration_since(bucket.refreshed) < self.evict_after);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(last: u8) -> IpAddr {
        format!("198.51.100.{last}").parse().expect("test ip")
    }

    fn limiter(requests_per_second: u64, burst: u64, max_tracked_clients: usize) -> RateLimiter {
        RateLimiter::new(&RateLimitConfig {
            enabled: true,
            requests_per_second,
            burst,
            max_tracked_clients,
        })
    }

    #[test]
    fn burst_admits_exactly_burst_then_denies_with_a_positive_wait() {
        let limiter = limiter(1_000, 3, 16);
        for _ in 0..3 {
            limiter.check(ip(1)).expect("burst token must admit");
        }
        let wait = limiter
            .check(ip(1))
            .expect_err("bucket must be empty after the burst");
        assert!(!wait.is_zero(), "a denial carries a positive wait");
        assert!(
            wait <= Duration::from_millis(2),
            "1000 rps refills one token in 1 ms, found {wait:?}"
        );
    }

    #[test]
    fn refill_restores_tokens_after_idle_time() {
        let limiter = limiter(1_000, 2, 16);
        limiter.check(ip(2)).expect("token 1");
        limiter.check(ip(2)).expect("token 2");
        limiter.check(ip(2)).expect_err("bucket drained");
        // ~15 ms at 1000 rps refills a full 2-token bucket; extra wall time
        // only saturates it, so the exact two admissions stay deterministic.
        std::thread::sleep(Duration::from_millis(15));
        limiter.check(ip(2)).expect("refilled token 1");
        limiter.check(ip(2)).expect("refilled token 2");
        limiter.check(ip(2)).expect_err("drained again");
    }

    #[test]
    fn sweeping_keeps_the_table_within_cap() {
        // 10/1000 = 10 ms full refill → 20 ms computationally, floored to
        // the 100 ms eviction minimum.
        let limiter = limiter(1_000, 10, 4);
        limiter.check(ip(10)).expect("first client");
        std::thread::sleep(Duration::from_millis(150));
        // Four more distinct IPs: the fifth insert only succeeds if the sweep
        // released the now-idle first bucket.
        for last in 11..=14 {
            limiter
                .check(ip(last))
                .expect("sweep must free a slot before this insert");
        }
        assert!(
            limiter.tracked_clients() <= 4,
            "table must stay within max_tracked_clients"
        );
    }

    #[test]
    fn full_table_refuses_new_clients_with_one_second_backoff() {
        let limiter = limiter(1_000, 10, 2);
        limiter.check(ip(20)).expect("resident a");
        limiter.check(ip(21)).expect("resident b");
        let wait = limiter
            .check(ip(22))
            .expect_err("third distinct client while full");
        assert_eq!(wait, Duration::from_secs(1));
        assert_eq!(
            limiter.tracked_clients(),
            2,
            "a refused client must not occupy a slot"
        );
        // Residents keep their service while strangers are refused.
        limiter.check(ip(20)).expect("resident unaffected");
    }

    #[test]
    fn concurrent_hammering_stays_within_burst_plus_refill() {
        let limiter = limiter(10_000, 50, 1_024);
        let started = Instant::now();
        let admitted: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let limiter = &limiter;
                    scope.spawn(move || (0..250).filter(|_| limiter.check(ip(30)).is_ok()).count())
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("worker survived"))
                .sum()
        });
        let elapsed = started.elapsed();
        let refill = usize::try_from((elapsed.as_micros() * 10_000) / 1_000_000)
            .expect("refill count fits usize");
        assert!(admitted >= 50, "the whole burst must be usable");
        assert!(
            admitted <= 50 + refill + 2,
            "{admitted} admissions exceed burst 50 + refill {refill} + slack"
        );
    }
}
