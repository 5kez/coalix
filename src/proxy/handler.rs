//! Request dispatch: routing → coalescing decision → leader / waiter / replay.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http::request::Parts;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::{Body, Incoming};
use hyper::header::HOST;
use hyper::{Request, Response, StatusCode, Uri};
use thiserror::Error;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::cache::{Cache, Lookup};
use crate::coalescer::{Coalescer, FlightKey, Join, SharedResponse};
use crate::config::Config;
use crate::metrics::{LiveGauges, Metrics};
use crate::resilience::{fallback_response, CircuitBreaker, Permit};

use super::access::{self, AccessRecord};
use super::Upstream;
use super::UpstreamError;
use super::{finish, full_body, replay_response, sanitize_headers, OutBody};

/// Why one upstream attempt produced no client response. Budget overruns
/// are formatted at the call site (the budget value lives there), so this
/// enum stays with body/transport failures.
#[derive(Debug, Error)]
enum FetchError {
    /// Transport or addressing failure.
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
    /// The response body died mid-stream.
    #[error("reading upstream body: {0}")]
    Body(#[source] hyper::Error),
}

/// Request router: applies `Config` policy, then uses the coalescer for
/// whitelisted reads. Shared behind one `Arc` by every connection task.
pub(crate) struct Handler {
    config: Arc<Config>,
    upstream: Arc<Upstream>,
    coalescer: Arc<Coalescer>,
    cache: Arc<Cache>,
    breaker: Arc<CircuitBreaker>,
    metrics: Arc<Metrics>,
}

impl Handler {
    pub(crate) fn new(
        config: Arc<Config>,
        upstream: Arc<Upstream>,
        coalescer: Arc<Coalescer>,
    ) -> Self {
        // Phase 3/4 policy objects derive purely from configuration, so the
        // server keeps its three-argument construction.
        let cache = Arc::new(Cache::new(&config.cache));
        let breaker = Arc::new(CircuitBreaker::new(&config.resilience.circuit_breaker));
        Self {
            config,
            upstream,
            coalescer,
            cache,
            breaker,
            metrics: Arc::new(Metrics::default()),
        }
    }

    /// Serves one request arriving from peer `client` — the remote address
    /// feeds the access log (and any future per-IP policy). `B` is generic
    /// so tests may inject bodies; the server path always hands in hyper's
    /// streaming `Incoming`.
    ///
    /// Request bodies are buffered before dispatch (phase 2 limitation — no
    /// upload streaming); coalescable methods are bodyless by policy, so
    /// the buffer only affects mutations, which bypass the engine anyway.
    pub(crate) async fn call<B>(&self, client: SocketAddr, request: Request<B>) -> Response<OutBody>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<super::BoxError> + Send + 'static,
    {
        let started = Instant::now();
        let (parts, body) = request.into_parts();
        // Phase 4: `/metrics` is reserved ahead of buffering, routing, and
        // coalescing — a scrape must stay invisible to every other metric
        // *and* to the access log.
        let is_metrics = parts.uri.path() == self.config.observability.metrics_path;
        // The request line is captured before dispatch consumes `parts`; the
        // access record needs nothing else from the request head.
        let method = parts.method.as_str().to_owned();
        let path = parts
            .uri
            .path_and_query()
            .map(|target| target.as_str().to_owned())
            .unwrap_or_else(|| parts.uri.path().to_owned());
        let version = parts.version;
        let response = if is_metrics {
            self.serve_metrics(&parts)
        } else {
            self.dispatch(parts, body).await
        };
        if !is_metrics && self.config.observability.access_log.enabled {
            let record = AccessRecord {
                client: client.ip(),
                method,
                path,
                version,
                status: response.status().as_u16(),
                latency: started.elapsed(),
                bytes: response_bytes(&response),
            };
            access::emit(&self.config.observability, &record);
        }
        response
    }

    /// The full dispatch pipeline: body buffering → request accounting →
    /// micro-cache → coalescer → breaker-guarded upstream. This used to be
    /// the body of `call`; the access log turned `call` into a wrapper that
    /// times the stage and records the outcome for every non-reserved path.
    async fn dispatch<B>(&self, parts: Parts, body: B) -> Response<OutBody>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<super::BoxError> + Send + 'static,
    {
        let buffered = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(err) => {
                let err = err.into();
                warn!(%err, "failed reading client request body");
                return finish(Response::builder().status(StatusCode::BAD_REQUEST).body(
                    full_body(Bytes::from_static(b"coalix: unreadable request body")),
                ));
            }
        };

        // One increment per handled request, labelled by the routing
        // decision; unmatched paths collapse onto the "default" series.
        let coalesced = self
            .config
            .should_coalesce(parts.method.as_str(), parts.uri.path());
        let route = self
            .config
            .find_route(parts.uri.path())
            .map(|route| route.name.as_str())
            .unwrap_or("default");
        self.metrics
            .record_request(route, parts.method.as_str(), coalesced);

        // Phase 3: the micro-cache sits *in front of* coalescing — a fresh
        // (or stale) row answers instantly, so a herd on cached data never
        // reaches the flight map or the upstream at all.
        let key = FlightKey::build(
            &parts.method,
            &parts.uri,
            &parts.headers,
            &self.config.coalescing.key_headers,
        );
        if self.cache.is_cacheable(&parts.method) {
            match self.cache.lookup(&key) {
                Lookup::Fresh(shared) => {
                    debug!("cache hit: serving fresh, no upstream");
                    return replay_response(&shared);
                }
                Lookup::Stale(shared) => {
                    // SWR: reply from the stale row now, refresh behind.
                    if self.cache.claim_revalidation(&key) {
                        debug!("cache stale hit: kicking one revalidation");
                        self.spawn_revalidation(key.clone(), parts.clone());
                    }
                    return replay_response(&shared);
                }
                Lookup::Miss => {}
            }
        }

        // Structural bypass: global switch / method whitelist / route rules.
        if !self
            .config
            .should_coalesce(parts.method.as_str(), parts.uri.path())
        {
            debug!(method = %parts.method, path = %parts.uri.path(), "coalescing bypassed by policy");
            return self.passthrough(&parts, buffered).await;
        }

        // The flight map consumes the key; a cacheable leader still needs
        // it afterwards to store its buffer, so give it one cheap clone.
        match self.coalescer.join(key.clone()) {
            Join::Bypass => self.passthrough(&parts, buffered).await,
            Join::Ready(shared) => {
                debug!("tail-joined a landed flight");
                self.metrics.record_saved();
                replay_response(&shared)
            }
            Join::Failed(message) => {
                warn!(%message, "airborne flight failed; fallback for tail joiner");
                self.fallback()
            }
            Join::Waiter(waiter) => {
                // The guard lifts `coalix_waiters` for the whole park and
                // books `coalix_wait_seconds` when it drops — every exit
                // path from this arm goes through the guard.
                let parked = self.metrics.enter_wait();
                let outcome = waiter.wait().await;
                drop(parked);
                match outcome {
                    Ok(shared) => {
                        self.metrics.record_saved();
                        replay_response(&shared)
                    }
                    Err(err) if err.is_upstream_failure() => {
                        warn!(%err, "leader failed while parked; serving fallback");
                        self.fallback()
                    }
                    Err(err) => {
                        debug!(%err, "park released without outcome; fetching directly");
                        self.passthrough(&parts, buffered).await
                    }
                }
            }
            Join::Leader(leader) => {
                let budget = self.upstream.request_timeout();
                let outbound = match build_upstream_request(&parts, buffered, &self.upstream) {
                    Ok(request) => request,
                    Err(err) => {
                        warn!(%err, "cannot address upstream");
                        leader.fail(err.to_string());
                        return self.fallback();
                    }
                };
                // Phase 3: every upstream dial (leader or bypass) rides on a
                // breaker permit; while denied the flight fails fast and the
                // client gets the fallback without any socket work.
                let permit = match self.breaker.admit() {
                    Ok(permit) => permit,
                    Err(denied) => {
                        warn!(%denied, "circuit denied the leader; fallback without dialing");
                        leader.fail(format!("circuit breaker: {denied}"));
                        return self.fallback();
                    }
                };
                // The dial counts only once breaker admission succeeded —
                // denied requests never reach the wire.
                self.metrics.record_upstream_call();
                let dialed_at = Instant::now();
                // One budget covers headers *and* body collection — a leader
                // must land (or fail) within `request_timeout_ms`.
                let fetched = timeout(budget, async {
                    let response = self.upstream.send(outbound).await?;
                    let status = response.status();
                    let headers = sanitize_headers(response.headers());
                    let body = response
                        .into_body()
                        .collect()
                        .await
                        .map_err(FetchError::Body)?
                        .to_bytes();
                    Ok::<_, FetchError>((status, headers, body))
                })
                .await;
                self.metrics.observe_upstream(dialed_at.elapsed());
                match fetched {
                    Ok(Ok((status, headers, body))) => {
                        let shared = leader.complete(status, headers, body);
                        // Land the buffered copy in the micro-cache *while*
                        // everyone still waits — waiters and later arrivals
                        // (including reuse within the TTL) read it free.
                        if self.cache.is_cacheable(&parts.method) {
                            self.cache.store(&key, shared.clone());
                        }
                        settle_breaker(permit, status);
                        replay_response(&shared)
                    }
                    Ok(Err(err)) => {
                        warn!(%err, "leader upstream call failed");
                        permit.record_failure();
                        leader.fail(err.to_string());
                        self.fallback()
                    }
                    Err(_elapsed) => {
                        let message = format!("upstream exceeded {} ms", budget.as_millis());
                        warn!(%message, "leader request budget exhausted");
                        permit.record_failure();
                        leader.fail(message);
                        self.fallback()
                    }
                }
            }
        }
    }

    /// Configured failure reply — the resilience module owns the policy,
    /// the proxy owns the shaping (texture of status, type, and body).
    /// Phase 4: the reserved `/metrics` endpoint. Only `GET`/`HEAD` are
    /// accepted; nothing consults the cache, the coalescer, the breaker, or
    /// the upstream, so a scrape can never perturb what it measures.
    fn serve_metrics(&self, parts: &Parts) -> Response<OutBody> {
        if parts.method != hyper::Method::GET && parts.method != hyper::Method::HEAD {
            return finish(
                Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header(hyper::header::ALLOW, "GET, HEAD")
                    .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(full_body(Bytes::from_static(
                        b"coalix: metrics endpoint accepts GET",
                    ))),
            );
        }
        let live = LiveGauges {
            flights_active: self.coalescer.flight_count(),
            entries: self.cache.len(),
            cache: self.cache.stats(),
            breaker: self.breaker.state_name(),
        };
        let body = self.metrics.render(&live);
        finish(
            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, crate::metrics::CONTENT_TYPE)
                .body(full_body(Bytes::from(body))),
        )
    }

    fn fallback(&self) -> Response<OutBody> {
        fallback_response(&self.config.resilience)
    }

    /// Forwards the request to the upstream and streams the reply back —
    /// the plain-proxy path used whenever coalescing is off the table.
    async fn passthrough(&self, parts: &Parts, body: Bytes) -> Response<OutBody> {
        let outbound = match build_upstream_request(parts, body, &self.upstream) {
            Ok(request) => request,
            Err(err) => {
                warn!(%err, "cannot address upstream");
                return self.fallback();
            }
        };
        let budget = self.upstream.request_timeout();
        // Phase 3: bypasses consult the breaker too — a dead upstream must
        // not be redialed by mutations riding straight through.
        let permit = match self.breaker.admit() {
            Ok(permit) => permit,
            Err(denied) => {
                warn!(%denied, "circuit denied the bypass; fallback without dialing");
                return self.fallback();
            }
        };
        self.metrics.record_upstream_call();
        let dialed_at = Instant::now();
        let dialed = timeout(budget, self.upstream.send(outbound)).await;
        self.metrics.observe_upstream(dialed_at.elapsed());
        match dialed {
            Ok(Ok(response)) => {
                settle_breaker(permit, response.status());
                stream_response(response)
            }
            Ok(Err(err)) => {
                warn!(%err, "upstream call failed");
                permit.record_failure();
                self.fallback()
            }
            Err(_elapsed) => {
                warn!(ms = budget.as_millis(), "upstream exceeded request budget");
                permit.record_failure();
                self.fallback()
            }
        }
    }
}

/// Buffered upstream reply as the revalidation task carries it: status,
/// sanitized end-to-end headers, and the complete body.
type FetchedReply = (
    StatusCode,
    Vec<(hyper::header::HeaderName, hyper::header::HeaderValue)>,
    Bytes,
);

impl Handler {
    /// Phase 3 background refresh: fetches a fresh copy of `key`, swaps it
    /// into the micro-cache, then releases the claim CAS taken by
    /// `Cache::claim_revalidation` — exactly one task can run per entry.
    ///
    /// The task consults the breaker *first*: while the circuit is open it
    /// drops the claim without dialing, and the stale row keeps serving
    /// until the next probe window makes a fresh attempt possible.
    fn spawn_revalidation(&self, key: FlightKey, parts: Parts) {
        let cache = self.cache.clone();
        let breaker = self.breaker.clone();
        let upstream = self.upstream.clone();
        let metrics = self.metrics.clone();
        let budget = upstream.request_timeout();
        tokio::spawn(async move {
            let permit = match breaker.admit() {
                Ok(permit) => permit,
                Err(denied) => {
                    debug!(%denied, "revalidation skipped: circuit open");
                    cache.finish_revalidation(&key);
                    return;
                }
            };
            let fetched: Result<FetchedReply, String> =
                match build_upstream_request(&parts, Bytes::new(), &upstream) {
                    Ok(outbound) => {
                        // Refreshes ride the same breaker contract and share
                        // the upstream-latency histogram with real dials.
                        metrics.record_upstream_call();
                        let dialed_at = std::time::Instant::now();
                        let outcome = match timeout(budget, async move {
                            let response = upstream
                                .send(outbound)
                                .await
                                .map_err(|err| err.to_string())?;
                            let status = response.status();
                            let headers = sanitize_headers(response.headers());
                            let body = response
                                .into_body()
                                .collect()
                                .await
                                .map_err(|err| err.to_string())?
                                .to_bytes();
                            Ok::<_, String>((status, headers, body))
                        })
                        .await
                        {
                            Ok(result) => result,
                            Err(_elapsed) => {
                                Err(format!("revalidation exceeded {} ms", budget.as_millis()))
                            }
                        };
                        metrics.observe_upstream(dialed_at.elapsed());
                        outcome
                    }
                    Err(err) => Err(err.to_string()),
                };
            match fetched {
                Ok((status, headers, body)) => {
                    // `store` re-applies the full policy (status, cache
                    // directives, window caps); if it refuses, the stale
                    // row carries on untouched.
                    cache.store(
                        &key,
                        Arc::new(SharedResponse {
                            status,
                            headers,
                            body,
                        }),
                    );
                    settle_breaker(permit, status);
                }
                Err(message) => {
                    warn!(%message, "revalidation failed; stale row keeps serving");
                    permit.record_failure();
                }
            }
            cache.finish_revalidation(&key);
        });
    }
}

/// Terminal accounting for one dial: server errors count as breaker
/// failures, everything below resets it; transport failures and timeouts
/// record directly at their call sites. Consumes the permit.
fn settle_breaker(permit: Permit<'_>, status: StatusCode) {
    permit.record_status(status);
}

/// Response body length for the access log: `Content-Length` when the head
/// states one, otherwise the body's exact size hint while it is bound —
/// streaming replies of unknown length log as `-`.
fn response_bytes(response: &Response<OutBody>) -> Option<u64> {
    response
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| response.body().size_hint().exact())
}

/// Origin-form target (`/path?query`) of a client URI — even when the
/// client speaks absolute-form directly to the proxy.
fn target_of(uri: &Uri) -> String {
    match uri.query() {
        Some(query) => format!("{}?{}", uri.path(), query),
        None => uri.path().to_owned(),
    }
}

/// Rebuilds a client request for the upstream: absolute URI against the
/// fixed base, sanitized headers, `Host` replaced with the upstream
/// authority. Client input can only ever contribute method, target
/// (path + query), and end-to-end header values.
fn build_upstream_request(
    parts: &Parts,
    body: Bytes,
    upstream: &Upstream,
) -> Result<Request<Full<Bytes>>, FetchError> {
    let uri = upstream.resolve(&target_of(&parts.uri))?;
    let mut builder = Request::builder().method(parts.method.clone()).uri(uri);
    for (name, value) in sanitize_headers(&parts.headers) {
        if name == HOST {
            continue; // replaced below — duplicate Host is a protocol error
        }
        builder = builder.header(name, value);
    }
    builder = builder.header(HOST, upstream.authority());
    builder
        .body(Full::new(body))
        .map_err(|err| FetchError::Upstream(UpstreamError::from(err)))
}

/// Forwards an upstream reply frame by frame (download streaming), minus
/// hop-by-hop headers — hyper regenerates framing from actual bytes on the
/// client side.
fn stream_response(response: Response<Incoming>) -> Response<OutBody> {
    let (parts, body) = response.into_parts();
    let mut builder = Response::builder().status(parts.status);
    for (name, value) in sanitize_headers(&parts.headers) {
        builder = builder.header(name, value);
    }
    finish(
        builder.body(
            body.map_err(|err| -> super::BoxError { Box::new(err) })
                .boxed(),
        ),
    )
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use hyper::HeaderMap;

    use super::*;
    use crate::config::UpstreamConfig;

    fn upstream() -> Upstream {
        Upstream::new(&UpstreamConfig {
            base_url: "http://backend:9000".to_owned(),
            ..UpstreamConfig::default()
        })
        .expect("valid base")
    }

    #[test]
    fn target_carries_path_and_query() {
        let uri: Uri = "/a/b?x=1&y=2".parse().expect("uri");
        assert_eq!(target_of(&uri), "/a/b?x=1&y=2");
        let plain: Uri = "/a/b".parse().expect("uri");
        assert_eq!(target_of(&plain), "/a/b");
    }

    #[test]
    fn outbound_request_fixes_authority_and_host() {
        let request = Request::builder()
            .method(hyper::Method::GET)
            .uri("/api/data?full=1")
            .header(hyper::header::HOST, "evil.test:1")
            .header(hyper::header::CONNECTION, "x-sneaky")
            .header("x-sneaky", "drop-me")
            .header("x-keep", "kept")
            .body(Full::new(Bytes::new()))
            .expect("request");
        let (parts, _client_body) = request.into_parts();
        let outbound = build_upstream_request(&parts, Bytes::new(), &upstream()).expect("builds");

        let expected: Uri = "http://backend:9000/api/data?full=1".parse().expect("uri");
        assert_eq!(outbound.uri(), &expected);
        assert_eq!(
            outbound.headers().get(HOST).expect("host present"),
            "backend:9000"
        );
        assert!(outbound.headers().get("x-sneaky").is_none());
        assert!(outbound.headers().get("x-keep").is_some());
        let hosts = outbound.headers().get_all(HOST).iter().count();
        assert_eq!(hosts, 1, "exactly one Host header");
    }

    #[test]
    fn non_string_header_values_survive_sanitizing() {
        let mut headers = HeaderMap::new();
        headers.insert(
            hyper::header::HeaderName::from_static("x-binary"),
            HeaderValue::from_bytes(b"\x80\x81").expect("byte value"),
        );
        assert_eq!(sanitize_headers(&headers).len(), 1);
    }
}
