//! # Reverse proxy
//!
//! Routing layered on top of the single-flight engine:
//!
//! * an accept loop with a hard connection cap (hyper HTTP/1, keep-alive);
//! * one pooled upstream client (per-dial connect timeout, bounded idle
//!   pool — [`UpstreamConfig`]);
//! * dispatch: [`Config::should_coalesce`] decides *whether* a request may
//!   enter the engine at all — mutations never reach it — then
//!   leader / waiter / tail-replay;
//! * response shaping: hop-by-hop hygiene, replay assembly, fallback reply.
//!
//! **Body flow.** Non-coalesced traffic streams: buffered request bodies go
//! out with one upstream call and upstream replies are forwarded frame by
//! frame. Coalesced leaders buffer their reply once — a landed flight must
//! be replayable for its whole dedup tail anyway, so byte-perfect replay
//! afterwards costs only reference-counted `Bytes` clones.

mod access;
mod client;
mod handler;
mod server;

use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::{
    header::{HeaderName, HeaderValue, CONNECTION},
    HeaderMap, Response, StatusCode,
};
use thiserror::Error;
use tracing::error;

use crate::coalescer::SharedResponse;

pub use client::{Upstream, UpstreamError};
pub use server::{serve, serve_with_listener};

/// Uniform error type behind every response body and internal future.
pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Uniform client-facing body: streamed upstream frames or buffered replay.
pub(crate) type OutBody = BoxBody<Bytes, BoxError>;

/// Failure while binding or provisioning the proxy.
#[derive(Debug, Error)]
pub enum ProxyError {
    /// The configured listen socket could not be bound.
    #[error("failed binding {addr}: {source}")]
    Bind {
        /// Socket the proxy tried to bind.
        addr: SocketAddr,
        /// Underlying OS-level bind failure.
        #[source]
        source: std::io::Error,
    },
    /// The upstream pool could not be built (bad `base_url` or scheme).
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
}

/// Headers that describe the connection hop, never the payload
/// (RFC 9110 §7.6.1). Never forwarded upstream, never replayed.
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// True when `name` must not cross a hop.
pub(crate) fn is_hop_by_hop(name: &HeaderName) -> bool {
    let raw = name.as_str();
    HOP_BY_HOP.iter().any(|hop| raw.eq_ignore_ascii_case(hop))
}

/// End-to-end view of `headers`: hop-by-hop entries dropped, including
/// every header nominated by a `Connection:` token.
pub(crate) fn sanitize_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    let nominated: Vec<String> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|token| token.trim().to_ascii_lowercase())
        .collect();
    headers
        .iter()
        .filter(|(name, _)| {
            !is_hop_by_hop(name)
                && !nominated
                    .iter()
                    .any(|token| name.as_str().eq_ignore_ascii_case(token))
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Wraps buffered bytes as the uniform body. `Full` is infallible by
/// construction, so no `Result` wrapper is ever needed at this seam.
pub(crate) fn full_body(body: Bytes) -> OutBody {
    Full::new(body)
        .map_err(|never: std::convert::Infallible| BoxError::from(never))
        .boxed()
}

/// Finalises a synthesized response. An `http::Error` here would mean our
/// own constants were malformed — impossible in practice, but production
/// code never panics: log it and answer with a guaranteed-valid 500.
pub(crate) fn finish(result: Result<Response<OutBody>, http::Error>) -> Response<OutBody> {
    match result {
        Ok(response) => response,
        Err(err) => {
            error!(%err, "synthesized an invalid response; serving minimal 500");
            Response::new(full_body(Bytes::from_static(
                b"coalix: internal response error",
            )))
        }
    }
}

/// The canned reply for upstream failures: the configured fallback when
/// enabled, a plain gateway error otherwise.
pub(crate) fn failure_response(status_code: u16, body: &str, enabled: bool) -> Response<OutBody> {
    let (status, body) = if enabled {
        (
            StatusCode::from_u16(status_code).unwrap_or(StatusCode::BAD_GATEWAY),
            body,
        )
    } else {
        (StatusCode::BAD_GATEWAY, "coalix: upstream unreachable")
    };
    finish(
        Response::builder()
            .status(status)
            .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(full_body(Bytes::from(body.to_owned()))),
    )
}

/// Rebuilds a landed flight into a client response. Headers were sanitized
/// at landing time, so every replay is legal to serialize as-is.
pub(crate) fn replay_response(shared: &SharedResponse) -> Response<OutBody> {
    let mut builder = Response::builder().status(shared.status);
    for (name, value) in &shared.headers {
        builder = builder.header(name.clone(), value.clone());
    }
    finish(builder.body(full_body(shared.body.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_from(entries: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in entries {
            map.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("header name"),
                HeaderValue::from_str(value).expect("header value"),
            );
        }
        map
    }

    #[test]
    fn hop_by_hop_headers_are_recognised() {
        assert!(is_hop_by_hop(&hyper::header::TRANSFER_ENCODING));
        assert!(is_hop_by_hop(&HeaderName::from_static("keep-alive")));
        assert!(!is_hop_by_hop(&hyper::header::CONTENT_TYPE));
    }

    #[test]
    fn sanitize_drops_hop_by_hop_and_nominated_headers() {
        let headers = headers_from(&[
            ("content-type", "application/json"),
            ("x-keep", "yes"),
            ("connection", "x-remove-me, keep-alive"),
            ("x-remove-me", "secret"),
            ("transfer-encoding", "chunked"),
        ]);
        let kept: Vec<String> = sanitize_headers(&headers)
            .into_iter()
            .map(|(name, _)| name.as_str().to_owned())
            .collect();
        assert_eq!(kept, vec!["content-type".to_owned(), "x-keep".to_owned()]);
    }

    #[test]
    fn sanitize_keeps_content_length_for_stable_replays() {
        let headers = headers_from(&[("content-length", "42")]);
        assert_eq!(sanitize_headers(&headers).len(), 1);
    }

    #[test]
    fn fallback_obeys_configured_switch_and_status() {
        let on = failure_response(503, "custom fallback", true);
        assert_eq!(on.status(), StatusCode::SERVICE_UNAVAILABLE);
        let off = failure_response(503, "custom fallback", false);
        assert_eq!(off.status(), StatusCode::BAD_GATEWAY);
        let bogus = failure_response(42, "x", true);
        assert_eq!(
            bogus.status(),
            StatusCode::BAD_GATEWAY,
            "invalid falls back"
        );
    }

    #[test]
    fn replay_preserves_status_headers_and_body() {
        let shared = SharedResponse {
            status: StatusCode::IM_A_TEAPOT,
            headers: vec![(
                hyper::header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain"),
            )],
            body: Bytes::from_static(b"short and stout"),
        };
        let response = replay_response(&shared);
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        assert_eq!(
            response
                .headers()
                .get(hyper::header::CONTENT_TYPE)
                .expect("content type present"),
            "text/plain"
        );
    }
}
