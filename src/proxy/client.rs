//! Upstream (backend) client pool.

use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::{body::Incoming, Request, Response, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use thiserror::Error;

use crate::config::UpstreamConfig;

/// Failures while addressing or reaching the upstream.
#[derive(Debug, Error)]
pub enum UpstreamError {
    /// `upstream.base_url` is not an absolute URL with scheme + authority.
    #[error("invalid upstream base_url {0:?}: expected an absolute http:// URL")]
    InvalidBase(String),
    /// Only plaintext HTTP upstreams are supported in phase 2 (no TLS).
    #[error("unsupported upstream scheme in {0:?}: only http:// is supported")]
    UnsupportedScheme(String),
    /// The upstream transport failed (refused, reset, protocol error).
    #[error("upstream request failed: {0}")]
    Transport(#[from] hyper_util::client::legacy::Error),
    /// The response body could not be buffered (connection died mid-body).
    #[error("failed reading upstream body: {0}")]
    Body(#[source] hyper::Error),
    /// A client request could not be rebuilt into an outbound request.
    #[error("failed building upstream request: {0}")]
    Build(#[from] http::Error),
    /// The request target did not combine into a valid absolute URI.
    #[error("invalid request target {0:?} for upstream {1}")]
    InvalidTarget(String, String),
}

/// Pooled HTTP/1.1 client for the single configured upstream origin.
///
/// One shared legacy client serves every request: connection pooling
/// bounded by `max_idle_per_host`, a TCP connect timeout applied per dial,
/// and a pre-validated `scheme://authority` so client-supplied targets can
/// never redirect the upstream.
pub struct Upstream {
    client: Client<HttpConnector, Full<Bytes>>,
    /// `scheme://authority` of `upstream.base_url`, trailing slashes trimmed.
    base: String,
    authority: String,
    request_timeout: Duration,
}

impl Upstream {
    /// Builds the pool, validating the base URL up front so a broken config
    /// fails at startup instead of on the first request.
    pub fn new(config: &UpstreamConfig) -> Result<Self, UpstreamError> {
        let trimmed = config.base_url.trim();
        let base = trimmed.trim_end_matches('/');
        let origin: Uri = base
            .parse()
            .map_err(|_| UpstreamError::InvalidBase(trimmed.to_owned()))?;
        match origin.scheme_str() {
            Some("http") => {}
            Some(_) => return Err(UpstreamError::UnsupportedScheme(trimmed.to_owned())),
            None => return Err(UpstreamError::InvalidBase(trimmed.to_owned())),
        }
        let authority = origin
            .authority()
            .ok_or_else(|| UpstreamError::InvalidBase(trimmed.to_owned()))?
            .as_str()
            .to_owned();

        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(Duration::from_millis(config.connect_timeout_ms)));
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(config.max_idle_per_host)
            .build(connector);

        Ok(Self {
            client,
            base: base.to_owned(),
            authority,
            request_timeout: Duration::from_millis(config.request_timeout_ms),
        })
    }

    /// `host:port` authority of the upstream — used as the outbound `Host`.
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// Full request budget (headers included) for one upstream attempt.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Resolves an origin-form target (`/path?query`) against the fixed
    /// upstream base — client input only ever contributes path + query.
    pub fn resolve(&self, target: &str) -> Result<Uri, UpstreamError> {
        let raw = format!("{}{}", self.base, target);
        raw.parse()
            .map_err(|_| UpstreamError::InvalidTarget(target.to_owned(), raw))
    }

    /// Sends one request. The timeout is deliberately **not** applied here:
    /// the caller wraps `send` so a leader's budget can cover headers *and*
    /// body collection under a single deadline.
    pub async fn send(
        &self,
        request: Request<Full<Bytes>>,
    ) -> Result<Response<Incoming>, UpstreamError> {
        Ok(self.client.request(request).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str) -> UpstreamConfig {
        UpstreamConfig {
            base_url: url.to_owned(),
            ..UpstreamConfig::default()
        }
    }

    #[test]
    fn valid_base_is_accepted_and_resolved() {
        let upstream = Upstream::new(&config("http://127.0.0.1:3000")).expect("valid base");
        assert_eq!(upstream.authority(), "127.0.0.1:3000");
        let uri = upstream.resolve("/api/x?a=1").expect("target resolves");
        assert_eq!(
            uri,
            "http://127.0.0.1:3000/api/x?a=1"
                .parse::<Uri>()
                .expect("uri")
        );
    }

    #[test]
    fn trailing_slash_base_does_not_double_slash() {
        let upstream = Upstream::new(&config("http://example.test:8080/")).expect("valid base");
        let uri = upstream.resolve("/health").expect("resolves");
        assert_eq!(
            uri,
            "http://example.test:8080/health"
                .parse::<Uri>()
                .expect("uri")
        );
    }

    #[test]
    fn client_supplied_authority_never_leaks_into_target() {
        let upstream = Upstream::new(&config("http://backend:9000")).expect("valid base");
        let uri = upstream.resolve("/@evil.test/x").expect("resolves as path");
        assert_eq!(uri.authority().expect("authority").as_str(), "backend:9000");
    }

    #[test]
    fn invalid_and_tls_bases_fail_fast() {
        assert!(matches!(
            Upstream::new(&config("backend:9000")),
            Err(UpstreamError::InvalidBase(_))
        ));
        assert!(matches!(
            Upstream::new(&config("ftp://backend")),
            Err(UpstreamError::UnsupportedScheme(_))
        ));
    }

    #[test]
    fn budget_reflects_configuration() {
        let upstream = Upstream::new(&UpstreamConfig {
            request_timeout_ms: 1234,
            ..config("http://b:1")
        })
        .expect("valid base");
        assert_eq!(upstream.request_timeout(), Duration::from_millis(1234));
    }
}
