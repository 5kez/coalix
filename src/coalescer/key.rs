//! Coalescing-key derivation.
//!
//! The key is exactly what the crate documentation promises: uppercased
//! method + path + query + the configured key headers, in configuration
//! order. A header absent from the request collapses to an empty string so
//! "no Accept-Language" is a stable, hashable identity of its own — two
//! requests without the header share a flight, two requests with different
//! values never do.

use hyper::{HeaderMap, Method, Uri};

/// Identity of a coalescable upstream call.
///
/// Two requests join the same flight only when every component of this key
/// compares equal, so responses that may legitimately differ can never be
/// replayed across one another.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlightKey {
    /// Uppercased HTTP method (GET never shares with HEAD).
    method: String,
    /// Path plus optional query string, as received.
    target: String,
    /// Configured key headers in configuration order: (lowercase name,
    /// folded value; `""` when the header is absent).
    headers: Vec<(String, String)>,
}

impl FlightKey {
    /// Derives the flight identity of one request.
    ///
    /// `key_headers` entries are normalised to lowercase before lookup, so
    /// configuration spelling (`Accept-Language` or `accept-language`) does
    /// not matter. A header carrying multiple values is folded with commas
    /// in wire order; non-UTF-8 values use lossy decoding instead of failing
    /// the request.
    pub fn build(method: &Method, uri: &Uri, headers: &HeaderMap, key_headers: &[String]) -> Self {
        let target = match uri.query() {
            Some(query) => format!("{}?{}", uri.path(), query),
            None => uri.path().to_owned(),
        };
        let folded = key_headers
            .iter()
            .map(|name| {
                let lookup = name.to_ascii_lowercase();
                let value = headers
                    .get_all(lookup.as_str())
                    .iter()
                    .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
                    .collect::<Vec<_>>()
                    .join(",");
                (lookup, value)
            })
            .collect();
        Self {
            method: method.as_str().to_ascii_uppercase(),
            target,
            headers: folded,
        }
    }
}

#[cfg(test)]
mod tests {
    use hyper::header::ACCEPT_LANGUAGE;

    use super::*;

    fn key(path: &str, hdrs: &[(&str, &str)], config_keys: &[&str]) -> FlightKey {
        let mut map = HeaderMap::new();
        for (name, value) in hdrs {
            map.append(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).expect("header name"),
                hyper::header::HeaderValue::from_str(value).expect("header value"),
            );
        }
        let config: Vec<String> = config_keys.iter().map(|s| (*s).to_owned()).collect();
        FlightKey::build(&Method::GET, &path.parse().expect("uri"), &map, &config)
    }

    #[test]
    fn same_request_same_key() {
        assert_eq!(key("/api/x", &[], &[]), key("/api/x", &[], &[]));
    }

    #[test]
    fn query_string_is_part_of_the_key() {
        assert_ne!(key("/api/x?a=1", &[], &[]), key("/api/x?a=2", &[], &[]));
        assert_ne!(key("/api/x", &[], &[]), key("/api/x?a=1", &[], &[]));
    }

    #[test]
    fn absent_header_collapses_to_empty_string() {
        let absent = key("/api/x", &[], &["accept-language"]);
        assert_eq!(absent.headers[0].1, "");
        let a = key(
            "/api/x",
            &[(ACCEPT_LANGUAGE.as_str(), "en")],
            &["accept-language"],
        );
        let b = key(
            "/api/x",
            &[(ACCEPT_LANGUAGE.as_str(), "th")],
            &["accept-language"],
        );
        assert_ne!(absent, a);
        assert_ne!(a, b);
    }

    #[test]
    fn config_spelling_and_request_case_do_not_matter() {
        let upper_cfg = key(
            "/api/x",
            &[(ACCEPT_LANGUAGE.as_str(), "en")],
            &["ACCEPT-LANGUAGE"],
        );
        let lower_req = key("/api/x", &[("accept-language", "en")], &["accept-language"]);
        assert_eq!(upper_cfg, lower_req);
    }

    #[test]
    fn multi_value_headers_fold_in_wire_order() {
        let multi = key(
            "/api/x",
            &[
                (ACCEPT_LANGUAGE.as_str(), "en"),
                (ACCEPT_LANGUAGE.as_str(), "th"),
            ],
            &["accept-language"],
        );
        assert_eq!(multi.headers[0].1, "en,th");
        assert_ne!(
            multi,
            key(
                "/api/x",
                &[
                    (ACCEPT_LANGUAGE.as_str(), "th"),
                    (ACCEPT_LANGUAGE.as_str(), "en")
                ],
                &["accept-language"]
            )
        );
    }

    #[test]
    fn unlisted_headers_are_irrelevant() {
        let a = key("/api/x", &[("x-trace", "1")], &["accept-language"]);
        let b = key("/api/x", &[("x-trace", "2")], &["accept-language"]);
        assert_eq!(a, b);
    }

    #[test]
    fn method_participates() {
        let get = FlightKey::build(
            &Method::GET,
            &"/".parse().expect("uri"),
            &HeaderMap::new(),
            &[],
        );
        let head = FlightKey::build(
            &Method::HEAD,
            &"/".parse().expect("uri"),
            &HeaderMap::new(),
            &[],
        );
        assert_ne!(get, head);
    }
}
