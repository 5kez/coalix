//! Structured access logging: one stdout line per finished request.
//!
//! The access log is deliberately independent of the tracing pipeline:
//! [`emit`] writes one self-contained line straight to stdout, so
//! `observability.log_level` can filter events without ever silencing
//! request accounting. Three encodings exist ([`AccessLogFormat`]):
//!
//! * `auto` — follows `observability.log_format` (text line or JSON object),
//! * `json` — one JSON object per line, always,
//! * `clf` — NCSA Common Log Format with UTC (`+0000`) timestamps, readable
//!   by offline analyzers (GoAccess, AWStats) without a timezone database.
//!
//! The reserved metrics path never reaches this module: a scrape must stay
//! invisible to every observable, logs included. Timestamps are computed
//! from `SystemTime` alone — zero dependencies, deterministic in tests.

use std::fmt::Write as _;
use std::io::Write as _;
use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::Version;

use crate::config::{AccessLogFormat, LogFormat, ObservabilityConfig};

/// One finished request, summarized as an access-log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccessRecord {
    /// Remote peer IP (port dropped — CLF host column).
    pub(crate) client: IpAddr,
    /// Request method as received.
    pub(crate) method: String,
    /// Origin-form target (`/path?query`).
    pub(crate) path: String,
    /// Wire protocol version rendered as `HTTP/1.x`.
    pub(crate) version: Version,
    /// Final response status code.
    pub(crate) status: u16,
    /// Request entry to response head.
    pub(crate) latency: Duration,
    /// Response body length when known exactly; `None` renders as `-`.
    pub(crate) bytes: Option<u64>,
}

/// Writes `record` to stdout in the configured encoding — a no-op while
/// `access_log.enabled` is false.
pub(crate) fn emit(config: &ObservabilityConfig, record: &AccessRecord) {
    if let Some(line) = line_for(config, record, SystemTime::now()) {
        // Best-effort by design: a closed or full stdout must never abort a
        // live proxy connection, so write errors are dropped here.
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        let _ = writeln!(lock, "{line}");
    }
}

/// The line `record` renders to, or `None` when the access log is disabled.
/// Split from [`emit`] so the enable gate and every encoding stay
/// unit-testable without capturing the process stdout.
pub(crate) fn line_for(
    config: &ObservabilityConfig,
    record: &AccessRecord,
    now: SystemTime,
) -> Option<String> {
    config
        .access_log
        .enabled
        .then(|| format_line(config, record, now))
}

/// Resolves `access_log.format` against `log_format` and renders one line.
pub(crate) fn format_line(
    config: &ObservabilityConfig,
    record: &AccessRecord,
    now: SystemTime,
) -> String {
    match config.access_log.format {
        AccessLogFormat::Auto => match config.log_format {
            LogFormat::Text => text_line(record, now),
            LogFormat::Json => json_line(record, now),
        },
        AccessLogFormat::Json => json_line(record, now),
        AccessLogFormat::Clf => clf_line(record, now),
    }
}

/// Human line: `{ts}  ACCESS  {client} {method} {path} {version} {status}
/// {bytes|-} {latency}ms` — token order mirrors CLF, latency appended.
fn text_line(record: &AccessRecord, now: SystemTime) -> String {
    let mut line = String::with_capacity(160);
    push_rfc3339(&mut line, now);
    line.push_str("  ACCESS  ");
    let _ = write!(
        line,
        "{} {} {} {} {} ",
        record.client,
        record.method,
        record.path,
        version_label(record.version),
        record.status
    );
    match record.bytes {
        Some(bytes) => {
            let _ = write!(line, "{bytes} ");
        }
        None => line.push_str("- "),
    }
    let _ = write!(line, "{}ms", record.latency.as_millis());
    line
}

/// One JSON object per line; `bytes` is omitted while unknown.
fn json_line(record: &AccessRecord, now: SystemTime) -> String {
    let mut line = String::with_capacity(224);
    line.push_str("{\"timestamp\":\"");
    push_rfc3339(&mut line, now);
    line.push_str("\",\"client\":\"");
    push_json_escaped(&mut line, &record.client.to_string());
    line.push_str("\",\"method\":\"");
    push_json_escaped(&mut line, &record.method);
    line.push_str("\",\"path\":\"");
    push_json_escaped(&mut line, &record.path);
    let _ = write!(line, "\",\"version\":\"{}", version_label(record.version));
    let _ = write!(line, "\",\"status\":{}", record.status);
    let _ = write!(line, ",\"latency_ms\":{}", record.latency.as_millis());
    if let Some(bytes) = record.bytes {
        let _ = write!(line, ",\"bytes\":{bytes}");
    }
    line.push('}');
    line
}

/// NCSA Common Log Format, UTC:
/// `{ip} - - [{dd/Mon/yyyy:HH:MM:SS +0000}] "{method} {path} {version}" {status} {bytes|-}`.
fn clf_line(record: &AccessRecord, now: SystemTime) -> String {
    let (secs, _) = unix_parts(now);
    let (year, month, day, hour, minute, second) = civil(secs);
    let bytes = record
        .bytes
        .map_or_else(|| "-".to_owned(), |value| value.to_string());
    format!(
        "{client} - - [{day:02}/{month_name}/{year:04}:{hour:02}:{minute:02}:{second:02} +0000] \
         \"{method} {path} {version}\" {status} {bytes}",
        client = record.client,
        month_name = MONTHS[(month - 1) as usize],
        method = record.method,
        path = record.path,
        version = version_label(record.version),
        status = record.status,
    )
}

/// Wire label of `version` (`HTTP/1.1`, …) — `http::Version` implements
/// neither `Display` nor a stable string accessor, so the mapping is ours.
fn version_label(version: Version) -> &'static str {
    if version == Version::HTTP_09 {
        "HTTP/0.9"
    } else if version == Version::HTTP_10 {
        "HTTP/1.0"
    } else if version == Version::HTTP_11 {
        "HTTP/1.1"
    } else if version == Version::HTTP_2 {
        "HTTP/2"
    } else if version == Version::HTTP_3 {
        "HTTP/3"
    } else {
        "HTTP/?"
    }
}

/// Appends `value` escaped as the body of a JSON string literal.
fn push_json_escaped(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if (control as u32) < 0x20 || control as u32 == 0x7F => {
                let _ = write!(out, "\\u{:04x}", control as u32);
            }
            plain => out.push(plain),
        }
    }
}

/// (unix seconds, subsecond milliseconds) of `at`, clamped at the epoch so
/// clock skew before 1970 cannot underflow the division below.
fn unix_parts(at: SystemTime) -> (u64, u32) {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    (since.as_secs(), since.subsec_millis())
}

/// Appends `YYYY-MM-DDTHH:MM:SS.mmmZ` (UTC) to `out`.
fn push_rfc3339(out: &mut String, at: SystemTime) {
    let (secs, millis) = unix_parts(at);
    let (year, month, day, hour, minute, second) = civil(secs);
    let _ = write!(
        out,
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
    );
}

/// Splits unix seconds into (year, month, day, hour, minute, second) in UTC.
fn civil(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let time_of_day = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    (
        year,
        month,
        day,
        (time_of_day / 3_600) as u32,
        ((time_of_day % 3_600) / 60) as u32,
        (time_of_day % 60) as u32,
    )
}

/// Howard Hinnant's `civil_from_days` for the proleptic Gregorian calendar —
/// converts days since 1970-01-01 to (year, month, day) without any
/// timezone database dependency.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let shifted = z + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365; // [0, 399]
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let month_prime = (5 * day_of_year + 2) / 153; // [0, 11] — 0 = March
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32; // [1, 31]
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    } as u32; // [1, 12]
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// NCSA month abbreviations (`month` is 1-based).
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AccessLogConfig, AccessLogFormat};

    fn record() -> AccessRecord {
        AccessRecord {
            client: "192.0.2.9".parse().expect("test ip"),
            method: "GET".to_owned(),
            path: "/api/items?page=2".to_owned(),
            version: Version::HTTP_11,
            status: 200,
            latency: Duration::from_millis(42),
            bytes: Some(1234),
        }
    }

    /// Fixed instant: 1_600_000_000 == 2020-09-13T12:26:40Z.
    fn at() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_600_000_000)
    }

    fn config(format: AccessLogFormat, log_format: LogFormat) -> ObservabilityConfig {
        ObservabilityConfig {
            access_log: AccessLogConfig {
                enabled: true,
                format,
            },
            log_format,
            ..ObservabilityConfig::default()
        }
    }

    #[test]
    fn clf_line_is_ncsa_common_format_in_utc() {
        assert_eq!(
            clf_line(&record(), at()),
            "192.0.2.9 - - [13/Sep/2020:12:26:40 +0000] \"GET /api/items?page=2 HTTP/1.1\" 200 1234"
        );
    }

    #[test]
    fn civil_handles_epoch_and_leap_day() {
        assert_eq!(civil(0), (1970, 1, 1, 0, 0, 0));
        // 951_782_400 == 2000-02-29T00:00:00Z (Gregorian leap year).
        assert_eq!(civil(951_782_400), (2000, 2, 29, 0, 0, 0));
        // Sub-day remainders survive the date split.
        assert_eq!(civil(86_399), (1970, 1, 1, 23, 59, 59));
    }

    #[test]
    fn json_line_is_one_object_with_optional_bytes() {
        assert_eq!(
            json_line(&record(), at()),
            "{\"timestamp\":\"2020-09-13T12:26:40.000Z\",\"client\":\"192.0.2.9\",\
             \"method\":\"GET\",\"path\":\"/api/items?page=2\",\"version\":\"HTTP/1.1\",\
             \"status\":200,\"latency_ms\":42,\"bytes\":1234}"
        );
        let unknown = AccessRecord {
            bytes: None,
            ..record()
        };
        assert!(!json_line(&unknown, at()).contains("\"bytes\""));
    }

    #[test]
    fn json_line_escapes_hostile_strings() {
        let hostile = AccessRecord {
            path: "/a\\b\"c\td".to_owned(),
            ..record()
        };
        assert!(
            json_line(&hostile, at()).contains("\"path\":\"/a\\\\b\\\"c\\td\""),
            "backslash, quote and tab must be json-escaped"
        );
    }

    #[test]
    fn format_matrix_follows_auto_and_explicit_encodings() {
        let now = at();
        let text = format_line(
            &config(AccessLogFormat::Auto, LogFormat::Text),
            &record(),
            now,
        );
        assert!(text.contains("  ACCESS  ") && !text.starts_with('{'));
        let json = format_line(
            &config(AccessLogFormat::Auto, LogFormat::Json),
            &record(),
            now,
        );
        assert!(json.starts_with('{') && json.ends_with('}'));
        let forced = format_line(
            &config(AccessLogFormat::Json, LogFormat::Text),
            &record(),
            now,
        );
        assert!(
            forced.starts_with('{'),
            "explicit json overrides log_format"
        );
        let clf = format_line(
            &config(AccessLogFormat::Clf, LogFormat::Json),
            &record(),
            now,
        );
        assert!(clf.contains(" +0000] ") && !clf.starts_with('{'));
    }

    #[test]
    fn unknown_bytes_render_as_dash() {
        let record = AccessRecord {
            bytes: None,
            ..record()
        };
        assert!(text_line(&record, at()).contains(" - "));
        assert!(clf_line(&record, at()).ends_with(" 200 -"));
    }

    #[test]
    fn disabled_config_produces_no_line() {
        let disabled = ObservabilityConfig {
            access_log: AccessLogConfig {
                enabled: false,
                format: AccessLogFormat::Auto,
            },
            ..ObservabilityConfig::default()
        };
        assert!(line_for(&disabled, &record(), at()).is_none());
        assert!(line_for(
            &config(AccessLogFormat::Auto, LogFormat::Text),
            &record(),
            at()
        )
        .is_some());
    }
}
