//! Environment-backed configuration, mirroring every other port's `Config`.
//!
//! Every getter reads its variable on each call, so a value changed at runtime
//! is picked up. Integer parsing is strict: the trimmed value must be all
//! digits and positive, or the default is kept; a typo never becomes a
//! surprising zero. A boolean must be one of `1/true/yes/on` or
//! `0/false/no/off`, case-insensitive. [`set`] overrides a variable inside the
//! process, for tests and for applications that cannot set their environment.
//!
//! The exchange names are part of the wire protocol: every process on one bus,
//! whatever its language, must agree on them.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Named message priorities, matching the other ports. RabbitMQ sorts a
/// message with no priority as 0.
pub const PRIORITY_NORMAL: u8 = 0;
pub const PRIORITY_HIGH: u8 = 1;
pub const PRIORITY_CONTROL: u8 = 2;

/// The `max_priority` that gives the three levels above. Keep the range small.
pub const RECOMMENDED_MAX_PRIORITY: u8 = 2;

/// Headers of the server-streaming wire protocol.
pub const HEADER_FINAL: &str = "x-protobus-final";
pub const HEADER_SEQ: &str = "x-protobus-seq";

fn overrides() -> &'static RwLock<HashMap<String, String>> {
    static OVERRIDES: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Override a variable for this process.
pub fn set(name: &str, value: &str) {
    overrides().write().unwrap().insert(name.to_string(), value.to_string());
}

/// Remove one override.
pub fn unset(name: &str) {
    overrides().write().unwrap().remove(name);
}

/// Remove every override.
pub fn reset() {
    overrides().write().unwrap().clear();
}

fn raw(name: &str) -> Option<String> {
    if let Some(v) = overrides().read().unwrap().get(name) {
        return Some(v.clone());
    }
    std::env::var(name).ok()
}

/// The TypeScript reference refuses anything beyond `Number.MAX_SAFE_INTEGER`; so does every port.
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

pub(crate) fn env_int(name: &str, fallback: u64) -> u64 {
    let Some(raw) = raw(name) else {
        return fallback;
    };
    let t = raw.trim();
    if t.is_empty() || !t.bytes().all(|b| b.is_ascii_digit()) {
        return fallback;
    }
    match t.parse::<u64>() {
        Ok(n) if n > 0 && n <= MAX_SAFE_INTEGER => n,
        _ => fallback,
    }
}

pub(crate) fn env_bool(name: &str, fallback: bool) -> bool {
    let Some(raw) = raw(name) else {
        return fallback;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => fallback,
    }
}

pub(crate) fn env_string(name: &str, fallback: &str) -> String {
    match raw(name) {
        Some(v) if !v.is_empty() => v,
        _ => fallback.to_string(),
    }
}

/// Send the message of an UNHANDLED service error back to the caller. On by
/// default: a protobus caller is another of your own services, inside the trust
/// boundary. Turn it off (`PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`) for a
/// service whose callers relay errors to untrusted clients. A handled error
/// always crosses.
pub fn expose_internal_errors() -> bool {
    env_bool("PROTOBUS_EXPOSE_INTERNAL_ERRORS", true)
}

/// RPC requests (topic). `BUS_EXCHANGE_NAME`, default `proto.bus`.
pub fn bus_exchange_name() -> String {
    env_string("BUS_EXCHANGE_NAME", "proto.bus")
}

/// RPC replies (direct). `CALLBACKS_EXCHANGE_NAME`, default `proto.bus.callback`.
pub fn callbacks_exchange_name() -> String {
    env_string("CALLBACKS_EXCHANGE_NAME", "proto.bus.callback")
}

/// Stream cancellation (fanout, because a cancel has to reach the one replica
/// running that stream). `CANCEL_EXCHANGE_NAME`, default `proto.bus.cancel`.
pub fn cancel_exchange_name() -> String {
    env_string("CANCEL_EXCHANGE_NAME", "proto.bus.cancel")
}

/// Events (topic). `EVENTS_EXCHANGE_NAME`, default `proto.bus.events`.
pub fn events_exchange_name() -> String {
    env_string("EVENTS_EXCHANGE_NAME", "proto.bus.events")
}

/// One attempt at a unary request. `MESSAGE_PROCESSING_TIMEOUT`, default 600000 ms.
pub fn message_processing_timeout_ms() -> u64 {
    env_int("MESSAGE_PROCESSING_TIMEOUT", 600_000)
}

/// How long a unary caller waits. `RPC_CALL_TIMEOUT_MS`, default 600000 ms.
pub fn rpc_call_timeout_ms() -> u64 {
    env_int("RPC_CALL_TIMEOUT_MS", 600_000)
}

/// The longest gap between stream chunks. `STREAM_IDLE_TIMEOUT_MS`, default 60000 ms.
pub fn stream_idle_timeout_ms() -> u64 {
    env_int("STREAM_IDLE_TIMEOUT_MS", 60_000)
}

/// Prefetch for late-ack consumers that set none. `DEFAULT_PREFETCH`, default 1.
pub fn default_prefetch() -> u64 {
    env_int("DEFAULT_PREFETCH", 1)
}

/// How long a publish waits for its confirm. Expiry is AMBIGUOUS: the broker
/// may have stored the message. `PUBLISH_CONFIRM_TIMEOUT_MS`, default 30000 ms.
pub fn publish_confirm_timeout_ms() -> u64 {
    env_int("PUBLISH_CONFIRM_TIMEOUT_MS", 30_000)
}

/// Heartbeat, in seconds. A `heartbeat` in the broker URL wins, and
/// `heartbeat=0` there disables it. `AMQP_HEARTBEAT_SECONDS`, default 30.
pub fn heartbeat_seconds() -> u64 {
    env_int("AMQP_HEARTBEAT_SECONDS", 30)
}

/// How long a publish waits through a reconnection.
/// `CONNECTION_READY_TIMEOUT_MS`, default 30000 ms.
pub fn connection_ready_timeout_ms() -> u64 {
    env_int("CONNECTION_READY_TIMEOUT_MS", 30_000)
}

/// Publishes the broker has not yet answered, per channel, a timed-out one
/// included; further ones wait for a slot. `MAX_OUTSTANDING_CONFIRMS`, default 256.
pub fn max_outstanding_confirms() -> u64 {
    env_int("MAX_OUTSTANDING_CONFIRMS", 256)
}

/// Unconsumed chunks per stream. `STREAM_MAX_BUFFERED_CHUNKS`, default 1024.
pub fn stream_max_buffered_chunks() -> u64 {
    env_int("STREAM_MAX_BUFFERED_CHUNKS", 1024)
}

/// Unconsumed bytes per stream. `STREAM_MAX_BUFFERED_BYTES`, default 64 MiB.
pub fn stream_max_buffered_bytes() -> u64 {
    env_int("STREAM_MAX_BUFFERED_BYTES", 64 * 1024 * 1024)
}

/// Unconsumed bytes across a context's streams.
/// `STREAM_MAX_TOTAL_BUFFERED_BYTES`, default 256 MiB.
pub fn stream_max_total_buffered_bytes() -> u64 {
    env_int("STREAM_MAX_TOTAL_BUFFERED_BYTES", 256 * 1024 * 1024)
}

/// How long shutdown waits for in-flight work. `SHUTDOWN_DRAIN_TIMEOUT_MS`, default 30000 ms.
pub fn shutdown_drain_timeout_ms() -> u64 {
    env_int("SHUTDOWN_DRAIN_TIMEOUT_MS", 30_000)
}

/// A broker URL with its password removed, for logs.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let Some(at) = authority.rfind('@') else {
        return url.to_string();
    };
    let userinfo = &authority[..at];
    let user = userinfo.split(':').next().unwrap_or("");
    if !userinfo.contains(':') {
        return url.to_string();
    }
    format!("{}://{}:***@{}", &url[..scheme_end], user, &rest[at + 1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_parse_strictly() {
        set("PB_T_INT", "250");
        assert_eq!(env_int("PB_T_INT", 7), 250);
        for bad in [
            "",
            " ",
            "0",
            "-5",
            "12ms",
            "1e3",
            "0x10",
            "99999999999999999999",
            "9007199254740992",
        ] {
            set("PB_T_INT", bad);
            assert_eq!(env_int("PB_T_INT", 7), 7, "{bad:?}");
        }
        set("PB_T_INT", " 42 ");
        assert_eq!(env_int("PB_T_INT", 7), 42);
        unset("PB_T_INT");
        assert_eq!(env_int("PB_T_INT", 7), 7);
    }

    #[test]
    fn booleans_parse_strictly() {
        for (v, want) in [
            ("1", true),
            ("TRUE", true),
            ("yes", true),
            ("On", true),
            ("0", false),
            ("no", false),
            ("OFF", false),
        ] {
            set("PB_T_BOOL", v);
            assert_eq!(env_bool("PB_T_BOOL", !want), want, "{v}");
        }
        set("PB_T_BOOL", "maybe");
        assert!(env_bool("PB_T_BOOL", true));
        assert!(!env_bool("PB_T_BOOL", false));
        unset("PB_T_BOOL");
    }

    #[test]
    fn urls_are_redacted() {
        assert_eq!(
            redact_url("amqp://guest:secret@host:5672/vh"),
            "amqp://guest:***@host:5672/vh"
        );
        assert_eq!(redact_url("amqp://host/"), "amqp://host/");
        assert_eq!(redact_url("amqps://u:p@h?heartbeat=5"), "amqps://u:***@h?heartbeat=5");
        assert_eq!(redact_url("amqp://user@host/"), "amqp://user@host/");
    }
}
