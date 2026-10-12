//! The seam between protobus and an AMQP 0-9-1 client.
//!
//! protobus talks to the broker only through the traits here. The production
//! implementation is [`LapinTransport`], a thin layer over
//! [lapin](https://docs.rs/lapin); tests substitute the in-memory broker of
//! [`crate::testing`], which has the same semantics and is what lets
//! settlement, retry, reconnection and streaming be tested deterministically
//! without RabbitMQ.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;

mod lapin_transport;

pub use lapin_transport::LapinTransport;

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An AMQP field table: message headers, queue arguments.
pub type FieldTable = BTreeMap<String, FieldValue>;

/// An AMQP field-table value. The kind is kept, so a header read from one
/// delivery is written back with the same encoding when the message is
/// republished on the retry and dead-letter paths.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Void,
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    Float(f32),
    Double(f64),
    Decimal {
        scale: u8,
        value: u32,
    },
    /// A long string holding UTF-8 text.
    String(String),
    /// A long string holding bytes that are not UTF-8.
    LongBytes(Vec<u8>),
    ShortString(String),
    /// A byte array (`x`).
    Bytes(Vec<u8>),
    /// Seconds since the epoch.
    Timestamp(u64),
    Table(FieldTable),
    Array(Vec<FieldValue>),
}

impl FieldValue {
    /// A tolerant integer reader, accepting every encoding peers produce:
    /// integers of any width, whole floats, or decimal text.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            FieldValue::I8(v) => Some(i64::from(*v)),
            FieldValue::U8(v) => Some(i64::from(*v)),
            FieldValue::I16(v) => Some(i64::from(*v)),
            FieldValue::U16(v) => Some(i64::from(*v)),
            FieldValue::I32(v) => Some(i64::from(*v)),
            FieldValue::U32(v) => Some(i64::from(*v)),
            FieldValue::I64(v) => Some(*v),
            FieldValue::U64(v) => i64::try_from(*v).ok(),
            FieldValue::Timestamp(v) => i64::try_from(*v).ok(),
            FieldValue::Float(v) => whole(f64::from(*v)),
            FieldValue::Double(v) => whole(*v),
            FieldValue::String(s) | FieldValue::ShortString(s) => s.trim().parse().ok(),
            FieldValue::LongBytes(b) | FieldValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().parse().ok(),
            _ => None,
        }
    }

    /// A tolerant boolean reader: a boolean, a non-zero number, or the text
    /// `true` or `1`. Anything else is false.
    pub fn as_bool(&self) -> bool {
        match self {
            FieldValue::Bool(b) => *b,
            FieldValue::Float(v) => *v != 0.0,
            FieldValue::Double(v) => *v != 0.0,
            FieldValue::String(s) | FieldValue::ShortString(s) => s.eq_ignore_ascii_case("true") || s == "1",
            FieldValue::LongBytes(b) | FieldValue::Bytes(b) => {
                std::str::from_utf8(b).is_ok_and(|s| s.eq_ignore_ascii_case("true") || s == "1")
            }
            other => other.as_int().is_some_and(|n| n != 0),
        }
    }

    /// Text for strings, a decimal rendering for numbers and booleans.
    pub fn as_text(&self) -> Option<String> {
        match self {
            FieldValue::String(s) | FieldValue::ShortString(s) => Some(s.clone()),
            FieldValue::LongBytes(b) | FieldValue::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
            FieldValue::Bool(b) => Some(b.to_string()),
            FieldValue::Float(v) => Some(v.to_string()),
            FieldValue::Double(v) => Some(v.to_string()),
            other => other.as_int().map(|n| n.to_string()),
        }
    }
}

fn whole(v: f64) -> Option<i64> {
    (v.is_finite() && v.fract() == 0.0 && v.abs() < 9.3e18).then_some(v as i64)
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        FieldValue::Bool(v)
    }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        FieldValue::I64(v)
    }
}

impl From<i32> for FieldValue {
    fn from(v: i32) -> Self {
        FieldValue::I32(v)
    }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        FieldValue::String(v.to_string())
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        FieldValue::String(v)
    }
}

/// AMQP basic properties. Absent stays absent: nothing is written for an unset
/// property.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Properties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<FieldTable>,
    /// 2 = persistent.
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    /// Seconds since the epoch, as AMQP carries it.
    pub timestamp: Option<u64>,
    pub r#type: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
}

/// One message as the broker delivered it.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivery {
    pub body: Vec<u8>,
    pub properties: Properties,
    pub exchange: String,
    pub routing_key: String,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
}

/// How the broker answered a publish on a confirm channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfirmOutcome {
    /// Stored.
    Ack,
    /// Refused (`basic.nack`).
    Nack,
    /// A mandatory publish matched no queue: returned, then acked.
    Returned,
    /// The channel closed with the publish unconfirmed: the outcome is UNKNOWN.
    Closed,
}

/// The broker's answer to one publish, with any detail it gave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Confirm {
    pub outcome: ConfirmOutcome,
    pub detail: String,
}

impl Confirm {
    pub fn new(outcome: ConfirmOutcome, detail: impl Into<String>) -> Self {
        Confirm {
            outcome,
            detail: detail.into(),
        }
    }
}

/// Resolves, exactly once, with the broker's answer to a publish. It always
/// resolves: with [`ConfirmOutcome::Closed`] when the channel goes first.
pub type ConfirmFuture = BoxFuture<'static, Confirm>;

/// A broker-reported failure: a channel or connection exception
/// (`reply_code` 404, 406, ...) or a client-side transport failure (0).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AmqpError {
    pub message: String,
    pub reply_code: u16,
}

impl AmqpError {
    pub fn new(message: impl Into<String>, reply_code: u16) -> Self {
        AmqpError {
            message: message.into(),
            reply_code,
        }
    }

    /// 406 PRECONDITION_FAILED: a declare whose arguments disagree with the
    /// existing object's.
    pub fn precondition_failed(&self) -> bool {
        self.reply_code == 406
    }
}

impl fmt::Display for AmqpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AmqpError {}

impl From<AmqpError> for crate::Error {
    fn from(e: AmqpError) -> Self {
        crate::Error::amqp(e.message, e.reply_code)
    }
}

/// Receives one consumer's deliveries. The next delivery is not handed over
/// until the future the previous call returned has completed, so it should
/// return quickly unless order is the point.
pub type DeliveryCallback = Arc<dyn Fn(Delivery) -> BoxFuture<'static, ()> + Send + Sync>;

/// Runs when the broker cancels a consumer (its queue was deleted, say).
pub type CancelCallback = Arc<dyn Fn() + Send + Sync>;

/// One confirm-mode channel. A channel-level failure closes the channel, as
/// AMQP specifies.
#[async_trait]
pub trait Channel: Send + Sync {
    async fn declare_exchange(
        &self,
        name: &str,
        kind: &str,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        arguments: &FieldTable,
    ) -> Result<(), AmqpError>;

    /// Returns the queue's name: the given one, or the broker's for `""`.
    async fn declare_queue(
        &self,
        name: &str,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        arguments: &FieldTable,
    ) -> Result<String, AmqpError>;

    async fn bind_queue(&self, queue: &str, exchange: &str, routing_key: &str) -> Result<(), AmqpError>;

    async fn unbind_queue(&self, queue: &str, exchange: &str, routing_key: &str) -> Result<(), AmqpError>;

    async fn delete_queue(&self, name: &str) -> Result<(), AmqpError>;

    async fn purge_queue(&self, name: &str) -> Result<(), AmqpError>;

    /// Per-consumer prefetch (`basic.qos`, global false).
    async fn prefetch(&self, count: u16) -> Result<(), AmqpError>;

    /// Start a consumer; returns its tag. `on_cancel` runs when the broker
    /// cancels it.
    async fn consume(
        &self,
        queue: &str,
        consumer_tag: &str,
        no_ack: bool,
        exclusive: bool,
        on_delivery: DeliveryCallback,
        on_cancel: Option<CancelCallback>,
    ) -> Result<String, AmqpError>;

    async fn cancel(&self, consumer_tag: &str) -> Result<(), AmqpError>;

    async fn ack(&self, delivery_tag: u64) -> Result<(), AmqpError>;

    async fn reject(&self, delivery_tag: u64, requeue: bool) -> Result<(), AmqpError>;

    /// Publish. Fails only when nothing could be written; otherwise the
    /// returned future reports the broker's answer.
    async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        properties: &Properties,
        mandatory: bool,
    ) -> Result<ConfirmFuture, AmqpError>;

    async fn close(&self) -> Result<(), AmqpError>;

    fn is_open(&self) -> bool;

    /// Called once when the channel closes for any reason; at once if it has.
    fn on_close(&self, listener: Box<dyn FnOnce(String) + Send>);
}

/// One broker connection.
#[async_trait]
pub trait Connection: Send + Sync {
    async fn open_channel(&self) -> Result<Arc<dyn Channel>, AmqpError>;

    /// A graceful close. The close listener then reports no error.
    async fn close(&self) -> Result<(), AmqpError>;

    fn is_open(&self) -> bool;

    /// Called once when the connection closes: with `None` after
    /// [`close`](Self::close), with the reason when it was lost. At once if it
    /// already has.
    fn on_close(&self, listener: Box<dyn FnOnce(Option<String>) + Send>);
}

/// Opens connections.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect and log in. `heartbeat_seconds` applies unless the URL carries a
    /// `heartbeat` parameter, which wins (0 disables heartbeats).
    async fn connect(&self, url: &str, heartbeat_seconds: u16) -> Result<Arc<dyn Connection>, AmqpError>;
}

/// Close listeners for a channel or connection: each runs exactly once.
pub(crate) struct CloseListeners<T: Clone + Send + 'static> {
    state: std::sync::Mutex<CloseState<T>>,
}

enum CloseState<T> {
    Open(Vec<Box<dyn FnOnce(T) + Send>>),
    Closed(T),
}

impl<T: Clone + Send + 'static> CloseListeners<T> {
    pub(crate) fn new() -> Self {
        CloseListeners {
            state: std::sync::Mutex::new(CloseState::Open(Vec::new())),
        }
    }

    pub(crate) fn add(&self, listener: Box<dyn FnOnce(T) + Send>) {
        let reason = {
            let mut state = self.state.lock().unwrap();
            match &mut *state {
                CloseState::Open(listeners) => {
                    listeners.push(listener);
                    return;
                }
                CloseState::Closed(reason) => reason.clone(),
            }
        };
        listener(reason);
    }

    /// Returns false when it had already fired.
    pub(crate) fn fire(&self, reason: T) -> bool {
        let listeners = {
            let mut state = self.state.lock().unwrap();
            match std::mem::replace(&mut *state, CloseState::Closed(reason.clone())) {
                CloseState::Open(listeners) => listeners,
                CloseState::Closed(first) => {
                    *state = CloseState::Closed(first);
                    return false;
                }
            }
        };
        for l in listeners {
            l(reason.clone());
        }
        true
    }

    pub(crate) fn fired(&self) -> bool {
        matches!(&*self.state.lock().unwrap(), CloseState::Closed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_read_from_any_encoding() {
        assert_eq!(FieldValue::I8(-3).as_int(), Some(-3));
        assert_eq!(FieldValue::U64(7).as_int(), Some(7));
        assert_eq!(FieldValue::U64(u64::MAX).as_int(), None);
        assert_eq!(FieldValue::Double(3.0).as_int(), Some(3));
        assert_eq!(FieldValue::Double(3.5).as_int(), None);
        assert_eq!(FieldValue::String(" 12 ".into()).as_int(), Some(12));
        assert_eq!(FieldValue::Bytes(b"9".to_vec()).as_int(), Some(9));
        assert_eq!(FieldValue::String("x".into()).as_int(), None);
        assert_eq!(FieldValue::Bool(true).as_int(), None);
    }

    #[test]
    fn booleans_read_from_any_encoding() {
        assert!(FieldValue::Bool(true).as_bool());
        assert!(FieldValue::I32(1).as_bool());
        assert!(!FieldValue::I32(0).as_bool());
        assert!(FieldValue::String("TRUE".into()).as_bool());
        assert!(FieldValue::String("1".into()).as_bool());
        assert!(!FieldValue::String("yes".into()).as_bool());
        assert!(!FieldValue::Void.as_bool());
    }

    #[test]
    fn close_listeners_fire_once() {
        let l = CloseListeners::<String>::new();
        let hits = Arc::new(std::sync::Mutex::new(Vec::new()));
        let h = hits.clone();
        l.add(Box::new(move |r| h.lock().unwrap().push(r)));
        assert!(l.fire("first".into()));
        assert!(!l.fire("second".into()));
        let h = hits.clone();
        l.add(Box::new(move |r| h.lock().unwrap().push(r)));
        assert_eq!(*hits.lock().unwrap(), ["first", "first"]);
    }
}
