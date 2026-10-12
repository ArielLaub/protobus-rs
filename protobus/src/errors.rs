//! The protobus error model.
//!
//! Every failure protobus reports is an [`Error`]: a [`kind`](Error::kind), a
//! message, and optionally a machine-readable [`code`](Error::code). The code
//! crosses the wire in a `ResponseError` and is shared with every other port;
//! the error's [`name`](Error::name) and code together form the `x-last-error`
//! header on retry and dead-letter copies.

use std::fmt;

use crate::config;

/// `Result` with a protobus [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// What kind of failure an [`Error`] is. Each corresponds to an error class of
/// the TypeScript reference, named by [`Error::name`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An expected failure a service answers with: never retried, and its
    /// message always reaches the caller.
    Handled,
    /// The message could not be understood. Handled by definition: it fails
    /// the same way on every redelivery.
    Protocol,
    /// The request named a method this service does not serve.
    InvalidMethod,
    /// Substituted for an unhandled service error when exposure is off.
    InternalService,
    /// An error a service answered with, as its caller receives it.
    Remote,
    /// A unary call got no reply within its timeout.
    RpcTimeout,
    /// A handler exceeded its processing timeout.
    ProcessingTimeout,
    /// The broker refused the message: definitely not stored.
    PublishNacked,
    /// A mandatory publish matched no queue.
    Unroutable,
    /// No confirm in time. AMBIGUOUS, unless the publish was never sent.
    PublishConfirmTimeout,
    /// The channel closed before the confirm. AMBIGUOUS.
    ChannelClosed,
    /// No stream chunk within the idle timeout.
    StreamTimeout,
    /// The stream's consumer fell behind past a buffer bound.
    StreamBackpressure,
    /// A stream chunk was lost: a gap in the sequence numbers.
    StreamSequence,
    /// The stream's caller cancelled it; seen by a producer that keeps writing.
    StreamCancelled,
    /// The connection is not carrying traffic and did not come back in time.
    NotReady,
    Reconnection,
    AlreadyConnected,
    /// The connection was lost, or the context closed, with a call pending.
    Disconnected,
    NotConnected,
    NotInitialized,
    AlreadyInitialized,
    AlreadyStarted,
    InvalidPriority,
    InvalidMessageId,
    RetryQueueMismatch,
    MissingProto,
    InvalidServiceName,
    InvalidRequest,
    InvalidResponse,
    InvalidResult,
    InvalidMessage,
    InvalidMethodName,
    UnknownMethod,
    Schema,
    CustomTypeRange,
    CustomTypeConflict,
    /// A broker-reported failure, or a client-side transport failure.
    Amqp,
    /// Anything else a handler fails with: an infrastructure failure, retried
    /// and, once the retries are spent, dead-lettered.
    Unhandled,
}

/// Error codes that travel on the wire, shared with the other ports.
pub mod codes {
    pub const HANDLED: &str = "HANDLED_ERROR";
    pub const PROTOCOL: &str = "PROTOCOL_ERROR";
    pub const INTERNAL: &str = "INTERNAL_ERROR";
    pub const PROCESSING_TIMEOUT: &str = "PROCESSING_TIMEOUT";
    pub const RPC_TIMEOUT: &str = "RPC_TIMEOUT";
    pub const PUBLISH_NACKED: &str = "PUBLISH_NACKED";
    pub const UNROUTABLE: &str = "UNROUTABLE";
    pub const PUBLISH_CONFIRM_TIMEOUT: &str = "PUBLISH_CONFIRM_TIMEOUT";
    pub const CHANNEL_CLOSED: &str = "CHANNEL_CLOSED";
    pub const NOT_READY: &str = "NOT_READY";
}

/// A protobus failure. One pointer wide, so a `Result` carrying it stays small.
#[derive(Clone)]
pub struct Error {
    inner: Box<Inner>,
}

#[derive(Clone, Debug)]
struct Inner {
    kind: ErrorKind,
    name: Option<String>,
    message: String,
    code: Option<String>,
    message_id: Option<String>,
    method: Option<String>,
    reply_code: u16,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Error");
        d.field("kind", &self.inner.kind).field("message", &self.inner.message);
        if let Some(code) = &self.inner.code {
            d.field("code", code);
        }
        if let Some(name) = &self.inner.name {
            d.field("name", name);
        }
        if let Some(id) = &self.inner.message_id {
            d.field("message_id", id);
        }
        if let Some(method) = &self.inner.method {
            d.field("method", method);
        }
        if self.inner.reply_code != 0 {
            d.field("reply_code", &self.inner.reply_code);
        }
        d.finish()
    }
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        let code = match kind {
            ErrorKind::Handled => Some(codes::HANDLED),
            ErrorKind::Protocol | ErrorKind::InvalidMethod => Some(codes::PROTOCOL),
            ErrorKind::InternalService => Some(codes::INTERNAL),
            ErrorKind::ProcessingTimeout => Some(codes::PROCESSING_TIMEOUT),
            ErrorKind::RpcTimeout => Some(codes::RPC_TIMEOUT),
            ErrorKind::PublishNacked => Some(codes::PUBLISH_NACKED),
            ErrorKind::Unroutable => Some(codes::UNROUTABLE),
            ErrorKind::PublishConfirmTimeout => Some(codes::PUBLISH_CONFIRM_TIMEOUT),
            ErrorKind::ChannelClosed => Some(codes::CHANNEL_CLOSED),
            ErrorKind::NotReady => Some(codes::NOT_READY),
            _ => None,
        };
        Error {
            inner: Box::new(Inner {
                kind,
                name: None,
                message: message.into(),
                code: code.map(str::to_string),
                message_id: None,
                method: None,
                reply_code: 0,
            }),
        }
    }

    /// An expected failure: a validation error, a business rule. The caller
    /// receives the message and the code (`HANDLED_ERROR` unless
    /// [`with_code`](Self::with_code) sets one), and the request is never retried.
    ///
    /// ```
    /// use protobus::Error;
    /// let e = Error::handled("no such account").with_code("NOT_FOUND");
    /// assert!(e.is_handled());
    /// assert_eq!(e.code(), Some("NOT_FOUND"));
    /// ```
    pub fn handled(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::Handled, message)
    }

    /// An infrastructure failure with a message of your own. The request is
    /// retried and, once the retries are spent, dead-lettered.
    pub fn failure(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::Unhandled, message)
    }

    /// An infrastructure failure wrapping any error, keeping its type's name
    /// for the `x-last-error` header:
    /// `db.get(id).await.map_err(Error::unhandled)?`.
    pub fn unhandled<E: fmt::Display>(error: E) -> Self {
        let mut e = Error::new(ErrorKind::Unhandled, error.to_string());
        let full = std::any::type_name::<E>();
        // The last path segment of the outermost type: `std::num::ParseIntError` -> `ParseIntError`.
        let outer = full.split('<').next().unwrap_or(full);
        let short = outer.rsplit("::").next().unwrap_or(outer).trim_start_matches('&');
        if !short.is_empty() && short != "String" && short != "str" {
            e.inner.name = Some(short.to_string());
        }
        e
    }

    /// The answer to a request for a method the service declares but does not
    /// implement: `PROTOCOL_ERROR`, never retried. Generated handler traits
    /// default every rpc to it.
    pub fn unimplemented(method: &str) -> Self {
        Error::invalid_method(format!("invalid service method {method}"))
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::Protocol, message)
    }

    pub(crate) fn invalid_method(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::InvalidMethod, message)
    }

    /// The error a caller sees for a service's `ResponseError`.
    pub fn remote(message: impl Into<String>, code: &str, method: impl Into<String>) -> Self {
        let mut e = Error::new(ErrorKind::Remote, message);
        e.inner.code = if code.is_empty() { None } else { Some(code.to_string()) };
        e.inner.method = Some(method.into());
        e
    }

    pub(crate) fn internal_service(correlation_id: &str) -> Self {
        Error::new(
            ErrorKind::InternalService,
            if correlation_id.is_empty() {
                "internal service error".to_string()
            } else {
                format!("internal service error (correlationId {correlation_id})")
            },
        )
    }

    pub(crate) fn amqp(message: impl Into<String>, reply_code: u16) -> Self {
        let mut e = Error::new(ErrorKind::Amqp, message);
        e.inner.reply_code = reply_code;
        e
    }

    /// Set the machine-readable code.
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        let code = code.into();
        self.inner.code = if code.is_empty() { None } else { Some(code) };
        self
    }

    /// Set the name this error is reported under in `x-last-error`.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.inner.name = Some(name.into());
        self
    }

    pub(crate) fn with_message_id(mut self, message_id: impl Into<String>) -> Self {
        self.inner.message_id = Some(message_id.into());
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.inner.kind
    }

    pub fn message(&self) -> &str {
        &self.inner.message
    }

    /// The stable, cross-language identifier of the failure (`RPC_TIMEOUT`,
    /// `UNROUTABLE`, a service's own `VALIDATION_ERROR`, ...), if it has one.
    pub fn code(&self) -> Option<&str> {
        self.inner.code.as_deref()
    }

    /// The `messageId` a failed publish carried.
    pub fn message_id(&self) -> Option<&str> {
        self.inner.message_id.as_deref()
    }

    /// The method a [`Remote`](ErrorKind::Remote) error was reported against.
    pub fn method(&self) -> Option<&str> {
        self.inner.method.as_deref()
    }

    /// The AMQP reply code of a broker-reported failure (404, 406, ...), or 0.
    pub fn reply_code(&self) -> u16 {
        self.inner.reply_code
    }

    /// True for an expected failure that is answered rather than retried.
    pub fn is_handled(&self) -> bool {
        matches!(
            self.inner.kind,
            ErrorKind::Handled | ErrorKind::Protocol | ErrorKind::InvalidMethod
        )
    }

    /// True for a publish that did not demonstrably reach a queue.
    pub fn is_publish_error(&self) -> bool {
        matches!(
            self.inner.kind,
            ErrorKind::PublishNacked
                | ErrorKind::Unroutable
                | ErrorKind::PublishConfirmTimeout
                | ErrorKind::ChannelClosed
        )
    }

    /// True for a publish whose outcome is UNKNOWN: the broker may have stored
    /// it. Republish with the same `messageId`.
    pub fn is_ambiguous(&self) -> bool {
        matches!(
            self.inner.kind,
            ErrorKind::PublishConfirmTimeout | ErrorKind::ChannelClosed
        )
    }

    /// True for a failure a streaming call raises on the caller's side.
    pub fn is_streaming_error(&self) -> bool {
        matches!(
            self.inner.kind,
            ErrorKind::StreamTimeout | ErrorKind::StreamBackpressure | ErrorKind::StreamSequence
        )
    }

    /// The error's name as it appears in logs and retry headers: the class
    /// name the other ports give the same failure.
    pub fn name(&self) -> &str {
        if let Some(name) = &self.inner.name {
            return name;
        }
        match self.inner.kind {
            ErrorKind::Handled => "HandledError",
            ErrorKind::Protocol => "ProtocolError",
            ErrorKind::InvalidMethod => "InvalidMethodError",
            ErrorKind::InternalService => "InternalServiceError",
            ErrorKind::Remote => "RemoteError",
            ErrorKind::RpcTimeout => "RpcTimeoutError",
            ErrorKind::ProcessingTimeout => "TimeoutError",
            ErrorKind::PublishNacked => "PublishNackedError",
            ErrorKind::Unroutable => "UnroutableError",
            ErrorKind::PublishConfirmTimeout => "PublishConfirmTimeoutError",
            ErrorKind::ChannelClosed => "ChannelClosedError",
            ErrorKind::StreamTimeout => "StreamTimeoutError",
            ErrorKind::StreamBackpressure => "StreamBackpressureError",
            ErrorKind::StreamSequence => "StreamSequenceError",
            ErrorKind::StreamCancelled => "StreamCancelledError",
            ErrorKind::NotReady => "NotReadyError",
            ErrorKind::Reconnection => "ReconnectionError",
            ErrorKind::AlreadyConnected => "AlreadyConnectedError",
            ErrorKind::Disconnected => "DisconnectedError",
            ErrorKind::NotConnected => "NotConnectedError",
            ErrorKind::NotInitialized => "NotInitializedError",
            ErrorKind::AlreadyInitialized => "AlreadyInitializedError",
            ErrorKind::AlreadyStarted => "AlreadyStartedError",
            ErrorKind::InvalidPriority => "InvalidPriorityError",
            ErrorKind::InvalidMessageId => "InvalidMessageIdError",
            ErrorKind::RetryQueueMismatch => "RetryQueueMismatchError",
            ErrorKind::MissingProto => "MissingProtoError",
            ErrorKind::InvalidServiceName => "InvalidServiceNameError",
            ErrorKind::InvalidRequest => "InvalidRequestError",
            ErrorKind::InvalidResponse => "InvalidResponseError",
            ErrorKind::InvalidResult => "InvalidResultError",
            ErrorKind::InvalidMessage => "InvalidMessageError",
            ErrorKind::InvalidMethodName => "InvalidMethodNameError",
            ErrorKind::UnknownMethod => "UnknownMethodError",
            ErrorKind::Schema => "SchemaError",
            ErrorKind::CustomTypeRange => "CustomTypeRangeError",
            ErrorKind::CustomTypeConflict => "CustomTypeConflictError",
            ErrorKind::Amqp => "AmqpError",
            ErrorKind::Unhandled => "Error",
        }
    }

    /// The message, or the name when there is none.
    pub(crate) fn describe(&self) -> &str {
        if self.inner.message.is_empty() {
            self.name()
        } else {
            &self.inner.message
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.describe())
    }
}

impl std::error::Error for Error {}

#[cfg(feature = "proto-text")]
impl From<protobus_schema::SchemaError> for Error {
    fn from(e: protobus_schema::SchemaError) -> Self {
        Error::new(ErrorKind::Schema, e.0)
    }
}

/// What an error looks like to the caller. A handled error and a processing
/// timeout pass through; anything else becomes `INTERNAL_ERROR` unless
/// `PROTOBUS_EXPOSE_INTERNAL_ERRORS` is on (the default).
pub fn sanitize_error_for_client(error: &Error, correlation_id: &str) -> Error {
    if error.is_handled() || error.kind() == ErrorKind::ProcessingTimeout || config::expose_internal_errors() {
        return error.clone();
    }
    Error::internal_service(correlation_id)
}

/// A non-disclosing description: name and code, never the message, except for
/// a handled error, which is meant to be seen. It travels in `x-last-error`.
pub fn safe_error_summary(error: &Error) -> String {
    let name = error.name();
    match (error.is_handled(), error.code()) {
        (true, code) => format!("{name}[{}]: {}", code.unwrap_or(""), error.message()),
        (false, Some(code)) => format!("{name}[{code}]"),
        (false, None) => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handled_errors_carry_a_default_code() {
        let e = Error::handled("nope");
        assert_eq!(e.code(), Some("HANDLED_ERROR"));
        assert_eq!(e.name(), "HandledError");
        assert!(e.is_handled());
        assert_eq!(Error::handled("x").with_code("NOT_FOUND").code(), Some("NOT_FOUND"));
    }

    #[test]
    fn protocol_errors_are_handled() {
        assert!(Error::protocol("bad").is_handled());
        assert_eq!(Error::invalid_method("bad").code(), Some("PROTOCOL_ERROR"));
    }

    #[test]
    fn unhandled_keeps_the_source_type_name() {
        let parse = "x".parse::<i32>().unwrap_err();
        let e = Error::unhandled(parse);
        assert_eq!(e.name(), "ParseIntError");
        assert!(!e.is_handled());
        assert_eq!(Error::unhandled("plain text").name(), "Error");
        assert_eq!(Error::failure("boom").name(), "Error");
    }

    #[test]
    fn the_summary_never_discloses_an_unhandled_message() {
        assert_eq!(safe_error_summary(&Error::failure("secret dsn")), "Error");
        assert_eq!(
            safe_error_summary(&Error::new(ErrorKind::ProcessingTimeout, "took too long")),
            "TimeoutError[PROCESSING_TIMEOUT]"
        );
        assert_eq!(
            safe_error_summary(&Error::handled("no such account").with_code("NOT_FOUND")),
            "HandledError[NOT_FOUND]: no such account"
        );
    }

    #[test]
    fn remote_errors_keep_what_crossed_the_wire() {
        let e = Error::remote("m", "", "a.B.c");
        assert_eq!(e.code(), None);
        assert_eq!(e.method(), Some("a.B.c"));
        assert_eq!(e.to_string(), "m");
    }

    #[test]
    fn publish_errors_classify_ambiguity() {
        assert!(Error::new(ErrorKind::ChannelClosed, "").is_ambiguous());
        assert!(Error::new(ErrorKind::PublishConfirmTimeout, "").is_ambiguous());
        assert!(!Error::new(ErrorKind::PublishNacked, "").is_ambiguous());
        assert!(Error::new(ErrorKind::Unroutable, "").is_publish_error());
    }
}
