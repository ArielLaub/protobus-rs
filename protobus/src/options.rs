//! Options for calls, services and contexts.

use std::sync::Arc;

use crate::amqp::Transport;
use crate::connection::ReconnectionOptions;
use crate::listeners::{EventRetryOptions, RetryOptions};

/// Options for a unary call or a one-way publish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallOptions {
    /// Free-text caller identity, carried in the envelope for tracing. Not
    /// authenticated.
    pub actor: Option<String>,
    /// `false`: publish without waiting for a reply; the call completes once
    /// the broker confirms it.
    pub rpc: bool,
    /// How long the call may take; `None` for `RPC_CALL_TIMEOUT_MS`. The
    /// deadline starts once the connection is ready and bounds the confirm as
    /// well as the reply.
    pub timeout_ms: Option<u64>,
    /// AMQP priority, 0-255; only a queue declared with a max priority honours it.
    pub priority: Option<i64>,
    /// The message's identity, as the consumer sees it; `None` for a fresh
    /// UUID. Set it to make a republish after an AMBIGUOUS failure
    /// recognisable: derive it from the work, never a clock. Refused when
    /// blank or longer than 255 bytes.
    pub message_id: Option<String>,
}

impl Default for CallOptions {
    fn default() -> Self {
        CallOptions {
            actor: None,
            rpc: true,
            timeout_ms: None,
            priority: None,
            message_id: None,
        }
    }
}

impl CallOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Publish without waiting for a reply.
    pub fn one_way(mut self) -> Self {
        self.rpc = false;
        self
    }

    pub fn timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = Some(ms);
        self
    }

    pub fn priority(mut self, priority: i64) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn message_id(mut self, id: impl Into<String>) -> Self {
        self.message_id = Some(id.into());
        self
    }
}

/// Options for a streaming call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamOptions {
    pub actor: Option<String>,
    /// The longest gap between chunks; `None` for `STREAM_IDLE_TIMEOUT_MS`.
    pub idle_timeout_ms: Option<u64>,
}

impl StreamOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    pub fn idle_timeout_ms(mut self, ms: u64) -> Self {
        self.idle_timeout_ms = Some(ms);
        self
    }
}

/// How a service consumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageServiceOptions {
    /// Requests handled in parallel by this process: the queue's prefetch.
    /// Default 1.
    pub max_concurrent: Option<u32>,
    pub retry: RetryOptions,
    /// Ack after the handler completes (the default) rather than on delivery.
    /// Acking on delivery disables retry, dead-lettering and error replies:
    /// set it false only for genuine at-most-once delivery.
    pub late_ack: bool,
    /// One attempt's limit; `None` for `MESSAGE_PROCESSING_TIMEOUT`.
    pub processing_timeout_ms: Option<u64>,
    /// Declare the queue with this `x-max-priority`; `None` for a plain queue.
    pub max_priority: Option<i64>,
    /// Retry for this service's event subscriptions; off by default.
    pub event_retry: EventRetryOptions,
    /// Run this service's handlers one at a time: requests, stream producers
    /// and events never overlap. Off by default: handlers run in parallel, and
    /// the `Send + Sync` bound on a handler already makes shared state safe.
    /// Turn it on when the work itself must not interleave. Time spent waiting
    /// for the turn counts against the processing timeout, and a handler that
    /// calls its own service and waits for the reply is refused instead of
    /// deadlocking.
    pub serialize_handlers: bool,
}

impl Default for MessageServiceOptions {
    fn default() -> Self {
        MessageServiceOptions {
            max_concurrent: None,
            retry: RetryOptions::default(),
            late_ack: true,
            processing_timeout_ms: None,
            max_priority: None,
            event_retry: EventRetryOptions::default(),
            serialize_handlers: false,
        }
    }
}

/// How a context connects.
#[derive(Clone, Default)]
pub struct ContextOptions {
    /// Reconnection backoff; `None` for the defaults.
    pub reconnection: Option<ReconnectionOptions>,
    /// The AMQP transport; `None` for RabbitMQ. Tests pass a
    /// [`MemoryBroker`](crate::testing::MemoryBroker).
    pub transport: Option<Arc<dyn Transport>>,
}
