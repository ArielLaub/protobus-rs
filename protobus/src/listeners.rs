//! Listeners: a queue and its consumer, declared on `init`, consuming from
//! `start`, and restored after a reconnection (or a channel lost on a live
//! connection) without the owner doing anything.

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use prost::Message;
use prost_reflect::DynamicMessage;

use crate::amqp::{BoxFuture, FieldTable, FieldValue};
use crate::config;
use crate::connection::{
    ChannelHandle, Connection, ConsumeOptions, ConsumeRetryOptions, ErrorReplyBuilder, HandlerResult, ListenerId,
    MessageHandler, MessageHandlerContext,
};
use crate::errors::{Error, ErrorKind, Result};
use crate::message_factory::MessageFactory;
use crate::priority;
use crate::trie::Trie;
use crate::wire;

/// Retry for a service's requests: a failed request waits on
/// `<Service>.Retry`, comes back, and is dead-lettered to `<Service>.DLQ` once
/// the hops are spent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryOptions {
    /// Retry hops before the DLQ; 0 disables retry.
    pub max_retries: u32,
    /// The retry queue's `x-message-ttl`; fixed when first declared.
    pub retry_delay_ms: u64,
    /// A TTL for the service's own queue, or `None` for none.
    pub message_ttl_ms: Option<u64>,
}

impl Default for RetryOptions {
    fn default() -> Self {
        RetryOptions {
            max_retries: 3,
            retry_delay_ms: 5000,
            message_ttl_ms: None,
        }
    }
}

/// Opt-in retry for event handlers. Without it a handler that fails loses its
/// event. Enabled, events climb the ladder requests do, and a retried event
/// re-runs every matching handler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRetryOptions {
    pub max_retries: u32,
    pub retry_delay_ms: u64,
}

impl Default for EventRetryOptions {
    fn default() -> Self {
        EventRetryOptions {
            max_retries: 0,
            retry_delay_ms: 5000,
        }
    }
}

/// Which retry topology a listener declares beyond its main queue.
#[derive(Clone, Debug)]
enum RetryTopology {
    None,
    /// The retry queue dead-letters back to the listener's own exchange.
    Message {
        max_retries: u32,
        retry_delay_ms: u64,
    },
    /// An event's retry queue cannot dead-letter back to the events exchange
    /// (every subscriber would get it again): it goes to a per-subscriber
    /// exchange bound only to this listener's queue.
    Event {
        max_retries: u32,
        retry_delay_ms: u64,
    },
}

#[derive(Clone, Debug)]
struct ListenerConfig {
    name: &'static str,
    exchange_name: String,
    exchange_type: &'static str,
    late_ack: bool,
    /// Prefetch for late-ack consumers; `None` falls back to `DEFAULT_PREFETCH`.
    max_concurrent: Option<u32>,
    message_ttl_ms: Option<u64>,
    max_priority: Option<u8>,
    processing_timeout_ms: Option<u64>,
    /// Handle deliveries in order; see [`ConsumeOptions::ordered`].
    ordered: bool,
    retry: RetryTopology,
}

#[derive(Default)]
struct ListenerState {
    channel: Option<ChannelHandle>,
    queue_name: String,
    configured_queue_name: String,
    is_anonymous: bool,
    consumer_tag: String,
    handler: Option<MessageHandler>,
    bindings: Vec<String>,
    initialized: bool,
    was_started: bool,
    closing: bool,
    consumer_lost: bool,
    rebuild_scheduled: bool,
    rebuild_for: Option<ChannelHandle>,
    rebuild_failures: u32,
    restorer: Option<ListenerId>,
    disconnected: Option<ListenerId>,
    /// (retry queue, retry exchange, DLQ), once declared.
    retry_names: Option<(String, String, String)>,
    build_error_reply: Option<ErrorReplyBuilder>,
    on_queue_replaced: Option<Arc<dyn Fn() + Send + Sync>>,
}

struct ListenerInner {
    connection: Connection,
    config: ListenerConfig,
    state: Mutex<ListenerState>,
    /// Serialises restoration against start and stop: a listener is never consuming twice.
    restore_lock: tokio::sync::Mutex<()>,
}

/// A queue and its consumer.
#[derive(Clone)]
pub struct BaseListener {
    inner: Arc<ListenerInner>,
}

fn default_handler(name: &'static str) -> MessageHandler {
    Arc::new(move |message, correlation_id, _| {
        Box::pin(async move {
            // Payload size and correlation id only: never the body.
            log::warn!(
                target: "protobus",
                "{name}: unhandled message by default handler ({} bytes, correlationId {})",
                message.len(),
                if correlation_id.is_empty() { "none" } else { &correlation_id }
            );
            Ok(HandlerResult::None)
        })
    })
}

impl BaseListener {
    fn new(connection: &Connection, config: ListenerConfig) -> Self {
        let listener = BaseListener {
            inner: Arc::new(ListenerInner {
                connection: connection.clone(),
                config,
                state: Mutex::new(ListenerState {
                    is_anonymous: true,
                    ..ListenerState::default()
                }),
                restore_lock: tokio::sync::Mutex::new(()),
            }),
        };
        listener.attach();
        listener
    }

    fn weak(&self) -> Weak<ListenerInner> {
        Arc::downgrade(&self.inner)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ListenerState> {
        self.inner.state.lock().unwrap()
    }

    fn name(&self) -> &'static str {
        self.inner.config.name
    }

    fn connection(&self) -> &Connection {
        &self.inner.connection
    }

    /// Register with the connection: restoration after a reconnect, and
    /// forgetting the channel when the connection is lost. Through weak
    /// references, so a dropped listener is skipped.
    fn attach(&self) {
        let mut s = self.state();
        if s.restorer.is_none() {
            let weak = self.weak();
            s.restorer = Some(self.connection().register_restorer(Arc::new(move |_| {
                let weak = weak.clone();
                Box::pin(async move {
                    match weak.upgrade() {
                        Some(inner) => BaseListener { inner }.restore().await,
                        None => Ok(()),
                    }
                })
            })));
        }
        if s.disconnected.is_none() {
            let weak = self.weak();
            s.disconnected = Some(self.connection().on_disconnected(move || {
                if let Some(inner) = weak.upgrade() {
                    // The connection was lost: the channel went with it.
                    let mut s = inner.state.lock().unwrap();
                    s.channel = None;
                    s.consumer_tag.clear();
                }
            }));
        }
    }

    fn detach_restorer(&self) {
        let id = self.state().restorer.take();
        if let Some(id) = id {
            self.connection().unregister_restorer(id);
        }
    }

    /// Run `listener` when a restoration declared the queue under a new name.
    pub fn on_queue_replaced(&self, listener: impl Fn() + Send + Sync + 'static) {
        self.state().on_queue_replaced = Some(Arc::new(listener));
    }

    pub(crate) fn set_build_error_reply(&self, build: ErrorReplyBuilder) {
        self.state().build_error_reply = Some(build);
    }

    pub fn is_initialized(&self) -> bool {
        self.state().initialized
    }

    /// The queue's name: the configured one, or the broker's for an anonymous queue.
    pub fn queue_name(&self) -> String {
        self.state().queue_name.clone()
    }

    /// The main queue's arguments. Each key is added only when its option is
    /// set, so a listener that configures nothing declares an empty table, as
    /// every port does.
    fn queue_arguments(&self) -> FieldTable {
        let mut args = FieldTable::new();
        if let Some(ttl) = self.inner.config.message_ttl_ms {
            args.insert("x-message-ttl".into(), FieldValue::I64(ttl as i64));
        }
        if let Some(max) = self.inner.config.max_priority {
            args.insert("x-max-priority".into(), FieldValue::I32(i32::from(max)));
        }
        args
    }

    /// Prefetch for late-ack consumers. 0 would mean unlimited, which with late
    /// ack lets the broker push a whole backlog into memory.
    fn effective_prefetch(&self) -> u16 {
        let n = match self.inner.config.max_concurrent {
            Some(n) if n > 0 => u64::from(n),
            _ => config::default_prefetch(),
        };
        n.min(u64::from(u16::MAX)) as u16
    }

    /// Open a channel and declare the exchange and the queue on it.
    async fn open(&self) -> Result<(ChannelHandle, String)> {
        let cfg = &self.inner.config;
        let (anonymous, configured) = {
            let s = self.state();
            (s.is_anonymous, s.configured_queue_name.clone())
        };
        let channel = self.connection().open_channel().await?;
        if cfg.late_ack {
            channel.raw().prefetch(self.effective_prefetch()).await?;
        }
        self.connection()
            .declare_exchange(&channel, &cfg.exchange_name, cfg.exchange_type)
            .await?;
        // An anonymous queue is exclusive and server-named; it goes with its connection.
        let name = channel
            .raw()
            .declare_queue(
                if anonymous { "" } else { &configured },
                !anonymous,
                anonymous,
                anonymous,
                &self.queue_arguments(),
            )
            .await?;
        // A direct-exchange listener is bound to its own name.
        if cfg.exchange_type == "direct" {
            channel.raw().bind_queue(&name, &cfg.exchange_name, &name).await?;
        }
        {
            let mut s = self.state();
            s.channel = Some(channel.clone());
            s.queue_name = name.clone();
        }
        self.watch(&channel);
        Ok((channel, name))
    }

    /// Open the channel and declare the exchange and the queue: `queue_name`,
    /// or an exclusive, server-named one when it is `None` or empty.
    pub async fn init(&self, handler: Option<MessageHandler>, queue_name: Option<&str>) -> Result<()> {
        if self.state().initialized {
            return Ok(());
        }
        if !self.connection().is_connected() {
            return Err(Error::new(ErrorKind::NotConnected, "not connected"));
        }
        let queue = queue_name.unwrap_or("");
        {
            let mut s = self.state();
            s.handler = Some(handler.unwrap_or_else(|| default_handler(self.name())));
            s.is_anonymous = queue.is_empty();
            s.configured_queue_name = queue.to_string();
            s.closing = false;
        }
        self.attach();
        self.open().await?;
        self.state().initialized = true;
        // Before start, so the first delivery already has somewhere to fail to.
        if matches!(self.inner.config.retry, RetryTopology::Event { .. }) {
            self.setup_retry_topology().await?;
        }
        Ok(())
    }

    /// A channel can close while the connection stays up. The listener is
    /// rebuilt then, as on a reconnection; otherwise it would go quiet behind
    /// a healthy connection.
    fn watch(&self, channel: &ChannelHandle) {
        let weak = self.weak();
        let watched = channel.clone();
        channel.raw().on_close(Box::new(move |reason| {
            let Some(inner) = weak.upgrade() else { return };
            let listener = BaseListener { inner };
            {
                let s = listener.state();
                let current = s.channel.as_ref().is_some_and(|c| c.same(&watched));
                if s.closing || !s.initialized || !current {
                    return;
                }
            }
            if !listener.connection().is_ready() {
                return; // the reconnection restores it
            }
            listener.schedule_rebuild(format!("channel closed on a live connection ({reason})"));
        }));
    }

    /// One rebuild pending at a time, backing off while they keep failing.
    fn schedule_rebuild(&self, reason: String) {
        let failures = {
            let mut s = self.state();
            if s.rebuild_scheduled || s.closing || !s.initialized {
                return;
            }
            s.rebuild_scheduled = true;
            s.rebuild_for = s.channel.clone();
            s.rebuild_failures
        };
        let delay = (100u64 << failures.min(9)).min(30_000);
        log::warn!(target: "protobus", "{}: {reason}; rebuilding it in {delay}ms", self.name());
        let weak = self.weak();
        self.connection().spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if let Some(inner) = weak.upgrade() {
                BaseListener { inner }.rebuild().await;
            }
        });
    }

    async fn rebuild(&self) {
        {
            let mut s = self.state();
            s.rebuild_scheduled = false;
            if s.closing || !s.initialized {
                return;
            }
            if s.consumer_lost && !s.was_started {
                s.consumer_lost = false;
                return;
            }
            // A channel lost with its connection can report its close first;
            // the reconnection restores the listener, and rebuilding again
            // would replace a working consumer.
            let same = match (&s.rebuild_for, &s.channel) {
                (Some(a), Some(b)) => a.same(b),
                (None, None) => true,
                _ => false,
            };
            if !same {
                return;
            }
        }
        if !self.connection().is_ready() {
            return;
        }
        self.state().rebuild_failures += 1;
        match self.restore().await {
            Ok(()) => self.state().rebuild_failures = 0,
            Err(e) => {
                log::error!(target: "protobus", "{}: failed to rebuild its channel: {e}", self.name());
                self.schedule_rebuild(e.to_string());
            }
        }
    }

    /// Put this listener's channel, queue, bindings and consumer back. A
    /// failure propagates to the connection, which treats the whole generation
    /// as unusable and retries.
    async fn restore(&self) -> Result<()> {
        let _serial = self.inner.restore_lock.lock().await;
        let (started, bindings, previous, previous_queue) = {
            let s = self.state();
            if !s.initialized || s.closing {
                return Ok(());
            }
            (
                s.was_started,
                s.bindings.clone(),
                s.channel.clone(),
                s.queue_name.clone(),
            )
        };
        log::info!(target: "protobus", "{}: reconnected, re-initializing...", self.name());
        let (channel, queue) = self.open().await?;
        if queue != previous_queue {
            let replaced = self.state().on_queue_replaced.clone();
            if let Some(f) = replaced {
                f();
            }
        }
        for key in &bindings {
            channel
                .raw()
                .bind_queue(&queue, &self.inner.config.exchange_name, key)
                .await?;
        }
        if self.state().retry_names.is_some() {
            self.setup_retry_topology().await?;
        }
        let start = {
            let mut s = self.state();
            s.consumer_lost = false;
            started && s.was_started && !s.closing
        };
        if start {
            self.start_consuming().await?;
        }
        // The channel this replaces can still be open (one whose consumer the
        // broker cancelled); closed only now, so its close is not mistaken for
        // a loss.
        if let Some(previous) = previous {
            if !previous.same(&channel) && previous.is_open() {
                let _ = previous.close().await;
            }
        }
        log::info!(target: "protobus", "{}: successfully re-initialized after reconnection", self.name());
        Ok(())
    }

    /// `basic.cancel` leaves the channel open, so neither recovery would notice it.
    fn on_consumer_cancelled(&self, tag: &str) {
        {
            let mut s = self.state();
            if s.closing || !s.initialized || !s.was_started || s.consumer_tag != tag {
                return;
            }
            s.consumer_tag.clear();
            s.consumer_lost = true;
        }
        self.schedule_rebuild("the broker cancelled its consumer".into());
    }

    async fn start_consuming(&self) -> Result<()> {
        let cfg = &self.inner.config;
        let tag = uuid::Uuid::new_v4().to_string();
        let (channel, queue, handler, anonymous, build_error_reply) = {
            let mut s = self.state();
            s.consumer_tag = tag.clone();
            (
                s.channel.clone(),
                s.queue_name.clone(),
                s.handler.clone(),
                s.is_anonymous,
                s.build_error_reply.clone(),
            )
        };
        let (Some(channel), Some(handler)) = (channel, handler) else {
            return Err(Error::new(
                ErrorKind::NotConnected,
                format!("{}: no channel to consume on", self.name()),
            ));
        };
        let weak = self.weak();
        let cancelled_tag = tag.clone();
        let options = ConsumeOptions {
            consumer_tag: tag,
            no_ack: false,
            exclusive: anonymous,
            ordered: cfg.ordered,
            build_error_reply,
            on_cancelled: Some(Arc::new(move || {
                if let Some(inner) = weak.upgrade() {
                    BaseListener { inner }.on_consumer_cancelled(&cancelled_tag);
                }
            })),
            // An early-ack consumer gets no prefetch from the broker, so its
            // parallelism is bounded here.
            max_concurrency: if cfg.late_ack || cfg.ordered {
                0
            } else {
                cfg.max_concurrent.unwrap_or(1).max(1) as usize
            },
        };
        self.connection()
            .consume(
                &channel,
                &queue,
                handler,
                options,
                cfg.late_ack,
                self.retry_options(),
                cfg.processing_timeout_ms,
            )
            .await?;
        log::debug!(target: "protobus", "{}: started consuming from {queue}", self.name());
        Ok(())
    }

    fn retry_options(&self) -> Option<ConsumeRetryOptions> {
        let max_retries = match self.inner.config.retry {
            RetryTopology::Message { max_retries, .. } | RetryTopology::Event { max_retries, .. } => max_retries,
            RetryTopology::None => 0,
        };
        let (retry_queue_name, retry_exchange_name, dlq_name) = self.state().retry_names.clone()?;
        (max_retries > 0).then_some(ConsumeRetryOptions {
            max_retries,
            retry_queue_name,
            retry_exchange_name,
            dlq_name,
        })
    }

    /// Declare the DLQ, the retry queue and the retry exchange, after the main
    /// queue exists.
    async fn setup_retry_topology(&self) -> Result<()> {
        let (max_retries, retry_delay_ms, event) = match self.inner.config.retry {
            RetryTopology::None => return Ok(()),
            RetryTopology::Message {
                max_retries,
                retry_delay_ms,
            } => (max_retries, retry_delay_ms, false),
            RetryTopology::Event {
                max_retries,
                retry_delay_ms,
            } => (max_retries, retry_delay_ms, true),
        };
        let (anonymous, base, queue, channel) = {
            let s = self.state();
            (
                s.is_anonymous,
                s.configured_queue_name.clone(),
                s.queue_name.clone(),
                s.channel.clone(),
            )
        };
        if max_retries == 0 || anonymous {
            return Ok(());
        }
        let Some(channel) = channel else {
            return Err(Error::new(
                ErrorKind::NotConnected,
                format!("{}: not connected", self.name()),
            ));
        };
        let raw = channel.raw();
        let dlq = format!("{base}.DLQ");
        let retry_queue = format!("{base}.Retry");
        let retry_exchange = format!("{base}.Retry.Exchange");
        let none = FieldTable::new();
        raw.declare_queue(&dlq, true, false, false, &none).await?;
        let dead_letter_exchange = if event {
            let redelivery = format!("{base}.Redelivery");
            self.connection()
                .declare_exchange(&channel, &redelivery, "topic")
                .await?;
            raw.bind_queue(&queue, &redelivery, "#").await?;
            redelivery
        } else {
            self.inner.config.exchange_name.clone()
        };
        let mut args = FieldTable::new();
        args.insert("x-message-ttl".into(), FieldValue::I64(retry_delay_ms as i64));
        // No x-dead-letter-routing-key: the message's own routing key is kept.
        args.insert("x-dead-letter-exchange".into(), FieldValue::from(dead_letter_exchange));
        if let Err(e) = raw.declare_queue(&retry_queue, true, false, false, &args).await {
            if e.precondition_failed() {
                return Err(Error::new(
                    ErrorKind::RetryQueueMismatch,
                    format!(
                        "retry queue '{retry_queue}' already exists with different arguments (most likely a different \
                         retryDelayMs, now {retry_delay_ms}ms). RabbitMQ cannot change a queue's x-message-ttl in place: \
                         drain and delete the queue, or keep the original retryDelayMs. Original error: {e}"
                    ),
                ));
            }
            return Err(e.into());
        }
        self.connection()
            .declare_exchange(&channel, &retry_exchange, "topic")
            .await?;
        raw.bind_queue(&retry_queue, &retry_exchange, "#").await?;
        self.state().retry_names = Some((retry_queue, retry_exchange, dlq));
        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        {
            let s = self.state();
            if !s.initialized {
                return Err(Error::new(ErrorKind::NotInitialized, "not initialized"));
            }
            if s.was_started && (!s.consumer_tag.is_empty() || s.consumer_lost) {
                return Err(Error::new(ErrorKind::AlreadyStarted, "already started"));
            }
        }
        if !self.connection().is_connected() {
            return Err(Error::new(ErrorKind::NotConnected, "not connected"));
        }
        self.attach();
        let _serial = self.inner.restore_lock.lock().await;
        self.start_consuming().await?;
        self.state().was_started = true;
        Ok(())
    }

    /// Stop accepting NEW deliveries, leaving the channel open so work in hand
    /// can still ack and reply: the first step of a graceful shutdown.
    /// Serialised with restoration.
    pub async fn stop_consuming(&self) {
        let _serial = self.inner.restore_lock.lock().await;
        let (tag, channel) = {
            let mut s = self.state();
            s.was_started = false;
            (std::mem::take(&mut s.consumer_tag), s.channel.clone())
        };
        self.detach_restorer();
        let Some(channel) = channel else { return };
        if tag.is_empty() || !self.connection().is_connected() {
            return;
        }
        if let Err(e) = channel.raw().cancel(&tag).await {
            log::debug!(target: "protobus", "{}: failed to cancel consumer '{tag}' during drain: {e}", self.name());
        }
    }

    pub async fn close(&self) {
        let (tag, channel, disconnected) = {
            let mut s = self.state();
            if !s.initialized {
                return;
            }
            s.closing = true;
            (s.consumer_tag.clone(), s.channel.clone(), s.disconnected.take())
        };
        self.detach_restorer();
        if let Some(id) = disconnected {
            self.connection().remove_listener(id);
        }
        if let Some(channel) = channel {
            if self.connection().is_connected() {
                if !tag.is_empty() {
                    if let Err(e) = channel.raw().cancel(&tag).await {
                        log::debug!(target: "protobus", "{}: error during close (may be expected): {e}", self.name());
                    }
                }
                if let Err(e) = channel.close().await {
                    log::debug!(target: "protobus", "{}: error during close (may be expected): {e}", self.name());
                }
            }
        }
        let mut s = self.state();
        s.consumer_tag.clear();
        s.channel = None;
        s.initialized = false;
        s.was_started = false;
        s.bindings.clear();
    }

    /// Bind the queue under a routing key, and remember it for restoration.
    async fn bind(&self, routing_key: &str) -> Result<()> {
        let (channel, queue) = {
            let s = self.state();
            (s.channel.clone(), s.queue_name.clone())
        };
        let Some(channel) = channel else {
            return Err(Error::new(
                ErrorKind::NotConnected,
                format!("{}: not connected", self.name()),
            ));
        };
        channel
            .raw()
            .bind_queue(&queue, &self.inner.config.exchange_name, routing_key)
            .await?;
        let mut s = self.state();
        if !s.bindings.iter().any(|b| b == routing_key) {
            s.bindings.push(routing_key.to_string());
        }
        Ok(())
    }
}

/// The context's reply queue: exclusive, server-named, handled in delivery order.
#[derive(Clone)]
pub struct CallbackListener {
    base: BaseListener,
}

impl CallbackListener {
    pub fn new(connection: &Connection) -> Self {
        CallbackListener {
            base: BaseListener::new(
                connection,
                ListenerConfig {
                    name: "CallbackListener",
                    exchange_name: config::callbacks_exchange_name(),
                    exchange_type: "direct",
                    late_ack: false,
                    max_concurrent: None,
                    message_ttl_ms: None,
                    max_priority: None,
                    processing_timeout_ms: None,
                    ordered: true,
                    retry: RetryTopology::None,
                },
            ),
        }
    }
}

impl std::ops::Deref for CallbackListener {
    type Target = BaseListener;

    fn deref(&self) -> &BaseListener {
        &self.base
    }
}

/// A service's request queue, with its retry queue, retry exchange and DLQ.
#[derive(Clone)]
pub struct MessageListener {
    base: BaseListener,
}

impl MessageListener {
    pub fn new(
        connection: &Connection,
        late_ack: bool,
        max_concurrent: Option<u32>,
        retry: Option<RetryOptions>,
        processing_timeout_ms: Option<u64>,
        max_priority: Option<i64>,
    ) -> Result<Self> {
        let max_priority = priority::validate_max_priority(max_priority)?;
        // Priority can only reorder messages still in the queue, and an
        // early-ack consumer has no prefetch: the broker hands it the whole
        // backlog. Refused, because it would do nothing.
        if max_priority.is_some() && !late_ack {
            return Err(Error::new(
                ErrorKind::InvalidPriority,
                "maxPriority requires lateAck: with lateAck off the consumer acks on delivery, RabbitMQ applies no \
                 prefetch, and priority has nothing left to reorder.",
            ));
        }
        let retry = retry.unwrap_or_default();
        Ok(MessageListener {
            base: BaseListener::new(
                connection,
                ListenerConfig {
                    name: "MessageListener",
                    exchange_name: config::bus_exchange_name(),
                    exchange_type: "topic",
                    late_ack,
                    max_concurrent: Some(max_concurrent.filter(|n| *n > 0).unwrap_or(1)),
                    message_ttl_ms: retry.message_ttl_ms,
                    max_priority,
                    processing_timeout_ms,
                    ordered: false,
                    retry: RetryTopology::Message {
                        max_retries: retry.max_retries,
                        retry_delay_ms: retry.retry_delay_ms,
                    },
                },
            ),
        })
    }

    /// Bind the queue to the topics, then declare the retry topology.
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        for topic in topics {
            self.base.bind(topic).await?;
        }
        self.base.setup_retry_topology().await
    }
}

impl std::ops::Deref for MessageListener {
    type Target = BaseListener;

    fn deref(&self) -> &BaseListener {
        &self.base
    }
}

/// What an event handler receives besides the event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventInfo {
    /// The event's full message type name.
    pub r#type: String,
    /// The topic the publisher named.
    pub topic: String,
}

type RawEventHandler = Arc<dyn Fn(Vec<u8>, EventInfo) -> BoxFuture<'static, Result<()>> + Send + Sync>;

struct Subscription {
    type_name: String,
    handler: RawEventHandler,
}

struct EventRouter {
    router: Mutex<Trie<Arc<Subscription>>>,
}

/// A service's event queue and the handlers subscribed on it, matched by topic
/// pattern as the broker matches bindings.
#[derive(Clone)]
pub struct EventListener {
    base: BaseListener,
    factory: MessageFactory,
    routes: Arc<EventRouter>,
}

impl EventListener {
    pub fn new(connection: &Connection, factory: &MessageFactory, retry: Option<EventRetryOptions>) -> Self {
        let retry = retry.unwrap_or_default();
        EventListener {
            base: BaseListener::new(
                connection,
                ListenerConfig {
                    name: "EventListener",
                    exchange_name: config::events_exchange_name(),
                    exchange_type: "topic",
                    late_ack: true,
                    max_concurrent: None,
                    message_ttl_ms: None,
                    max_priority: None,
                    processing_timeout_ms: None,
                    ordered: false,
                    retry: RetryTopology::Event {
                        max_retries: retry.max_retries,
                        retry_delay_ms: retry.retry_delay_ms,
                    },
                },
            ),
            factory: factory.clone(),
            routes: Arc::new(EventRouter {
                router: Mutex::new(Trie::new()),
            }),
        }
    }

    /// Declare the queue (`queue_name`, or an exclusive server-named one) and
    /// the retry ladder when event retry is on.
    pub async fn init(&self, queue_name: Option<&str>) -> Result<()> {
        let routes = self.routes.clone();
        let handler: MessageHandler = Arc::new(move |body, _, context| {
            let routes = routes.clone();
            Box::pin(async move {
                Ok(dispatch_event(&routes, body, context)
                    .await
                    .map(|()| HandlerResult::None)?)
            })
        });
        self.base.init(Some(handler), queue_name).await
    }

    fn add(&self, type_name: String, topic: Option<&str>, handler: RawEventHandler) -> String {
        let topic = match topic {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => format!("EVENT.{type_name}"),
        };
        self.routes
            .router
            .lock()
            .unwrap()
            .add(&topic, Arc::new(Subscription { type_name, handler }));
        topic
    }

    /// Subscribe a typed handler. The topic defaults to `EVENT.<type>`; a
    /// pattern receives every matching event of this handler's type. A handler
    /// that fails loses the event, or retries it when event retry is on.
    pub async fn subscribe<T, F, Fut>(&self, handler: F, topic: Option<&str>) -> Result<()>
    where
        T: Message + prost::Name + Default + 'static,
        F: Fn(T, EventInfo) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let type_name = T::full_name();
        let factory = self.factory.clone();
        let handler = Arc::new(handler);
        let raw: RawEventHandler = Arc::new(move |data, info| {
            let decoded = factory.validate(&info.r#type, &data).and_then(|()| {
                T::decode(data.as_slice())
                    .map_err(|_| Error::protocol(format!("event of type {} did not decode", info.r#type)))
            });
            let handler = handler.clone();
            Box::pin(async move { handler(decoded?, info).await })
        });
        let topic = self.add(type_name, topic, raw);
        self.base.bind(&topic).await
    }

    /// Subscribe a handler over dynamic messages, for a type the factory knows
    /// by name.
    pub async fn subscribe_dynamic<F, Fut>(&self, type_name: &str, handler: F, topic: Option<&str>) -> Result<()>
    where
        F: Fn(DynamicMessage, EventInfo) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let factory = self.factory.clone();
        let handler = Arc::new(handler);
        let raw: RawEventHandler = Arc::new(move |data, info| {
            let decoded = factory.decode_message(&info.r#type, &data).map_err(|e| match e.kind() {
                ErrorKind::CustomTypeRange => e,
                _ => Error::protocol(format!("event of type {} did not decode", info.r#type)),
            });
            let handler = handler.clone();
            Box::pin(async move { handler(decoded?, info).await })
        });
        let topic = self.add(type_name.to_string(), topic, raw);
        self.base.bind(&topic).await
    }

    /// The retry queue's and the DLQ's names, or `None` when event retry is off.
    pub fn retry_topology(&self) -> Option<(String, String)> {
        self.base
            .state()
            .retry_names
            .as_ref()
            .map(|(retry, _, dlq)| (retry.clone(), dlq.clone()))
    }
}

impl std::ops::Deref for EventListener {
    type Target = BaseListener;

    fn deref(&self) -> &BaseListener {
        &self.base
    }
}

async fn dispatch_event(routes: &EventRouter, body: Vec<u8>, context: MessageHandlerContext) -> Result<()> {
    let event = wire::decode_event(&body).map_err(|_| Error::protocol("event envelope did not decode"))?;
    // The routing key the broker delivered on, not the topic in the body: the
    // body is publisher-controlled, and trusting it would let a publisher
    // reach handlers its routing key was never permitted to reach.
    let match_topic = if context.routing_key.is_empty() {
        &event.topic
    } else {
        &context.routing_key
    };
    if match_topic.is_empty() {
        log::warn!(target: "protobus", "ignoring unhandled event of type '{}' (no topic to route on)", event.r#type);
        return Ok(());
    }
    let handlers: Vec<Arc<Subscription>> = routes
        .router
        .lock()
        .unwrap()
        .matches(match_topic)
        .into_iter()
        .cloned()
        .collect();
    for s in handlers {
        if s.type_name != event.r#type {
            // The topic matched, but the payload is another type: decoding it
            // as this handler's type would hand it garbage.
            log::warn!(
                target: "protobus",
                "event of type '{}' on {} skipped by a handler of {}",
                event.r#type,
                event.topic,
                s.type_name
            );
            continue;
        }
        let info = EventInfo {
            r#type: event.r#type.clone(),
            topic: event.topic.clone(),
        };
        (s.handler)(event.data.clone(), info).await?;
    }
    Ok(())
}

/// Hears stream-cancellation notices and stops the matching in-flight stream
/// in this process. Best effort by design: a deployment that cannot declare
/// the cancel exchange keeps working.
#[derive(Clone)]
pub struct CancelListener {
    inner: Arc<CancelInner>,
}

struct CancelInner {
    connection: Connection,
    state: Mutex<CancelState>,
}

#[derive(Default)]
struct CancelState {
    channel: Option<ChannelHandle>,
    consumer_tag: String,
    started: bool,
    restorer: Option<ListenerId>,
}

impl CancelListener {
    pub fn new(connection: &Connection) -> Self {
        let listener = CancelListener {
            inner: Arc::new(CancelInner {
                connection: connection.clone(),
                state: Mutex::new(CancelState::default()),
            }),
        };
        let weak = Arc::downgrade(&listener.inner);
        let id = connection.register_restorer(Arc::new(move |_| {
            let weak = weak.clone();
            Box::pin(async move {
                if let Some(inner) = weak.upgrade() {
                    let listener = CancelListener { inner };
                    if listener.inner.state.lock().unwrap().started {
                        listener.start().await;
                    }
                }
                Ok(())
            })
        }));
        listener.inner.state.lock().unwrap().restorer = Some(id);
        listener
    }

    async fn open(&self) -> Result<(ChannelHandle, String)> {
        let connection = &self.inner.connection;
        let channel = connection.open_channel().await?;
        let exchange = config::cancel_exchange_name();
        connection.declare_exchange(&channel, &exchange, "fanout").await?;
        let queue = channel
            .raw()
            .declare_queue("", false, true, true, &FieldTable::new())
            .await?;
        channel.raw().bind_queue(&queue, &exchange, "").await?;
        let target = connection.clone();
        let tag = channel
            .raw()
            .consume(
                &queue,
                "",
                true,
                true,
                Arc::new(move |delivery| {
                    // Every replica hears every cancel; only the one running that stream acts.
                    if let Some(id) = delivery
                        .properties
                        .correlation_id
                        .as_deref()
                        .filter(|id| !id.is_empty())
                    {
                        target.cancel_stream(id);
                    }
                    Box::pin(async {})
                }),
                None,
            )
            .await?;
        Ok((channel, tag))
    }

    pub async fn start(&self) {
        match self.open().await {
            Ok((channel, tag)) => {
                let mut s = self.inner.state.lock().unwrap();
                s.channel = Some(channel);
                s.consumer_tag = tag;
                s.started = true;
            }
            Err(e) => {
                log::warn!(
                    target: "protobus",
                    "CancelListener: stream cancellation unavailable ({e}). Streams will run to completion; everything else is unaffected."
                );
                let mut s = self.inner.state.lock().unwrap();
                s.started = true;
                s.channel = None;
            }
        }
    }

    pub async fn close(&self) {
        let (channel, tag, restorer) = {
            let mut s = self.inner.state.lock().unwrap();
            s.started = false;
            (s.channel.take(), std::mem::take(&mut s.consumer_tag), s.restorer.take())
        };
        if let Some(id) = restorer {
            self.inner.connection.unregister_restorer(id);
        }
        let Some(channel) = channel else { return };
        if self.inner.connection.is_connected() {
            if !tag.is_empty() {
                let _ = channel.raw().cancel(&tag).await;
            }
            if let Err(e) = channel.close().await {
                log::debug!(target: "protobus", "CancelListener: error during close: {e}");
            }
        }
    }
}
