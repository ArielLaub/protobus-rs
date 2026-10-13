//! The broker connection every protobus component of a context shares:
//! reconnection with coordinated restoration, confirmed publishing, and the
//! consume loop that runs a handler and settles its delivery (reply, ack,
//! retry, dead-letter).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{Stream, StreamExt};
use tokio::sync::{oneshot, watch, Notify, Semaphore};
use tokio::time::Instant;

use crate::amqp::{self, BoxFuture, ConfirmOutcome, FieldTable, FieldValue, Properties, Transport};
use crate::config;
use crate::errors::{safe_error_summary, Error, ErrorKind, Result};

/// Reconnection backoff.
#[derive(Clone, Debug, PartialEq)]
pub struct ReconnectionOptions {
    /// Consecutive failed attempts before giving up; 0 retries forever.
    pub max_retries: u32,
    /// The first delay; each later one is multiplied, up to the maximum, with
    /// up to 30% jitter.
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

impl Default for ReconnectionOptions {
    fn default() -> Self {
        ReconnectionOptions {
            max_retries: 10,
            initial_delay_ms: 1000,
            max_delay_ms: 30_000,
            backoff_multiplier: 2.0,
        }
    }
}

/// A cooperative stop signal handed to a handler. It fires when the processing
/// timeout elapses or, for a streaming reply, when the caller cancels. A
/// handler is never aborted, so one doing long work should observe it.
#[derive(Clone, Debug)]
pub struct CancelToken {
    state: Arc<watch::Sender<bool>>,
}

impl CancelToken {
    pub(crate) fn new() -> Self {
        CancelToken {
            state: Arc::new(watch::channel(false).0),
        }
    }

    pub(crate) fn cancel(&self) {
        self.state.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.state.borrow()
    }

    /// Completes once the token has fired; at once if it already has.
    pub async fn cancelled(&self) {
        let mut rx = self.state.subscribe();
        // The sender lives in `self`, so this cannot fail while we wait.
        let _ = rx.wait_for(|fired| *fired).await;
    }
}

/// Extra context handed to a message handler.
#[derive(Clone, Debug)]
pub struct MessageHandlerContext {
    pub cancel: CancelToken,
    /// The routing key the broker delivered on.
    pub routing_key: String,
    /// Stable across every redelivery and retry hop: deduplicate on it. `None`
    /// only if the publisher set none.
    pub message_id: Option<String>,
    /// The broker has delivered this message before.
    pub redelivered: bool,
    pub headers: Arc<FieldTable>,
}

/// The encoded chunks of a streaming reply. An `Err` item abandons the stream:
/// a failure the caller should see is sent as a chunk instead.
pub type ReplyStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>;

/// What a handler returns: nothing (no reply), one encoded reply, or a stream
/// of encoded chunks, each published with `x-protobus-final=false` and the
/// last with `true`.
pub enum HandlerResult {
    None,
    Reply(Vec<u8>),
    Stream(ReplyStream),
}

/// A handler's failure, optionally carrying the reply its caller should
/// receive on a terminal path (dead-lettered, or rejected without retry).
/// `MessageService` attaches one for an unhandled handler error, pre-encoded
/// and sanitised.
#[derive(Debug)]
pub struct HandlerFailure {
    pub error: Error,
    pub reply: Option<Vec<u8>>,
}

impl From<Error> for HandlerFailure {
    fn from(error: Error) -> Self {
        HandlerFailure { error, reply: None }
    }
}

/// Handles one delivery: its body, its correlation id and its context.
pub type MessageHandler = Arc<
    dyn Fn(
            Vec<u8>,
            String,
            MessageHandlerContext,
        ) -> BoxFuture<'static, std::result::Result<HandlerResult, HandlerFailure>>
        + Send
        + Sync,
>;

/// Builds the caller's reply for a failure that carries none, such as a
/// processing timeout.
pub type ErrorReplyBuilder = Arc<dyn Fn(&[u8], &Error) -> Option<Vec<u8>> + Send + Sync>;

/// How a consumer consumes.
#[derive(Clone, Default)]
pub struct ConsumeOptions {
    pub consumer_tag: String,
    pub no_ack: bool,
    pub exclusive: bool,
    /// Handle deliveries one at a time, in arrival order, rather than in
    /// parallel. Replies use it: a stream's chunks must be seen in the order
    /// they arrived. No processing timeout applies.
    pub ordered: bool,
    /// Handlers this consumer runs at once; 0 leaves it to the prefetch, which
    /// bounds a late-ack consumer.
    pub max_concurrency: usize,
    pub build_error_reply: Option<ErrorReplyBuilder>,
    /// Called when the broker cancels the consumer.
    pub on_cancelled: Option<amqp::CancelCallback>,
}

/// Retry for a consumer.
#[derive(Clone, Debug)]
pub struct ConsumeRetryOptions {
    pub max_retries: u32,
    pub retry_queue_name: String,
    /// The topic exchange the retry queue is bound to with `#`. A retry is
    /// published here under the original routing key, which is what lets the
    /// post-TTL dead-letter hop route it back.
    pub retry_exchange_name: String,
    pub dlq_name: String,
}

/// What to publish with.
#[derive(Clone, Debug, Default)]
pub struct PublishOptions {
    pub properties: Properties,
    pub mandatory: bool,
}

/// A channel opened through a [`Connection`], with the bookkeeping confirmed
/// publishing needs.
#[derive(Clone)]
pub struct ChannelHandle {
    inner: Arc<ChannelInner>,
}

struct ChannelInner {
    raw: Arc<dyn amqp::Channel>,
    /// One permit per publish the broker has not answered.
    slots: Arc<Semaphore>,
    slot_count: u64,
}

impl ChannelHandle {
    fn new(raw: Arc<dyn amqp::Channel>) -> Self {
        let slot_count = config::max_outstanding_confirms().min(Semaphore::MAX_PERMITS as u64);
        let slots = Arc::new(Semaphore::new(slot_count as usize));
        // A publish waiting for a slot is failed, not resumed, when the channel goes.
        let closing = slots.clone();
        raw.on_close(Box::new(move |_| closing.close()));
        ChannelHandle {
            inner: Arc::new(ChannelInner { raw, slots, slot_count }),
        }
    }

    /// The transport's channel.
    pub fn raw(&self) -> &Arc<dyn amqp::Channel> {
        &self.inner.raw
    }

    pub fn is_open(&self) -> bool {
        self.inner.raw.is_open()
    }

    pub(crate) fn same(&self, other: &ChannelHandle) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) async fn close(&self) -> Result<()> {
        Ok(self.inner.raw.close().await?)
    }
}

/// Identifies a registered restorer or listener, for removing it.
pub type ListenerId = u64;

/// Restores one component's topology after the socket comes back: its channel,
/// queues, bindings and consumers. Receives the generation it is restoring.
pub type Restorer = Arc<dyn Fn(u64) -> BoxFuture<'static, Result<()>> + Send + Sync>;

type Registered<F> = Vec<(ListenerId, Arc<F>)>;

#[derive(Default)]
struct Listeners {
    reconnecting: Registered<dyn Fn(u32, u64) + Send + Sync>,
    reconnected: Registered<dyn Fn() + Send + Sync>,
    disconnected: Registered<dyn Fn() + Send + Sync>,
    error: Registered<dyn Fn(&Error) + Send + Sync>,
}

#[derive(Default)]
struct State {
    url: String,
    reconnection: ReconnectionOptions,
    handle: Option<Arc<dyn amqp::Connection>>,
    handle_id: u64,
    connected: bool,
    reconnecting: bool,
    /// The connection was lost and has neither come back nor been given up
    /// on: true from the moment of the loss, across every attempt and the
    /// pauses between them.
    lost: bool,
    ready: bool,
    manual_disconnect: bool,
    shut_down: bool,
    generation: u64,
    reconnect_attempts: u32,
    reconnect_timer: Option<tokio::task::JoinHandle<()>>,
    abandoned: Option<String>,
    runtime: Option<tokio::runtime::Handle>,
}

struct DeliveryEntry {
    cancel: CancelToken,
    cancelled: AtomicBool,
}

struct Inner {
    transport: Arc<dyn Transport>,
    state: Mutex<State>,
    ready_changed: Notify,
    connect_mutex: tokio::sync::Mutex<()>,
    next_id: AtomicU64,
    restorers: Mutex<Vec<(ListenerId, Restorer)>>,
    listeners: Mutex<Listeners>,
    active: Mutex<HashMap<String, Vec<Arc<DeliveryEntry>>>>,
    /// (deliveries being settled, handlers still running)
    drain: Mutex<(usize, usize)>,
    drained: Notify,
}

/// The broker connection. Cheap to clone: every clone is the same connection.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn torn_down(during: &str) -> Error {
    Error::new(
        ErrorKind::Reconnection,
        format!("connection was torn down while {during}"),
    )
}

impl Connection {
    /// A connection over `transport`, or over RabbitMQ when `None`.
    pub fn new(transport: Option<Arc<dyn Transport>>) -> Self {
        Connection {
            inner: Arc::new(Inner {
                transport: transport.unwrap_or_else(|| Arc::new(amqp::LapinTransport::new())),
                state: Mutex::new(State::default()),
                ready_changed: Notify::new(),
                connect_mutex: tokio::sync::Mutex::new(()),
                next_id: AtomicU64::new(1),
                restorers: Mutex::new(Vec::new()),
                listeners: Mutex::new(Listeners::default()),
                active: Mutex::new(HashMap::new()),
                drain: Mutex::new((0, 0)),
                drained: Notify::new(),
            }),
        }
    }

    fn next_id(&self) -> u64 {
        self.inner.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Run a task on the runtime the connection was opened on, so work started
    /// from a transport callback has somewhere to run.
    pub(crate) fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let runtime = self.inner.state.lock().unwrap().runtime.clone();
        match runtime {
            Some(rt) => rt.spawn(future),
            None => tokio::spawn(future),
        }
    }

    // ---- state -------------------------------------------------------------------

    pub fn is_connected(&self) -> bool {
        self.inner.state.lock().unwrap().connected
    }

    /// The connection was lost and is being re-established. True from the
    /// moment of the loss until it is ready again or gives up, so there is no
    /// instant at which a lost connection looks merely absent.
    pub fn is_reconnecting(&self) -> bool {
        let s = self.inner.state.lock().unwrap();
        s.reconnecting || s.lost
    }

    /// The socket is up AND every restorer has finished: what a publisher wants.
    pub fn is_ready(&self) -> bool {
        self.inner.state.lock().unwrap().ready
    }

    pub(crate) fn generation(&self) -> u64 {
        self.inner.state.lock().unwrap().generation
    }

    // ---- events ------------------------------------------------------------------

    /// The connection is going to reconnect: attempt number and delay in ms.
    pub fn on_reconnecting(&self, f: impl Fn(u32, u64) + Send + Sync + 'static) -> ListenerId {
        let id = self.next_id();
        self.inner
            .listeners
            .lock()
            .unwrap()
            .reconnecting
            .push((id, Arc::new(f)));
        id
    }

    /// The connection came back and every component is restored.
    pub fn on_reconnected(&self, f: impl Fn() + Send + Sync + 'static) -> ListenerId {
        let id = self.next_id();
        self.inner.listeners.lock().unwrap().reconnected.push((id, Arc::new(f)));
        id
    }

    /// The connection went down: lost (a reconnection follows) or closed by
    /// [`disconnect`](Self::disconnect). Must not block.
    pub fn on_disconnected(&self, f: impl Fn() + Send + Sync + 'static) -> ListenerId {
        let id = self.next_id();
        self.inner
            .listeners
            .lock()
            .unwrap()
            .disconnected
            .push((id, Arc::new(f)));
        id
    }

    /// A connection-level failure, such as giving up reconnecting.
    pub fn on_error(&self, f: impl Fn(&Error) + Send + Sync + 'static) -> ListenerId {
        let id = self.next_id();
        self.inner.listeners.lock().unwrap().error.push((id, Arc::new(f)));
        id
    }

    pub fn remove_listener(&self, id: ListenerId) {
        let mut l = self.inner.listeners.lock().unwrap();
        l.reconnecting.retain(|(i, _)| *i != id);
        l.reconnected.retain(|(i, _)| *i != id);
        l.disconnected.retain(|(i, _)| *i != id);
        l.error.retain(|(i, _)| *i != id);
    }

    fn emit_disconnected(&self) {
        let listeners: Vec<_> = self
            .inner
            .listeners
            .lock()
            .unwrap()
            .disconnected
            .iter()
            .map(|(_, f)| f.clone())
            .collect();
        for f in listeners {
            f();
        }
    }

    // ---- restorers and readiness -------------------------------------------------

    /// Register topology to restore on reconnection. Restorers run in
    /// registration order, and the connection reports itself reconnected only
    /// once all have finished; one that fails fails the attempt, which is
    /// retried.
    pub fn register_restorer(&self, restorer: Restorer) -> ListenerId {
        let id = self.next_id();
        self.inner.restorers.lock().unwrap().push((id, restorer));
        id
    }

    pub fn unregister_restorer(&self, id: ListenerId) {
        self.inner.restorers.lock().unwrap().retain(|(i, _)| *i != id);
    }

    /// Wait until the connection carries traffic. Fails with `NOT_READY` when
    /// it is closed or gives up first, or after the timeout
    /// (`CONNECTION_READY_TIMEOUT_MS` when `None`).
    pub async fn when_ready(&self, timeout_ms: Option<u64>) -> Result<()> {
        let limit = timeout_ms.unwrap_or_else(config::connection_ready_timeout_ms);
        let deadline = Instant::now() + Duration::from_millis(limit);
        loop {
            let changed = self.inner.ready_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let s = self.inner.state.lock().unwrap();
                if s.ready {
                    return Ok(());
                }
                if s.manual_disconnect {
                    return Err(Error::new(ErrorKind::NotReady, "the connection has been closed"));
                }
                if let Some(why) = &s.abandoned {
                    return Err(Error::new(ErrorKind::NotReady, why.clone()));
                }
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(Error::new(
                    ErrorKind::NotReady,
                    format!("the connection did not become ready within {limit}ms"),
                ));
            }
        }
    }

    // ---- lifecycle ---------------------------------------------------------------

    /// Connect. Fails when already connected, or when the broker cannot be reached.
    pub async fn connect(&self, url: &str, options: Option<ReconnectionOptions>) -> Result<()> {
        {
            let mut s = self.inner.state.lock().unwrap();
            if s.connected {
                return Err(Error::new(ErrorKind::AlreadyConnected, "already connected"));
            }
            s.url = url.to_string();
            s.reconnection = options.unwrap_or_default();
            s.manual_disconnect = false;
            s.abandoned = None;
            s.runtime = tokio::runtime::Handle::try_current().ok();
        }
        self.do_connect().await?;
        // Nothing to restore on a first connect: components initialise against
        // it, so the socket coming up is readiness, unless it already went again.
        {
            let mut s = self.inner.state.lock().unwrap();
            if !s.connected {
                return Ok(());
            }
            s.reconnecting = false;
            s.ready = true;
        }
        self.inner.ready_changed.notify_waiters();
        Ok(())
    }

    async fn do_connect(&self) -> Result<()> {
        let _single_flight = self.inner.connect_mutex.lock().await;
        let (generation, url) = {
            let s = self.inner.state.lock().unwrap();
            (s.generation, s.url.clone())
        };
        log::info!(target: "protobus", "connecting to bus - {}", config::redact_url(&url));
        let heartbeat = config::heartbeat_seconds().min(u64::from(u16::MAX)) as u16;
        let handle = match self.inner.transport.connect(&url, heartbeat).await {
            Ok(h) => h,
            Err(e) => {
                log::error!(target: "protobus", "failed to connect: {e}");
                self.inner.state.lock().unwrap().connected = false;
                return Err(e.into());
            }
        };
        let id = self.next_id();
        let stale = {
            let mut s = self.inner.state.lock().unwrap();
            let stale = generation != s.generation || s.manual_disconnect;
            if !stale {
                s.handle = Some(handle.clone());
                s.handle_id = id;
                s.connected = true;
                s.reconnect_attempts = 0;
                // `reconnecting` stays set: on the reconnect path restoration
                // still has to run, and clearing it would let a socket dying
                // mid-restore schedule a second reconnection alongside the one
                // its failure schedules.
            }
            stale
        };
        if stale {
            log::info!(target: "protobus", "discarding a connection that completed after disconnect");
            let _ = handle.close().await;
            return Err(torn_down("connecting"));
        }
        let me = self.clone();
        handle.on_close(Box::new(move |reason| me.on_handle_closed(id, reason)));
        log::info!(target: "protobus", "connected to message bus");
        Ok(())
    }

    fn on_handle_closed(&self, id: u64, reason: Option<String>) {
        {
            let mut s = self.inner.state.lock().unwrap();
            if s.handle_id != id || s.handle.is_none() {
                return;
            }
            if s.manual_disconnect {
                log::info!(target: "protobus", "connection closed (manual disconnect)");
                return;
            }
            s.connected = false;
            s.ready = false;
            s.handle = None;
            s.lost = !s.shut_down;
            // Retire this generation: a restoration in flight against it is now
            // working on a dead socket, and its next generation check aborts it.
            s.generation += 1;
        }
        log::warn!(
            target: "protobus",
            "connection closed unexpectedly{}",
            reason.map(|r| format!(": {r}")).unwrap_or_default()
        );
        self.emit_disconnected();
        self.schedule_reconnect();
    }

    fn schedule_reconnect(&self) {
        let (attempt, delay) = {
            let mut s = self.inner.state.lock().unwrap();
            if s.manual_disconnect || s.reconnecting || s.shut_down {
                return;
            }
            if s.reconnection.max_retries > 0 && s.reconnect_attempts >= s.reconnection.max_retries {
                let give_up = format!("max reconnection attempts ({}) exceeded", s.reconnection.max_retries);
                s.abandoned = Some(give_up.clone());
                s.ready = false;
                s.lost = false;
                drop(s);
                log::error!(target: "protobus", "{give_up}");
                self.inner.ready_changed.notify_waiters();
                let error = Error::new(ErrorKind::Reconnection, give_up);
                let listeners: Vec<_> = self
                    .inner
                    .listeners
                    .lock()
                    .unwrap()
                    .error
                    .iter()
                    .map(|(_, f)| f.clone())
                    .collect();
                for f in listeners {
                    f(&error);
                }
                return;
            }
            s.reconnecting = true;
            s.reconnect_attempts += 1;
            let attempt = s.reconnect_attempts;
            let base = (s.reconnection.initial_delay_ms as f64
                * s.reconnection.backoff_multiplier.powi(attempt as i32 - 1))
            .min(s.reconnection.max_delay_ms as f64);
            let delay = (base + jitter() * 0.3 * base).floor() as u64;
            (attempt, delay)
        };
        log::info!(target: "protobus", "scheduling reconnection attempt {attempt} in {delay}ms");
        let me = self.clone();
        let timer = self.spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            me.inner.state.lock().unwrap().reconnect_timer = None;
            // Detached: cancelling the timer must not cancel an attempt under way.
            let attempt = me.clone();
            me.spawn(async move { attempt.reconnect_attempt().await });
        });
        self.inner.state.lock().unwrap().reconnect_timer = Some(timer);
        let listeners: Vec<_> = self
            .inner
            .listeners
            .lock()
            .unwrap()
            .reconnecting
            .iter()
            .map(|(_, f)| f.clone())
            .collect();
        for f in listeners {
            f(attempt, delay);
        }
    }

    async fn reconnect_attempt(&self) {
        let attempt = {
            let mut s = self.inner.state.lock().unwrap();
            if s.manual_disconnect {
                s.reconnecting = false;
                return;
            }
            // Read before connecting: a successful connect resets it.
            s.reconnect_attempts
        };
        match self.reconnect_once().await {
            Ok(()) => {
                log::info!(target: "protobus", "reconnection successful after {attempt} attempts");
                let listeners: Vec<_> = self
                    .inner
                    .listeners
                    .lock()
                    .unwrap()
                    .reconnected
                    .iter()
                    .map(|(_, f)| f.clone())
                    .collect();
                for f in listeners {
                    f();
                }
            }
            Err(e) => {
                {
                    let mut s = self.inner.state.lock().unwrap();
                    if s.manual_disconnect {
                        s.reconnecting = false;
                        return;
                    }
                }
                log::error!(target: "protobus", "reconnection attempt {attempt} failed: {e}");
                // A generation that connected but could not be restored looks
                // healthy and serves nothing. Drop it and let the backoff try again.
                self.discard_generation().await;
                {
                    let mut s = self.inner.state.lock().unwrap();
                    s.reconnect_attempts = attempt;
                    s.reconnecting = false;
                }
                self.schedule_reconnect();
            }
        }
    }

    async fn reconnect_once(&self) -> Result<()> {
        self.do_connect().await?;
        let generation = self.generation();
        // Restoration is part of reconnecting: until every component has its
        // channel, queues and consumers back, the socket is up but cannot be used.
        let restorers: Vec<Restorer> = self
            .inner
            .restorers
            .lock()
            .unwrap()
            .iter()
            .map(|(_, r)| r.clone())
            .collect();
        for restore in restorers {
            if self.generation() != generation {
                return Err(torn_down("restoring"));
            }
            restore(generation).await?;
        }
        {
            let mut s = self.inner.state.lock().unwrap();
            if s.generation != generation || !s.connected {
                return Err(torn_down("restoring"));
            }
            s.reconnecting = false;
            s.lost = false;
            s.ready = true;
        }
        self.inner.ready_changed.notify_waiters();
        Ok(())
    }

    async fn discard_generation(&self) {
        let handle = {
            let mut s = self.inner.state.lock().unwrap();
            s.generation += 1;
            s.ready = false;
            s.connected = false;
            s.handle.take()
        };
        if let Some(h) = handle {
            if let Err(e) = h.close().await {
                log::debug!(target: "protobus", "failed closing an unrestorable connection: {e}");
            }
        }
    }

    /// Close deliberately. Waiters on readiness fail with `NOT_READY`, and the
    /// disconnected listeners run first, so pending calls and streams fail at once.
    pub async fn disconnect(&self) {
        let handle = {
            let mut s = self.inner.state.lock().unwrap();
            s.manual_disconnect = true;
            s.generation += 1;
            if let Some(timer) = s.reconnect_timer.take() {
                timer.abort();
            }
            s.reconnecting = false;
            s.lost = false;
            s.ready = false;
            s.connected = false;
            s.handle.take()
        };
        self.inner.ready_changed.notify_waiters();
        // Listeners first, so pending calls and streams fail as disconnected
        // before closing the socket can fail a queued write under them.
        self.emit_disconnected();
        if let Some(h) = handle {
            if let Err(e) = h.close().await {
                log::debug!(target: "protobus", "closing the connection: {e}");
            }
            log::info!(target: "protobus", "connection closed (manual disconnect)");
        }
    }

    // ---- streams and draining ----------------------------------------------------

    /// Stop producing a streaming reply the caller has abandoned: fire the
    /// handler's cancel token and publish nothing more it produces.
    /// Cooperative. Returns whether a matching in-flight delivery was found.
    pub fn cancel_stream(&self, correlation_id: &str) -> bool {
        let entries = match self.inner.active.lock().unwrap().get(correlation_id) {
            Some(entries) if !entries.is_empty() => entries.clone(),
            _ => return false,
        };
        // A redelivery can overlap its predecessor; both are the caller's stream.
        for e in entries {
            e.cancelled.store(true, Ordering::SeqCst);
            e.cancel.cancel();
        }
        log::debug!(target: "protobus", "stream {correlation_id} cancelled by the caller");
        true
    }

    /// Messages being handled, counting handlers still running after a
    /// processing timeout settled them.
    pub fn in_flight_deliveries(&self) -> usize {
        let d = self.inner.drain.lock().unwrap();
        d.0.max(d.1)
    }

    /// Wait for in-flight work to finish; false when the deadline passed with
    /// work still running.
    pub async fn drain_in_flight(&self, timeout_ms: u64) -> bool {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let drained = self.inner.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.in_flight_deliveries() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, drained).await.is_err() {
                return self.in_flight_deliveries() == 0;
            }
        }
    }

    fn drain_update(&self, f: impl FnOnce(&mut (usize, usize))) {
        let idle = {
            let mut d = self.inner.drain.lock().unwrap();
            f(&mut d);
            d.0 == 0 && d.1 == 0
        };
        if idle {
            self.inner.drained.notify_waiters();
        }
    }

    /// Wait for readiness, then obtain a channel with `open`. A connection
    /// lost in between is waited through again rather than reported: nothing
    /// has been sent yet, so the caller is exactly where one arriving a moment
    /// later would be.
    pub(crate) async fn ready_channel<F, Fut>(&self, open: F) -> Result<ChannelHandle>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Option<ChannelHandle>>>,
    {
        let limit = config::connection_ready_timeout_ms();
        let deadline = Instant::now() + Duration::from_millis(limit);
        loop {
            self.when_ready(None).await?;
            let outcome = open().await;
            if let Ok(Some(channel)) = &outcome {
                if channel.is_open() {
                    return Ok(channel.clone());
                }
            }
            // The transport can know the socket is gone a moment before this
            // connection has been told, and reports it as a failure of its
            // own (no broker reply code). Either way it is a loss to wait
            // through, not a failure of the caller's.
            let transport_failure = matches!(&outcome, Err(e) if e.kind() == ErrorKind::Amqp && e.reply_code() == 0);
            if !self.is_ready() || !self.transport_is_open() || transport_failure {
                if Instant::now() >= deadline {
                    return Err(Error::new(
                        ErrorKind::NotReady,
                        format!("the connection did not become ready within {limit}ms"),
                    ));
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }
            return match outcome {
                Err(e) => Err(e),
                Ok(_) => Err(Error::new(ErrorKind::NotConnected, "no channel to publish on")),
            };
        }
    }

    fn transport_is_open(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap()
            .handle
            .as_ref()
            .is_some_and(|h| h.is_open())
    }

    // ---- channel operations ------------------------------------------------------

    pub async fn open_channel(&self) -> Result<ChannelHandle> {
        let handle = self.inner.state.lock().unwrap().handle.clone();
        let Some(handle) = handle else {
            return Err(Error::new(ErrorKind::NotConnected, "no broker connection"));
        };
        Ok(ChannelHandle::new(handle.open_channel().await?))
    }

    /// Declare a durable exchange.
    pub async fn declare_exchange(&self, channel: &ChannelHandle, exchange: &str, kind: &str) -> Result<()> {
        Ok(channel
            .raw()
            .declare_exchange(exchange, kind, true, false, false, &FieldTable::new())
            .await?)
    }

    // ---- consuming ---------------------------------------------------------------

    /// Consume `queue` with `handler`. An early-ack consumer acknowledges on
    /// delivery and never retries. A late-ack consumer settles after the
    /// handler: it replies, then acks; on a failure it retries, dead-letters
    /// once the retries are spent, and answers the caller on every terminal
    /// path. Returns the consumer tag.
    #[allow(clippy::too_many_arguments)]
    pub async fn consume(
        &self,
        channel: &ChannelHandle,
        queue: &str,
        handler: MessageHandler,
        options: ConsumeOptions,
        late_ack: bool,
        retry: Option<ConsumeRetryOptions>,
        processing_timeout_ms: Option<u64>,
    ) -> Result<String> {
        let limiter = (options.max_concurrency > 0).then(|| Arc::new(Semaphore::new(options.max_concurrency)));
        let consumer = Arc::new(Consumer {
            connection: self.clone(),
            channel: channel.clone(),
            queue: queue.to_string(),
            handler,
            options: options.clone(),
            late_ack,
            retry,
            processing_timeout_ms,
        });
        let on_delivery: amqp::DeliveryCallback = Arc::new(move |delivery| {
            let consumer = consumer.clone();
            let limiter = limiter.clone();
            Box::pin(async move {
                // Counted for the whole settle, so a graceful shutdown waits for
                // the reply, retry or dead-letter publish and not just the handler.
                consumer.connection.drain_update(|d| d.0 += 1);
                let d = InFlight::new(consumer.clone(), delivery);
                // Ordered (replies): the next is delivered only once this one is handled.
                if consumer.options.ordered {
                    d.handle().await;
                    return;
                }
                let connection = consumer.connection.clone();
                connection.spawn(async move {
                    let _slot = match &limiter {
                        Some(l) => l.clone().acquire_owned().await.ok(),
                        None => None,
                    };
                    d.handle().await;
                });
            })
        });
        let queue_name = queue.to_string();
        let cancelled = options.on_cancelled.clone();
        let on_cancel: amqp::CancelCallback = Arc::new(move || {
            log::warn!(target: "protobus", "consumer for {queue_name} was cancelled by the broker");
            if let Some(f) = &cancelled {
                f();
            }
        });
        Ok(channel
            .raw()
            .consume(
                queue,
                &options.consumer_tag,
                options.no_ack,
                options.exclusive,
                on_delivery,
                Some(on_cancel),
            )
            .await?)
    }

    // ---- publishing --------------------------------------------------------------

    /// Publish and complete only once RabbitMQ has confirmed the message:
    /// positively and, for a mandatory publish, routed. Everything else fails
    /// with a publish error; `PUBLISH_CONFIRM_TIMEOUT` and `CHANNEL_CLOSED`
    /// are AMBIGUOUS, which is why every publish carries a `messageId` (a UUID
    /// unless one is set). Returns the `messageId`.
    ///
    /// At most `MAX_OUTSTANDING_CONFIRMS` publishes await the broker's answer
    /// per channel. A publish whose confirm timed out keeps its slot until the
    /// broker answers or the channel closes, so a stalled broker cannot be
    /// handed an unbounded backlog; one still waiting for a slot when its
    /// deadline passes fails without being sent.
    pub async fn publish(
        &self,
        channel: Option<&ChannelHandle>,
        exchange: &str,
        routing_key: &str,
        content: Vec<u8>,
        options: PublishOptions,
    ) -> Result<String> {
        let mut properties = options.properties;
        let message_id = match properties.message_id.as_deref() {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => uuid::Uuid::new_v4().to_string(),
        };
        properties.message_id = Some(message_id.clone());
        let describe = format!(
            "{} -> {routing_key}",
            if exchange.is_empty() { "(default)" } else { exchange }
        );
        let closed = |what: String| Error::new(ErrorKind::ChannelClosed, what).with_message_id(&message_id);
        let Some(channel) = channel.filter(|c| c.is_open()) else {
            return Err(closed(format!("{describe}: the channel is closed")));
        };
        // The caller's deadline runs from here, so waiting for a slot counts against it too.
        let confirm_timeout = config::publish_confirm_timeout_ms();
        let deadline = Instant::now() + Duration::from_millis(confirm_timeout);
        let timed_out = |what: String| Error::new(ErrorKind::PublishConfirmTimeout, what).with_message_id(&message_id);
        let not_sent = format!(
            "{describe} waited {confirm_timeout}ms for one of the channel's {} confirm slots and was not sent",
            channel.inner.slot_count
        );
        let slot = match tokio::time::timeout_at(deadline, channel.inner.slots.clone().acquire_owned()).await {
            Err(_) => return Err(timed_out(not_sent)),
            Ok(Err(_)) => {
                return Err(closed(format!(
                    "{describe} was not sent: the channel closed while it waited for a confirm slot"
                )))
            }
            Ok(Ok(slot)) => slot,
        };

        // The write and its confirm run on a task of their own: the slot is held
        // until the broker answers or the channel closes, not until the caller
        // stops waiting. A timed-out publish may still be stored, and the bound
        // is on what the broker has not answered.
        let (done, outcome) = oneshot::channel::<Result<()>>();
        let raw = channel.raw().clone();
        let (exchange, routing_key) = (exchange.to_string(), routing_key.to_string());
        let (id, what) = (message_id.clone(), describe.clone());
        self.spawn(async move {
            let _slot = slot;
            if Instant::now() >= deadline {
                let _ = done.send(Err(
                    Error::new(ErrorKind::PublishConfirmTimeout, not_sent).with_message_id(&id)
                ));
                return;
            }
            let fail = |kind: ErrorKind, text: String| Err(Error::new(kind, text).with_message_id(&id));
            let confirm = match raw
                .publish(&exchange, &routing_key, &content, &properties, options.mandatory)
                .await
            {
                Ok(confirm) => confirm,
                Err(e) => {
                    let _ = done.send(fail(
                        ErrorKind::ChannelClosed,
                        format!("{what} could not be written: {e}"),
                    ));
                    return;
                }
            };
            let answer = confirm.await;
            let detail = |sep: &str| {
                if answer.detail.is_empty() {
                    String::new()
                } else {
                    format!("{sep}{}", answer.detail)
                }
            };
            let result = match answer.outcome {
                ConfirmOutcome::Ack => Ok(()),
                ConfirmOutcome::Nack => fail(
                    ErrorKind::PublishNacked,
                    format!("broker nacked {what}{}", detail(": ")),
                ),
                ConfirmOutcome::Returned => fail(
                    ErrorKind::Unroutable,
                    format!("{what} was confirmed but returned as unroutable"),
                ),
                ConfirmOutcome::Closed => fail(
                    ErrorKind::ChannelClosed,
                    format!("{what} was unconfirmed when the channel closed{}", detail(": ")),
                ),
            };
            // A caller whose deadline already passed keeps that answer.
            let _ = done.send(result);
        });

        match tokio::time::timeout_at(deadline, outcome).await {
            Err(_) => Err(timed_out(format!(
                "no broker confirm for {describe} within {confirm_timeout}ms"
            ))),
            Ok(Err(_)) => Err(closed(format!("{describe} was abandoned before the broker answered"))),
            Ok(Ok(result)) => result.map(|()| message_id),
        }
    }
}

fn jitter() -> f64 {
    // A UUID is the randomness already at hand; this needs no better.
    (uuid::Uuid::new_v4().as_u128() % 10_000) as f64 / 10_000.0
}

/// The properties a retry or dead-letter republish copies from the original.
/// Not carried: the delivery mode (persistent at every hop), `expiration` (it
/// would race the retry queue's TTL, and on the DLQ delete the evidence) and
/// `user_id` (validated against the publishing connection's user).
fn carried(p: &Properties) -> Properties {
    Properties {
        content_type: p.content_type.clone(),
        content_encoding: p.content_encoding.clone(),
        priority: p.priority,
        timestamp: p.timestamp,
        r#type: p.r#type.clone(),
        app_id: p.app_id.clone(),
        ..Properties::default()
    }
}

fn reply_properties(correlation_id: &str, headers: Option<FieldTable>) -> Properties {
    Properties {
        content_type: Some("application/octet-stream".into()),
        correlation_id: Some(correlation_id.to_string()),
        headers,
        ..Properties::default()
    }
}

fn retry_count(headers: &FieldTable) -> u32 {
    headers
        .get("x-retry-count")
        .and_then(FieldValue::as_int)
        .filter(|n| *n >= 0)
        .map_or(0, |n| n.min(i64::from(u32::MAX)) as u32)
}

struct Consumer {
    connection: Connection,
    channel: ChannelHandle,
    queue: String,
    handler: MessageHandler,
    options: ConsumeOptions,
    late_ack: bool,
    retry: Option<ConsumeRetryOptions>,
    processing_timeout_ms: Option<u64>,
}

/// One delivery, from arrival to settlement.
struct InFlight {
    consumer: Arc<Consumer>,
    delivery: amqp::Delivery,
    entry: Arc<DeliveryEntry>,
    correlation_id: String,
    reply_to: Option<String>,
    headers: Arc<FieldTable>,
    finished: AtomicBool,
}

/// Counts a handler as running until it returns, panics or is dropped.
pub(crate) struct RunningHandler(Connection);

impl Drop for RunningHandler {
    fn drop(&mut self) {
        self.0.drain_update(|d| d.1 = d.1.saturating_sub(1));
    }
}

impl Connection {
    /// Count handler work as running, for draining, until the guard drops:
    /// hold it in any task that runs handler code past its delivery's
    /// settlement, such as a streaming producer.
    pub(crate) fn running_handler(&self) -> RunningHandler {
        self.drain_update(|d| d.1 += 1);
        RunningHandler(self.clone())
    }
}

impl InFlight {
    fn new(consumer: Arc<Consumer>, delivery: amqp::Delivery) -> Arc<Self> {
        Arc::new(InFlight {
            consumer,
            correlation_id: delivery.properties.correlation_id.clone().unwrap_or_default(),
            reply_to: delivery.properties.reply_to.clone().filter(|r| !r.is_empty()),
            headers: Arc::new(delivery.properties.headers.clone().unwrap_or_default()),
            delivery,
            entry: Arc::new(DeliveryEntry {
                cancel: CancelToken::new(),
                cancelled: AtomicBool::new(false),
            }),
            finished: AtomicBool::new(false),
        })
    }

    fn connection(&self) -> &Connection {
        &self.consumer.connection
    }

    fn settles_late(&self) -> bool {
        !self.consumer.options.no_ack && self.consumer.late_ack
    }

    async fn ack(&self) -> Result<()> {
        Ok(self.consumer.channel.raw().ack(self.delivery.delivery_tag).await?)
    }

    async fn handle(self: Arc<Self>) {
        let c = &self.consumer;
        let retries = retry_count(&self.headers);
        log::debug!(
            target: "protobus",
            "incoming message on {} (routing key {}){}",
            c.queue,
            self.delivery.routing_key,
            if retries > 0 { format!(" (retry {retries})") } else { String::new() }
        );

        if !c.options.no_ack && !c.late_ack {
            if let Err(e) = self.ack().await {
                log::debug!(target: "protobus", "early ack failed on {}: {e}", c.queue);
            }
        }

        // Registered for the whole delivery, so cancel_stream() reaches the handler at any point.
        self.connection()
            .inner
            .active
            .lock()
            .unwrap()
            .entry(self.correlation_id.clone())
            .or_default()
            .push(self.entry.clone());

        let context = MessageHandlerContext {
            cancel: self.entry.cancel.clone(),
            routing_key: self.delivery.routing_key.clone(),
            message_id: self.delivery.properties.message_id.clone(),
            redelivered: self.delivery.redelivered,
            headers: self.headers.clone(),
        };
        let running = self.connection().running_handler();
        let work = (c.handler)(self.delivery.body.clone(), self.correlation_id.clone(), context);

        if c.options.ordered {
            // Reply routing: inline, to keep the order; no processing timeout applies.
            let result = work.await;
            drop(running);
            self.settle(result).await;
            return;
        }

        // The handler runs on a task of its own: a panic is contained, and a
        // handler that outlives its deadline keeps running without holding up
        // the settlement.
        let limit = c
            .processing_timeout_ms
            .unwrap_or_else(config::message_processing_timeout_ms);
        let mut task = self.connection().spawn(async move {
            let _running = running;
            work.await
        });
        tokio::select! {
            joined = &mut task => match joined {
                Ok(result) => self.settle(result).await,
                Err(e) => {
                    let error = Error::failure(format!("the handler panicked: {e}")).with_name("Panic");
                    self.settle_error(error, None).await;
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(limit)) => {
                // The handler runs on, but its result is discarded: the attempt has failed.
                self.entry.cancel.cancel();
                let error = Error::new(
                    ErrorKind::ProcessingTimeout,
                    format!("message {} exceeded the {limit}ms processing timeout", self.correlation_id),
                );
                self.settle_error(error, None).await;
            }
        }
    }

    async fn settle(&self, result: std::result::Result<HandlerResult, HandlerFailure>) {
        let result = match result {
            Ok(r) => r,
            Err(failure) => return self.settle_error(failure.error, failure.reply).await,
        };
        // The reply goes before the settlement, so the worst case is a
        // redelivered request rather than a settled one whose reply was lost.
        let replied: Result<()> = async {
            match (result, &self.reply_to) {
                (HandlerResult::Stream(chunks), Some(reply_to)) => self.publish_stream_reply(reply_to, chunks).await?,
                (HandlerResult::Reply(reply), Some(reply_to)) => {
                    self.publish_reply(reply_to, reply, None).await?;
                }
                _ => {}
            }
            if self.settles_late() {
                self.ack().await?;
            }
            Ok(())
        }
        .await;
        match replied {
            Ok(()) => self.finish(),
            Err(e) => self.settle_error(e, None).await,
        }
    }

    async fn publish_reply(&self, reply_to: &str, body: Vec<u8>, headers: Option<FieldTable>) -> Result<String> {
        let options = PublishOptions {
            properties: reply_properties(&self.correlation_id, headers),
            mandatory: false,
        };
        self.connection()
            .publish(
                Some(&self.consumer.channel),
                &config::callbacks_exchange_name(),
                reply_to,
                body,
                options,
            )
            .await
    }

    fn finish(&self) {
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            let mut active = self.connection().inner.active.lock().unwrap();
            if let Some(group) = active.get_mut(&self.correlation_id) {
                group.retain(|e| !Arc::ptr_eq(e, &self.entry));
                if group.is_empty() {
                    active.remove(&self.correlation_id);
                }
            }
        }
        self.connection().drain_update(|d| d.0 = d.0.saturating_sub(1));
    }

    async fn settle_error(&self, error: Error, reply: Option<Vec<u8>>) {
        if let Err(e) = self.settle_error_or_fail(error, reply).await {
            // Every path that can fail runs before the settlement, so the
            // message is still unacknowledged. Hand it back to the broker,
            // after a pause so a persistent failure does not spin.
            let late = self.settles_late();
            log::error!(
                target: "protobus",
                "failed to settle message on {}: {e}{}",
                self.consumer.queue,
                if late { ". Requeueing it in 1 s." } else { "" }
            );
            if late {
                let raw = self.consumer.channel.raw().clone();
                let tag = self.delivery.delivery_tag;
                self.connection().spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if let Err(e) = raw.reject(tag, true).await {
                        log::debug!(target: "protobus", "requeue failed (the channel is likely gone, which requeues it anyway): {e}");
                    }
                });
            }
        }
        self.finish();
    }

    async fn settle_error_or_fail(&self, error: Error, reply: Option<Vec<u8>>) -> Result<()> {
        let c = &self.consumer;
        let raw = c.channel.raw();
        let tag = self.delivery.delivery_tag;
        // A cancelled delivery is a normal outcome: the caller asked to stop.
        if self.entry.cancelled.load(Ordering::SeqCst) {
            log::debug!(target: "protobus", "message {} ended because its stream was cancelled", self.correlation_id);
            if self.settles_late() {
                self.ack().await?;
            }
            return Ok(());
        }

        let reply = reply.or_else(|| {
            c.options
                .build_error_reply
                .as_ref()
                .and_then(|build| build(&self.delivery.body, &error))
        });
        log::error!(target: "protobus", "unhandled error consuming bus message - {error}");

        // BEST EFFORT, deliberately: the reply may be lost (the caller has a
        // timeout), while the dead-letter queue is the only durable record of
        // the message.
        let publish_error_reply = || async {
            let (Some(reply_to), Some(reply)) = (&self.reply_to, reply.clone()) else {
                return;
            };
            if let Err(e) = self.publish_reply(reply_to, reply, None).await {
                log::error!(
                    target: "protobus",
                    "failed to publish the error reply for {} to {reply_to}: {e}. The caller will time out; settling the message anyway.",
                    self.correlation_id
                );
            }
        };

        if !self.settles_late() {
            // Acked before processing: retry and dead-lettering are impossible,
            // but the caller must still be told.
            publish_error_reply().await;
            return Ok(());
        }

        let handled = error.is_handled();
        let original = &self.delivery.properties;
        let retries = retry_count(&self.headers);
        // The delivered routing key, which the retry hop preserves. The header
        // is written for operators; nothing routes by it.
        let original_routing_key = self.delivery.routing_key.clone();
        let first_failure = self
            .headers
            .get("x-first-failure-time")
            .cloned()
            .unwrap_or(FieldValue::I64(now_ms()));

        match &c.retry {
            Some(retry) if !handled && retry.max_retries > 0 => {
                let mut headers = (*self.headers).clone();
                headers.insert(
                    "x-original-routing-key".into(),
                    FieldValue::from(original_routing_key.as_str()),
                );
                headers.insert("x-first-failure-time".into(), first_failure);
                headers.insert("x-last-error".into(), FieldValue::from(safe_error_summary(&error)));
                let mut properties = carried(original);
                properties.delivery_mode = Some(2);
                properties.correlation_id = Some(self.correlation_id.clone());
                properties.message_id = original.message_id.clone();
                if retries < retry.max_retries {
                    let next = retries + 1;
                    log::warn!(target: "protobus", "retrying message {} (attempt {next}/{})", self.correlation_id, retry.max_retries);
                    headers.insert("x-retry-count".into(), FieldValue::I64(i64::from(next)));
                    properties.reply_to = self.reply_to.clone();
                    properties.headers = Some(headers);
                    let options = PublishOptions {
                        properties,
                        mandatory: true,
                    };
                    // Published to the retry EXCHANGE under the original routing
                    // key, so the post-TTL dead-letter hop routes it back to the
                    // service queue. The caller stays parked.
                    let (exchange, key) = if retry.retry_exchange_name.is_empty() {
                        ("", retry.retry_queue_name.as_str())
                    } else {
                        (retry.retry_exchange_name.as_str(), original_routing_key.as_str())
                    };
                    self.connection()
                        .publish(Some(&c.channel), exchange, key, self.delivery.body.clone(), options)
                        .await?;
                } else {
                    log::error!(
                        target: "protobus",
                        "message {} exceeded max retries ({}), sending to DLQ",
                        self.correlation_id,
                        retry.max_retries
                    );
                    publish_error_reply().await;
                    headers.insert("x-retry-count".into(), FieldValue::I64(i64::from(retries)));
                    headers.insert("x-original-queue".into(), FieldValue::from(c.queue.as_str()));
                    headers.insert("x-dlq-time".into(), FieldValue::I64(now_ms()));
                    properties.headers = Some(headers);
                    let options = PublishOptions {
                        properties,
                        mandatory: true,
                    };
                    self.connection()
                        .publish(
                            Some(&c.channel),
                            "",
                            &retry.dlq_name,
                            self.delivery.body.clone(),
                            options,
                        )
                        .await?;
                }
                self.ack().await?;
            }
            _ => {
                if handled {
                    log::warn!(target: "protobus", "handled error for message {}, not retrying: {error}", self.correlation_id);
                }
                publish_error_reply().await;
                log::warn!(target: "protobus", "rejecting message {}", self.correlation_id);
                raw.reject(tag, false).await?;
            }
        }
        Ok(())
    }

    /// Publish a streaming reply: every chunk carries the correlation id and
    /// an `x-protobus-seq` from 0; all but the last `x-protobus-final=false`,
    /// the last `true`. A producer that yields nothing produces one empty
    /// final chunk. Look-ahead by one: a chunk is held until the next exists
    /// or the producer ends.
    async fn publish_stream_reply(&self, reply_to: &str, mut chunks: ReplyStream) -> Result<()> {
        let cancelled = || Error::new(ErrorKind::StreamCancelled, "the stream was cancelled by its caller");
        let is_cancelled = || self.entry.cancelled.load(Ordering::SeqCst);
        let headers = |seq: i64, last: bool| {
            let mut h = FieldTable::new();
            h.insert(config::HEADER_FINAL.into(), FieldValue::Bool(last));
            h.insert(config::HEADER_SEQ.into(), FieldValue::I64(seq));
            Some(h)
        };
        let mut buffered: Option<Vec<u8>> = None;
        let mut seq = 0i64;
        loop {
            let next = tokio::select! {
                biased;
                _ = self.entry.cancel.cancelled() => None,
                next = chunks.next() => Some(next),
            };
            // A caller that cancelled is not listening: stop sending, and drop
            // the producer's stream, which is how it learns to stop.
            let Some(next) = next.filter(|_| !is_cancelled()) else {
                log::debug!(target: "protobus", "stream {} cancelled after {seq} chunk(s)", self.correlation_id);
                return Err(cancelled());
            };
            let Some(next) = next else { break };
            let chunk = next?;
            if let Some(previous) = buffered.replace(chunk) {
                self.publish_reply(reply_to, previous, headers(seq, false)).await?;
                seq += 1;
            }
        }
        if is_cancelled() {
            return Err(cancelled());
        }
        match buffered {
            Some(last) => self.publish_reply(reply_to, last, headers(seq, true)).await?,
            None => self.publish_reply(reply_to, Vec::new(), headers(0, true)).await?,
        };
        Ok(())
    }
}

impl Connection {
    pub(crate) fn mark_shut_down(&self) {
        self.inner.state.lock().unwrap().shut_down = true;
    }
}
