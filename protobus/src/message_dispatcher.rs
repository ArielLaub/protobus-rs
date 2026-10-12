//! The caller's side of RPC: publishes requests and routes their replies,
//! unary and streaming, back to the call that is waiting for them.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::{oneshot, Notify};
use tokio::time::Instant;

use crate::amqp::{FieldTable, FieldValue, Properties};
use crate::config;
use crate::connection::{ChannelHandle, Connection, HandlerResult, ListenerId, MessageHandler, PublishOptions};
use crate::errors::{Error, ErrorKind, Result};
use crate::listeners::CallbackListener;
use crate::options::{CallOptions, StreamOptions};
use crate::priority;

/// AMQP carries `message-id` as a shortstr: one length byte, so 255 at most.
pub const MAX_MESSAGE_ID_BYTES: usize = 255;

tokio::task_local! {
    /// The service whose handler turn the current task holds, when its
    /// handlers are serialized: a call back into it could never be answered.
    pub(crate) static SERIALIZED_SERVICE: String;
}

fn disconnected(message: &str) -> Error {
    Error::new(ErrorKind::Disconnected, message)
}

fn not_connected() -> Error {
    Error::new(ErrorKind::NotConnected, "not connected")
}

/// A caller-supplied messageId, or `None` to have one minted. Blank is refused
/// rather than treated as absent: an id derived from a field that came out
/// empty would give every attempt a different identity, and no deduplication
/// at all.
pub(crate) fn validate_message_id(message_id: Option<&str>) -> Result<Option<String>> {
    let Some(id) = message_id else {
        return Ok(None);
    };
    if id.trim().is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidMessageId,
            format!("messageId must be a non-empty string, got \"{id}\". Leave it unset to have one generated."),
        ));
    }
    if id.len() > MAX_MESSAGE_ID_BYTES {
        return Err(Error::new(
            ErrorKind::InvalidMessageId,
            format!(
                "messageId is {} bytes; AMQP carries message-id as a shortstr, so it must be at most \
                 {MAX_MESSAGE_ID_BYTES}. Hash a long key rather than concatenating it.",
                id.len()
            ),
        ));
    }
    Ok(Some(id.to_string()))
}

struct Inner {
    connection: Connection,
    callback_listener: CallbackListener,
    callbacks: Mutex<HashMap<String, oneshot::Sender<Result<Vec<u8>>>>>,
    /// correlationId → in-flight streaming reply. Distinct from `callbacks`,
    /// so a streaming reply cannot resolve an unrelated unary call.
    streams: Mutex<HashMap<String, Arc<StreamCall>>>,
    /// Bytes buffered across every pending stream: a bound on the process, not
    /// one call.
    total_buffered: AtomicU64,
    channel: Mutex<Option<ChannelHandle>>,
    channel_lock: tokio::sync::Mutex<()>,
    initialized: AtomicBool,
    registrations: Mutex<Option<(ListenerId, ListenerId)>>,
}

/// The caller's side of RPC. Cheap to clone.
#[derive(Clone)]
pub struct MessageDispatcher {
    inner: Arc<Inner>,
}

/// Removes a pending unary call when its caller stops waiting, however it stops.
struct PendingCall<'a> {
    inner: &'a Inner,
    id: &'a str,
}

impl Drop for PendingCall<'_> {
    fn drop(&mut self) {
        self.inner.callbacks.lock().unwrap().remove(self.id);
    }
}

impl MessageDispatcher {
    pub fn new(connection: &Connection) -> Self {
        let dispatcher = MessageDispatcher {
            inner: Arc::new(Inner {
                connection: connection.clone(),
                callback_listener: CallbackListener::new(connection),
                callbacks: Mutex::new(HashMap::new()),
                streams: Mutex::new(HashMap::new()),
                total_buffered: AtomicU64::new(0),
                channel: Mutex::new(None),
                channel_lock: tokio::sync::Mutex::new(()),
                initialized: AtomicBool::new(false),
                registrations: Mutex::new(None),
            }),
        };
        let weak = Arc::downgrade(&dispatcher.inner);
        let on_disconnected = connection.on_disconnected(move || {
            if let Some(inner) = weak.upgrade() {
                // Their replies went with the connection.
                log::debug!(target: "protobus", "MessageDispatcher: connection lost, rejecting pending callbacks");
                *inner.channel.lock().unwrap() = None;
                // A connection that is coming back is waited for by what has
                // not been sent yet; one that was closed is not coming back.
                let closed = !inner.connection.is_reconnecting();
                inner.fail_pending(disconnected("Connection lost during RPC call"), closed);
            }
        });
        let weak = Arc::downgrade(&dispatcher.inner);
        let restorer = connection.register_restorer(Arc::new(move |_| {
            let weak = weak.clone();
            Box::pin(async move {
                let Some(inner) = weak.upgrade() else { return Ok(()) };
                if !inner.initialized.load(Ordering::SeqCst) {
                    return Ok(());
                }
                log::info!(target: "protobus", "MessageDispatcher: reconnected, re-initializing channel");
                inner.open_publish_channel().await.map(|_| ())
            })
        }));
        *dispatcher.inner.registrations.lock().unwrap() = Some((on_disconnected, restorer));
        dispatcher
    }

    pub fn is_initialized(&self) -> bool {
        self.inner.initialized.load(Ordering::SeqCst)
    }

    pub async fn init(&self) -> Result<()> {
        if self.is_initialized() {
            return Ok(());
        }
        self.inner.open_publish_channel().await?;
        let weak = Arc::downgrade(&self.inner);
        let on_result: MessageHandler = Arc::new(move |content, id, context| {
            // Routed right here, in delivery order, and never waiting on anything.
            if let Some(inner) = weak.upgrade() {
                inner.on_result(content, &id, &context.headers);
            }
            Box::pin(async { Ok(HandlerResult::None) })
        });
        let listener = &self.inner.callback_listener;
        listener.init(Some(on_result), None).await?;
        // A reply queue rebuilt on a live connection is a new, server-named
        // queue: requests in flight name the old one, so their replies will
        // never come.
        let weak: Weak<Inner> = Arc::downgrade(&self.inner);
        listener.on_queue_replaced(move || {
            if let Some(inner) = weak.upgrade() {
                inner.fail_pending(
                    disconnected("the reply queue was replaced while the call was pending"),
                    false,
                );
            }
        });
        listener.start().await?;
        self.inner.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Publish a request and wait for its reply (or, with `rpc` false, for the
    /// broker's confirm). The deadline, `timeout_ms` or `RPC_CALL_TIMEOUT_MS`,
    /// starts once the connection is ready and bounds the confirm as well as
    /// the reply. When the publish fails, its error wins over the deadline:
    /// "the request never left" is the more specific answer.
    ///
    /// Dropping the returned future stops waiting; the request may already
    /// have been delivered. Returns the raw reply, or `None` when `rpc` is false.
    pub async fn publish(&self, content: Vec<u8>, routing_key: &str, options: &CallOptions) -> Result<Option<Vec<u8>>> {
        let inner = &self.inner;
        let priority = priority::validate_priority(options.priority)?;
        let caller_message_id = validate_message_id(options.message_id.as_deref())?;
        let connection = &inner.connection;
        // A reconnection is waited through; anything else with no connection is
        // a caller error, reported at once.
        if !connection.is_connected() && !connection.is_reconnecting() {
            return Err(not_connected());
        }
        if options.rpc {
            refuse_serialized_self_call(routing_key)?;
        }
        let channel = connection.ready_channel(|| inner.publish_channel()).await?;

        let id = uuid::Uuid::new_v4().to_string();
        let properties = Properties {
            content_type: Some("application/octet-stream".into()),
            correlation_id: Some(id.clone()),
            delivery_mode: Some(2),
            reply_to: options.rpc.then(|| inner.callback_listener.queue_name()),
            // Set only when asked for, so a publish with no priority carries none.
            priority,
            message_id: caller_message_id,
            ..Properties::default()
        };
        let exchange = config::bus_exchange_name();
        // An RPC request that routes nowhere means no service is bound to the
        // key: worth learning now, as UNROUTABLE, rather than after the
        // timeout. Not for one-way publishes, where no consumer may be normal.
        let publish_options = PublishOptions {
            properties,
            mandatory: options.rpc,
        };
        if !options.rpc {
            connection
                .publish(Some(&channel), &exchange, routing_key, content, publish_options)
                .await?;
            return Ok(None);
        }

        let limit = options.timeout_ms.unwrap_or_else(config::rpc_call_timeout_ms);
        // Armed BEFORE publishing: a fast service can reply while the confirm
        // is still in flight.
        let (sender, mut receiver) = oneshot::channel();
        inner.callbacks.lock().unwrap().insert(id.clone(), sender);
        let _pending = PendingCall { inner, id: &id };
        // The deadline is the caller's: it ends the call whatever is still
        // outstanding, the reply or the request's own confirm.
        let deadline = tokio::time::sleep(Duration::from_millis(limit));
        let published = connection.publish(Some(&channel), &exchange, routing_key, content, publish_options);
        tokio::pin!(deadline, published);
        let mut confirmed = false;
        let mut reply: Option<Vec<u8>> = None;
        loop {
            tokio::select! {
                // In this order: a disconnect is reported as one, not as the
                // write it failed under the call; and a failed publish, being
                // the more specific answer, wins over the deadline.
                biased;
                answer = &mut receiver, if reply.is_none() => {
                    // A disconnect settles the call at once, without waiting for
                    // the confirm. A reply, though, is only taken once the
                    // request is confirmed, because a failed publish is the
                    // more specific answer.
                    let body = answer.unwrap_or_else(|_| Err(disconnected("Connection lost during RPC call")))?;
                    if confirmed {
                        return Ok(Some(body));
                    }
                    reply = Some(body);
                }
                result = &mut published, if !confirmed => {
                    // The request never made it: no reply is coming.
                    result?;
                    confirmed = true;
                    if let Some(reply) = reply.take() {
                        return Ok(Some(reply));
                    }
                }
                _ = &mut deadline => {
                    return Err(Error::new(
                        ErrorKind::RpcTimeout,
                        format!("no reply for {routing_key} (correlationId {id}) within {limit}ms"),
                    ));
                }
            }
        }
    }

    /// Publish a request expecting a streaming reply. Returns at once; the
    /// request is published as soon as the connection is ready, and a failure
    /// to publish surfaces from the stream's first read.
    ///
    /// Dropping the stream before its end, or calling
    /// [`cancel`](ChunkStream::cancel), releases its buffer and tells the
    /// producer to stop. That notice is best effort and cooperative: a
    /// producer that ignores its token runs to completion, and its chunks are
    /// dropped here.
    pub fn publish_streaming(
        &self,
        content: Vec<u8>,
        routing_key: &str,
        options: &StreamOptions,
    ) -> Result<ChunkStream> {
        let inner = &self.inner;
        let connection = inner.connection.clone();
        if !connection.is_connected() && !connection.is_reconnecting() {
            return Err(not_connected());
        }
        refuse_serialized_self_call(routing_key)?;
        let id = uuid::Uuid::new_v4().to_string();
        let idle_ms = options.idle_timeout_ms.unwrap_or_else(config::stream_idle_timeout_ms);
        let call = Arc::new(StreamCall {
            owner: Arc::downgrade(inner),
            id: id.clone(),
            idle_ms,
            state: Mutex::new(StreamState::default()),
            wake: Notify::new(),
        });
        inner.streams.lock().unwrap().insert(id.clone(), call.clone());
        // Armed at call time, so a caller that never reads still releases what
        // the call holds.
        call.arm_idle();
        let watchdog = connection.spawn(StreamCall::watch_idle(call.clone()));
        call.state.lock().unwrap().watchdog = Some(watchdog);

        let weak = Arc::downgrade(inner);
        let stream = call.clone();
        let routing_key = routing_key.to_string();
        connection.clone().spawn(async move {
            let published: Result<()> = async {
                let Some(inner) = weak.upgrade() else {
                    return Ok(());
                };
                let channel = connection.ready_channel(|| inner.publish_channel()).await?;
                // Read after readiness: a reconnection replaces the reply queue
                // and the channel.
                let properties = Properties {
                    content_type: Some("application/octet-stream".into()),
                    correlation_id: Some(id),
                    reply_to: Some(inner.callback_listener.queue_name()),
                    delivery_mode: Some(2),
                    ..Properties::default()
                };
                if !stream.mark_sent() {
                    // Cancelled or timed out while it waited: nothing to send.
                    return Ok(());
                }
                let options = PublishOptions {
                    properties,
                    mandatory: false,
                };
                connection
                    .publish(
                        Some(&channel),
                        &config::bus_exchange_name(),
                        &routing_key,
                        content,
                        options,
                    )
                    .await?;
                Ok(())
            }
            .await;
            if let Err(e) = published {
                stream.fail(e);
            }
        });
        Ok(ChunkStream { call })
    }

    /// Streaming calls still holding an entry.
    pub fn pending_stream_count(&self) -> usize {
        self.inner.streams.lock().unwrap().len()
    }

    /// Unary calls still waiting for a reply.
    pub fn pending_call_count(&self) -> usize {
        self.inner.callbacks.lock().unwrap().len()
    }

    /// Bytes buffered across every pending stream.
    pub fn buffered_stream_bytes(&self) -> u64 {
        self.inner.total_buffered.load(Ordering::SeqCst)
    }

    pub async fn close(&self) {
        let inner = &self.inner;
        if let Some((on_disconnected, restorer)) = inner.registrations.lock().unwrap().take() {
            inner.connection.remove_listener(on_disconnected);
            inner.connection.unregister_restorer(restorer);
        }
        if inner.callback_listener.is_initialized() {
            inner.callback_listener.close().await;
        }
        let channel = inner.channel.lock().unwrap().take();
        if let Some(channel) = channel.filter(ChannelHandle::is_open) {
            let _ = channel.close().await;
        }
    }
}

fn refuse_serialized_self_call(routing_key: &str) -> Result<()> {
    let own = SERIALIZED_SERVICE
        .try_with(|service| routing_key.starts_with(&format!("REQUEST.{service}.")))
        .unwrap_or(false);
    if own {
        return Err(Error::failure(format!(
            "{routing_key} was called from a handler of the same service, whose handlers are serialized: \
             the request could only run after this handler returns, so waiting for it would deadlock"
        ))
        .with_name("SerializedSelfCall"));
    }
    Ok(())
}

impl Inner {
    /// Open the publishing channel and declare the exchanges this dispatcher
    /// publishes to, so a client works against a broker no service has touched yet.
    async fn open_publish_channel(&self) -> Result<ChannelHandle> {
        let channel = self.connection.open_channel().await?;
        self.connection
            .declare_exchange(&channel, &config::bus_exchange_name(), "topic")
            .await?;
        self.connection
            .declare_exchange(&channel, &config::cancel_exchange_name(), "fanout")
            .await?;
        *self.channel.lock().unwrap() = Some(channel.clone());
        Ok(channel)
    }

    /// The publishing channel, reopened when it was lost on a live connection
    /// (a channel error closes only that channel).
    async fn publish_channel(&self) -> Result<Option<ChannelHandle>> {
        let current = || self.channel.lock().unwrap().clone();
        if let Some(channel) = current().filter(ChannelHandle::is_open) {
            return Ok(Some(channel));
        }
        let _one_at_a_time = self.channel_lock.lock().await;
        let channel = current();
        if channel.as_ref().is_some_and(ChannelHandle::is_open) || !self.connection.is_ready() {
            return Ok(channel);
        }
        log::warn!(target: "protobus", "MessageDispatcher: publishing channel lost on a live connection; reopening it");
        self.open_publish_channel().await.map(Some)
    }

    /// Fail every pending call, and every stream whose request has left. A
    /// stream still waiting to be sent has lost nothing, and waits for the
    /// connection as a unary publish would, unless `unsent_too`.
    fn fail_pending(&self, error: Error, unsent_too: bool) {
        let callbacks: Vec<_> = self.callbacks.lock().unwrap().drain().collect();
        for (_, sender) in callbacks {
            let _ = sender.send(Err(error.clone()));
        }
        let streams: Vec<_> = self
            .streams
            .lock()
            .unwrap()
            .values()
            .filter(|s| unsent_too || s.sent())
            .cloned()
            .collect();
        for stream in streams {
            stream.fail(error.clone());
        }
    }

    /// A reply, delivered in order on the reply queue.
    fn on_result(&self, content: Vec<u8>, id: &str, headers: &FieldTable) {
        let stream = self.streams.lock().unwrap().get(id).cloned();
        if let Some(stream) = stream {
            stream.on_chunk(content, headers);
            return;
        }
        // The deadline stays armed: it belongs to the caller's result, which
        // may still be waiting for the request's confirm.
        if let Some(sender) = self.callbacks.lock().unwrap().remove(id) {
            let _ = sender.send(Ok(content));
        }
    }

    fn subtract_buffered(&self, bytes: u64) {
        let mut current = self.total_buffered.load(Ordering::SeqCst);
        loop {
            let next = current.saturating_sub(bytes);
            match self
                .total_buffered
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}

/// Read `x-protobus-seq`; absent or unparseable disables validation rather
/// than inventing a violation.
pub(crate) fn parse_seq(headers: &FieldTable) -> Option<i64> {
    headers
        .get(config::HEADER_SEQ)
        .and_then(FieldValue::as_int)
        .filter(|n| *n >= 0)
}

pub(crate) fn parse_final(headers: &FieldTable) -> bool {
    headers.get(config::HEADER_FINAL).is_some_and(FieldValue::as_bool)
}

#[derive(Default)]
struct StreamState {
    chunks: VecDeque<Vec<u8>>,
    buffered_bytes: u64,
    /// Highest sequence accepted, or `None` before the first and for peers
    /// that send none.
    last_seq: Option<i64>,
    ended: bool,
    error: Option<Error>,
    cancelled: bool,
    /// The request has been handed to the broker (or is about to be).
    sent: bool,
    /// When the stream expires unless either side makes progress first.
    idle_deadline: Option<Instant>,
    watchdog: Option<tokio::task::JoinHandle<()>>,
}

/// One pending streaming call: chunks buffered until the caller takes them.
struct StreamCall {
    owner: Weak<Inner>,
    id: String,
    idle_ms: u64,
    state: Mutex<StreamState>,
    /// Wakes the reader. One permit is kept when nobody waits, so a wake-up
    /// between a check and the wait is never lost.
    wake: Notify,
}

impl StreamCall {
    fn end(&self, s: &mut StreamState, error: Option<Error>) {
        if error.is_some() && s.error.is_none() {
            s.error = error;
        }
        s.ended = true;
        s.idle_deadline = None;
        self.wake.notify_one();
    }

    /// Restart the idle deadline, on progress from either side: a chunk
    /// arriving, or a chunk handed to the caller.
    fn arm_idle(&self) {
        let mut s = self.state.lock().unwrap();
        self.arm_idle_locked(&mut s);
    }

    fn arm_idle_locked(&self, s: &mut StreamState) {
        s.idle_deadline = (!s.ended).then(|| Instant::now() + Duration::from_millis(self.idle_ms));
    }

    /// Expires the stream when its deadline passes without having moved. The
    /// deadline is read under the lock that every progress path takes, so a
    /// chunk arriving as the timer fires wins.
    async fn watch_idle(call: Arc<StreamCall>) {
        loop {
            let deadline = {
                let s = call.state.lock().unwrap();
                match s.idle_deadline {
                    Some(deadline) if !s.ended => deadline,
                    _ => return,
                }
            };
            tokio::time::sleep_until(deadline).await;
            {
                let mut s = call.state.lock().unwrap();
                match s.idle_deadline {
                    Some(current) if !s.ended && current <= Instant::now() => {
                        let error = Error::new(
                            ErrorKind::StreamTimeout,
                            format!("No streaming chunk received within {}ms", call.idle_ms),
                        );
                        call.end(&mut s, Some(error));
                    }
                    Some(_) if !s.ended => continue,
                    _ => return,
                }
            }
            // The producer is still working for a caller that stopped listening.
            call.cancel(true);
            return;
        }
    }

    fn sent(&self) -> bool {
        self.state.lock().unwrap().sent
    }

    /// Record that the request is leaving; false when the stream already ended.
    fn mark_sent(&self) -> bool {
        let mut s = self.state.lock().unwrap();
        s.sent = !s.ended;
        s.sent
    }

    fn fail(&self, error: Error) {
        let completed = {
            let mut s = self.state.lock().unwrap();
            // A stream that already ended without an error (its final chunk
            // arrived, or its caller cancelled it) keeps that outcome: a late
            // publish failure or a disconnect cannot turn it into an error.
            let completed = s.ended && s.error.is_none();
            if completed {
                self.wake.notify_one();
            } else {
                self.end(&mut s, Some(error));
            }
            completed
        };
        if completed {
            // No more replies are coming for it; only the registration goes.
            self.unregister();
        } else {
            // Released now, keeping the error for the caller: one that never
            // reads the stream again must not hold its entry.
            self.release();
        }
    }

    fn unregister(&self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut streams = owner.streams.lock().unwrap();
            if streams
                .get(&self.id)
                .is_some_and(|s| std::ptr::eq(Arc::as_ptr(s), self))
            {
                streams.remove(&self.id);
            }
        }
    }

    fn release_buffer(&self) {
        let bytes = {
            let mut s = self.state.lock().unwrap();
            s.chunks.clear();
            std::mem::take(&mut s.buffered_bytes)
        };
        if bytes > 0 {
            if let Some(owner) = self.owner.upgrade() {
                owner.subtract_buffered(bytes);
            }
        }
    }

    /// Everything the call holds, released on any terminal outcome.
    fn release(&self) {
        let watchdog = {
            let mut s = self.state.lock().unwrap();
            s.idle_deadline = None;
            s.watchdog.take()
        };
        if let Some(watchdog) = watchdog {
            watchdog.abort();
        }
        self.unregister();
        self.release_buffer();
    }

    /// Stop the producer and release the call. `notify_only` is for the
    /// failure paths, which have already recorded the error the caller is
    /// about to see.
    fn cancel(&self, notify_only: bool) {
        {
            let mut s = self.state.lock().unwrap();
            if s.cancelled {
                return;
            }
            s.cancelled = true;
            if !notify_only {
                self.end(&mut s, None);
            }
        }
        self.release();
        log::debug!(target: "protobus", "cancelling stream {}", self.id);
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let properties = Properties {
            correlation_id: Some(self.id.clone()),
            content_type: Some("application/octet-stream".into()),
            ..Properties::default()
        };
        let id = self.id.clone();
        // Fire and forget: the caller has stopped waiting.
        owner.connection.clone().spawn(async move {
            let sent: Result<()> = async {
                let channel = owner.publish_channel().await?;
                let options = PublishOptions {
                    properties,
                    mandatory: false,
                };
                owner
                    .connection
                    .publish(
                        channel.as_ref(),
                        &config::cancel_exchange_name(),
                        "",
                        Vec::new(),
                        options,
                    )
                    .await?;
                Ok(())
            }
            .await;
            if let Err(e) = sent {
                log::debug!(target: "protobus", "failed to publish cancel for stream {id}: {e}");
            }
        });
    }

    /// A chunk, delivered in order on the reply queue.
    fn on_chunk(&self, body: Vec<u8>, headers: &FieldTable) {
        let is_final = parse_final(headers);
        let seq = parse_seq(headers);
        let mut failed = false;
        {
            let mut s = self.state.lock().unwrap();
            if s.ended {
                return;
            }
            if let Some(seq) = seq {
                let expected = s.last_seq.map_or(0, |n| n + 1);
                if seq < expected {
                    // Already seen: a redelivery, not new data.
                    log::debug!(target: "protobus", "stream {}: dropping duplicate chunk seq={seq}", self.id);
                    if is_final {
                        self.end(&mut s, None);
                    }
                    return;
                }
                if seq > expected {
                    let error = Error::new(
                        ErrorKind::StreamSequence,
                        format!(
                            "stream {} lost at least one chunk: got seq={seq}, expected {expected}",
                            self.id
                        ),
                    );
                    self.end(&mut s, Some(error));
                    failed = true;
                } else {
                    s.last_seq = Some(seq);
                }
            }
            if !failed && !body.is_empty() {
                let owner = self.owner.upgrade();
                let size = body.len() as u64;
                let (max_chunks, max_bytes, max_total) = (
                    config::stream_max_buffered_chunks(),
                    config::stream_max_buffered_bytes(),
                    config::stream_max_total_buffered_bytes(),
                );
                let count = s.chunks.len() as u64 + 1;
                let would_be = s.buffered_bytes + size;
                let would_be_total = owner.as_ref().map_or(0, |o| o.total_buffered.load(Ordering::SeqCst)) + size;
                if count > max_chunks || would_be > max_bytes || would_be_total > max_total {
                    let error = Error::new(
                        ErrorKind::StreamBackpressure,
                        format!(
                            "stream {} exceeded a buffer limit ({count} chunks / {would_be} bytes for this call, \
                             {would_be_total} bytes across all calls; limits are {max_chunks} chunks / {max_bytes} bytes / \
                             {max_total} bytes total): the consumer is not keeping up with the producer",
                            self.id
                        ),
                    );
                    self.end(&mut s, Some(error));
                    failed = true;
                } else {
                    s.chunks.push_back(body);
                    s.buffered_bytes = would_be;
                    if let Some(owner) = &owner {
                        owner.total_buffered.fetch_add(size, Ordering::SeqCst);
                    }
                }
            }
            if !failed {
                if is_final {
                    self.end(&mut s, None);
                } else {
                    self.arm_idle_locked(&mut s);
                    self.wake.notify_one();
                }
            }
        }
        if failed {
            self.release_buffer();
            // The producer would keep going for a stream that has failed.
            self.cancel(true);
        }
    }

    /// The next chunk, or `None` at the end; fails with the stream's failure.
    /// A failure to publish the request is one of those outcomes, so a reader
    /// waits on all of them at once: an idle timeout or a cancel wakes it
    /// while the request's confirm is still outstanding.
    async fn next(&self) -> Option<Result<Vec<u8>>> {
        loop {
            {
                let mut s = self.state.lock().unwrap();
                if let Some(error) = s.error.clone() {
                    drop(s);
                    self.release();
                    return Some(Err(error));
                }
                if let Some(chunk) = s.chunks.pop_front() {
                    let size = chunk.len() as u64;
                    s.buffered_bytes = s.buffered_bytes.saturating_sub(size);
                    if !s.ended {
                        self.arm_idle_locked(&mut s);
                    }
                    drop(s);
                    if let Some(owner) = self.owner.upgrade() {
                        owner.subtract_buffered(size);
                    }
                    return Some(Ok(chunk));
                }
                if s.ended {
                    drop(s);
                    self.release();
                    return None;
                }
            }
            self.wake.notified().await;
        }
    }

    fn ended(&self) -> bool {
        self.state.lock().unwrap().ended
    }

    fn finished(&self) -> bool {
        let s = self.state.lock().unwrap();
        s.ended && s.chunks.is_empty()
    }
}

/// The raw reply bodies of one streaming call. Dropping it before the end
/// cancels the call. Read it from one task at a time.
pub struct ChunkStream {
    call: Arc<StreamCall>,
}

impl ChunkStream {
    /// The next chunk, `None` once the stream has ended, or the stream's failure.
    pub async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        self.call.next().await
    }

    /// Cancel: the producer's token fires and its chunks stop being published.
    /// Idempotent.
    pub fn cancel(&self) {
        if self.call.finished() {
            self.call.release();
        } else {
            self.call.cancel(false);
        }
    }

    /// The stream has ended and every chunk has been read.
    pub fn is_finished(&self) -> bool {
        self.call.finished()
    }

    /// Whether the stream's final chunk (or its failure) has arrived, read or not.
    pub fn has_ended(&self) -> bool {
        self.call.ended()
    }

    /// The stream's correlation id.
    pub fn id(&self) -> &str {
        &self.call.id
    }
}

impl Drop for ChunkStream {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_ids_are_validated() {
        assert_eq!(validate_message_id(None).unwrap(), None);
        assert_eq!(
            validate_message_id(Some("order-1")).unwrap().as_deref(),
            Some("order-1")
        );
        assert_eq!(
            validate_message_id(Some("")).unwrap_err().kind(),
            ErrorKind::InvalidMessageId
        );
        assert_eq!(
            validate_message_id(Some("  ")).unwrap_err().kind(),
            ErrorKind::InvalidMessageId
        );
        assert!(validate_message_id(Some(&"x".repeat(255))).is_ok());
        assert!(validate_message_id(Some(&"x".repeat(256))).is_err());
        // Bytes, not characters.
        assert!(validate_message_id(Some(&"é".repeat(128))).is_err());
    }

    #[test]
    fn stream_headers_parse_tolerantly() {
        let mut h = FieldTable::new();
        assert_eq!(parse_seq(&h), None);
        assert!(!parse_final(&h));
        h.insert(config::HEADER_SEQ.into(), FieldValue::String("3".into()));
        h.insert(config::HEADER_FINAL.into(), FieldValue::String("true".into()));
        assert_eq!(parse_seq(&h), Some(3));
        assert!(parse_final(&h));
        h.insert(config::HEADER_SEQ.into(), FieldValue::I32(-1));
        h.insert(config::HEADER_FINAL.into(), FieldValue::I8(0));
        assert_eq!(parse_seq(&h), None);
        assert!(!parse_final(&h));
    }
}
