//! An in-memory AMQP broker.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::amqp::{
    AmqpError, CancelCallback, Channel, CloseListeners, Confirm, ConfirmFuture, ConfirmOutcome, Connection, Delivery,
    DeliveryCallback, FieldTable, FieldValue, Properties, Transport,
};
use crate::trie::topic_matches;

/// How the broker answers publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfirmMode {
    /// Confirm every publish (the default).
    Ack,
    /// Refuse every publish.
    Nack,
    /// Never confirm: publishes are routed, and wait out their confirm timeout.
    Drop,
}

struct Exchange {
    kind: String,
    /// (queue, routing key)
    bindings: Vec<(String, String)>,
}

#[derive(Clone)]
struct Stored {
    id: u64,
    body: Vec<u8>,
    properties: Properties,
    exchange: String,
    routing_key: String,
    redelivered: bool,
}

struct Queue {
    durable: bool,
    /// The connection an exclusive queue belongs to.
    owner: Option<u64>,
    auto_delete: bool,
    arguments: FieldTable,
    messages: VecDeque<Stored>,
    /// (channel, consumer tag), in subscription order.
    consumers: Vec<(u64, String)>,
    next_consumer: usize,
}

impl Queue {
    fn ttl(&self) -> Option<u64> {
        self.arguments
            .get("x-message-ttl")
            .and_then(FieldValue::as_int)
            .map(|n| n.max(0) as u64)
    }

    fn dead_letter_exchange(&self) -> Option<String> {
        self.arguments
            .get("x-dead-letter-exchange")
            .and_then(FieldValue::as_text)
    }

    fn max_priority(&self) -> Option<u8> {
        self.arguments
            .get("x-max-priority")
            .and_then(FieldValue::as_int)
            .map(|n| n.clamp(0, 255) as u8)
    }
}

struct Consumer {
    queue: String,
    no_ack: bool,
    deliveries: mpsc::UnboundedSender<Delivery>,
    on_cancel: Option<CancelCallback>,
    unacked: usize,
}

struct ChannelState {
    connection: u64,
    prefetch: u16,
    next_tag: u64,
    /// delivery tag -> (queue, message, consumer tag)
    unacked: BTreeMap<u64, (String, Stored, String)>,
    consumers: HashMap<String, Consumer>,
    declared: HashSet<String>,
    close: Arc<CloseListeners<String>>,
}

struct Held {
    channel: u64,
    outcome: ConfirmOutcome,
    answer: oneshot::Sender<Confirm>,
}

struct ConnectionState {
    close: Arc<CloseListeners<Option<String>>>,
}

#[derive(Default)]
struct State {
    exchanges: HashMap<String, Exchange>,
    queues: HashMap<String, Queue>,
    channels: HashMap<u64, ChannelState>,
    connections: HashMap<u64, ConnectionState>,
    next_id: u64,
    refuse: bool,
    confirm_mode: Option<ConfirmMode>,
    held: Vec<Held>,
    connect_attempts: usize,
    /// Deliveries handed to a consumer whose callback has not yet returned.
    pending: Arc<AtomicUsize>,
}

type ClosedConnection = (Arc<CloseListeners<Option<String>>>, Option<String>);

/// Work to do once the broker's lock is released: listeners are application
/// code, and must never run under it.
#[derive(Default)]
struct Effects {
    closed_channels: Vec<(Arc<CloseListeners<String>>, String)>,
    closed_connections: Vec<ClosedConnection>,
    cancelled: Vec<CancelCallback>,
    /// (queue, message id, ttl): messages that now wait on a TTL.
    expiring: Vec<(String, u64, u64)>,
}

struct Inner {
    state: Mutex<State>,
    pending: Arc<AtomicUsize>,
}

/// An AMQP broker in memory, for testing services and clients without
/// RabbitMQ.
///
/// It implements the RabbitMQ behaviour protobus depends on: direct, topic and
/// fanout exchanges and the default exchange; durable, exclusive, auto-delete
/// and server-named queues; per-queue message TTL with dead-lettering;
/// priority queues; per-consumer prefetch with acknowledgement, rejection and
/// redelivery; publisher confirms with mandatory returns; and the channel
/// errors RabbitMQ raises for a missing exchange or queue (404), a
/// redeclaration with different arguments (406) and an exclusive queue owned
/// elsewhere (405). Deliveries to one consumer run one at a time, each awaited
/// before the next.
///
/// Cheap to clone: every clone is the same broker.
#[derive(Clone)]
pub struct MemoryBroker {
    inner: Arc<Inner>,
}

impl Default for MemoryBroker {
    fn default() -> Self {
        Self::new()
    }
}

fn closed_channel() -> AmqpError {
    AmqpError::new("channel is closed", 0)
}

impl State {
    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn mode(&self) -> ConfirmMode {
        self.confirm_mode.unwrap_or(ConfirmMode::Ack)
    }

    /// The queues a publish reaches, each once.
    fn route(&self, exchange: &str, routing_key: &str) -> Option<Vec<String>> {
        if exchange.is_empty() {
            return Some(if self.queues.contains_key(routing_key) {
                vec![routing_key.to_string()]
            } else {
                Vec::new()
            });
        }
        let x = self.exchanges.get(exchange)?;
        let mut queues: Vec<String> = Vec::new();
        for (queue, key) in &x.bindings {
            let matched = match x.kind.as_str() {
                "fanout" => true,
                "topic" => topic_matches(key, routing_key),
                _ => key == routing_key,
            };
            if matched && !queues.contains(queue) && self.queues.contains_key(queue) {
                queues.push(queue.clone());
            }
        }
        Some(queues)
    }

    fn enqueue(&mut self, queue: &str, message: Stored, front: bool, effects: &mut Effects) {
        let Some(q) = self.queues.get_mut(queue) else {
            return;
        };
        if let Some(ttl) = q.ttl() {
            effects.expiring.push((queue.to_string(), message.id, ttl));
        }
        if front {
            q.messages.push_front(message);
        } else {
            q.messages.push_back(message);
        }
        self.pump(queue);
    }

    /// Route a message into every queue it reaches; returns how many.
    fn deliver(
        &mut self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        properties: &Properties,
        effects: &mut Effects,
    ) -> usize {
        let queues = self.route(exchange, routing_key).unwrap_or_default();
        for queue in &queues {
            let message = Stored {
                id: self.id(),
                body: body.to_vec(),
                properties: properties.clone(),
                exchange: exchange.to_string(),
                routing_key: routing_key.to_string(),
                redelivered: false,
            };
            self.enqueue(queue, message, false, effects);
        }
        queues.len()
    }

    /// Hand waiting messages to consumers with room for them.
    fn pump(&mut self, queue: &str) {
        loop {
            let Some(q) = self.queues.get_mut(queue) else {
                return;
            };
            if q.messages.is_empty() || q.consumers.is_empty() {
                return;
            }
            // Round-robin over consumers with capacity.
            let count = q.consumers.len();
            let mut chosen = None;
            for step in 0..count {
                let index = (q.next_consumer + step) % count;
                let (channel_id, tag) = &q.consumers[index];
                let Some(channel) = self.channels.get(channel_id) else {
                    continue;
                };
                let Some(consumer) = channel.consumers.get(tag) else {
                    continue;
                };
                let room = consumer.no_ack || channel.prefetch == 0 || consumer.unacked < usize::from(channel.prefetch);
                if room {
                    chosen = Some((index, *channel_id, tag.clone()));
                    break;
                }
            }
            let Some((index, channel_id, tag)) = chosen else {
                return;
            };
            q.next_consumer = (index + 1) % count;
            // The highest priority first, in arrival order among equals.
            let position = match q.max_priority() {
                Some(max) => {
                    let priority = |m: &Stored| m.properties.priority.unwrap_or(0).min(max);
                    let best = q.messages.iter().map(priority).max().unwrap_or(0);
                    q.messages.iter().position(|m| priority(m) == best).unwrap_or(0)
                }
                None => 0,
            };
            let Some(message) = q.messages.remove(position) else {
                return;
            };
            let channel = self
                .channels
                .get_mut(&channel_id)
                .expect("the consumer's channel exists");
            channel.next_tag += 1;
            let delivery_tag = channel.next_tag;
            let delivery = Delivery {
                body: message.body.clone(),
                properties: message.properties.clone(),
                exchange: message.exchange.clone(),
                routing_key: message.routing_key.clone(),
                consumer_tag: tag.clone(),
                delivery_tag,
                redelivered: message.redelivered,
            };
            let consumer = channel.consumers.get_mut(&tag).expect("the consumer exists");
            if !consumer.no_ack {
                consumer.unacked += 1;
                channel.unacked.insert(delivery_tag, (queue.to_string(), message, tag));
            }
            if consumer.deliveries.send(delivery).is_ok() {
                self.pending.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn dead_letter(&mut self, queue: &str, message: Stored, effects: &mut Effects) {
        let Some(exchange) = self.queues.get(queue).and_then(Queue::dead_letter_exchange) else {
            return;
        };
        let (routing_key, body, properties) = (message.routing_key, message.body, message.properties);
        self.deliver(&exchange, &routing_key, &body, &properties, effects);
    }

    fn remove_consumer(&mut self, channel_id: u64, tag: &str) -> Option<Consumer> {
        let consumer = self.channels.get_mut(&channel_id)?.consumers.remove(tag)?;
        let mut delete = false;
        if let Some(q) = self.queues.get_mut(&consumer.queue) {
            q.consumers.retain(|(c, t)| !(*c == channel_id && t == tag));
            q.next_consumer = 0;
            delete = q.auto_delete && q.consumers.is_empty();
        }
        if delete {
            self.delete_queue(&consumer.queue, &mut Effects::default());
        }
        Some(consumer)
    }

    /// Delete a queue; its remaining consumers are cancelled by the broker.
    fn delete_queue(&mut self, name: &str, effects: &mut Effects) {
        let Some(queue) = self.queues.remove(name) else {
            return;
        };
        for (channel_id, tag) in queue.consumers {
            if let Some(consumer) = self
                .channels
                .get_mut(&channel_id)
                .and_then(|c| c.consumers.remove(&tag))
            {
                effects.cancelled.extend(consumer.on_cancel);
            }
        }
        for x in self.exchanges.values_mut() {
            x.bindings.retain(|(q, _)| q != name);
        }
    }

    fn close_channel(&mut self, id: u64, reason: &str, effects: &mut Effects) {
        let Some(channel) = self.channels.get(&id) else {
            return;
        };
        let tags: Vec<String> = channel.consumers.keys().cloned().collect();
        for tag in tags {
            self.remove_consumer(id, &tag);
        }
        let Some(mut channel) = self.channels.remove(&id) else {
            return;
        };
        // Unacknowledged deliveries go back to the front of their queues, in order.
        for (_, (queue, mut message, _)) in std::mem::take(&mut channel.unacked).into_iter().rev() {
            message.redelivered = true;
            self.enqueue(&queue, message, true, effects);
        }
        // Publishes the broker never answered: their outcome is now unknown.
        let (gone, kept): (Vec<Held>, Vec<Held>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|h| h.channel == id);
        self.held = kept;
        for held in gone {
            let _ = held.answer.send(Confirm::new(ConfirmOutcome::Closed, reason));
        }
        effects.closed_channels.push((channel.close, reason.to_string()));
    }

    fn close_connection(&mut self, id: u64, reason: Option<String>, effects: &mut Effects) {
        let Some(connection) = self.connections.remove(&id) else {
            return;
        };
        let channels: Vec<u64> = self
            .channels
            .iter()
            .filter(|(_, c)| c.connection == id)
            .map(|(k, _)| *k)
            .collect();
        let why = reason.clone().unwrap_or_else(|| "connection closed".into());
        for channel in channels {
            self.close_channel(channel, &why, effects);
        }
        let exclusive: Vec<String> = self
            .queues
            .iter()
            .filter(|(_, q)| q.owner == Some(id))
            .map(|(k, _)| k.clone())
            .collect();
        for queue in exclusive {
            self.delete_queue(&queue, effects);
        }
        effects.closed_connections.push((connection.close, reason));
    }
}

impl Inner {
    /// Run `f` under the lock, then its effects outside it.
    fn with<R>(self: &Arc<Self>, f: impl FnOnce(&mut State, &mut Effects) -> R) -> R {
        let mut effects = Effects::default();
        let result = {
            let mut state = self.state.lock().unwrap();
            f(&mut state, &mut effects)
        };
        self.run(effects);
        result
    }

    fn run(self: &Arc<Self>, effects: Effects) {
        for (queue, id, ttl) in effects.expiring {
            let inner = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(ttl)).await;
                inner.with(|state, effects| {
                    let Some(q) = state.queues.get_mut(&queue) else {
                        return;
                    };
                    let Some(position) = q.messages.iter().position(|m| m.id == id) else {
                        return;
                    };
                    let message = q.messages.remove(position).expect("the position was just found");
                    state.dead_letter(&queue, message, effects);
                });
            });
        }
        for on_cancel in effects.cancelled {
            on_cancel();
        }
        for (listeners, reason) in effects.closed_channels {
            listeners.fire(reason);
        }
        for (listeners, reason) in effects.closed_connections {
            listeners.fire(reason);
        }
    }

    /// Fail a channel operation as RabbitMQ does: the channel closes.
    fn channel_error(self: &Arc<Self>, channel: u64, code: u16, text: String) -> AmqpError {
        self.with(|state, effects| state.close_channel(channel, &text, effects));
        AmqpError::new(text, code)
    }
}

impl MemoryBroker {
    pub fn new() -> Self {
        let pending = Arc::new(AtomicUsize::new(0));
        let state = State {
            pending: pending.clone(),
            ..State::default()
        };
        MemoryBroker {
            inner: Arc::new(Inner {
                state: Mutex::new(state),
                pending,
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().unwrap()
    }

    // ---- fault injection ---------------------------------------------------------

    /// Drop every open connection as a lost socket would: unacknowledged
    /// deliveries are requeued, exclusive queues deleted, and each
    /// connection's close listeners receive `reason`.
    pub fn kill_connections(&self, reason: &str) {
        self.inner.with(|state, effects| {
            let ids: Vec<u64> = state.connections.keys().copied().collect();
            for id in ids {
                state.close_connection(id, Some(reason.to_string()), effects);
            }
        });
    }

    /// While set, connecting fails.
    pub fn refuse_connections(&self, refuse: bool) {
        self.state().refuse = refuse;
    }

    pub fn set_confirm_mode(&self, mode: ConfirmMode) {
        self.state().confirm_mode = Some(mode);
    }

    /// The publishes [`ConfirmMode::Drop`] is holding, across every open channel.
    pub fn held_confirms(&self) -> usize {
        self.state().held.len()
    }

    /// Deliver every held confirm now, as a broker confirming late would.
    /// `None` gives each the outcome it would have had; returns how many were
    /// delivered.
    pub fn release_held_confirms(&self, outcome: Option<ConfirmOutcome>) -> usize {
        let held = std::mem::take(&mut self.state().held);
        let count = held.len();
        for h in held {
            let _ = h.answer.send(Confirm::new(outcome.unwrap_or(h.outcome), ""));
        }
        count
    }

    /// Close every channel that has `queue` declared or consumed, as a broker
    /// would close a channel over a consumer error: the connection stays up.
    pub fn close_channels_consuming(&self, queue: &str, reason: &str) {
        self.inner.with(|state, effects| {
            let ids: Vec<u64> = state
                .channels
                .iter()
                .filter(|(_, c)| c.declared.contains(queue) || c.consumers.values().any(|k| k.queue == queue))
                .map(|(id, _)| *id)
                .collect();
            for id in ids {
                state.close_channel(id, reason, effects);
            }
        });
    }

    /// Cancel the queue's consumers from the broker side (`basic.cancel`),
    /// leaving their channels open.
    pub fn cancel_consumers(&self, queue: &str) {
        self.inner.with(|state, effects| {
            let consumers: Vec<(u64, String)> =
                state.queues.get(queue).map(|q| q.consumers.clone()).unwrap_or_default();
            for (channel, tag) in consumers {
                if let Some(consumer) = state.remove_consumer(channel, &tag) {
                    effects.cancelled.extend(consumer.on_cancel);
                }
            }
        });
    }

    /// Delete a queue, as an operator would; its consumers are cancelled.
    pub fn delete_queue(&self, name: &str) {
        self.inner.with(|state, effects| state.delete_queue(name, effects));
    }

    // ---- inspection --------------------------------------------------------------

    pub fn queue_exists(&self, name: &str) -> bool {
        self.state().queues.contains_key(name)
    }

    pub fn exchange_exists(&self, name: &str) -> bool {
        self.state().exchanges.contains_key(name)
    }

    pub fn queue_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.state().queues.keys().cloned().collect();
        names.sort();
        names
    }

    /// Messages waiting in the queue, not counting delivered ones.
    pub fn queue_depth(&self, name: &str) -> usize {
        self.state().queues.get(name).map_or(0, |q| q.messages.len())
    }

    /// Messages delivered from the queue and not yet settled.
    pub fn unacked_count(&self, name: &str) -> usize {
        self.state()
            .channels
            .values()
            .map(|c| c.unacked.values().filter(|(q, _, _)| q == name).count())
            .sum()
    }

    pub fn consumer_count(&self, name: &str) -> usize {
        self.state().queues.get(name).map_or(0, |q| q.consumers.len())
    }

    pub fn queue_arguments(&self, name: &str) -> Option<FieldTable> {
        self.state().queues.get(name).map(|q| q.arguments.clone())
    }

    /// Whether the queue was declared durable.
    pub fn queue_is_durable(&self, name: &str) -> bool {
        self.state().queues.get(name).is_some_and(|q| q.durable)
    }

    /// The messages waiting in a queue, in delivery order, without removing them.
    pub fn peek(&self, name: &str) -> Vec<Delivery> {
        let state = self.state();
        let Some(q) = state.queues.get(name) else {
            return Vec::new();
        };
        q.messages
            .iter()
            .map(|m| Delivery {
                body: m.body.clone(),
                properties: m.properties.clone(),
                exchange: m.exchange.clone(),
                routing_key: m.routing_key.clone(),
                consumer_tag: String::new(),
                delivery_tag: 0,
                redelivered: m.redelivered,
            })
            .collect()
    }

    /// The routing keys bound from `exchange` to `queue`.
    pub fn bindings(&self, queue: &str, exchange: &str) -> Vec<String> {
        self.state()
            .exchanges
            .get(exchange)
            .map(|x| {
                x.bindings
                    .iter()
                    .filter(|(q, _)| q == queue)
                    .map(|(_, k)| k.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn open_connections(&self) -> usize {
        self.state().connections.len()
    }

    pub fn connect_attempts(&self) -> usize {
        self.state().connect_attempts
    }

    /// Publish straight into the broker, as a foreign client would: no
    /// protobus envelope or property is added. Returns how many queues it reached.
    pub fn publish_raw(&self, exchange: &str, routing_key: &str, body: &[u8], properties: Properties) -> usize {
        self.inner
            .with(|state, effects| state.deliver(exchange, routing_key, body, &properties, effects))
    }

    /// Wait until every delivery handed to a consumer so far has been taken
    /// by its callback. Handlers the callback started may still be running.
    pub async fn flush(&self) {
        while self.inner.pending.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Wait until `predicate` holds, polling, up to `timeout`. Returns whether it did.
    pub async fn wait_for(predicate: impl Fn() -> bool, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while !predicate() {
            if tokio::time::Instant::now() >= deadline {
                return predicate();
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        true
    }
}

#[async_trait]
impl Transport for MemoryBroker {
    async fn connect(&self, _url: &str, _heartbeat_seconds: u16) -> Result<Arc<dyn Connection>, AmqpError> {
        let mut state = self.state();
        state.connect_attempts += 1;
        if state.refuse {
            return Err(AmqpError::new("connection refused", 0));
        }
        let id = state.id();
        let close = Arc::new(CloseListeners::new());
        state.connections.insert(id, ConnectionState { close: close.clone() });
        Ok(Arc::new(MemoryConnection {
            broker: self.inner.clone(),
            id,
            close,
        }))
    }
}

struct MemoryConnection {
    broker: Arc<Inner>,
    id: u64,
    close: Arc<CloseListeners<Option<String>>>,
}

#[async_trait]
impl Connection for MemoryConnection {
    async fn open_channel(&self) -> Result<Arc<dyn Channel>, AmqpError> {
        let mut state = self.broker.state.lock().unwrap();
        if !state.connections.contains_key(&self.id) {
            return Err(AmqpError::new("connection is closed", 0));
        }
        let id = state.id();
        let close = Arc::new(CloseListeners::new());
        state.channels.insert(
            id,
            ChannelState {
                connection: self.id,
                prefetch: 0,
                next_tag: 0,
                unacked: BTreeMap::new(),
                consumers: HashMap::new(),
                declared: HashSet::new(),
                close: close.clone(),
            },
        );
        Ok(Arc::new(MemoryChannel {
            broker: self.broker.clone(),
            id,
            connection: self.id,
            close,
        }))
    }

    async fn close(&self) -> Result<(), AmqpError> {
        self.broker
            .with(|state, effects| state.close_connection(self.id, None, effects));
        Ok(())
    }

    fn is_open(&self) -> bool {
        self.broker.state.lock().unwrap().connections.contains_key(&self.id)
    }

    fn on_close(&self, listener: Box<dyn FnOnce(Option<String>) + Send>) {
        self.close.add(listener);
    }
}

struct MemoryChannel {
    broker: Arc<Inner>,
    id: u64,
    connection: u64,
    close: Arc<CloseListeners<String>>,
}

impl MemoryChannel {
    /// Run `f` on the open channel's state; an `Err` closes the channel, as
    /// RabbitMQ does on a channel exception.
    fn op<R>(&self, f: impl FnOnce(&mut State, &mut Effects) -> Result<R, (u16, String)>) -> Result<R, AmqpError> {
        let outcome = self.broker.with(|state, effects| {
            if !state.channels.contains_key(&self.id) {
                return Err(None);
            }
            f(state, effects).map_err(Some)
        });
        match outcome {
            Ok(r) => Ok(r),
            Err(None) => Err(closed_channel()),
            Err(Some((code, text))) => Err(self.broker.channel_error(self.id, code, text)),
        }
    }
}

#[async_trait]
impl Channel for MemoryChannel {
    async fn declare_exchange(
        &self,
        name: &str,
        kind: &str,
        _durable: bool,
        _auto_delete: bool,
        _internal: bool,
        _arguments: &FieldTable,
    ) -> Result<(), AmqpError> {
        self.op(|state, _| {
            match state.exchanges.get(name) {
                Some(existing) if existing.kind != kind => {
                    return Err((
                        406,
                        format!(
                            "PRECONDITION_FAILED - inequivalent arg 'type' for exchange '{name}': received '{kind}' but current is '{}'",
                            existing.kind
                        ),
                    ))
                }
                Some(_) => {}
                None => {
                    state.exchanges.insert(name.to_string(), Exchange { kind: kind.to_string(), bindings: Vec::new() });
                }
            }
            Ok(())
        })
    }

    async fn declare_queue(
        &self,
        name: &str,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        arguments: &FieldTable,
    ) -> Result<String, AmqpError> {
        self.op(|state, _| {
            let name = if name.is_empty() {
                format!("amq.gen-{}", uuid::Uuid::new_v4().simple())
            } else {
                name.to_string()
            };
            if let Some(existing) = state.queues.get(&name) {
                if existing.owner.is_some_and(|owner| owner != self.connection) {
                    return Err((
                        405,
                        format!("RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{name}'"),
                    ));
                }
                // RabbitMQ compares integer arguments by value, whatever their width.
                let same = |a: &FieldTable, b: &FieldTable| {
                    a.len() == b.len()
                        && a.iter()
                            .all(|(k, v)| match (v.as_int(), b.get(k).and_then(FieldValue::as_int)) {
                                (Some(x), Some(y)) => x == y,
                                _ => b.get(k) == Some(v),
                            })
                };
                if existing.durable != durable
                    || existing.auto_delete != auto_delete
                    || !same(&existing.arguments, arguments)
                {
                    return Err((
                        406,
                        format!("PRECONDITION_FAILED - inequivalent arg for queue '{name}'"),
                    ));
                }
            } else {
                state.queues.insert(
                    name.clone(),
                    Queue {
                        durable,
                        owner: exclusive.then_some(self.connection),
                        auto_delete,
                        arguments: arguments.clone(),
                        messages: VecDeque::new(),
                        consumers: Vec::new(),
                        next_consumer: 0,
                    },
                );
            }
            if let Some(channel) = state.channels.get_mut(&self.id) {
                channel.declared.insert(name.clone());
            }
            Ok(name)
        })
    }

    async fn bind_queue(&self, queue: &str, exchange: &str, routing_key: &str) -> Result<(), AmqpError> {
        self.op(|state, _| {
            if !state.queues.contains_key(queue) {
                return Err((404, format!("NOT_FOUND - no queue '{queue}'")));
            }
            let Some(x) = state.exchanges.get_mut(exchange) else {
                return Err((404, format!("NOT_FOUND - no exchange '{exchange}'")));
            };
            let binding = (queue.to_string(), routing_key.to_string());
            if !x.bindings.contains(&binding) {
                x.bindings.push(binding);
            }
            Ok(())
        })
    }

    async fn unbind_queue(&self, queue: &str, exchange: &str, routing_key: &str) -> Result<(), AmqpError> {
        self.op(|state, _| {
            if let Some(x) = state.exchanges.get_mut(exchange) {
                x.bindings.retain(|(q, k)| !(q == queue && k == routing_key));
            }
            Ok(())
        })
    }

    async fn delete_queue(&self, name: &str) -> Result<(), AmqpError> {
        self.op(|state, effects| {
            state.delete_queue(name, effects);
            Ok(())
        })
    }

    async fn purge_queue(&self, name: &str) -> Result<(), AmqpError> {
        self.op(|state, _| match state.queues.get_mut(name) {
            Some(q) => {
                q.messages.clear();
                Ok(())
            }
            None => Err((404, format!("NOT_FOUND - no queue '{name}'"))),
        })
    }

    async fn prefetch(&self, count: u16) -> Result<(), AmqpError> {
        self.op(|state, _| {
            if let Some(channel) = state.channels.get_mut(&self.id) {
                channel.prefetch = count;
            }
            Ok(())
        })
    }

    async fn consume(
        &self,
        queue: &str,
        consumer_tag: &str,
        no_ack: bool,
        _exclusive: bool,
        on_delivery: DeliveryCallback,
        on_cancel: Option<CancelCallback>,
    ) -> Result<String, AmqpError> {
        let (deliveries, mut inbox) = mpsc::unbounded_channel::<Delivery>();
        let tag = if consumer_tag.is_empty() {
            format!("amq.ctag-{}", uuid::Uuid::new_v4().simple())
        } else {
            consumer_tag.to_string()
        };
        let registered = tag.clone();
        // The reader exists before the consumer does, so the first delivery has somewhere to go.
        let pending = self.broker.pending.clone();
        let reader = tokio::spawn(async move {
            while let Some(delivery) = inbox.recv().await {
                on_delivery(delivery).await;
                pending.fetch_sub(1, Ordering::SeqCst);
            }
        });
        let counted = mpsc::UnboundedSender::clone(&deliveries);
        let result = self.op(move |state, _| {
            if !state.queues.contains_key(queue) {
                return Err((404, format!("NOT_FOUND - no queue '{queue}'")));
            }
            let channel = state.channels.get_mut(&self.id).expect("op checked the channel");
            channel.consumers.insert(
                registered.clone(),
                Consumer {
                    queue: queue.to_string(),
                    no_ack,
                    deliveries: counted,
                    on_cancel,
                    unacked: 0,
                },
            );
            state
                .queues
                .get_mut(queue)
                .expect("the queue was just checked")
                .consumers
                .push((self.id, registered.clone()));
            state.pump(queue);
            Ok(registered)
        });
        if result.is_err() {
            reader.abort();
        }
        drop(deliveries);
        result
    }

    async fn cancel(&self, consumer_tag: &str) -> Result<(), AmqpError> {
        self.op(|state, _| {
            state.remove_consumer(self.id, consumer_tag);
            Ok(())
        })
    }

    async fn ack(&self, delivery_tag: u64) -> Result<(), AmqpError> {
        self.op(|state, _| {
            let channel = state.channels.get_mut(&self.id).expect("op checked the channel");
            let Some((queue, _, tag)) = channel.unacked.remove(&delivery_tag) else {
                return Err((
                    406,
                    format!("PRECONDITION_FAILED - unknown delivery tag {delivery_tag}"),
                ));
            };
            if let Some(consumer) = channel.consumers.get_mut(&tag) {
                consumer.unacked = consumer.unacked.saturating_sub(1);
            }
            state.pump(&queue);
            Ok(())
        })
    }

    async fn reject(&self, delivery_tag: u64, requeue: bool) -> Result<(), AmqpError> {
        self.op(|state, effects| {
            let channel = state.channels.get_mut(&self.id).expect("op checked the channel");
            let Some((queue, mut message, tag)) = channel.unacked.remove(&delivery_tag) else {
                return Err((
                    406,
                    format!("PRECONDITION_FAILED - unknown delivery tag {delivery_tag}"),
                ));
            };
            if let Some(consumer) = channel.consumers.get_mut(&tag) {
                consumer.unacked = consumer.unacked.saturating_sub(1);
            }
            if requeue {
                message.redelivered = true;
                state.enqueue(&queue, message, true, effects);
            } else {
                state.dead_letter(&queue, message, effects);
            }
            state.pump(&queue);
            Ok(())
        })
    }

    async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        properties: &Properties,
        mandatory: bool,
    ) -> Result<ConfirmFuture, AmqpError> {
        let (answer, confirm) = oneshot::channel();
        let wait: ConfirmFuture = Box::pin(async move {
            // The broker going away with the publish unanswered leaves its outcome unknown.
            confirm
                .await
                .unwrap_or_else(|_| Confirm::new(ConfirmOutcome::Closed, "the broker went away"))
        });
        let mut missing_exchange = false;
        self.broker.with(|state, effects| {
            if !state.channels.contains_key(&self.id) {
                return Err(closed_channel());
            }
            if !exchange.is_empty() && !state.exchanges.contains_key(exchange) {
                missing_exchange = true;
                let _ = answer.send(Confirm::new(
                    ConfirmOutcome::Closed,
                    format!("NOT_FOUND - no exchange '{exchange}'"),
                ));
                return Ok(());
            }
            let mode = state.mode();
            if mode == ConfirmMode::Nack {
                let _ = answer.send(Confirm::new(ConfirmOutcome::Nack, "refused by the test broker"));
                return Ok(());
            }
            let reached = state.deliver(exchange, routing_key, body, properties, effects);
            let outcome = if mandatory && reached == 0 {
                ConfirmOutcome::Returned
            } else {
                ConfirmOutcome::Ack
            };
            if mode == ConfirmMode::Drop {
                state.held.push(Held {
                    channel: self.id,
                    outcome,
                    answer,
                });
            } else {
                let detail = if outcome == ConfirmOutcome::Returned {
                    "NO_ROUTE"
                } else {
                    ""
                };
                let _ = answer.send(Confirm::new(outcome, detail));
            }
            Ok(())
        })?;
        if missing_exchange {
            // The publish was written; the channel exception follows it.
            self.broker
                .channel_error(self.id, 404, format!("NOT_FOUND - no exchange '{exchange}'"));
        }
        Ok(wait)
    }

    async fn close(&self) -> Result<(), AmqpError> {
        self.broker
            .with(|state, effects| state.close_channel(self.id, "channel closed", effects));
        Ok(())
    }

    fn is_open(&self) -> bool {
        self.broker.state.lock().unwrap().channels.contains_key(&self.id)
    }

    fn on_close(&self, listener: Box<dyn FnOnce(String) + Send>) {
        self.close.add(listener);
    }
}
