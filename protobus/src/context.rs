//! One process's place on the bus.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage};

use crate::config;
use crate::connection::Connection;
use crate::errors::Result;
use crate::event_dispatcher::EventDispatcher;
use crate::message_dispatcher::{ChunkStream, MessageDispatcher};
use crate::message_factory::MessageFactory;
use crate::options::{CallOptions, ContextOptions, StreamOptions};

struct Inner {
    options: ContextOptions,
    connection: Connection,
    factory: MessageFactory,
    message_dispatcher: MessageDispatcher,
    event_dispatcher: EventDispatcher,
    closed: AtomicBool,
}

/// One process's place on the bus. It owns the connection, the message factory
/// and the two dispatchers, and is what services and proxies are built on.
///
/// ```no_run
/// # async fn run() -> protobus::Result<()> {
/// let ctx = protobus::Context::new(Default::default());
/// ctx.init("amqp://guest:guest@localhost:5672/", &[] as &[&str]).await?;
/// // ... services and proxies ...
/// ctx.close().await;
/// # Ok(()) }
/// ```
///
/// Cheap to clone: every clone is the same context, shared freely between
/// tasks and threads. Services and proxies hold a clone, so the context cannot
/// be destroyed under them; [`close`](Self::close) it when the process is done.
#[derive(Clone)]
pub struct Context {
    inner: Arc<Inner>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new(ContextOptions::default())
    }
}

impl Context {
    pub fn new(options: ContextOptions) -> Self {
        let connection = Connection::new(options.transport.clone());
        connection.on_reconnecting(|attempt, delay| {
            log::info!(target: "protobus", "Context: reconnecting (attempt {attempt}, delay {delay}ms)");
        });
        connection.on_reconnected(|| log::info!(target: "protobus", "Context: reconnected successfully"));
        connection.on_error(|e| log::error!(target: "protobus", "Context: connection error - {e}"));
        Context {
            inner: Arc::new(Inner {
                message_dispatcher: MessageDispatcher::new(&connection),
                event_dispatcher: EventDispatcher::new(&connection),
                factory: MessageFactory::new(),
                connection,
                options,
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// Load the schemas at `schema_locations` (none is fine when every schema
    /// is compiled in; see [`MessageFactory::load`]), connect, and start the
    /// dispatchers.
    pub async fn init<P: AsRef<Path>>(&self, amqp_url: &str, schema_locations: &[P]) -> Result<()> {
        let inner = &self.inner;
        inner.factory.load(schema_locations)?;
        inner
            .connection
            .connect(amqp_url, inner.options.reconnection.clone())
            .await?;
        inner.message_dispatcher.init().await?;
        inner.event_dispatcher.init().await
    }

    /// Whether two handles are the same context.
    pub fn same(&self, other: &Context) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn is_connected(&self) -> bool {
        self.inner.connection.is_connected()
    }

    pub fn is_reconnecting(&self) -> bool {
        self.inner.connection.is_reconnecting()
    }

    pub fn factory(&self) -> &MessageFactory {
        &self.inner.factory
    }

    pub fn connection(&self) -> &Connection {
        &self.inner.connection
    }

    pub fn message_dispatcher(&self) -> &MessageDispatcher {
        &self.inner.message_dispatcher
    }

    /// Publish an encoded `RequestContainer` and return the raw reply (`None`
    /// when `rpc` is false).
    pub async fn publish_message(
        &self,
        content: Vec<u8>,
        routing_key: &str,
        options: &CallOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.inner
            .message_dispatcher
            .publish(content, routing_key, options)
            .await
    }

    /// Publish an encoded `RequestContainer` expecting a streaming reply.
    pub fn publish_streaming_message(
        &self,
        content: Vec<u8>,
        routing_key: &str,
        options: &StreamOptions,
    ) -> Result<ChunkStream> {
        self.inner
            .message_dispatcher
            .publish_streaming(content, routing_key, options)
    }

    /// Publish an event of the message's own type, on `EVENT.<type>` unless a
    /// topic is given. Completes once the broker has confirmed it.
    pub async fn publish_event<T: Message + prost::Name>(&self, event: &T, topic: Option<&str>) -> Result<()> {
        let payload = self.inner.factory.encode(event)?;
        self.inner
            .event_dispatcher
            .publish(&T::full_name(), payload, topic)
            .await
    }

    /// Publish a dynamic message as an event of its own type.
    pub async fn publish_dynamic_event(&self, event: &DynamicMessage, topic: Option<&str>) -> Result<()> {
        let payload = self.inner.factory.encode_message(event)?;
        self.inner
            .event_dispatcher
            .publish(event.descriptor().full_name(), payload, topic)
            .await
    }

    /// Close the connection and the dispatchers, then wait for running
    /// handlers. Pending calls and streams fail at once with a disconnected
    /// error; handlers get up to `SHUTDOWN_DRAIN_TIMEOUT_MS` to finish. Safe
    /// to call more than once.
    pub async fn close(&self) {
        let inner = &self.inner;
        if inner.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        inner.connection.mark_shut_down();
        // Disconnect first: it is what fails pending calls and streams,
        // through the dispatchers' disconnect listeners, which closing them
        // would detach.
        inner.connection.disconnect().await;
        inner.message_dispatcher.close().await;
        inner.event_dispatcher.close().await;
        let budget = config::shutdown_drain_timeout_ms();
        if !inner.connection.drain_in_flight(budget).await {
            log::warn!(
                target: "protobus",
                "Context: {} handler(s) still running after {budget}ms; closing anyway",
                inner.connection.in_flight_deliveries()
            );
        }
    }
}
