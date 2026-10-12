//! Publishes events to the events exchange.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::amqp::Properties;
use crate::config;
use crate::connection::{ChannelHandle, Connection, ListenerId, PublishOptions};
use crate::errors::{Error, ErrorKind, Result};
use crate::message_factory::MessageFactory;

struct Inner {
    connection: Connection,
    channel: Mutex<Option<ChannelHandle>>,
    channel_lock: tokio::sync::Mutex<()>,
    initialized: AtomicBool,
    registrations: Mutex<Option<(ListenerId, ListenerId)>>,
}

/// Publishes events. Cheap to clone.
#[derive(Clone)]
pub struct EventDispatcher {
    inner: Arc<Inner>,
}

impl EventDispatcher {
    pub fn new(connection: &Connection) -> Self {
        let dispatcher = EventDispatcher {
            inner: Arc::new(Inner {
                connection: connection.clone(),
                channel: Mutex::new(None),
                channel_lock: tokio::sync::Mutex::new(()),
                initialized: AtomicBool::new(false),
                registrations: Mutex::new(None),
            }),
        };
        let weak = Arc::downgrade(&dispatcher.inner);
        let on_disconnected = connection.on_disconnected(move || {
            if let Some(inner) = weak.upgrade() {
                *inner.channel.lock().unwrap() = None;
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
                log::info!(target: "protobus", "EventDispatcher: reconnected, re-initializing channel");
                inner.open().await.map(|_| ())
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
        self.inner.open().await?;
        self.inner.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Publish an event whose payload is already serialized. `type` is the
    /// payload's full message name; the topic defaults to `EVENT.<type>`.
    /// Completes once the broker has confirmed it. Not mandatory: an event
    /// nobody subscribes to is normal.
    pub async fn publish(&self, r#type: &str, payload: Vec<u8>, topic: Option<&str>) -> Result<()> {
        let connection = &self.inner.connection;
        if !connection.is_connected() && !connection.is_reconnecting() {
            return Err(Error::new(ErrorKind::NotConnected, "not connected"));
        }
        let topic = match topic {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => format!("EVENT.{type}"),
        };
        let event = MessageFactory::build_encoded_event(r#type, payload, &topic);
        let properties = Properties {
            correlation_id: Some(uuid::Uuid::new_v4().to_string()),
            content_type: Some("application/octet-stream".into()),
            delivery_mode: Some(2),
            ..Properties::default()
        };
        let channel = connection.ready_channel(|| self.inner.publish_channel()).await?;
        let options = PublishOptions {
            properties,
            mandatory: false,
        };
        connection
            .publish(Some(&channel), &config::events_exchange_name(), &topic, event, options)
            .await?;
        Ok(())
    }

    pub async fn close(&self) {
        let inner = &self.inner;
        if let Some((on_disconnected, restorer)) = inner.registrations.lock().unwrap().take() {
            inner.connection.remove_listener(on_disconnected);
            inner.connection.unregister_restorer(restorer);
        }
        let channel = inner.channel.lock().unwrap().take();
        if let Some(channel) = channel.filter(ChannelHandle::is_open) {
            let _ = channel.close().await;
        }
    }
}

impl Inner {
    async fn open(&self) -> Result<ChannelHandle> {
        let channel = self.connection.open_channel().await?;
        // Declared by the publisher too, so an event published before any
        // subscriber exists is not refused for want of an exchange.
        self.connection
            .declare_exchange(&channel, &config::events_exchange_name(), "topic")
            .await?;
        *self.channel.lock().unwrap() = Some(channel.clone());
        Ok(channel)
    }

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
        log::warn!(target: "protobus", "EventDispatcher: publishing channel lost on a live connection; reopening it");
        self.open().await.map(Some)
    }
}
