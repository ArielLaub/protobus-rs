//! The production transport, over lapin.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, BasicRejectOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    QueuePurgeOptions,
};
use lapin::types::{AMQPValue, DecimalValue, FieldArray, LongString};
use lapin::uri::AMQPUri;
use lapin::{BasicProperties, Confirmation, ConnectionProperties, ExchangeKind};

use super::{
    AmqpError, CancelCallback, Channel, CloseListeners, Confirm, ConfirmFuture, ConfirmOutcome, Connection, Delivery,
    DeliveryCallback, FieldTable, FieldValue, Properties, Transport,
};

/// Connects to RabbitMQ with [lapin](https://docs.rs/lapin). Supports `amqp://`
/// and `amqps://` URLs with the RabbitMQ URI query parameters lapin
/// understands (`heartbeat`, `connection_timeout`, `channel_max`, `frame_max`).
///
/// lapin's own automatic recovery stays off: protobus restores its topology
/// itself, the same way in every port.
#[derive(Clone, Debug, Default)]
pub struct LapinTransport {
    connection_name: Option<String>,
}

impl LapinTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Label connections in the RabbitMQ management UI. The default is `protobus-rs`.
    pub fn with_connection_name(mut self, name: impl Into<String>) -> Self {
        self.connection_name = Some(name.into());
        self
    }
}

fn amqp_error(e: &lapin::Error) -> AmqpError {
    let reply_code = match e.kind() {
        lapin::ErrorKind::ProtocolError(protocol) => protocol.get_id(),
        _ => 0,
    };
    AmqpError::new(e.to_string(), reply_code)
}

#[async_trait]
impl Transport for LapinTransport {
    async fn connect(&self, url: &str, heartbeat_seconds: u16) -> Result<Arc<dyn Connection>, AmqpError> {
        let mut uri = AMQPUri::from_str(url).map_err(|e| AmqpError::new(format!("invalid broker URL: {e}"), 0))?;
        // A heartbeat in the URL wins, 0 included.
        if uri.query.heartbeat.is_none() {
            uri.query.heartbeat = Some(heartbeat_seconds);
        }
        // `amqp://host/` names the default vhost, as in every other port.
        if uri.vhost.is_empty() {
            uri.vhost = "/".into();
        }
        let properties = ConnectionProperties::default()
            .with_connection_name(
                self.connection_name
                    .clone()
                    .unwrap_or_else(|| "protobus-rs".into())
                    .into(),
            )
            .with_client_property("product".into(), "protobus-rs".into())
            .with_client_property("version".into(), env!("CARGO_PKG_VERSION").into());
        let connection = lapin::Connection::connect_uri(uri, properties)
            .await
            .map_err(|e| amqp_error(&e))?;
        let handle = Arc::new(LapinConnection {
            connection,
            close: Arc::new(CloseListeners::new()),
            channels: Arc::new(Mutex::new(Vec::new())),
        });
        handle.watch();
        Ok(handle)
    }
}

struct LapinConnection {
    connection: lapin::Connection,
    close: Arc<CloseListeners<Option<String>>>,
    channels: Arc<Mutex<Vec<Arc<CloseListeners<String>>>>>,
}

impl LapinConnection {
    /// Report the connection's loss, from its event stream or, failing that,
    /// from its status: a lost socket must never go unnoticed.
    fn watch(&self) {
        let mut events = self.connection.events_listener();
        let status = self.connection.status().clone();
        let close = self.close.clone();
        let channels = self.channels.clone();
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(Duration::from_millis(100));
            let mut last_error = None;
            let reason = loop {
                tokio::select! {
                    event = events.next() => match event {
                        // lapin reports channel exceptions here too, and those
                        // leave the connection up: the status decides.
                        Some(lapin::Event::Error(e)) => {
                            let text = e.to_string();
                            if !status.connected() && !status.connecting() {
                                break text;
                            }
                            last_error = Some(text);
                        }
                        Some(_) => {}
                        None => break last_error.unwrap_or_else(|| "connection closed".to_string()),
                    },
                    _ = poll.tick() => {
                        if close.fired() {
                            return;
                        }
                        if !status.connected() && !status.connecting() {
                            break last_error.unwrap_or_else(|| "connection closed".to_string());
                        }
                    }
                }
            };
            let lost: Vec<_> = channels.lock().unwrap().drain(..).collect();
            for channel in lost {
                channel.fire(reason.clone());
            }
            close.fire(Some(reason));
        });
    }
}

#[async_trait]
impl Connection for LapinConnection {
    async fn open_channel(&self) -> Result<Arc<dyn Channel>, AmqpError> {
        let channel = self.connection.create_channel().await.map_err(|e| amqp_error(&e))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| amqp_error(&e))?;
        let close = Arc::new(CloseListeners::new());
        self.channels.lock().unwrap().push(close.clone());
        Ok(Arc::new(LapinChannel {
            channel,
            close,
            cancelled: Arc::new(Mutex::new(HashSet::new())),
        }))
    }

    async fn close(&self) -> Result<(), AmqpError> {
        // A deliberate close reports no error, to the connection's listeners
        // and to those of the channels that go with it.
        let closed: Vec<_> = self.channels.lock().unwrap().drain(..).collect();
        self.close.fire(None);
        for channel in closed {
            channel.fire("connection closed".into());
        }
        self.connection
            .close(200, "bye".into())
            .await
            .map_err(|e| amqp_error(&e))
    }

    fn is_open(&self) -> bool {
        self.connection.status().connected() && !self.close.fired()
    }

    fn on_close(&self, listener: Box<dyn FnOnce(Option<String>) + Send>) {
        self.close.add(listener);
    }
}

struct LapinChannel {
    channel: lapin::Channel,
    close: Arc<CloseListeners<String>>,
    /// Consumers this side cancelled: their streams ending is not the broker's doing.
    cancelled: Arc<Mutex<HashSet<String>>>,
}

impl LapinChannel {
    /// Convert an operation's failure, noticing a channel it closed.
    fn failed(&self, e: lapin::Error) -> AmqpError {
        let error = amqp_error(&e);
        if !self.channel.status().connected() {
            self.close.fire(error.message.clone());
        }
        error
    }
}

#[async_trait]
impl Channel for LapinChannel {
    async fn declare_exchange(
        &self,
        name: &str,
        kind: &str,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        arguments: &FieldTable,
    ) -> Result<(), AmqpError> {
        let kind = match kind {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "topic" => ExchangeKind::Topic,
            "headers" => ExchangeKind::Headers,
            other => ExchangeKind::Custom(other.to_string()),
        };
        let options = ExchangeDeclareOptions {
            durable,
            auto_delete,
            internal,
            ..Default::default()
        };
        self.channel
            .exchange_declare(name.into(), kind, options, to_table(arguments))
            .await
            .map_err(|e| self.failed(e))
    }

    async fn declare_queue(
        &self,
        name: &str,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        arguments: &FieldTable,
    ) -> Result<String, AmqpError> {
        let options = QueueDeclareOptions {
            durable,
            exclusive,
            auto_delete,
            ..Default::default()
        };
        let queue = self
            .channel
            .queue_declare(name.into(), options, to_table(arguments))
            .await
            .map_err(|e| self.failed(e))?;
        Ok(queue.name().as_str().to_string())
    }

    async fn bind_queue(&self, queue: &str, exchange: &str, routing_key: &str) -> Result<(), AmqpError> {
        self.channel
            .queue_bind(
                queue.into(),
                exchange.into(),
                routing_key.into(),
                QueueBindOptions::default(),
                Default::default(),
            )
            .await
            .map_err(|e| self.failed(e))
    }

    async fn unbind_queue(&self, queue: &str, exchange: &str, routing_key: &str) -> Result<(), AmqpError> {
        self.channel
            .queue_unbind(queue.into(), exchange.into(), routing_key.into(), Default::default())
            .await
            .map_err(|e| self.failed(e))
    }

    async fn delete_queue(&self, name: &str) -> Result<(), AmqpError> {
        self.channel
            .queue_delete(name.into(), QueueDeleteOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| self.failed(e))
    }

    async fn purge_queue(&self, name: &str) -> Result<(), AmqpError> {
        self.channel
            .queue_purge(name.into(), QueuePurgeOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| self.failed(e))
    }

    async fn prefetch(&self, count: u16) -> Result<(), AmqpError> {
        self.channel
            .basic_qos(count, BasicQosOptions::default())
            .await
            .map_err(|e| self.failed(e))
    }

    async fn consume(
        &self,
        queue: &str,
        consumer_tag: &str,
        no_ack: bool,
        exclusive: bool,
        on_delivery: DeliveryCallback,
        on_cancel: Option<CancelCallback>,
    ) -> Result<String, AmqpError> {
        let options = BasicConsumeOptions {
            no_ack,
            exclusive,
            ..Default::default()
        };
        let mut consumer = self
            .channel
            .basic_consume(queue.into(), consumer_tag.into(), options, Default::default())
            .await
            .map_err(|e| self.failed(e))?;
        let tag = consumer.tag().as_str().to_string();
        let (status, close, cancelled) = (
            self.channel.status().clone(),
            self.close.clone(),
            self.cancelled.clone(),
        );
        let consumer_tag = tag.clone();
        // One delivery at a time, each awaited before the next is taken.
        tokio::spawn(async move {
            let mut failure = None;
            while let Some(next) = consumer.next().await {
                match next {
                    Ok(delivery) => on_delivery(from_delivery(delivery, &consumer_tag)).await,
                    Err(e) => {
                        failure = Some(e.to_string());
                        break;
                    }
                }
            }
            if cancelled.lock().unwrap().remove(&consumer_tag) {
                return;
            }
            if failure.is_none() && status.connected() {
                // The stream ended on an open channel: the broker cancelled the consumer.
                if let Some(on_cancel) = on_cancel {
                    on_cancel();
                }
                return;
            }
            close.fire(failure.unwrap_or_else(|| "channel closed".into()));
        });
        Ok(tag)
    }

    async fn cancel(&self, consumer_tag: &str) -> Result<(), AmqpError> {
        self.cancelled.lock().unwrap().insert(consumer_tag.to_string());
        self.channel
            .basic_cancel(consumer_tag.into(), BasicCancelOptions::default())
            .await
            .map_err(|e| self.failed(e))
    }

    async fn ack(&self, delivery_tag: u64) -> Result<(), AmqpError> {
        self.channel
            .basic_ack(delivery_tag, BasicAckOptions::default())
            .await
            .map_err(|e| self.failed(e))
    }

    async fn reject(&self, delivery_tag: u64, requeue: bool) -> Result<(), AmqpError> {
        self.channel
            .basic_reject(delivery_tag, BasicRejectOptions { requeue })
            .await
            .map_err(|e| self.failed(e))
    }

    async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        properties: &Properties,
        mandatory: bool,
    ) -> Result<ConfirmFuture, AmqpError> {
        let options = BasicPublishOptions {
            mandatory,
            immediate: false,
        };
        let confirm = self
            .channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                options,
                body,
                to_properties(properties),
            )
            .await
            .map_err(|e| self.failed(e))?;
        Ok(Box::pin(async move {
            match confirm.await {
                Ok(Confirmation::Ack(None)) | Ok(Confirmation::NotRequested) => Confirm::new(ConfirmOutcome::Ack, ""),
                // A mandatory publish that matched no queue comes back, then is acked.
                Ok(Confirmation::Ack(Some(returned))) => {
                    Confirm::new(ConfirmOutcome::Returned, returned.reply_text.as_str())
                }
                Ok(Confirmation::Nack(_)) => Confirm::new(ConfirmOutcome::Nack, ""),
                Err(e) => Confirm::new(ConfirmOutcome::Closed, e.to_string()),
            }
        }))
    }

    async fn close(&self) -> Result<(), AmqpError> {
        self.close.fire("channel closed".into());
        self.channel.close(200, "bye".into()).await.map_err(|e| amqp_error(&e))
    }

    fn is_open(&self) -> bool {
        self.channel.status().connected() && !self.close.fired()
    }

    fn on_close(&self, listener: Box<dyn FnOnce(String) + Send>) {
        self.close.add(listener);
    }
}

// ---- conversions -----------------------------------------------------------------

fn to_value(v: &FieldValue) -> AMQPValue {
    match v {
        FieldValue::Void => AMQPValue::Void,
        FieldValue::Bool(b) => AMQPValue::Boolean(*b),
        FieldValue::I8(n) => AMQPValue::ShortShortInt(*n),
        FieldValue::U8(n) => AMQPValue::ShortShortUInt(*n),
        FieldValue::I16(n) => AMQPValue::ShortInt(*n),
        FieldValue::U16(n) => AMQPValue::ShortUInt(*n),
        FieldValue::I32(n) => AMQPValue::LongInt(*n),
        FieldValue::U32(n) => AMQPValue::LongUInt(*n),
        FieldValue::I64(n) => AMQPValue::LongLongInt(*n),
        // RabbitMQ's field tables have no unsigned 64-bit integer.
        FieldValue::U64(n) => AMQPValue::LongLongInt(i64::try_from(*n).unwrap_or(i64::MAX)),
        FieldValue::Float(n) => AMQPValue::Float(*n),
        FieldValue::Double(n) => AMQPValue::Double(*n),
        FieldValue::Decimal { scale, value } => AMQPValue::DecimalValue(DecimalValue {
            scale: *scale,
            value: *value,
        }),
        FieldValue::String(s) => AMQPValue::LongString(LongString::from(s.as_str())),
        FieldValue::LongBytes(b) => AMQPValue::LongString(LongString::from(b.clone())),
        FieldValue::ShortString(s) => match lapin::types::ShortString::try_new(s.clone()) {
            Ok(short) => AMQPValue::ShortString(short),
            Err(_) => AMQPValue::LongString(LongString::from(s.as_str())),
        },
        FieldValue::Bytes(b) => AMQPValue::ByteArray(b.clone().into()),
        FieldValue::Timestamp(t) => AMQPValue::Timestamp(*t),
        FieldValue::Table(t) => AMQPValue::FieldTable(to_table(t)),
        FieldValue::Array(a) => {
            let mut array = FieldArray::default();
            for item in a {
                array.push(to_value(item));
            }
            AMQPValue::FieldArray(array)
        }
    }
}

fn to_table(table: &FieldTable) -> lapin::types::FieldTable {
    let mut out = lapin::types::FieldTable::default();
    for (k, v) in table {
        // A key AMQP cannot carry (over 255 bytes) is dropped rather than failing the publish.
        if let Ok(key) = lapin::types::ShortString::try_new(k.clone()) {
            out.insert(key, to_value(v));
        }
    }
    out
}

fn from_value(v: &AMQPValue) -> FieldValue {
    match v {
        AMQPValue::Void => FieldValue::Void,
        AMQPValue::Boolean(b) => FieldValue::Bool(*b),
        AMQPValue::ShortShortInt(n) => FieldValue::I8(*n),
        AMQPValue::ShortShortUInt(n) => FieldValue::U8(*n),
        AMQPValue::ShortInt(n) => FieldValue::I16(*n),
        AMQPValue::ShortUInt(n) => FieldValue::U16(*n),
        AMQPValue::LongInt(n) => FieldValue::I32(*n),
        AMQPValue::LongUInt(n) => FieldValue::U32(*n),
        AMQPValue::LongLongInt(n) => FieldValue::I64(*n),
        AMQPValue::Float(n) => FieldValue::Float(*n),
        AMQPValue::Double(n) => FieldValue::Double(*n),
        AMQPValue::DecimalValue(d) => FieldValue::Decimal {
            scale: d.scale,
            value: d.value,
        },
        AMQPValue::ShortString(s) => FieldValue::ShortString(s.as_str().to_string()),
        AMQPValue::LongString(s) => match std::str::from_utf8(s.as_bytes()) {
            Ok(text) => FieldValue::String(text.to_string()),
            Err(_) => FieldValue::LongBytes(s.as_bytes().to_vec()),
        },
        AMQPValue::ByteArray(b) => FieldValue::Bytes(b.as_slice().to_vec()),
        AMQPValue::Timestamp(t) => FieldValue::Timestamp(*t),
        AMQPValue::FieldTable(t) => FieldValue::Table(from_table(t)),
        AMQPValue::FieldArray(a) => FieldValue::Array(a.as_slice().iter().map(from_value).collect()),
    }
}

fn from_table(table: &lapin::types::FieldTable) -> FieldTable {
    table
        .inner()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), from_value(v)))
        .collect()
}

fn short(value: &Option<String>) -> Option<lapin::types::ShortString> {
    value
        .as_ref()
        .and_then(|v| lapin::types::ShortString::try_new(v.clone()).ok())
}

fn to_properties(p: &Properties) -> BasicProperties {
    let mut out = BasicProperties::default();
    if let Some(v) = short(&p.content_type) {
        out = out.with_content_type(v);
    }
    if let Some(v) = short(&p.content_encoding) {
        out = out.with_content_encoding(v);
    }
    if let Some(h) = &p.headers {
        out = out.with_headers(to_table(h));
    }
    if let Some(v) = p.delivery_mode {
        out = out.with_delivery_mode(v);
    }
    if let Some(v) = p.priority {
        out = out.with_priority(v);
    }
    if let Some(v) = short(&p.correlation_id) {
        out = out.with_correlation_id(v);
    }
    if let Some(v) = short(&p.reply_to) {
        out = out.with_reply_to(v);
    }
    if let Some(v) = short(&p.expiration) {
        out = out.with_expiration(v);
    }
    if let Some(v) = short(&p.message_id) {
        out = out.with_message_id(v);
    }
    if let Some(v) = p.timestamp {
        out = out.with_timestamp(v);
    }
    if let Some(v) = short(&p.r#type) {
        out = out.with_type(v);
    }
    if let Some(v) = short(&p.user_id) {
        out = out.with_user_id(v);
    }
    if let Some(v) = short(&p.app_id) {
        out = out.with_app_id(v);
    }
    out
}

fn text(value: &Option<lapin::types::ShortString>) -> Option<String> {
    value.as_ref().map(|v| v.as_str().to_string())
}

fn from_properties(p: &BasicProperties) -> Properties {
    Properties {
        content_type: text(p.content_type()),
        content_encoding: text(p.content_encoding()),
        headers: p.headers().as_ref().map(from_table),
        delivery_mode: *p.delivery_mode(),
        priority: *p.priority(),
        correlation_id: text(p.correlation_id()),
        reply_to: text(p.reply_to()),
        expiration: text(p.expiration()),
        message_id: text(p.message_id()),
        timestamp: *p.timestamp(),
        r#type: text(p.kind()),
        user_id: text(p.user_id()),
        app_id: text(p.app_id()),
    }
}

fn from_delivery(d: lapin::message::Delivery, consumer_tag: &str) -> Delivery {
    Delivery {
        properties: from_properties(&d.properties),
        exchange: d.exchange.as_str().to_string(),
        routing_key: d.routing_key.as_str().to_string(),
        consumer_tag: consumer_tag.to_string(),
        delivery_tag: d.delivery_tag,
        redelivered: d.redelivered,
        body: d.data,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_tables_round_trip_with_their_kinds() {
        let mut inner = FieldTable::new();
        inner.insert("n".into(), FieldValue::I16(-2));
        let mut table = FieldTable::new();
        table.insert("bool".into(), FieldValue::Bool(true));
        table.insert("i8".into(), FieldValue::I8(-1));
        table.insert("u8".into(), FieldValue::U8(1));
        table.insert("i32".into(), FieldValue::I32(7));
        table.insert("u32".into(), FieldValue::U32(8));
        table.insert("i64".into(), FieldValue::I64(1 << 40));
        table.insert("double".into(), FieldValue::Double(0.5));
        table.insert("text".into(), FieldValue::String("héllo".into()));
        table.insert("raw".into(), FieldValue::LongBytes(vec![0xff, 0xfe]));
        table.insert("bytes".into(), FieldValue::Bytes(vec![1, 2]));
        table.insert("ts".into(), FieldValue::Timestamp(1_700_000_000));
        table.insert("table".into(), FieldValue::Table(inner));
        table.insert(
            "array".into(),
            FieldValue::Array(vec![FieldValue::I32(1), FieldValue::String("a".into())]),
        );
        table.insert("void".into(), FieldValue::Void);
        assert_eq!(from_table(&to_table(&table)), table);
    }

    #[test]
    fn absent_properties_stay_absent() {
        let empty = to_properties(&Properties::default());
        assert_eq!(empty, BasicProperties::default());
        let p = Properties {
            content_type: Some("application/octet-stream".into()),
            delivery_mode: Some(2),
            priority: Some(0),
            correlation_id: Some("c".into()),
            message_id: Some("m".into()),
            r#type: Some("t".into()),
            timestamp: Some(5),
            ..Properties::default()
        };
        assert_eq!(from_properties(&to_properties(&p)), p);
    }
}
