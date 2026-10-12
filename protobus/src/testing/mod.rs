//! Testing services and clients without RabbitMQ.
//!
//! [`MemoryBroker`] is an AMQP broker in memory. Give it to a context as its
//! transport and everything runs in-process:
//!
//! ```
//! # async fn run() -> protobus::Result<()> {
//! use std::sync::Arc;
//! use protobus::{testing::MemoryBroker, Context, ContextOptions};
//!
//! let broker = MemoryBroker::new();
//! let ctx = Context::new(ContextOptions { transport: Some(Arc::new(broker.clone())), ..Default::default() });
//! ctx.init("amqp://memory/", &[] as &[&str]).await?;
//! // ... init services, call them through proxies ...
//! broker.kill_connections("connection reset by peer"); // a lost socket: reconnection runs
//! ctx.close().await;
//! # Ok(()) }
//! ```

mod memory_broker;

pub use memory_broker::{ConfirmMode, MemoryBroker};
