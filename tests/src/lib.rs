//! Shared by protobus-rs's suites: the generated test schema, an in-memory
//! bus, and a `Calc` service with every behaviour the suites need to provoke.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use protobus::testing::MemoryBroker;
use protobus::{
    CallContext, Context, ContextOptions, Error, MessageService, MessageServiceOptions, ReconnectionOptions, Result,
    RetryOptions, StreamWriter,
};

pub mod real_broker;

pub mod proto {
    protobus::include_protos!();
}

pub use proto::suite::*;

/// `futures_core::Stream`, for the test of `Stream::into_stream`.
pub mod futures_core_stream {
    pub use futures_core::Stream;
}

struct TestLog(std::time::Instant);

impl log::Log for TestLog {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        eprintln!(
            "{:>8.3} [{}] {}",
            self.0.elapsed().as_secs_f64(),
            record.level(),
            record.args()
        );
    }

    fn flush(&self) {}
}

/// `PROTOBUS_TEST_LOG=1` turns the library's logging on in every suite.
pub fn init_log() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("PROTOBUS_TEST_LOG").is_some() {
            let _ = log::set_logger(Box::leak(Box::new(TestLog(std::time::Instant::now()))));
            log::set_max_level(log::LevelFilter::Debug);
        }
    });
}

/// A context on an in-memory broker.
pub async fn connect(broker: &MemoryBroker) -> Context {
    connect_with(broker, None).await
}

pub async fn connect_with(broker: &MemoryBroker, reconnection: Option<ReconnectionOptions>) -> Context {
    init_log();
    let ctx = Context::new(ContextOptions {
        transport: Some(Arc::new(broker.clone())),
        reconnection,
    });
    ctx.init("amqp://memory/", &[] as &[&str])
        .await
        .expect("the in-memory broker accepts connections");
    ctx
}

/// Reconnect quickly, so a test's lost connection is back within milliseconds.
pub fn fast_reconnect(max_retries: u32) -> ReconnectionOptions {
    ReconnectionOptions {
        max_retries,
        initial_delay_ms: 5,
        max_delay_ms: 20,
        backoff_multiplier: 1.5,
    }
}

/// Retry quickly: `max_retries` hops of 20 ms.
pub fn fast_retry(max_retries: u32) -> MessageServiceOptions {
    MessageServiceOptions {
        retry: RetryOptions {
            max_retries,
            retry_delay_ms: 20,
            message_ttl_ms: None,
        },
        ..Default::default()
    }
}

/// Wait until `predicate` holds, up to five seconds.
pub async fn eventually(predicate: impl Fn() -> bool) -> bool {
    MemoryBroker::wait_for(predicate, Duration::from_secs(5)).await
}

/// What the `Calc` handler has seen.
#[derive(Default)]
pub struct Seen {
    /// Attempts per message id.
    pub attempts: Mutex<HashMap<String, usize>>,
    pub calls: AtomicUsize,
    /// Handlers running right now, and the most ever at once.
    pub running: AtomicUsize,
    pub peak: AtomicUsize,
    /// Chunks the last `count` stream produced, and whether it was cancelled.
    pub yielded: AtomicUsize,
    pub stopped_early: AtomicUsize,
    pub cancelled_calls: AtomicUsize,
    pub threads: Mutex<std::collections::HashSet<std::thread::ThreadId>>,
}

impl Seen {
    pub fn attempts_of(&self, message_id: &str) -> usize {
        self.attempts.lock().unwrap().get(message_id).copied().unwrap_or(0)
    }

    pub fn total_attempts(&self) -> usize {
        self.attempts.lock().unwrap().values().sum()
    }

    fn enter(&self) -> Running<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        self.threads.lock().unwrap().insert(std::thread::current().id());
        Running(self)
    }
}

struct Running<'a>(&'a Seen);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The `suite.Calc` handler.
#[derive(Clone, Default)]
pub struct Calc {
    pub seen: Arc<Seen>,
}

impl CalcHandler for Calc {
    async fn add(&self, r: AddRequest, _: CallContext) -> Result<AddResponse> {
        let _running = self.seen.enter();
        Ok(AddResponse { result: r.a + r.b })
    }

    async fn slow(&self, r: SlowRequest, call: CallContext) -> Result<AddResponse> {
        let _running = self.seen.enter();
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(r.delay_ms as u64)) => Ok(AddResponse { result: r.delay_ms }),
            _ = call.cancel.cancelled() => {
                self.seen.cancelled_calls.fetch_add(1, Ordering::SeqCst);
                Err(Error::failure("told to stop"))
            }
        }
    }

    async fn fail(&self, r: FailRequest, call: CallContext) -> Result<AddResponse> {
        let _running = self.seen.enter();
        let attempt = {
            let mut attempts = self.seen.attempts.lock().unwrap();
            let n = attempts.entry(call.message_id.clone().unwrap_or_default()).or_default();
            *n += 1;
            *n
        };
        if r.times > 0 && attempt > r.times as usize {
            return Ok(AddResponse { result: attempt as i32 });
        }
        match r.how.as_str() {
            "handled" => {
                let e = Error::handled("not allowed");
                Err(if r.code.is_empty() { e } else { e.with_code(r.code) })
            }
            "panic" => panic!("the handler panicked on purpose"),
            "parse" => Err(Error::unhandled("x".parse::<i32>().unwrap_err())),
            _ => Err(Error::failure("database password is hunter2")),
        }
    }

    async fn count(&self, r: CountRequest, call: CallContext, out: StreamWriter<Number>) -> Result<()> {
        let _running = self.seen.enter();
        self.seen.yielded.store(0, Ordering::SeqCst);
        self.seen.stopped_early.store(0, Ordering::SeqCst);
        for value in 0..r.count {
            if r.fail_at > 0 && value >= r.fail_at {
                return Err(if r.unhandled {
                    Error::failure("stream broke")
                } else {
                    Error::handled(format!("failed at {value}")).with_code("STREAM_FAIL")
                });
            }
            let sent = out
                .send(Number {
                    value,
                    padding: vec![7; r.chunk_bytes as usize],
                })
                .await;
            if sent.is_err() {
                self.seen.stopped_early.store(1, Ordering::SeqCst);
                return sent;
            }
            self.seen.yielded.fetch_add(1, Ordering::SeqCst);
            if r.delay_ms > 0 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(r.delay_ms as u64)) => {}
                    _ = call.cancel.cancelled() => {
                        self.seen.stopped_early.store(1, Ordering::SeqCst);
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    async fn wallet(&self, r: Money, _: CallContext) -> Result<Money> {
        Ok(r)
    }

    async fn who(&self, _: Empty, call: CallContext) -> Result<Who> {
        Ok(Who {
            actor: call.actor,
            message_id: call.message_id.unwrap_or_default(),
            routing_key: call.routing_key,
            redelivered: call.redelivered,
            correlation_id: call.correlation_id,
            method: call.method,
            retry_count: call.headers.get("x-retry-count").and_then(|v| v.as_int()).unwrap_or(0),
        })
    }
}

/// A bus with one `suite.Calc` service and a proxy to it on a second context.
pub struct Bus {
    pub broker: MemoryBroker,
    pub server: Context,
    pub client: Context,
    pub service: MessageService,
    pub calc: CalcProxy,
    pub seen: Arc<Seen>,
}

impl Bus {
    pub async fn new() -> Bus {
        Bus::with_options(MessageServiceOptions::default()).await
    }

    pub async fn with_options(options: MessageServiceOptions) -> Bus {
        Bus::build(MemoryBroker::new(), options, None).await
    }

    pub async fn build(
        broker: MemoryBroker,
        options: MessageServiceOptions,
        reconnection: Option<ReconnectionOptions>,
    ) -> Bus {
        let server = connect_with(&broker, reconnection.clone()).await;
        let handler = Calc::default();
        let seen = handler.seen.clone();
        let service: MessageService = CalcService::with_options(&server, CalcService::NAME, handler, options)
            .unwrap()
            .into();
        service.init().await.unwrap();
        let client = connect_with(&broker, reconnection).await;
        let calc = CalcProxy::new(&client).unwrap();
        Bus {
            broker,
            server,
            client,
            service,
            calc,
            seen,
        }
    }

    pub async fn close(self) {
        self.client.close().await;
        self.service.close().await;
        self.server.close().await;
    }
}
