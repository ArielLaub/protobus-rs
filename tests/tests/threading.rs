//! protobus-rs in a multi-threaded runtime: what is shared, what runs in
//! parallel, and what the bounds are.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use protobus::testing::MemoryBroker;
use protobus::{
    CallContext, CallOptions, Context, ErrorKind, EventListener, MessageDispatcher, MessageFactory, MessageService,
    MessageServiceOptions, Result, ServiceProxy, Stream,
};
use protobus_tests::*;

fn send_and_sync<T: Send + Sync>() {}
fn send<T: Send>() {}

#[test]
fn everything_a_program_shares_is_send_and_sync() {
    send_and_sync::<Context>();
    send_and_sync::<protobus::Connection>();
    send_and_sync::<MessageFactory>();
    send_and_sync::<MessageDispatcher>();
    send_and_sync::<MessageService>();
    send_and_sync::<ServiceProxy>();
    send_and_sync::<CalcProxy>();
    send_and_sync::<EventListener>();
    send_and_sync::<MemoryBroker>();
    send_and_sync::<protobus::Error>();
    send_and_sync::<protobus::CancelToken>();
    send_and_sync::<CallContext>();
    send_and_sync::<protobus::StreamWriter<Number>>();
    // A stream is read by one task at a time, and may move between them.
    send::<Stream<Number>>();
    send::<protobus::ChunkStream>();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_tasks_share_one_proxy() {
    let bus = Bus::with_options(MessageServiceOptions {
        max_concurrent: Some(16),
        ..Default::default()
    })
    .await;
    let mut tasks = Vec::new();
    for task in 0..32 {
        let calc = bus.calc.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..40 {
                let reply = calc.add(AddRequest { a: task, b: i }).await.unwrap();
                assert_eq!(reply.result, task + i, "a reply reached the wrong call");
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(bus.seen.calls.load(Ordering::SeqCst), 32 * 40);
    assert_eq!(bus.client.message_dispatcher().pending_call_count(), 0);
    assert_eq!(bus.broker.unacked_count("suite.Calc"), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn handlers_run_in_parallel_up_to_max_concurrent_and_no_further() {
    let bus = Bus::with_options(MessageServiceOptions {
        max_concurrent: Some(4),
        ..Default::default()
    })
    .await;
    let mut calls = Vec::new();
    for _ in 0..24 {
        let calc = bus.calc.clone();
        calls.push(tokio::spawn(async move {
            calc.slow(SlowRequest { delay_ms: 25 }).await.unwrap()
        }));
    }
    for call in calls {
        call.await.unwrap();
    }
    assert_eq!(
        bus.seen.peak.load(Ordering::SeqCst),
        4,
        "the prefetch bounds the handlers at once"
    );
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_is_one_request_at_a_time() {
    let bus = Bus::new().await;
    let mut calls = Vec::new();
    for _ in 0..8 {
        let calc = bus.calc.clone();
        calls.push(tokio::spawn(async move {
            calc.slow(SlowRequest { delay_ms: 5 }).await.unwrap()
        }));
    }
    for call in calls {
        call.await.unwrap();
    }
    assert_eq!(bus.seen.peak.load(Ordering::SeqCst), 1);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_early_ack_service_bounds_its_own_parallelism() {
    let options = MessageServiceOptions {
        late_ack: false,
        max_concurrent: Some(3),
        ..Default::default()
    };
    let bus = Bus::with_options(options).await;
    let mut calls = Vec::new();
    for _ in 0..15 {
        let calc = bus.calc.clone();
        calls.push(tokio::spawn(async move {
            calc.slow(SlowRequest { delay_ms: 20 }).await.unwrap()
        }));
    }
    for call in calls {
        call.await.unwrap();
    }
    assert_eq!(
        bus.seen.peak.load(Ordering::SeqCst),
        3,
        "no prefetch from the broker: bounded here"
    );
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn handlers_run_on_the_runtimes_worker_threads() {
    let bus = Bus::with_options(MessageServiceOptions {
        max_concurrent: Some(8),
        ..Default::default()
    })
    .await;
    let mut calls = Vec::new();
    for _ in 0..200 {
        let calc = bus.calc.clone();
        calls.push(tokio::spawn(async move {
            calc.add(AddRequest { a: 1, b: 1 }).await.unwrap()
        }));
    }
    for call in calls {
        call.await.unwrap();
    }
    assert!(
        bus.seen.threads.lock().unwrap().len() > 1,
        "every handler ran on one thread"
    );
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_streams_keep_their_own_chunks_in_order() {
    let bus = Bus::with_options(MessageServiceOptions {
        max_concurrent: Some(8),
        ..Default::default()
    })
    .await;
    let mut streams = Vec::new();
    for n in 1..=16 {
        let calc = bus.calc.clone();
        streams.push(tokio::spawn(async move {
            let numbers = calc
                .count(CountRequest {
                    count: n * 10,
                    ..Default::default()
                })
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                numbers.iter().map(|x| x.value).collect::<Vec<_>>(),
                (0..n * 10).collect::<Vec<_>>()
            );
        }));
    }
    for stream in streams {
        stream.await.unwrap();
    }
    assert_eq!(bus.client.message_dispatcher().pending_stream_count(), 0);
    assert_eq!(bus.client.message_dispatcher().buffered_stream_bytes(), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn calls_survive_connections_dropping_under_them() {
    let bus = Bus::build(
        MemoryBroker::new(),
        MessageServiceOptions {
            max_concurrent: Some(8),
            ..fast_retry(3)
        },
        Some(fast_reconnect(0)),
    )
    .await;
    let done = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for task in 0..8 {
        let (calc, done) = (bus.calc.clone(), done.clone());
        tasks.push(tokio::spawn(async move {
            let mut answered = 0;
            for i in 0..60 {
                // A call can fail while the connection is away; a wrong answer never.
                match calc
                    .add_with(AddRequest { a: task, b: i }, CallOptions::new().timeout_ms(2000))
                    .await
                {
                    Ok(reply) => {
                        assert_eq!(reply.result, task + i);
                        answered += 1;
                    }
                    Err(e) => assert!(
                        matches!(
                            e.kind(),
                            ErrorKind::Disconnected
                                | ErrorKind::ChannelClosed
                                | ErrorKind::NotReady
                                | ErrorKind::RpcTimeout
                                | ErrorKind::Unroutable
                        ),
                        "{e} ({})",
                        e.name()
                    ),
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            done.fetch_add(1, Ordering::SeqCst);
            answered
        }));
    }
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        bus.broker.kill_connections("connection reset by peer");
    }
    let mut answered = 0;
    for task in tasks {
        answered += task.await.unwrap();
    }
    assert_eq!(done.load(Ordering::SeqCst), 8);
    assert!(answered > 0, "nothing was ever answered");
    // Once the dust settles, the bus is whole: one consumer, nothing pending.
    let (server, client) = (bus.server.clone(), bus.client.clone());
    assert!(eventually(|| server.connection().is_ready() && client.connection().is_ready()).await);
    assert_eq!(bus.calc.add(AddRequest { a: 1, b: 1 }).await.unwrap().result, 2);
    assert!(eventually(|| bus.broker.consumer_count("suite.Calc") == 1).await);
    assert_eq!(bus.client.message_dispatcher().pending_call_count(), 0);
    bus.close().await;
}

/// A handler whose state is deliberately not atomic across an await: it is
/// only correct when its service's handlers never interleave.
struct Ledger {
    balance: AtomicI64,
    overlaps: AtomicUsize,
    busy: AtomicUsize,
    own: std::sync::OnceLock<LoopProxy>,
}

impl LoopHandler for Ledger {
    async fn relay(&self, r: AddRequest, _: CallContext) -> Result<AddResponse> {
        if self.busy.fetch_add(1, Ordering::SeqCst) > 0 {
            self.overlaps.fetch_add(1, Ordering::SeqCst);
        }
        if r.b == -1 {
            // Call our own service and wait for the answer.
            let own = self.own.get().expect("the proxy is set").clone();
            let result = own.relay(AddRequest { a: r.a, b: 0 }).await;
            self.busy.fetch_sub(1, Ordering::SeqCst);
            return result;
        }
        // Read, yield, write: a lost update unless handlers take turns.
        let before = self.balance.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1)).await;
        self.balance.store(before + i64::from(r.a), Ordering::SeqCst);
        self.busy.fetch_sub(1, Ordering::SeqCst);
        Ok(AddResponse {
            result: (before + i64::from(r.a)) as i32,
        })
    }
}

async fn ledger(serialize: bool) -> (MemoryBroker, Context, MessageService, LoopProxy, Arc<Ledger>) {
    let broker = MemoryBroker::new();
    let ctx = connect(&broker).await;
    let ledger = Arc::new(Ledger {
        balance: AtomicI64::new(0),
        overlaps: AtomicUsize::new(0),
        busy: AtomicUsize::new(0),
        own: std::sync::OnceLock::new(),
    });
    let options = MessageServiceOptions {
        max_concurrent: Some(8),
        serialize_handlers: serialize,
        ..fast_retry(0)
    };
    let service: MessageService = LoopService::from_arc(&ctx, LoopService::NAME, ledger.clone(), options)
        .unwrap()
        .into();
    service.init().await.unwrap();
    let proxy = LoopProxy::new(&ctx).unwrap();
    let _ = ledger.own.set(proxy.clone());
    (broker, ctx, service, proxy, ledger)
}

async fn deposit(proxy: &LoopProxy, times: i32) {
    let mut calls = Vec::new();
    for _ in 0..times {
        let proxy = proxy.clone();
        calls.push(tokio::spawn(async move {
            proxy.relay(AddRequest { a: 1, b: 0 }).await.unwrap()
        }));
    }
    for call in calls {
        call.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn serialized_handlers_take_turns() {
    let (_broker, ctx, service, proxy, ledger) = ledger(true).await;
    deposit(&proxy, 60).await;
    assert_eq!(ledger.overlaps.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.balance.load(Ordering::SeqCst), 60, "no update was lost");
    service.close().await;
    ctx.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn without_serialization_the_same_handlers_do_interleave() {
    // The control for the test above: its guarantee is the option's doing.
    let (_broker, ctx, service, proxy, ledger) = ledger(false).await;
    deposit(&proxy, 60).await;
    assert!(ledger.overlaps.load(Ordering::SeqCst) > 0);
    service.close().await;
    ctx.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_serialized_handler_calling_its_own_service_is_refused_not_deadlocked() {
    let (_broker, ctx, service, proxy, _ledger) = ledger(true).await;
    let e = proxy
        .relay_with(AddRequest { a: 1, b: -1 }, CallOptions::new().timeout_ms(3000))
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Remote, "{e}");
    assert!(e.message().contains("would deadlock"), "{e}");
    // The service is not stuck behind it.
    assert_eq!(proxy.relay(AddRequest { a: 5, b: 0 }).await.unwrap().result, 5);
    service.close().await;
    ctx.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unserialized_handler_may_call_its_own_service() {
    let (_broker, ctx, service, proxy, _ledger) = ledger(false).await;
    assert_eq!(proxy.relay(AddRequest { a: 7, b: -1 }).await.unwrap().result, 7);
    service.close().await;
    ctx.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_single_threaded_runtime_works_too() {
    let bus = Bus::with_options(MessageServiceOptions {
        max_concurrent: Some(4),
        ..Default::default()
    })
    .await;
    assert_eq!(bus.calc.add(AddRequest { a: 1, b: 2 }).await.unwrap().result, 3);
    assert_eq!(
        bus.calc
            .count(CountRequest {
                count: 5,
                ..Default::default()
            })
            .unwrap()
            .collect()
            .await
            .unwrap()
            .len(),
        5
    );
    bus.close().await;
}
