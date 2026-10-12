//! The parts only a real broker can prove: TTL and dead-lettering, returns,
//! priorities, header encodings as RabbitMQ hands them back, consumer
//! cancellation and a connection the broker closes.
//!
//! Needs `PROTOBUS_TEST_AMQP_URL` and `PROTOBUS_TEST_MGMT_URL`; every test
//! runs in a virtual host of its own and skips when they are unset.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use protobus::amqp::{Delivery, FieldTable, FieldValue};
use protobus::{
    CallOptions, Context, ContextOptions, Error, ErrorKind, EventRetryOptions, MessageService, MessageServiceOptions,
    RetryOptions,
};
use protobus_tests::real_broker::VHost;
use protobus_tests::*;

struct Real {
    vhost: VHost,
    server: Context,
    client: Context,
    service: MessageService,
    calc: CalcProxy,
    seen: Arc<Seen>,
}

async fn context(vhost: &VHost) -> Context {
    init_log();
    let ctx = Context::new(ContextOptions {
        transport: None,
        reconnection: Some(fast_reconnect(0)),
    });
    ctx.init(vhost.url(), &[] as &[&str])
        .await
        .expect("the test broker accepts connections");
    ctx
}

impl Real {
    async fn new(options: MessageServiceOptions) -> Option<Real> {
        let vhost = VHost::create()?;
        let server = context(&vhost).await;
        let handler = Calc::default();
        let seen = handler.seen.clone();
        let service: MessageService = CalcService::with_options(&server, CalcService::NAME, handler, options)
            .unwrap()
            .into();
        service.init().await.unwrap();
        let client = context(&vhost).await;
        let calc = CalcProxy::new(&client).unwrap();
        Some(Real {
            vhost,
            server,
            client,
            service,
            calc,
            seen,
        })
    }

    async fn close(self) {
        self.client.close().await;
        self.service.close().await;
        self.server.close().await;
    }

    /// Take `count` messages off a queue.
    async fn drain(&self, queue: &str, count: usize) -> Vec<Delivery> {
        let channel = self.client.connection().open_channel().await.unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        channel
            .raw()
            .consume(
                queue,
                "",
                true,
                false,
                Arc::new(move |delivery| {
                    let _ = sender.send(delivery);
                    Box::pin(async {})
                }),
                None,
            )
            .await
            .unwrap();
        let mut taken = Vec::new();
        for _ in 0..count {
            let next = tokio::time::timeout(Duration::from_secs(15), receiver.recv()).await;
            taken.push(
                next.unwrap_or_else(|_| panic!("{queue} never held {count} message(s)"))
                    .unwrap(),
            );
        }
        let _ = channel.raw().close().await;
        taken
    }
}

fn remote(result: protobus::Result<AddResponse>) -> Error {
    let e = result.expect_err("the call should fail");
    assert_eq!(e.kind(), ErrorKind::Remote, "{e} ({})", e.name());
    e
}

async fn settled(predicate: impl Fn() -> bool) -> bool {
    protobus::testing::MemoryBroker::wait_for(predicate, Duration::from_secs(20)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn calls_streams_and_events_work_over_rabbitmq() {
    let Some(real) = Real::new(Default::default()).await else {
        return;
    };
    assert_eq!(real.calc.add(AddRequest { a: 20, b: 22 }).await.unwrap().result, 42);
    let numbers = real
        .calc
        .count(CountRequest {
            count: 25,
            ..Default::default()
        })
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        numbers.iter().map(|n| n.value).collect::<Vec<_>>(),
        (0..25).collect::<Vec<_>>()
    );
    assert!(real
        .calc
        .count(CountRequest::default())
        .unwrap()
        .collect()
        .await
        .unwrap()
        .is_empty());

    let who = real
        .calc
        .who_with(
            Empty {},
            CallOptions::new().actor("user:1").message_id("m-real").priority(1),
        )
        .await
        .unwrap();
    assert_eq!((who.actor.as_str(), who.message_id.as_str()), ("user:1", "m-real"));
    assert_eq!(who.routing_key, "REQUEST.suite.Calc.who");

    let placed = Arc::new(Mutex::new(Vec::new()));
    let seen = placed.clone();
    real.service
        .subscribe_event(
            move |event: Placed, info| {
                seen.lock().unwrap().push((event.id, info.topic));
                async { Ok(()) }
            },
            Some("orders.*.placed"),
        )
        .await
        .unwrap();
    real.client
        .publish_event(
            &Placed {
                id: "o-1".into(),
                amount: Some(9u64.into()),
            },
            Some("orders.eu.placed"),
        )
        .await
        .unwrap();
    assert!(settled(|| placed.lock().unwrap().len() == 1).await);
    assert_eq!(
        placed.lock().unwrap()[0],
        ("o-1".to_string(), "orders.eu.placed".to_string())
    );
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failure_climbs_the_retry_ladder_into_the_dlq() {
    let options = MessageServiceOptions {
        retry: RetryOptions {
            max_retries: 2,
            retry_delay_ms: 150,
            message_ttl_ms: None,
        },
        ..Default::default()
    };
    let Some(real) = Real::new(options).await else { return };
    let request = FailRequest {
        how: "unhandled".into(),
        ..Default::default()
    };
    let e = remote(
        real.calc
            .fail_with(request, CallOptions::new().message_id("m-dead").priority(1))
            .await,
    );
    assert_eq!(e.message(), "database password is hunter2");
    assert_eq!(
        real.seen.attempts_of("m-dead"),
        3,
        "RabbitMQ's TTL and dead-lettering brought it back twice"
    );

    let dead = real.drain("suite.Calc.DLQ", 1).await.remove(0);
    let h = dead.properties.headers.clone().unwrap();
    assert_eq!(h["x-retry-count"].as_int(), Some(2));
    assert_eq!(h["x-original-queue"].as_text().as_deref(), Some("suite.Calc"));
    assert_eq!(
        h["x-original-routing-key"].as_text().as_deref(),
        Some("REQUEST.suite.Calc.fail")
    );
    assert_eq!(h["x-last-error"].as_text().as_deref(), Some("Error"));
    assert!(h["x-dlq-time"].as_int().unwrap() >= h["x-first-failure-time"].as_int().unwrap());
    // RabbitMQ's own record of the hops through the retry queue rides along.
    assert!(matches!(h.get("x-death"), Some(FieldValue::Array(_))), "{h:?}");
    assert_eq!(dead.properties.message_id.as_deref(), Some("m-dead"));
    assert_eq!(dead.properties.delivery_mode, Some(2));
    assert_eq!(dead.properties.priority, Some(1));
    assert_eq!(dead.routing_key, "suite.Calc.DLQ");
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_that_succeeds_answers_the_parked_caller() {
    let options = MessageServiceOptions {
        retry: RetryOptions {
            max_retries: 3,
            retry_delay_ms: 100,
            message_ttl_ms: None,
        },
        ..Default::default()
    };
    let Some(real) = Real::new(options).await else { return };
    let request = FailRequest {
        how: "unhandled".into(),
        code: String::new(),
        times: 2,
    };
    assert_eq!(
        real.calc
            .fail_with(request, CallOptions::new().message_id("m-third"))
            .await
            .unwrap()
            .result,
        3
    );
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mandatory_request_nobody_serves_is_returned() {
    let Some(vhost) = VHost::create() else { return };
    let client = context(&vhost).await;
    let calc = CalcProxy::new(&client).unwrap();
    let e = calc.add(AddRequest { a: 1, b: 1 }).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Unroutable, "{e} ({})", e.name());
    assert!(e.message_id().is_some());
    // A one-way publish and an event with no subscriber are both fine.
    calc.add_with(AddRequest::default(), CallOptions::new().one_way())
        .await
        .unwrap();
    client.publish_event(&Placed::default(), None).await.unwrap();
    client.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_priority_queue_serves_the_highest_priority_first() {
    let options = MessageServiceOptions {
        max_priority: Some(2),
        ..Default::default()
    };
    let Some(real) = Real::new(options).await else { return };
    let order = Arc::new(Mutex::new(Vec::new()));
    // Hold the single handler slot, queue a backlog behind it, then watch the order it drains in.
    let blocker = tokio::spawn({
        let calc = real.calc.clone();
        async move { calc.slow(SlowRequest { delay_ms: 600 }).await }
    });
    assert!(settled(|| real.seen.running.load(Ordering::SeqCst) == 1).await);
    let mut calls = Vec::new();
    for (name, priority) in [("normal-1", 0), ("normal-2", 0), ("high", 1), ("control", 2)] {
        let (calc, order) = (real.calc.clone(), order.clone());
        calls.push(tokio::spawn(async move {
            let who = calc
                .who_with(Empty {}, CallOptions::new().message_id(name).priority(priority))
                .await
                .unwrap();
            order.lock().unwrap().push(who.message_id);
        }));
        // Publish in this order: the queue, not the publish order, decides.
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    blocker.await.unwrap().unwrap();
    for call in calls {
        call.await.unwrap();
    }
    assert_eq!(*order.lock().unwrap(), ["control", "high", "normal-1", "normal-2"]);
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_declare_equivalent_queues_and_a_changed_delay_is_explained() {
    let Some(real) = Real::new(Default::default()).await else {
        return;
    };
    // A second replica redeclares everything with the same arguments.
    let replica: MessageService = CalcService::new(&real.server, Calc::default()).unwrap().into();
    replica.init().await.unwrap();
    replica.close().await;

    let other = context(&real.vhost).await;
    let changed = MessageServiceOptions {
        retry: RetryOptions {
            retry_delay_ms: 777,
            ..Default::default()
        },
        ..Default::default()
    };
    let e = CalcService::with_options(&other, CalcService::NAME, Calc::default(), changed)
        .unwrap()
        .init()
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::RetryQueueMismatch, "{e} ({})", e.name());
    assert!(e.message().contains("777ms"), "{e}");
    other.close().await;
    assert_eq!(real.calc.add(AddRequest { a: 1, b: 1 }).await.unwrap().result, 2);
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_handler_that_fails_climbs_its_own_ladder() {
    let options = MessageServiceOptions {
        event_retry: EventRetryOptions {
            max_retries: 2,
            retry_delay_ms: 100,
        },
        ..Default::default()
    };
    let Some(real) = Real::new(options).await else { return };
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    real.service
        .subscribe_event(
            move |_: Placed, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                async { Err(Error::failure("cannot record it")) }
            },
            None,
        )
        .await
        .unwrap();
    real.client
        .publish_event(
            &Placed {
                id: "o-2".into(),
                amount: None,
            },
            None,
        )
        .await
        .unwrap();
    let dead = real.drain("suite.Calc.Events.DLQ", 1).await.remove(0);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    let h = dead.properties.headers.unwrap();
    assert_eq!(h["x-retry-count"].as_int(), Some(2));
    assert_eq!(
        h["x-original-routing-key"].as_text().as_deref(),
        Some("EVENT.suite.Placed")
    );
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_stream_reaches_the_producer_through_the_broker() {
    let Some(real) = Real::new(Default::default()).await else {
        return;
    };
    let mut numbers = real
        .calc
        .count(CountRequest {
            count: 100_000,
            delay_ms: 2,
            ..Default::default()
        })
        .unwrap();
    for _ in 0..3 {
        numbers.next().await.unwrap().unwrap();
    }
    drop(numbers);
    assert!(
        settled(|| real.seen.stopped_early.load(Ordering::SeqCst) == 1).await,
        "the producer never heard"
    );
    assert!(real.seen.yielded.load(Ordering::SeqCst) < 100_000);
    // The service is free for the next request.
    assert_eq!(real.calc.add(AddRequest { a: 2, b: 2 }).await.unwrap().result, 4);
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_channel_exception_closes_only_its_channel() {
    let Some(real) = Real::new(Default::default()).await else {
        return;
    };
    // Publishing to an exchange that does not exist is a channel exception.
    let channel = real.client.connection().open_channel().await.unwrap();
    let e = real
        .client
        .connection()
        .publish(Some(&channel), "no.such.exchange", "k", vec![1], Default::default())
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::ChannelClosed, "{e} ({})", e.name());
    assert!(settled(|| !channel.is_open()).await);
    assert!(real.client.is_connected());
    // Redeclaring a queue with other arguments reports the broker's 406.
    let channel = real.client.connection().open_channel().await.unwrap();
    let mut args = FieldTable::new();
    args.insert("x-message-ttl".into(), FieldValue::I64(1));
    let e = channel
        .raw()
        .declare_queue("suite.Calc", true, false, false, &args)
        .await
        .unwrap_err();
    assert_eq!(e.reply_code, 406, "{e}");
    assert!(e.precondition_failed());
    assert_eq!(real.calc.add(AddRequest { a: 3, b: 3 }).await.unwrap().result, 6);
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_the_broker_cancels_is_rebuilt() {
    let Some(real) = Real::new(Default::default()).await else {
        return;
    };
    // Deleting the queue makes RabbitMQ cancel its consumer (basic.cancel),
    // leaving the channel and the connection open.
    real.vhost.delete_queue("suite.Calc");
    assert!(
        settled(|| real.vhost.queue_exists("suite.Calc")).await,
        "the listener declared it again"
    );
    let calc = real.calc.clone();
    let mut answered = false;
    for _ in 0..40 {
        if matches!(calc.add_with(AddRequest { a: 4, b: 4 }, CallOptions::new().timeout_ms(1000)).await, Ok(r) if r.result == 8)
        {
            answered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(answered, "the rebuilt consumer never answered");
    real.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_the_broker_closes_is_restored() {
    // Room for the request that is redelivered after the reconnection, and
    // runs again, beside the ones this test makes afterwards.
    let options = MessageServiceOptions {
        max_concurrent: Some(4),
        ..Default::default()
    };
    let Some(real) = Real::new(options).await else {
        return;
    };
    let reconnected = Arc::new(AtomicUsize::new(0));
    for ctx in [&real.server, &real.client] {
        let counter = reconnected.clone();
        ctx.connection().on_reconnected(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        });
    }
    let pending = tokio::spawn({
        let calc = real.calc.clone();
        async move {
            calc.slow_with(SlowRequest { delay_ms: 60_000 }, CallOptions::new().timeout_ms(60_000))
                .await
        }
    });
    let closed = tokio::task::block_in_place(|| real.vhost.close_connections(2));
    assert_eq!(closed, 2);
    // The call in flight fails at once, rather than waiting out its timeout.
    let e = tokio::time::timeout(Duration::from_secs(20), pending)
        .await
        .expect("the pending call was failed")
        .unwrap()
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Disconnected, "{e} ({})", e.name());

    assert!(
        settled(|| reconnected.load(Ordering::SeqCst) == 2).await,
        "both contexts reconnected"
    );
    assert_eq!(real.calc.add(AddRequest { a: 5, b: 5 }).await.unwrap().result, 10);
    let numbers = real
        .calc
        .count(CountRequest {
            count: 3,
            ..Default::default()
        })
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(numbers.len(), 3);
    real.client.close().await;
    real.service.close().await;
    // The redelivered slow request is still running: do not wait a minute for it.
    protobus::config::set("SHUTDOWN_DRAIN_TIMEOUT_MS", "200");
    real.server.close().await;
    protobus::config::unset("SHUTDOWN_DRAIN_TIMEOUT_MS");
}
