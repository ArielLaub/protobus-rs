//! Losing things and getting them back: the connection, a channel, a consumer.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use protobus::testing::MemoryBroker;
use protobus::{CallOptions, ErrorKind, StreamOptions};
use protobus_tests::*;

async fn reconnecting_bus() -> Bus {
    Bus::build(MemoryBroker::new(), fast_retry(3), Some(fast_reconnect(0))).await
}

async fn ready(bus: &Bus) {
    let (server, client) = (bus.server.clone(), bus.client.clone());
    assert!(
        eventually(|| server.connection().is_ready() && client.connection().is_ready()).await,
        "never reconnected"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_connection_is_restored_with_everything_on_it() {
    let bus = reconnecting_bus().await;
    let events = Arc::new(AtomicUsize::new(0));
    let seen = events.clone();
    bus.service
        .subscribe_event(
            move |_: Placed, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
            None,
        )
        .await
        .unwrap();
    let reconnected = Arc::new(AtomicUsize::new(0));
    let (counter, disconnected) = (reconnected.clone(), Arc::new(AtomicUsize::new(0)));
    bus.server.connection().on_reconnected(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    let counter = disconnected.clone();
    bus.server.connection().on_disconnected(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });

    for round in 1..=3 {
        bus.broker.kill_connections("connection reset by peer");
        assert!(!bus.server.connection().is_ready());
        ready(&bus).await;
        assert_eq!(
            bus.calc.add(AddRequest { a: round, b: 1 }).await.unwrap().result,
            round + 1
        );
        bus.client.publish_event(&Placed::default(), None).await.unwrap();
        assert!(eventually(|| events.load(Ordering::SeqCst) == round as usize).await);
        assert_eq!(disconnected.load(Ordering::SeqCst), round as usize);
        assert!(eventually(|| reconnected.load(Ordering::SeqCst) == round as usize).await);
    }
    // One consumer each, not one per reconnection.
    assert_eq!(bus.broker.consumer_count("suite.Calc"), 1);
    assert_eq!(bus.broker.consumer_count("suite.Calc.Events"), 1);
    assert_eq!(bus.broker.bindings("suite.Calc", "proto.bus"), ["REQUEST.suite.Calc.*"]);
    assert_eq!(bus.broker.open_connections(), 2);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_pending_when_the_connection_drops_fails_at_once() {
    let bus = reconnecting_bus().await;
    let pending = tokio::spawn({
        let calc = bus.calc.clone();
        async move { calc.slow(SlowRequest { delay_ms: 300 }).await }
    });
    let stream = bus
        .calc
        .count(CountRequest {
            count: 20,
            delay_ms: 20,
            ..Default::default()
        })
        .unwrap();
    let dispatcher = bus.client.message_dispatcher().clone();
    assert!(eventually(|| dispatcher.pending_call_count() == 1 && bus.broker.unacked_count("suite.Calc") >= 1).await);

    bus.broker.kill_connections("connection reset by peer");
    let e = pending.await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Disconnected, "{e}");
    let e = stream.collect().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Disconnected, "{e}");
    assert_eq!(dispatcher.pending_stream_count(), 0);

    ready(&bus).await;
    assert_eq!(bus.calc.add(AddRequest { a: 1, b: 1 }).await.unwrap().result, 2);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_waits_through_a_reconnection() {
    let bus = reconnecting_bus().await;
    bus.broker.refuse_connections(true);
    bus.broker.kill_connections("connection reset by peer");
    let call = tokio::spawn({
        let calc = bus.calc.clone();
        async move { calc.add(AddRequest { a: 40, b: 2 }).await }
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!call.is_finished(), "it waits while the broker is away");
    assert!(bus.client.is_reconnecting());
    bus.broker.refuse_connections(false);
    assert_eq!(call.await.unwrap().unwrap().result, 42);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnection_gives_up_after_its_retries_and_says_so() {
    let broker = MemoryBroker::new();
    let ctx = connect_with(&broker, Some(fast_reconnect(3))).await;
    let calc = CalcProxy::new(&ctx).unwrap();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let (sink, attempts) = (errors.clone(), Arc::new(AtomicUsize::new(0)));
    ctx.connection()
        .on_error(move |e| sink.lock().unwrap().push(e.to_string()));
    let counter = attempts.clone();
    ctx.connection().on_reconnecting(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
    });

    broker.refuse_connections(true);
    broker.kill_connections("connection reset by peer");
    assert!(eventually(|| !errors.lock().unwrap().is_empty()).await);
    assert_eq!(errors.lock().unwrap()[0], "max reconnection attempts (3) exceeded");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(broker.connect_attempts(), 1 + 3);
    // Nothing is attempted on a connection that has given up.
    let e = calc.add(AddRequest::default()).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotConnected, "{e}");
    let e = ctx.connection().when_ready(Some(50)).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotReady);
    assert!(e.message().contains("max reconnection attempts"), "{e}");
    ctx.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn waiting_for_readiness_has_a_deadline() {
    let broker = MemoryBroker::new();
    let ctx = connect_with(&broker, Some(fast_reconnect(0))).await;
    broker.refuse_connections(true);
    broker.kill_connections("gone");
    let e = ctx.connection().when_ready(Some(40)).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotReady);
    assert!(e.message().contains("within 40ms"), "{e}");
    ctx.close().await;
    // Closed: readiness will never come, and says why.
    let e = ctx.connection().when_ready(Some(40)).await.unwrap_err();
    assert!(e.message().contains("has been closed"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restoration_that_fails_is_retried_as_a_whole() {
    let bus = reconnecting_bus().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    bus.server.connection().on_reconnecting(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    // While the server is away, its retry queue reappears with another TTL:
    // the listener's restoration fails on it, so the connection must not
    // report itself ready with a service that is not consuming.
    bus.broker.refuse_connections(true);
    bus.broker.kill_connections("connection reset by peer");
    bus.broker.delete_queue("suite.Calc.Retry");
    let other = MemoryBroker::clone(&bus.broker);
    other.refuse_connections(false);
    let squatter = connect(&other).await;
    let channel = squatter.connection().open_channel().await.unwrap();
    let mut args = protobus::amqp::FieldTable::new();
    args.insert("x-message-ttl".into(), protobus::amqp::FieldValue::I64(1));
    channel
        .raw()
        .declare_queue("suite.Calc.Retry", true, false, false, &args)
        .await
        .unwrap();

    assert!(
        eventually(|| attempts.load(Ordering::SeqCst) >= 3).await,
        "it keeps trying"
    );
    assert!(!bus.server.connection().is_ready());
    assert_eq!(bus.broker.consumer_count("suite.Calc"), 0);

    channel.raw().delete_queue("suite.Calc.Retry").await.unwrap();
    ready(&bus).await;
    assert_eq!(bus.calc.add(AddRequest { a: 1, b: 2 }).await.unwrap().result, 3);
    assert_eq!(bus.broker.consumer_count("suite.Calc"), 1);
    squatter.close().await;
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_channel_lost_on_a_live_connection_is_rebuilt() {
    let bus = reconnecting_bus().await;
    let before = bus.broker.connect_attempts();
    bus.broker.close_channels_consuming("suite.Calc", "541 INTERNAL_ERROR");
    assert!(
        eventually(|| bus.broker.consumer_count("suite.Calc") == 1).await,
        "the listener came back"
    );
    assert_eq!(bus.calc.add(AddRequest { a: 2, b: 2 }).await.unwrap().result, 4);
    assert_eq!(bus.broker.connect_attempts(), before, "the connection never dropped");
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_the_broker_cancels_is_rebuilt() {
    let bus = reconnecting_bus().await;
    bus.broker.cancel_consumers("suite.Calc");
    assert_eq!(bus.broker.consumer_count("suite.Calc"), 0);
    assert!(eventually(|| bus.broker.consumer_count("suite.Calc") == 1).await);
    assert_eq!(bus.calc.add(AddRequest { a: 3, b: 3 }).await.unwrap().result, 6);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_queue_is_declared_again() {
    let bus = reconnecting_bus().await;
    bus.broker.delete_queue("suite.Calc");
    assert!(eventually(|| bus.broker.consumer_count("suite.Calc") == 1).await);
    assert_eq!(bus.broker.bindings("suite.Calc", "proto.bus"), ["REQUEST.suite.Calc.*"]);
    assert_eq!(bus.calc.add(AddRequest { a: 4, b: 4 }).await.unwrap().result, 8);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn calls_pending_on_a_replaced_reply_queue_fail_rather_than_hang() {
    let bus = reconnecting_bus().await;
    let pending = tokio::spawn({
        let calc = bus.calc.clone();
        async move {
            calc.slow_with(SlowRequest { delay_ms: 400 }, CallOptions::new().timeout_ms(5000))
                .await
        }
    });
    let dispatcher = bus.client.message_dispatcher().clone();
    assert!(eventually(|| dispatcher.pending_call_count() == 1 && bus.broker.unacked_count("suite.Calc") == 1).await);
    let reply_to = bus
        .broker
        .queue_names()
        .into_iter()
        .filter(|q| q.starts_with("amq.gen-") && !bus.broker.bindings(q, "proto.bus.callback").is_empty())
        .collect::<Vec<_>>();
    // Close the channels of every reply queue: the caller's is among them.
    for queue in &reply_to {
        bus.broker.close_channels_consuming(queue, "541 INTERNAL_ERROR");
    }
    let e = pending.await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Disconnected, "{e}");
    assert!(e.message().contains("reply queue was replaced"), "{e}");
    // The rebuilt reply queue serves the next call.
    assert!(eventually(|| bus.client.connection().is_ready()).await);
    assert_eq!(bus.calc.add(AddRequest { a: 5, b: 5 }).await.unwrap().result, 10);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publishing_channel_lost_on_a_live_connection_is_reopened() {
    let bus = reconnecting_bus().await;
    // A publish to a missing exchange closes the dispatcher's channel, as RabbitMQ does.
    let e = bus
        .client
        .connection()
        .publish(None, "nowhere", "k", vec![], Default::default())
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::ChannelClosed);
    assert_eq!(bus.calc.add(AddRequest { a: 6, b: 6 }).await.unwrap().result, 12);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unacknowledged_request_is_redelivered_after_a_reconnection() {
    let bus = reconnecting_bus().await;
    let call = tokio::spawn({
        let calc = bus.calc.clone();
        async move {
            calc.slow_with(
                SlowRequest { delay_ms: 150 },
                CallOptions::new().message_id("m-redeliver"),
            )
            .await
        }
    });
    assert!(eventually(|| bus.broker.unacked_count("suite.Calc") == 1).await);
    bus.broker.kill_connections("connection reset by peer");
    // The caller's reply went with its connection...
    assert_eq!(call.await.unwrap().unwrap_err().kind(), ErrorKind::Disconnected);
    // ...and the request, never acknowledged, is handled again.
    assert!(eventually(|| bus.seen.calls.load(Ordering::SeqCst) == 2).await);
    assert!(
        eventually(|| bus.broker.queue_depth("suite.Calc") == 0 && bus.broker.unacked_count("suite.Calc") == 0).await
    );
    ready(&bus).await;
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_opened_while_reconnecting_publishes_once_ready() {
    let bus = reconnecting_bus().await;
    bus.broker.refuse_connections(true);
    bus.broker.kill_connections("connection reset by peer");
    let stream = bus
        .calc
        .count_with(
            CountRequest {
                count: 3,
                ..Default::default()
            },
            StreamOptions::new().idle_timeout_ms(5000),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    bus.broker.refuse_connections(false);
    assert_eq!(stream.collect().await.unwrap().len(), 3);
    bus.close().await;
}
