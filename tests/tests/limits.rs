//! Publish confirms and the bounds that depend on process-wide configuration.
//! Every test here changes that configuration, so they run one at a time.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use protobus::amqp::ConfirmOutcome;
use protobus::testing::{ConfirmMode, MemoryBroker};
use protobus::{config, CallOptions, Error, ErrorKind};
use protobus_tests::*;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Holds the configuration lock, and restores the defaults when dropped.
struct Config(#[allow(dead_code)] tokio::sync::MutexGuard<'static, ()>);

impl Config {
    async fn with(settings: &[(&str, &str)]) -> Config {
        let guard = SERIAL.lock().await;
        config::reset();
        for (name, value) in settings {
            config::set(name, value);
        }
        Config(guard)
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        config::reset();
    }
}

fn remote(result: protobus::Result<AddResponse>) -> Error {
    let e = result.expect_err("the call should fail");
    assert_eq!(e.kind(), ErrorKind::Remote, "{e}");
    e
}

#[tokio::test(flavor = "multi_thread")]
async fn internal_errors_can_be_hidden_from_callers() {
    let _config = Config::with(&[("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "false")]).await;
    let bus = Bus::with_options(fast_retry(0)).await;
    let e = remote(
        bus.calc
            .fail(FailRequest {
                how: "unhandled".into(),
                ..Default::default()
            })
            .await,
    );
    assert_eq!(e.code(), Some("INTERNAL_ERROR"));
    assert!(e.message().starts_with("internal service error (correlationId "), "{e}");
    assert!(!e.message().contains("hunter2"));
    // A handled error always crosses.
    assert_eq!(
        remote(
            bus.calc
                .fail(FailRequest {
                    how: "handled".into(),
                    ..Default::default()
                })
                .await
        )
        .message(),
        "not allowed"
    );
    // And so does a stream's.
    let e = bus
        .calc
        .count(CountRequest {
            count: 5,
            fail_at: 1,
            unhandled: true,
            ..Default::default()
        })
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some("INTERNAL_ERROR"));
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_exchange_names_come_from_the_environment() {
    let _config = Config::with(&[
        ("BUS_EXCHANGE_NAME", "acme.bus"),
        ("CALLBACKS_EXCHANGE_NAME", "acme.replies"),
        ("EVENTS_EXCHANGE_NAME", "acme.events"),
        ("CANCEL_EXCHANGE_NAME", "acme.cancel"),
    ])
    .await;
    let bus = Bus::new().await;
    for exchange in ["acme.bus", "acme.replies", "acme.events", "acme.cancel"] {
        assert!(bus.broker.exchange_exists(exchange), "{exchange}");
    }
    assert!(!bus.broker.exchange_exists("proto.bus"));
    assert_eq!(bus.calc.add(AddRequest { a: 1, b: 1 }).await.unwrap().result, 2);
    assert_eq!(
        bus.calc
            .count(CountRequest {
                count: 2,
                ..Default::default()
            })
            .unwrap()
            .collect()
            .await
            .unwrap()
            .len(),
        2
    );
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nacked_publish_is_definitely_not_stored() {
    let _config = Config::with(&[]).await;
    let bus = Bus::new().await;
    bus.broker.set_confirm_mode(ConfirmMode::Nack);
    let e = bus
        .calc
        .add_with(AddRequest::default(), CallOptions::new().message_id("m-nack"))
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::PublishNacked, "{e}");
    assert_eq!(e.code(), Some("PUBLISH_NACKED"));
    assert_eq!(e.message_id(), Some("m-nack"));
    assert!(e.is_publish_error() && !e.is_ambiguous());
    let e = bus.client.publish_event(&Placed::default(), None).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::PublishNacked);
    bus.broker.set_confirm_mode(ConfirmMode::Ack);
    assert_eq!(bus.calc.add(AddRequest { a: 1, b: 1 }).await.unwrap().result, 2);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_confirm_that_never_comes_is_ambiguous() {
    let _config = Config::with(&[("PUBLISH_CONFIRM_TIMEOUT_MS", "60")]).await;
    let bus = Bus::new().await;
    bus.broker.set_confirm_mode(ConfirmMode::Drop);
    let started = Instant::now();
    let e = bus.client.publish_event(&Placed::default(), None).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::PublishConfirmTimeout, "{e}");
    assert!(
        e.message().contains("no broker confirm") && e.message().contains("60ms"),
        "{e}"
    );
    assert!(e.is_ambiguous());
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(bus.broker.held_confirms(), 1, "the broker still owes an answer");
    bus.broker.set_confirm_mode(ConfirmMode::Ack);
    assert_eq!(bus.broker.release_held_confirms(None), 1);
    bus.client.publish_event(&Placed::default(), None).await.unwrap();
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_publish_wins_over_the_call_deadline() {
    let _config = Config::with(&[("PUBLISH_CONFIRM_TIMEOUT_MS", "60")]).await;
    let bus = Bus::new().await;
    bus.broker.set_confirm_mode(ConfirmMode::Drop);
    // The request is routed and answered, but its confirm never comes: "the
    // request may not have left" is the more specific answer than the reply.
    let e = bus
        .calc
        .add_with(AddRequest { a: 1, b: 1 }, CallOptions::new().timeout_ms(5000))
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::PublishConfirmTimeout, "{e}");
    // A deadline shorter than the confirm timeout ends the call first.
    let e = bus
        .calc
        .add_with(AddRequest { a: 1, b: 1 }, CallOptions::new().timeout_ms(20))
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::RpcTimeout, "{e}");
    bus.broker.set_confirm_mode(ConfirmMode::Ack);
    bus.broker.release_held_confirms(None);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unanswered_publishes_are_bounded_per_channel() {
    let _config = Config::with(&[("MAX_OUTSTANDING_CONFIRMS", "2"), ("PUBLISH_CONFIRM_TIMEOUT_MS", "80")]).await;
    let broker = MemoryBroker::new();
    let ctx = connect(&broker).await;
    broker.set_confirm_mode(ConfirmMode::Drop);
    let publish = || {
        let ctx = ctx.clone();
        tokio::spawn(async move { ctx.publish_event(&Placed::default(), None).await })
    };
    let (a, b) = (publish(), publish());
    assert!(eventually(|| broker.held_confirms() == 2).await);
    // Both slots are taken by publishes the broker has not answered. A third
    // waits for one, and fails without being sent when none comes free.
    let third = ctx.publish_event(&Placed::default(), None).await.unwrap_err();
    assert_eq!(third.kind(), ErrorKind::PublishConfirmTimeout);
    assert!(third.message().contains("confirm slots and was not sent"), "{third}");
    assert_eq!(broker.held_confirms(), 2, "it never reached the broker");
    for timed_out in [a, b] {
        assert!(timed_out
            .await
            .unwrap()
            .unwrap_err()
            .message()
            .contains("no broker confirm"));
    }
    // A timed-out publish keeps its slot until the broker answers.
    let fourth = ctx.publish_event(&Placed::default(), None).await.unwrap_err();
    assert!(fourth.message().contains("was not sent"), "{fourth}");
    // The broker answering, however late, frees the slots.
    broker.set_confirm_mode(ConfirmMode::Ack);
    assert_eq!(broker.release_held_confirms(Some(ConfirmOutcome::Ack)), 2);
    ctx.publish_event(&Placed::default(), None).await.unwrap();
    ctx.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_channel_that_closes_fails_its_unanswered_publishes_as_ambiguous() {
    let _config = Config::with(&[("PUBLISH_CONFIRM_TIMEOUT_MS", "5000")]).await;
    let broker = MemoryBroker::new();
    let ctx = connect_with(&broker, Some(fast_reconnect(0))).await;
    broker.set_confirm_mode(ConfirmMode::Drop);
    let pending = tokio::spawn({
        let ctx = ctx.clone();
        async move { ctx.publish_event(&Placed::default(), None).await }
    });
    assert!(eventually(|| broker.held_confirms() == 1).await);
    broker.kill_connections("connection reset by peer");
    let e = pending.await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::ChannelClosed, "{e}");
    assert!(e.is_ambiguous());
    assert!(e.message_id().is_some());
    broker.set_confirm_mode(ConfirmMode::Ack);
    ctx.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_consumer_fails_its_stream_at_the_chunk_bound() {
    let _config = Config::with(&[("STREAM_MAX_BUFFERED_CHUNKS", "5")]).await;
    let bus = Bus::new().await;
    let mut numbers = bus
        .calc
        .count(CountRequest {
            count: 1000,
            ..Default::default()
        })
        .unwrap();
    // Read nothing until the buffer has overflowed.
    assert!(eventually(|| numbers.is_finished() || bus.seen.stopped_early.load(Ordering::SeqCst) == 1).await);
    let e = loop {
        match numbers.next().await {
            Some(Ok(_)) => continue,
            Some(Err(e)) => break e,
            None => panic!("the stream ended without an error"),
        }
    };
    assert_eq!(e.kind(), ErrorKind::StreamBackpressure, "{e}");
    assert!(e.message().contains("limits are 5 chunks"), "{e}");
    assert!(
        eventually(|| bus.seen.stopped_early.load(Ordering::SeqCst) == 1).await,
        "the producer was told to stop"
    );
    assert_eq!(bus.client.message_dispatcher().buffered_stream_bytes(), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_bytes_are_bounded_per_stream_and_across_streams() {
    let _config = Config::with(&[("STREAM_MAX_BUFFERED_BYTES", "4000")]).await;
    let bus = Bus::new().await;
    let request = CountRequest {
        count: 1000,
        chunk_bytes: 1000,
        ..Default::default()
    };
    let e = bus.calc.count(request).unwrap();
    assert!(eventually(|| bus.seen.stopped_early.load(Ordering::SeqCst) == 1).await);
    let e = e.collect().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::StreamBackpressure, "{e}");
    assert!(e.message().contains("4000 bytes"), "{e}");

    config::set("STREAM_MAX_BUFFERED_BYTES", "1000000");
    config::set("STREAM_MAX_TOTAL_BUFFERED_BYTES", "6000");
    let (first, second) = (bus.calc.count(request).unwrap(), bus.calc.count(request).unwrap());
    let dispatcher = bus.client.message_dispatcher().clone();
    assert!(
        eventually(|| dispatcher.pending_stream_count() < 2).await,
        "one of them hit the shared bound"
    );
    assert!(dispatcher.buffered_stream_bytes() <= 6000);
    drop((first, second));
    assert!(eventually(|| dispatcher.buffered_stream_bytes() == 0).await);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_processing_timeout_and_prefetch_come_from_the_environment() {
    let _config = Config::with(&[("MESSAGE_PROCESSING_TIMEOUT", "40"), ("RPC_CALL_TIMEOUT_MS", "3000")]).await;
    let bus = Bus::with_options(fast_retry(0)).await;
    let e = remote(bus.calc.slow(SlowRequest { delay_ms: 2000 }).await);
    assert_eq!(e.code(), Some("PROCESSING_TIMEOUT"));
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_a_context_waits_for_running_handlers_up_to_the_drain_timeout() {
    let _config = Config::with(&[("SHUTDOWN_DRAIN_TIMEOUT_MS", "60")]).await;
    let bus = Bus::new().await;
    let call = tokio::spawn({
        let calc = bus.calc.clone();
        async move { calc.add_with(AddRequest::default(), CallOptions::new().one_way()).await }
    });
    call.await.unwrap().unwrap();
    // A handler that ignores its cancel token for longer than the budget.
    let raw = bus.calc.raw().clone();
    let stuck = tokio::spawn(async move {
        raw.call_raw(
            "slow",
            prost::Message::encode_to_vec(&SlowRequest { delay_ms: 400 }),
            &CallOptions::new().one_way(),
        )
        .await
    });
    stuck.await.unwrap().unwrap();
    // The one-way add may still be settling: wait for the slow handler itself.
    assert!(
        eventually(|| {
            bus.seen.calls.load(Ordering::SeqCst) == 2 && bus.server.connection().in_flight_deliveries() == 1
        })
        .await
    );
    let started = Instant::now();
    bus.server.close().await;
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(50) && waited < Duration::from_millis(350),
        "{waited:?}"
    );
    bus.client.close().await;
}
