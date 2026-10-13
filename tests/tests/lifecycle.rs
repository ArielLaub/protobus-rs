//! Starting, stopping and shutting down.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use protobus::testing::MemoryBroker;
use protobus::{CallOptions, ErrorKind, MessageService, RunnableService, ServiceProxy};
use protobus_tests::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_context_must_be_connected_before_use() {
    let broker = MemoryBroker::new();
    let ctx = protobus::Context::new(protobus::ContextOptions {
        transport: Some(Arc::new(broker.clone())),
        reconnection: None,
    });
    let proxy = CalcProxy::new(&ctx).unwrap();
    assert_eq!(
        proxy.add(AddRequest::default()).await.unwrap_err().kind(),
        ErrorKind::NotConnected
    );
    assert_eq!(
        ctx.publish_event(&Placed::default(), None).await.unwrap_err().kind(),
        ErrorKind::NotConnected
    );
    let service = CalcService::new(&ctx, Calc::default()).unwrap();
    assert_eq!(service.init().await.unwrap_err().kind(), ErrorKind::NotConnected);

    broker.refuse_connections(true);
    let e = ctx.init("amqp://memory/", &[] as &[&str]).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Amqp, "{e}");
    broker.refuse_connections(false);
    ctx.init("amqp://memory/", &[] as &[&str]).await.unwrap();
    assert!(ctx.is_connected());
    let e = ctx.connection().connect("amqp://memory/", None).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::AlreadyConnected);
    ctx.close().await;
    ctx.close().await; // twice is fine
    assert!(!ctx.is_connected());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_proxy_is_initialised_once() {
    let bus = Bus::new().await;
    let proxy = ServiceProxy::new(&bus.client, "suite.Calc");
    assert!(!proxy.is_initialized());
    let e = proxy.call_raw("add", vec![], &Default::default()).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotInitialized);
    proxy.init().unwrap();
    assert_eq!(proxy.init().unwrap_err().kind(), ErrorKind::AlreadyInitialized);
    assert_eq!(proxy.service_name(), "suite.Calc");
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_a_context_fails_what_is_pending_on_it() {
    let bus = Bus::new().await;
    let pending = tokio::spawn({
        let calc = bus.calc.clone();
        async move { calc.slow(SlowRequest { delay_ms: 200 }).await }
    });
    let stream = bus
        .calc
        .count(CountRequest {
            count: 50,
            delay_ms: 10,
            ..Default::default()
        })
        .unwrap();
    let dispatcher = bus.client.message_dispatcher().clone();
    assert!(eventually(|| dispatcher.pending_call_count() == 1).await);
    bus.client.close().await;
    assert_eq!(pending.await.unwrap().unwrap_err().kind(), ErrorKind::Disconnected);
    assert_eq!(stream.collect().await.unwrap_err().kind(), ErrorKind::Disconnected);
    let e = bus.calc.add(AddRequest::default()).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotConnected);
    bus.service.close().await;
    bus.server.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_service_finishes_what_it_has_and_takes_nothing_new() {
    let bus = Bus::new().await;
    let in_hand = tokio::spawn({
        let calc = bus.calc.clone();
        async move { calc.slow(SlowRequest { delay_ms: 80 }).await }
    });
    assert!(eventually(|| bus.broker.unacked_count("suite.Calc") == 1).await);
    bus.service.stop_consuming().await;
    assert_eq!(bus.broker.consumer_count("suite.Calc"), 0);
    // The request in hand is still answered: its channel stayed open.
    assert!(bus.server.connection().drain_in_flight(2000).await);
    assert_eq!(in_hand.await.unwrap().unwrap().result, 80);
    assert_eq!(bus.server.connection().in_flight_deliveries(), 0);
    // A new one waits in the queue for another replica.
    bus.calc
        .add_with(AddRequest::default(), CallOptions::new().one_way())
        .await
        .unwrap();
    assert_eq!(bus.broker.queue_depth("suite.Calc"), 1);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_reports_work_that_outlives_the_deadline() {
    let bus = Bus::new().await;
    let call = tokio::spawn({
        let calc = bus.calc.clone();
        async move { calc.slow(SlowRequest { delay_ms: 150 }).await }
    });
    assert!(eventually(|| bus.server.connection().in_flight_deliveries() == 1).await);
    assert!(!bus.server.connection().drain_in_flight(20).await);
    assert!(bus.server.connection().drain_in_flight(2000).await);
    call.await.unwrap().unwrap();
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn runnable_services_shut_down_in_order() {
    RunnableService::reset();
    let broker = MemoryBroker::new();
    let server = connect(&broker).await;
    let handler = Calc::default();
    let seen = handler.seen.clone();
    let service = CalcService::new(&server, handler).unwrap();
    let (cleaned, running_at_cleanup) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(99)));
    let (flag, observed, connection) = (cleaned.clone(), running_at_cleanup.clone(), server.connection().clone());
    RunnableService::start_with_cleanup(&service, move || async move {
        observed.store(connection.in_flight_deliveries(), Ordering::SeqCst);
        flag.store(true, Ordering::SeqCst);
    })
    .await
    .unwrap();

    let client = connect(&broker).await;
    let calc = CalcProxy::new(&client).unwrap();
    let in_hand = tokio::spawn({
        let calc = calc.clone();
        async move { calc.slow(SlowRequest { delay_ms: 100 }).await }
    });
    assert!(eventually(|| seen.running.load(Ordering::SeqCst) == 1).await);

    RunnableService::request_shutdown();
    let code = tokio::time::timeout(Duration::from_secs(5), RunnableService::wait_for_shutdown())
        .await
        .unwrap();
    assert_eq!(code, 0);
    // The request in hand was answered, and cleanup only ran once it had been.
    assert_eq!(in_hand.await.unwrap().unwrap().result, 100);
    assert!(cleaned.load(Ordering::SeqCst));
    assert_eq!(running_at_cleanup.load(Ordering::SeqCst), 0);
    assert!(!server.is_connected(), "the context was closed");
    // A waiter that arrives after the shutdown completed returns at once.
    let code = tokio::time::timeout(Duration::from_secs(1), RunnableService::wait_for_shutdown())
        .await
        .expect("a late waiter does not hang");
    assert_eq!(code, 0);
    client.close().await;

    // A startup failure closes the context and is reported.
    RunnableService::reset();
    let ctx = connect(&broker).await;
    let orphan = MessageService::new(&ctx, "nowhere.Service", Default::default()).unwrap();
    assert_eq!(
        RunnableService::start(&orphan).await.unwrap_err().kind(),
        ErrorKind::MissingProto
    );
    assert!(!ctx.is_connected());
    RunnableService::reset();
}
