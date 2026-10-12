//! Unary calls: answers, errors, retries and dead letters, call metadata, and
//! the checks a service makes before it runs anything.

use protobus::wire::{self, Response};
use protobus::{CallOptions, Error, ErrorKind, MessageFactory, MessageServiceOptions, RetryOptions, Timestamp, U256};
use protobus_tests::proto::Uuid;
use protobus_tests::*;

fn fail(how: &str) -> FailRequest {
    FailRequest {
        how: how.into(),
        ..Default::default()
    }
}

fn remote(result: protobus::Result<AddResponse>) -> Error {
    let e = result.expect_err("the call should fail");
    assert_eq!(e.kind(), ErrorKind::Remote, "{e}");
    e
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_is_answered() {
    let bus = Bus::new().await;
    assert_eq!(bus.calc.add(AddRequest { a: 20, b: 22 }).await.unwrap().result, 42);
    // A proto3 default survives the round trip as itself.
    assert_eq!(bus.calc.add(AddRequest { a: 0, b: 0 }).await.unwrap().result, 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_topology_matches_every_other_port() {
    let bus = Bus::new().await;
    let b = &bus.broker;
    for exchange in [
        "proto.bus",
        "proto.bus.callback",
        "proto.bus.events",
        "proto.bus.cancel",
        "suite.Calc.Retry.Exchange",
    ] {
        assert!(b.exchange_exists(exchange), "{exchange}");
    }
    for queue in ["suite.Calc", "suite.Calc.Retry", "suite.Calc.DLQ", "suite.Calc.Events"] {
        assert!(b.queue_exists(queue), "{queue}");
        assert!(b.queue_is_durable(queue), "{queue}");
    }
    assert_eq!(b.bindings("suite.Calc", "proto.bus"), ["REQUEST.suite.Calc.*"]);
    assert_eq!(b.bindings("suite.Calc.Retry", "suite.Calc.Retry.Exchange"), ["#"]);
    // A service that configures nothing declares its queue with no arguments.
    assert!(b.queue_arguments("suite.Calc").unwrap().is_empty());
    let retry = b.queue_arguments("suite.Calc.Retry").unwrap();
    assert_eq!(retry["x-message-ttl"].as_int(), Some(5000));
    assert_eq!(retry["x-dead-letter-exchange"].as_text().as_deref(), Some("proto.bus"));
    assert_eq!(retry.len(), 2);
    // Event retry is off: no ladder for events.
    assert!(!b.queue_exists("suite.Calc.Events.Retry"));
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handled_error_is_answered_and_never_retried() {
    let bus = Bus::with_options(fast_retry(3)).await;
    let options = CallOptions::new().message_id("m-handled");
    let e = remote(
        bus.calc
            .fail_with(
                FailRequest {
                    how: "handled".into(),
                    code: "NOT_ALLOWED".into(),
                    times: 0,
                },
                options,
            )
            .await,
    );
    assert_eq!(e.code(), Some("NOT_ALLOWED"));
    assert_eq!(e.message(), "not allowed");
    assert_eq!(e.method(), Some("suite.Calc.fail"));
    assert_eq!(
        remote(bus.calc.fail(fail("handled")).await).code(),
        Some("HANDLED_ERROR")
    );
    assert_eq!(bus.seen.attempts_of("m-handled"), 1);
    assert_eq!(bus.broker.queue_depth("suite.Calc.DLQ"), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unhandled_error_is_retried_then_dead_lettered_and_the_caller_answered() {
    let bus = Bus::with_options(fast_retry(3)).await;
    let options = CallOptions::new().message_id("m-1").priority(1);
    let e = remote(bus.calc.fail_with(fail("unhandled"), options).await);
    // Exposure is on by default: the caller is another of your own services.
    assert_eq!(e.message(), "database password is hunter2");
    assert_eq!(e.code(), None);
    assert_eq!(bus.seen.attempts_of("m-1"), 4, "one attempt and three retries");

    assert!(eventually(|| bus.broker.queue_depth("suite.Calc.DLQ") == 1).await);
    let dead = &bus.broker.peek("suite.Calc.DLQ")[0];
    let h = dead.properties.headers.as_ref().unwrap();
    assert_eq!(h["x-retry-count"].as_int(), Some(3));
    assert_eq!(h["x-original-queue"].as_text().as_deref(), Some("suite.Calc"));
    assert_eq!(
        h["x-original-routing-key"].as_text().as_deref(),
        Some("REQUEST.suite.Calc.fail")
    );
    // The class and code of the error, never an unhandled error's message.
    assert_eq!(h["x-last-error"].as_text().as_deref(), Some("Error"));
    assert!(h["x-first-failure-time"].as_int().unwrap() > 1_600_000_000_000);
    assert!(h["x-dlq-time"].as_int().unwrap() >= h["x-first-failure-time"].as_int().unwrap());
    // The copy keeps what identifies the message, and stays persistent.
    assert_eq!(dead.properties.message_id.as_deref(), Some("m-1"));
    assert_eq!(dead.properties.delivery_mode, Some(2));
    assert_eq!(dead.properties.priority, Some(1));
    assert_eq!(
        dead.properties.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(dead.properties.reply_to, None);
    assert_eq!(wire::decode_request(&dead.body).unwrap().method, "suite.Calc.fail");
    assert_eq!(bus.broker.queue_depth("suite.Calc"), 0);
    assert_eq!(bus.broker.unacked_count("suite.Calc"), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_that_succeeds_answers_the_caller() {
    let bus = Bus::with_options(fast_retry(3)).await;
    let options = CallOptions::new().message_id("m-flaky");
    let reply = bus
        .calc
        .fail_with(
            FailRequest {
                how: "unhandled".into(),
                code: String::new(),
                times: 2,
            },
            options,
        )
        .await;
    assert_eq!(reply.unwrap().result, 3, "answered by the third attempt");
    assert_eq!(bus.broker.queue_depth("suite.Calc.DLQ"), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn x_last_error_names_the_error_type() {
    let bus = Bus::with_options(fast_retry(1)).await;
    remote(bus.calc.fail(fail("parse")).await);
    assert!(eventually(|| bus.broker.queue_depth("suite.Calc.DLQ") == 1).await);
    let h = bus.broker.peek("suite.Calc.DLQ")[0].properties.headers.clone().unwrap();
    assert_eq!(h["x-last-error"].as_text().as_deref(), Some("ParseIntError"));
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_handler_is_retried_like_any_failure_and_the_service_keeps_serving() {
    let bus = Bus::with_options(fast_retry(1)).await;
    let e = remote(
        bus.calc
            .fail_with(fail("panic"), CallOptions::new().message_id("m-panic"))
            .await,
    );
    assert!(e.message().contains("panicked"), "{e}");
    assert_eq!(bus.seen.attempts_of("m-panic"), 2);
    assert!(eventually(|| bus.broker.queue_depth("suite.Calc.DLQ") == 1).await);
    let h = bus.broker.peek("suite.Calc.DLQ")[0].properties.headers.clone().unwrap();
    assert_eq!(h["x-last-error"].as_text().as_deref(), Some("Panic"));
    assert_eq!(bus.calc.add(AddRequest { a: 1, b: 1 }).await.unwrap().result, 2);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_retry_a_failure_is_answered_and_rejected() {
    let bus = Bus::with_options(fast_retry(0)).await;
    assert!(!bus.broker.queue_exists("suite.Calc.Retry"));
    assert!(!bus.broker.queue_exists("suite.Calc.DLQ"));
    let e = remote(
        bus.calc
            .fail_with(fail("unhandled"), CallOptions::new().message_id("m-0"))
            .await,
    );
    assert_eq!(e.message(), "database password is hunter2");
    assert_eq!(bus.seen.attempts_of("m-0"), 1);
    assert!(eventually(|| bus.broker.unacked_count("suite.Calc") == 0).await);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_early_ack_service_still_answers_its_failures() {
    let bus = Bus::with_options(MessageServiceOptions {
        late_ack: false,
        ..fast_retry(3)
    })
    .await;
    let e = remote(
        bus.calc
            .fail_with(fail("unhandled"), CallOptions::new().message_id("m-early"))
            .await,
    );
    assert_eq!(e.message(), "database password is hunter2");
    assert_eq!(bus.seen.attempts_of("m-early"), 1, "acked on delivery: no retry");
    assert_eq!(bus.calc.add(AddRequest { a: 2, b: 2 }).await.unwrap().result, 4);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_processing_timeout_is_retried_and_answered_once_the_retries_are_spent() {
    let options = MessageServiceOptions {
        processing_timeout_ms: Some(40),
        ..fast_retry(1)
    };
    let bus = Bus::with_options(options).await;
    let e = remote(bus.calc.slow(SlowRequest { delay_ms: 5000 }).await);
    assert_eq!(e.code(), Some("PROCESSING_TIMEOUT"));
    assert!(e.message().contains("40ms processing timeout"), "{e}");
    // The handler was told to stop, each time.
    assert!(eventually(|| bus.seen.cancelled_calls.load(std::sync::atomic::Ordering::SeqCst) == 2).await);
    assert!(eventually(|| bus.broker.queue_depth("suite.Calc.DLQ") == 1).await);
    let h = bus.broker.peek("suite.Calc.DLQ")[0].properties.headers.clone().unwrap();
    assert_eq!(
        h["x-last-error"].as_text().as_deref(),
        Some("TimeoutError[PROCESSING_TIMEOUT]")
    );
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_times_out_on_its_own_deadline() {
    let bus = Bus::new().await;
    let e = bus
        .calc
        .slow_with(SlowRequest { delay_ms: 2000 }, CallOptions::new().timeout_ms(30))
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::RpcTimeout);
    assert_eq!(e.code(), Some("RPC_TIMEOUT"));
    assert_eq!(
        bus.client.message_dispatcher().pending_call_count(),
        0,
        "the pending call is released"
    );
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_call_releases_it() {
    let bus = Bus::new().await;
    let call = tokio::spawn({
        let calc = bus.calc.clone();
        async move { calc.slow(SlowRequest { delay_ms: 400 }).await }
    });
    let dispatcher = bus.client.message_dispatcher().clone();
    assert!(eventually(|| dispatcher.pending_call_count() == 1).await);
    call.abort();
    assert!(eventually(|| dispatcher.pending_call_count() == 0).await);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_no_service_is_bound_for_is_unroutable() {
    let broker = protobus::testing::MemoryBroker::new();
    let client = connect(&broker).await;
    let calc = CalcProxy::new(&client).unwrap();
    let e = calc.add(AddRequest { a: 1, b: 2 }).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Unroutable, "{e}");
    assert_eq!(e.code(), Some("UNROUTABLE"));
    assert!(e.message_id().is_some());
    // A one-way publish to nobody is normal.
    calc.add_with(AddRequest { a: 1, b: 2 }, CallOptions::new().one_way())
        .await
        .unwrap();
    client.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_one_way_call_completes_on_the_confirm() {
    let bus = Bus::new().await;
    let reply = bus
        .calc
        .add_with(AddRequest { a: 5, b: 5 }, CallOptions::new().one_way())
        .await
        .unwrap();
    assert_eq!(reply, AddResponse::default());
    assert!(eventually(|| bus.seen.calls.load(std::sync::atomic::Ordering::SeqCst) == 1).await);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn call_metadata_reaches_the_handler() {
    let bus = Bus::new().await;
    let who = bus
        .calc
        .who_with(Empty {}, CallOptions::new().actor("user:42").message_id("order-7"))
        .await
        .unwrap();
    assert_eq!(who.actor, "user:42");
    assert_eq!(who.message_id, "order-7");
    assert_eq!(who.routing_key, "REQUEST.suite.Calc.who");
    assert_eq!(who.method, "suite.Calc.who");
    assert!(!who.redelivered);
    assert_eq!(who.correlation_id.len(), 36);
    // With none given, every publish still carries a messageId: a UUID.
    let anonymous = bus.calc.who(Empty {}).await.unwrap();
    assert_eq!(anonymous.message_id.len(), 36);
    assert_eq!(anonymous.actor, "");
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn options_are_validated_before_anything_is_sent() {
    let bus = Bus::new().await;
    let add = |options: CallOptions| bus.calc.add_with(AddRequest { a: 1, b: 1 }, options);
    assert_eq!(
        add(CallOptions::new().message_id("")).await.unwrap_err().kind(),
        ErrorKind::InvalidMessageId
    );
    assert_eq!(
        add(CallOptions::new().message_id("x".repeat(256)))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidMessageId
    );
    assert_eq!(
        add(CallOptions::new().priority(256)).await.unwrap_err().kind(),
        ErrorKind::InvalidPriority
    );
    assert_eq!(
        add(CallOptions::new().priority(-1)).await.unwrap_err().kind(),
        ErrorKind::InvalidPriority
    );
    assert_eq!(add(CallOptions::new().priority(0)).await.unwrap().result, 2);
    assert_eq!(bus.seen.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_priority_queue_needs_late_ack_and_a_sane_range() {
    let broker = protobus::testing::MemoryBroker::new();
    let ctx = connect(&broker).await;
    let build = |options| CalcService::with_options(&ctx, CalcService::NAME, Calc::default(), options);
    let early = MessageServiceOptions {
        max_priority: Some(2),
        late_ack: false,
        ..Default::default()
    };
    assert_eq!(build(early).err().unwrap().kind(), ErrorKind::InvalidPriority);
    let zero = MessageServiceOptions {
        max_priority: Some(0),
        ..Default::default()
    };
    assert_eq!(build(zero).err().unwrap().kind(), ErrorKind::InvalidPriority);
    let service = build(MessageServiceOptions {
        max_priority: Some(2),
        ..Default::default()
    })
    .unwrap();
    service.init().await.unwrap();
    assert_eq!(
        broker.queue_arguments("suite.Calc").unwrap()["x-max-priority"].as_int(),
        Some(2)
    );
    service.close().await;
    ctx.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_retry_delay_is_explained() {
    let bus = Bus::with_options(fast_retry(3)).await;
    let other = connect(&bus.broker).await;
    let options = MessageServiceOptions {
        retry: RetryOptions {
            retry_delay_ms: 999,
            ..Default::default()
        },
        ..Default::default()
    };
    let service = CalcService::with_options(&other, CalcService::NAME, Calc::default(), options).unwrap();
    let e = service.init().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::RetryQueueMismatch, "{e}");
    assert!(
        e.message().contains("suite.Calc.Retry") && e.message().contains("999ms"),
        "{e}"
    );
    other.close().await;
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn instances_share_a_contract_under_their_own_names() {
    let bus = Bus::new().await;
    let one = CalcService::with_options(&bus.server, "suite.Calc.one", Calc::default(), Default::default()).unwrap();
    one.init().await.unwrap();
    assert_eq!(one.contract_service_name().as_deref(), Some("suite.Calc"));
    let proxy = CalcProxy::with_name(&bus.client, "suite.Calc.one").unwrap();
    assert_eq!(proxy.raw().contract_service_name(), Some("suite.Calc"));
    let who = proxy.who(Empty {}).await.unwrap();
    assert_eq!(who.routing_key, "REQUEST.suite.Calc.one.who");
    assert_eq!(who.method, "suite.Calc.who", "the envelope names the contract method");
    assert!(bus.broker.queue_exists("suite.Calc.one"));
    // A name no schema declares, at any prefix.
    assert_eq!(
        CalcProxy::with_name(&bus.client, "nowhere.Calc").err().unwrap().kind(),
        ErrorKind::InvalidServiceName
    );
    let orphan = protobus::MessageService::new(&bus.server, "nowhere.Calc", Default::default()).unwrap();
    assert_eq!(orphan.init().await.unwrap_err().kind(), ErrorKind::MissingProto);
    one.close().await;
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unimplemented_rpc_answers_protocol_error() {
    let bus = Bus::with_options(fast_retry(3)).await;
    let e = bus.calc.missing(Empty {}).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Remote);
    assert_eq!(e.code(), Some("PROTOCOL_ERROR"));
    assert!(e.message().contains("suite.Calc.missing"), "{e}");
    assert_eq!(bus.broker.queue_depth("suite.Calc.DLQ"), 0, "never retried");
    bus.close().await;
}

/// Publish a hand-built request and return the service's error reply.
async fn raw_call(bus: &Bus, routing_key: &str, method: &str, payload: &[u8]) -> wire::ResponseError {
    let content = MessageFactory::build_encoded_request(method, payload.to_vec(), None);
    let reply = bus
        .client
        .publish_message(content, routing_key, &CallOptions::new().timeout_ms(2000))
        .await
        .unwrap()
        .unwrap();
    match wire::decode_response(&reply).unwrap() {
        Response::Error(e) => e,
        Response::Result(r) => panic!("expected an error, got a result for {}", r.method),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_is_checked_against_the_contract_before_it_runs() {
    let bus = Bus::with_options(fast_retry(3)).await;
    let add = prost::Message::encode_to_vec(&AddRequest { a: 1, b: 2 });

    // The body names another method than the routing key: route to add, run fail.
    let e = raw_call(&bus, "REQUEST.suite.Calc.add", "suite.Calc.fail", &add).await;
    assert_eq!(e.code, "PROTOCOL_ERROR");
    assert!(e.message.contains("contradicts routing key"), "{}", e.message);
    // Reported against the method the routing key names.
    assert_eq!(e.method, "suite.Calc.add");

    // A method of another contract, with the right last segment.
    let e = raw_call(&bus, "REQUEST.suite.Calc.add", "suite.Loop.add", &add).await;
    assert!(e.message.contains("is not a method of suite.Calc"), "{}", e.message);

    // An unqualified method name.
    let e = raw_call(&bus, "REQUEST.suite.Calc.add", "add", &add).await;
    assert!(
        e.message.contains("not a qualified method name") || e.message.contains("is not a method of"),
        "{}",
        e.message
    );

    // A method the contract does not declare.
    let e = raw_call(&bus, "REQUEST.suite.Calc.nope", "suite.Calc.nope", &add).await;
    assert!(e.message.contains("declares no method nope"), "{}", e.message);

    // A payload that is not the request type.
    let e = raw_call(&bus, "REQUEST.suite.Calc.add", "suite.Calc.add", &[0xff, 0xff, 0xff]).await;
    assert_eq!(e.code, "PROTOCOL_ERROR");
    assert!(e.message.contains("payload did not decode"), "{}", e.message);

    // An envelope that is not an envelope.
    let reply = bus
        .client
        .publish_message(
            vec![0x0a, 0xff],
            "REQUEST.suite.Calc.add",
            &CallOptions::new().timeout_ms(2000),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(wire::decode_response(&reply).unwrap(), Response::Error(e) if e.code == "PROTOCOL_ERROR"));

    assert_eq!(
        bus.seen.calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "nothing ran"
    );
    assert_eq!(bus.broker.queue_depth("suite.Calc.DLQ"), 0, "nothing was retried");
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn custom_types_cross_intact_and_out_of_range_values_are_refused() {
    let bus = Bus::new().await;
    let big = U256::from(10u64).checked_pow(30).unwrap();
    let money = Money {
        amount: Some(big.into()),
        at: Some(Timestamp::from_millis(-14_182_940_000).unwrap()),
        parts: vec![Money {
            amount: Some(7u64.into()),
            ..Default::default()
        }],
        by_name: [("k".to_string(), U256::MAX.into())].into(),
        id: Some(Uuid {
            value: Some("3f2a".into()),
        }),
    };
    let back = bus.calc.wallet(money.clone()).await.unwrap();
    assert_eq!(back, money);
    let amount = back.amount.unwrap();
    assert_eq!(amount.to_u256().unwrap().to_string(), "1000000000000000000000000000000");
    assert_eq!(amount.value.unwrap().len(), 32, "exactly 32 bytes on the wire");

    // A bigint wider than 32 bytes never leaves the caller, wherever it is nested.
    let wide = protobus::Bigint {
        value: Some(vec![1; 33]),
    };
    for bad in [
        Money {
            amount: Some(wide.clone()),
            ..Default::default()
        },
        Money {
            parts: vec![Money {
                amount: Some(wide.clone()),
                ..Default::default()
            }],
            ..Default::default()
        },
        Money {
            by_name: [("k".to_string(), wide.clone())].into(),
            ..Default::default()
        },
        Money {
            at: Some(Timestamp {
                value: Some(protobus::MAX_TIMESTAMP_MS + 1),
            }),
            ..Default::default()
        },
    ] {
        assert_eq!(
            bus.calc.wallet(bad).await.unwrap_err().kind(),
            ErrorKind::CustomTypeRange
        );
    }

    // One sent by a peer that does not check is answered PROTOCOL_ERROR.
    let payload = prost::Message::encode_to_vec(&Money {
        amount: Some(wide),
        ..Default::default()
    });
    let e = raw_call(&bus, "REQUEST.suite.Calc.wallet", "suite.Calc.wallet", &payload).await;
    assert_eq!(e.code, "PROTOCOL_ERROR");
    assert!(e.message.contains("33 bytes"), "{}", e.message);
    bus.close().await;
}
