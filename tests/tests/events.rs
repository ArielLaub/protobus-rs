//! Events: publishing, subscribing by topic pattern, and the opt-in retry ladder.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use protobus::amqp::Properties;
use protobus::{Error, EventInfo, EventListener, EventRetryOptions, MessageFactory, MessageServiceOptions};
use protobus_tests::*;

type Log = Arc<Mutex<Vec<(String, String, String)>>>;

fn recorder(
    log: &Log,
) -> impl Fn(Placed, EventInfo) -> std::future::Ready<protobus::Result<()>> + Send + Sync + 'static {
    let log = log.clone();
    move |placed, info| {
        log.lock().unwrap().push((placed.id, info.r#type, info.topic));
        std::future::ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_reaches_its_subscribers_on_the_default_topic() {
    let bus = Bus::new().await;
    let log = Log::default();
    bus.service.subscribe_event(recorder(&log), None).await.unwrap();
    assert_eq!(
        bus.broker.bindings("suite.Calc.Events", "proto.bus.events"),
        ["EVENT.suite.Placed"]
    );

    bus.client
        .publish_event(
            &Placed {
                id: "o-1".into(),
                amount: Some(5u64.into()),
            },
            None,
        )
        .await
        .unwrap();
    assert!(eventually(|| log.lock().unwrap().len() == 1).await);
    assert_eq!(
        log.lock().unwrap()[0],
        (
            "o-1".to_string(),
            "suite.Placed".to_string(),
            "EVENT.suite.Placed".to_string()
        )
    );
    // An event nobody subscribes to is normal.
    bus.client
        .publish_event(&Cancelled { id: "o-1".into() }, None)
        .await
        .unwrap();
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_patterns_route_as_the_broker_binds() {
    let bus = Bus::new().await;
    let (exact, star, hash) = (Log::default(), Log::default(), Log::default());
    bus.service
        .subscribe_event(recorder(&exact), Some("orders.eu.placed"))
        .await
        .unwrap();
    bus.service
        .subscribe_event(recorder(&star), Some("orders.*.placed"))
        .await
        .unwrap();
    bus.service
        .subscribe_event(recorder(&hash), Some("orders.#"))
        .await
        .unwrap();

    let publish = |id: &str, topic: &'static str| {
        let client = bus.client.clone();
        let placed = Placed {
            id: id.into(),
            amount: None,
        };
        async move { client.publish_event(&placed, Some(topic)).await.unwrap() }
    };
    publish("a", "orders.eu.placed").await;
    publish("b", "orders.us.placed").await;
    publish("c", "orders.us.west.placed").await;
    publish("d", "payments.eu.placed").await;

    assert!(eventually(|| hash.lock().unwrap().len() == 3).await);
    let ids = |log: &Log| log.lock().unwrap().iter().map(|e| e.0.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&exact), ["a"]);
    assert_eq!(ids(&star), ["a", "b"]);
    assert_eq!(ids(&hash), ["a", "b", "c"]);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_only_sees_events_of_its_own_type() {
    let bus = Bus::new().await;
    let placed = Log::default();
    let cancelled = Arc::new(AtomicUsize::new(0));
    bus.service
        .subscribe_event(recorder(&placed), Some("orders.#"))
        .await
        .unwrap();
    let seen = cancelled.clone();
    bus.service
        .subscribe_event(
            move |_: Cancelled, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
            Some("orders.#"),
        )
        .await
        .unwrap();
    bus.client
        .publish_event(&Cancelled { id: "x".into() }, Some("orders.cancelled"))
        .await
        .unwrap();
    bus.client
        .publish_event(
            &Placed {
                id: "y".into(),
                amount: None,
            },
            Some("orders.placed"),
        )
        .await
        .unwrap();
    assert!(eventually(|| cancelled.load(Ordering::SeqCst) == 1 && placed.lock().unwrap().len() == 1).await);
    assert_eq!(placed.lock().unwrap()[0].0, "y");
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handlers_route_on_the_delivered_key_not_the_topic_in_the_body() {
    let bus = Bus::new().await;
    let (public, private) = (Log::default(), Log::default());
    bus.service
        .subscribe_event(recorder(&public), Some("public.placed"))
        .await
        .unwrap();
    bus.service
        .subscribe_event(recorder(&private), Some("private.placed"))
        .await
        .unwrap();
    // A publisher allowed to write public.* claims another topic in the body.
    let payload = prost::Message::encode_to_vec(&Placed {
        id: "forged".into(),
        amount: None,
    });
    let event = MessageFactory::build_encoded_event("suite.Placed", payload, "private.placed");
    bus.broker
        .publish_raw("proto.bus.events", "public.placed", &event, Properties::default());
    assert!(eventually(|| public.lock().unwrap().len() == 1).await);
    assert!(private.lock().unwrap().is_empty());
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_event_retry_a_failing_handler_loses_the_event() {
    let bus = Bus::new().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    bus.service
        .subscribe_event(
            move |_: Placed, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                async { Err(Error::failure("cannot record it")) }
            },
            None,
        )
        .await
        .unwrap();
    bus.client.publish_event(&Placed::default(), None).await.unwrap();
    assert!(eventually(|| attempts.load(Ordering::SeqCst) == 1).await);
    assert!(eventually(|| bus.broker.unacked_count("suite.Calc.Events") == 0).await);
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "dropped, not redelivered");
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn with_event_retry_a_failing_handler_climbs_its_own_ladder() {
    let options = MessageServiceOptions {
        event_retry: EventRetryOptions {
            max_retries: 2,
            retry_delay_ms: 20,
        },
        ..Default::default()
    };
    let bus = Bus::with_options(options).await;
    let b = &bus.broker;
    for queue in ["suite.Calc.Events.Retry", "suite.Calc.Events.DLQ"] {
        assert!(b.queue_exists(queue), "{queue}");
    }
    assert!(b.exchange_exists("suite.Calc.Events.Retry.Exchange") && b.exchange_exists("suite.Calc.Events.Redelivery"));
    // The retry queue comes back through a per-subscriber exchange, never the
    // shared events exchange: other subscribers must not see the event again.
    let retry = b.queue_arguments("suite.Calc.Events.Retry").unwrap();
    assert_eq!(
        retry["x-dead-letter-exchange"].as_text().as_deref(),
        Some("suite.Calc.Events.Redelivery")
    );
    assert_eq!(b.bindings("suite.Calc.Events", "suite.Calc.Events.Redelivery"), ["#"]);
    assert_eq!(
        bus.service.event_listener().retry_topology(),
        Some((
            "suite.Calc.Events.Retry".to_string(),
            "suite.Calc.Events.DLQ".to_string()
        ))
    );

    // A second subscriber on its own queue, which must see the event exactly once.
    let other = EventListener::new(bus.client.connection(), bus.client.factory(), None);
    other.init(None).await.unwrap();
    let once = Log::default();
    other.subscribe(recorder(&once), None).await.unwrap();
    other.start().await.unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    bus.service
        .subscribe_event(
            move |_: Placed, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                async { Err(Error::failure("cannot record it")) }
            },
            None,
        )
        .await
        .unwrap();
    bus.client
        .publish_event(
            &Placed {
                id: "o-9".into(),
                amount: None,
            },
            None,
        )
        .await
        .unwrap();

    assert!(eventually(|| b.queue_depth("suite.Calc.Events.DLQ") == 1).await);
    assert_eq!(attempts.load(Ordering::SeqCst), 3, "one attempt and two retries");
    let dead = &b.peek("suite.Calc.Events.DLQ")[0];
    let h = dead.properties.headers.as_ref().unwrap();
    assert_eq!(h["x-retry-count"].as_int(), Some(2));
    assert_eq!(h["x-original-queue"].as_text().as_deref(), Some("suite.Calc.Events"));
    assert_eq!(
        h["x-original-routing-key"].as_text().as_deref(),
        Some("EVENT.suite.Placed")
    );
    assert_eq!(once.lock().unwrap().len(), 1);
    other.close().await;
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handled_error_from_an_event_handler_is_dropped_even_with_retry_on() {
    let options = MessageServiceOptions {
        event_retry: EventRetryOptions {
            max_retries: 2,
            retry_delay_ms: 20,
        },
        ..Default::default()
    };
    let bus = Bus::with_options(options).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    bus.service
        .subscribe_event(
            move |_: Placed, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                async { Err(Error::handled("not for us")) }
            },
            None,
        )
        .await
        .unwrap();
    bus.client.publish_event(&Placed::default(), None).await.unwrap();
    assert!(eventually(|| attempts.load(Ordering::SeqCst) == 1).await);
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(bus.broker.queue_depth("suite.Calc.Events.DLQ"), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_no_port_could_read_is_refused_on_the_way_out() {
    let bus = Bus::new().await;
    let wide = protobus::Bigint {
        value: Some(vec![1; 40]),
    };
    let e = bus
        .client
        .publish_event(
            &Placed {
                id: "x".into(),
                amount: Some(wide),
            },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.kind(), protobus::ErrorKind::CustomTypeRange);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn events_can_be_handled_as_dynamic_messages() {
    let bus = Bus::new().await;
    let ids = Arc::new(Mutex::new(Vec::new()));
    let seen = ids.clone();
    bus.service
        .subscribe_dynamic_event(
            "suite.Placed",
            move |event, info| {
                let id = event
                    .get_field_by_name("id")
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default();
                seen.lock().unwrap().push((id, info.r#type));
                async { Ok(()) }
            },
            None,
        )
        .await
        .unwrap();
    let mut event = bus.client.factory().new_message("suite.Placed").unwrap();
    event.set_field_by_name("id", protobus::prost_reflect::Value::String("dyn-1".into()));
    bus.client.publish_dynamic_event(&event, None).await.unwrap();
    assert!(eventually(|| ids.lock().unwrap().len() == 1).await);
    assert_eq!(
        ids.lock().unwrap()[0],
        ("dyn-1".to_string(), "suite.Placed".to_string())
    );
    bus.close().await;
}
