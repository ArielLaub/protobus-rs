//! Server streaming: order, errors, cancellation, and what the caller's side
//! does with sequence numbers.

use std::sync::atomic::Ordering;
use std::time::Duration;

use protobus::amqp::{FieldTable, FieldValue, Properties};
use protobus::{ErrorKind, MessageFactory, StreamOptions};
use protobus_tests::*;

fn count(n: i32) -> CountRequest {
    CountRequest {
        count: n,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn chunks_arrive_in_order() {
    let bus = Bus::new().await;
    let numbers = bus.calc.count(count(50)).unwrap().collect().await.unwrap();
    assert_eq!(
        numbers.iter().map(|n| n.value).collect::<Vec<_>>(),
        (0..50).collect::<Vec<_>>()
    );
    assert_eq!(bus.client.message_dispatcher().pending_stream_count(), 0);
    assert_eq!(bus.client.message_dispatcher().buffered_stream_bytes(), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_stream_ends_at_once() {
    let bus = Bus::new().await;
    let mut numbers = bus.calc.count(count(0)).unwrap();
    assert!(numbers.next().await.is_none());
    assert!(numbers.next().await.is_none(), "and stays ended");
    assert!(numbers.is_finished());
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_adapts_to_the_futures_combinators() {
    use std::future::poll_fn;
    use std::pin::pin;
    let bus = Bus::new().await;
    let mut stream = pin!(bus.calc.count(count(3)).unwrap().into_stream());
    let mut values = Vec::new();
    while let Some(item) = poll_fn(|cx| futures_poll(stream.as_mut(), cx)).await {
        values.push(item.unwrap().value);
    }
    assert_eq!(values, [0, 1, 2]);
    bus.close().await;
}

fn futures_poll<S: FuturesStream>(
    stream: std::pin::Pin<&mut S>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<Option<S::Item>> {
    stream.poll_next(cx)
}

use protobus_tests::futures_core_stream::Stream as FuturesStream;

#[tokio::test(flavor = "multi_thread")]
async fn a_handled_error_ends_the_stream_after_the_chunks_before_it() {
    let bus = Bus::with_options(fast_retry(3)).await;
    let mut numbers = bus
        .calc
        .count(CountRequest {
            count: 10,
            fail_at: 2,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(numbers.next().await.unwrap().unwrap().value, 0);
    assert_eq!(numbers.next().await.unwrap().unwrap().value, 1);
    let e = numbers.next().await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Remote);
    assert_eq!(e.code(), Some("STREAM_FAIL"));
    assert_eq!(e.message(), "failed at 2");
    assert!(numbers.next().await.is_none());
    // Streams are never retried.
    assert_eq!(bus.seen.calls.load(Ordering::SeqCst), 1);
    assert_eq!(bus.broker.queue_depth("suite.Calc.DLQ"), 0);
    assert!(eventually(|| bus.broker.unacked_count("suite.Calc") == 0).await);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unhandled_error_ends_the_stream_too() {
    let bus = Bus::new().await;
    let request = CountRequest {
        count: 10,
        fail_at: 1,
        unhandled: true,
        ..Default::default()
    };
    let e = bus.calc.count(request).unwrap().collect().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Remote);
    assert_eq!(e.message(), "stream broke");
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_stream_stops_its_producer() {
    let bus = Bus::new().await;
    let mut numbers = bus
        .calc
        .count(CountRequest {
            count: 10_000,
            delay_ms: 2,
            ..Default::default()
        })
        .unwrap();
    for _ in 0..3 {
        numbers.next().await.unwrap().unwrap();
    }
    drop(numbers);
    assert!(
        eventually(|| bus.seen.stopped_early.load(Ordering::SeqCst) == 1).await,
        "the producer never heard"
    );
    assert!(bus.seen.yielded.load(Ordering::SeqCst) < 10_000);
    assert_eq!(bus.client.message_dispatcher().pending_stream_count(), 0);
    // The cancelled delivery is settled, not retried.
    assert!(eventually(|| bus.broker.unacked_count("suite.Calc") == 0).await);
    assert_eq!(bus.seen.calls.load(Ordering::SeqCst), 1);
    assert!(eventually(|| bus.server.connection().in_flight_deliveries() == 0).await);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_ends_the_stream_without_an_error() {
    let bus = Bus::new().await;
    let mut numbers = bus
        .calc
        .count(CountRequest {
            count: 10_000,
            delay_ms: 2,
            ..Default::default()
        })
        .unwrap();
    numbers.next().await.unwrap().unwrap();
    numbers.cancel();
    assert!(numbers.next().await.is_none());
    numbers.cancel();
    assert!(eventually(|| bus.seen.stopped_early.load(Ordering::SeqCst) == 1).await);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_goes_quiet_times_out_and_stops_its_producer() {
    let bus = Bus::new().await;
    let request = CountRequest {
        count: 100,
        delay_ms: 400,
        ..Default::default()
    };
    let mut numbers = bus
        .calc
        .count_with(request, StreamOptions::new().idle_timeout_ms(60))
        .unwrap();
    // Look-ahead by one: the first chunk is held until the second exists.
    let e = numbers.next().await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::StreamTimeout, "{e}");
    assert!(e.message().contains("60ms"), "{e}");
    assert!(e.is_streaming_error());
    assert!(eventually(|| bus.seen.stopped_early.load(Ordering::SeqCst) == 1).await);
    assert_eq!(bus.client.message_dispatcher().pending_stream_count(), 0);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_nobody_reads_still_releases_what_it_holds() {
    let bus = Bus::new().await;
    let request = CountRequest {
        count: 100,
        delay_ms: 300,
        ..Default::default()
    };
    let numbers = bus
        .calc
        .count_with(request, StreamOptions::new().idle_timeout_ms(40))
        .unwrap();
    let dispatcher = bus.client.message_dispatcher().clone();
    assert_eq!(dispatcher.pending_stream_count(), 1);
    assert!(
        eventually(|| dispatcher.pending_stream_count() == 0).await,
        "the idle deadline released it"
    );
    drop(numbers);
    bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_unary_method_cannot_be_streamed_nor_a_stream_called() {
    let bus = Bus::new().await;
    let raw = bus.calc.raw();
    let e = raw
        .call_stream_raw("add", vec![], &StreamOptions::default())
        .err()
        .unwrap();
    assert_eq!(e.kind(), ErrorKind::InvalidRequest);
    assert!(e.message().contains("unary"), "{e}");
    let e = raw.call_raw("count", vec![], &Default::default()).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidRequest);
    assert!(e.message().contains("server-streaming"), "{e}");
    let e = raw.call_raw("nope", vec![], &Default::default()).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::UnknownMethod);
    assert!(raw.is_streaming("count") && !raw.is_streaming("add"));
    assert_eq!(
        raw.methods(),
        ["add", "count", "fail", "missing", "slow", "wallet", "who"]
    );
    bus.close().await;
}

// ---- the caller's side, fed hand-made chunks --------------------------------------

/// A caller with a stream open to a service that never answers: the test
/// plays the service, publishing chunks to the reply queue itself.
struct Fed {
    bus: Bus,
    reply_queue: String,
    correlation_id: String,
}

impl Fed {
    async fn open(idle_timeout_ms: u64) -> (Fed, protobus::Stream<Number>) {
        let bus = Bus::new().await;
        bus.service.stop_consuming().await;
        let stream = bus
            .calc
            .count_with(count(1), StreamOptions::new().idle_timeout_ms(idle_timeout_ms))
            .unwrap();
        assert!(eventually(|| bus.broker.queue_depth("suite.Calc") == 1).await);
        let request = bus.broker.peek("suite.Calc").remove(0);
        let fed = Fed {
            reply_queue: request.properties.reply_to.unwrap(),
            correlation_id: request.properties.correlation_id.unwrap(),
            bus,
        };
        (fed, stream)
    }

    fn chunk(&self, value: i32, seq: Option<FieldValue>, last: Option<FieldValue>) {
        let payload = prost::Message::encode_to_vec(&Number { value, padding: vec![] });
        self.send(
            MessageFactory::build_encoded_response("suite.Calc.count", payload),
            seq,
            last,
        );
    }

    fn send(&self, body: Vec<u8>, seq: Option<FieldValue>, last: Option<FieldValue>) {
        let mut headers = FieldTable::new();
        if let Some(seq) = seq {
            headers.insert("x-protobus-seq".into(), seq);
        }
        if let Some(last) = last {
            headers.insert("x-protobus-final".into(), last);
        }
        let properties = Properties {
            correlation_id: Some(self.correlation_id.clone()),
            headers: Some(headers),
            ..Properties::default()
        };
        assert_eq!(
            self.bus
                .broker
                .publish_raw("proto.bus.callback", &self.reply_queue, &body, properties),
            1
        );
    }
}

fn seq(n: i64) -> Option<FieldValue> {
    Some(FieldValue::I64(n))
}

const MORE: Option<FieldValue> = Some(FieldValue::Bool(false));
const LAST: Option<FieldValue> = Some(FieldValue::Bool(true));

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_chunks_are_dropped() {
    let (fed, mut stream) = Fed::open(5000).await;
    fed.chunk(0, seq(0), MORE);
    fed.chunk(0, seq(0), MORE); // a redelivery
    fed.chunk(1, seq(1), MORE);
    fed.chunk(0, seq(0), MORE);
    fed.chunk(2, seq(2), LAST);
    let values: Vec<i32> = (&mut stream).collect_values().await;
    assert_eq!(values, [0, 1, 2]);
    fed.bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gap_in_the_sequence_fails_the_stream_and_cancels_the_producer() {
    let (fed, mut stream) = Fed::open(5000).await;
    fed.chunk(0, seq(0), MORE);
    assert_eq!(stream.next().await.unwrap().unwrap().value, 0);
    fed.chunk(2, seq(2), MORE);
    let e = stream.next().await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::StreamSequence, "{e}");
    assert!(e.message().contains("got seq=2, expected 1"), "{e}");
    // The cancel notice went out for the producer.
    assert!(fed.bus.broker.exchange_exists("proto.bus.cancel"));
    fed.bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_header_encoding_a_peer_produces_is_read() {
    let (fed, mut stream) = Fed::open(5000).await;
    // Integers of any width or as text; final as a boolean, number or text.
    fed.chunk(0, Some(FieldValue::I8(0)), Some(FieldValue::I32(0)));
    fed.chunk(
        1,
        Some(FieldValue::String("1".into())),
        Some(FieldValue::String("false".into())),
    );
    fed.chunk(2, Some(FieldValue::U32(2)), Some(FieldValue::String("true".into())));
    assert_eq!((&mut stream).collect_values().await, [0, 1, 2]);
    fed.bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chunks_without_sequence_numbers_are_taken_as_they_come() {
    let (fed, mut stream) = Fed::open(5000).await;
    fed.chunk(5, None, MORE);
    fed.chunk(5, None, MORE);
    fed.send(Vec::new(), None, LAST); // an empty final frame
    assert_eq!((&mut stream).collect_values().await, [5, 5]);
    fed.bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chunk_that_is_not_a_reply_fails_the_stream() {
    let (fed, mut stream) = Fed::open(5000).await;
    fed.send(vec![0xff, 0xff], seq(0), MORE);
    let e = stream.next().await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidResponse, "{e}");
    assert!(stream.next().await.is_none());
    fed.bus.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn progress_from_either_side_resets_the_idle_deadline() {
    let (fed, mut stream) = Fed::open(150).await;
    for value in 0..6 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        fed.chunk(value, seq(i64::from(value)), MORE);
        assert_eq!(stream.next().await.unwrap().unwrap().value, value);
    }
    // 360 ms in, more than twice the deadline: it has been reset each time.
    let e = stream.next().await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::StreamTimeout);
    fed.bus.close().await;
}

trait CollectValues {
    async fn collect_values(self) -> Vec<i32>;
}

impl CollectValues for &mut protobus::Stream<Number> {
    async fn collect_values(self) -> Vec<i32> {
        let mut values = Vec::new();
        while let Some(item) = self.next().await {
            values.push(item.unwrap().value);
        }
        values
    }
}
