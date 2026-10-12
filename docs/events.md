# Events

An event is a message published to whoever cares, with no reply. It is
published on a topic, `EVENT.<message type>` unless you name one, and a
service receives the topics it subscribed to on its own queue,
`<Service>.Events`. Replicas of a service share that queue, so each event is
handled by one of them; different services each get their own copy.

<!-- doc-check: compile -->
```rust
use protobus::{Context, MessageService, Result, Timestamp};
use crate::proto::calculator::Calculated;

pub async fn publish(context: &Context) -> Result<()> {
    let event = Calculated { operation: "add".into(), at: Some(Timestamp::now()) };
    // On EVENT.Calculator.Calculated:
    context.publish_event(&event, None).await?;
    // On a topic of your own:
    context.publish_event(&event, Some("audit.calculator.add")).await
}

pub async fn subscribe(service: &MessageService) -> Result<()> {
    // After service.init(). By type, on its default topic:
    service
        .subscribe_event(|event: Calculated, _| async move {
            println!("{}", event.operation);
            Ok(())
        }, None)
        .await?;
    // By pattern: `*` is exactly one word, `#` zero or more.
    service
        .subscribe_event(|event: Calculated, info| async move {
            println!("{} on {}", event.operation, info.topic);
            Ok(())
        }, Some("audit.#"))
        .await
}
```

`publish_event` completes when the broker has confirmed the event. An event
nobody subscribes to is not an error.

A handler receives events of its own message type only: one that matches a
pattern but carries another type is skipped. Handlers are matched on the
routing key the broker delivered on, never on the topic written in the body.

## A subscriber that is not a service

`EventListener` subscribes on a queue of its own: an exclusive, server-named
one that goes with the connection, or a durable named one.

<!-- doc-check: compile -->
```rust
use protobus::{Context, EventListener, Result};
use crate::proto::calculator::Calculated;

pub async fn listen(context: &Context) -> Result<EventListener> {
    let listener = EventListener::new(context.connection(), context.factory(), None);
    listener.init(None).await?; // Some("audit") for a durable queue named audit
    listener
        .subscribe(|event: Calculated, _| async move {
            println!("{}", event.operation);
            Ok(())
        }, None)
        .await?;
    listener.start().await?;
    Ok(listener)
}
```

## When a handler fails

By default a handler that fails loses its event: it is rejected, not
redelivered. Turn on event retry to give events the ladder requests have:

<!-- doc-check: compile -->
```rust
use protobus::{EventRetryOptions, MessageServiceOptions};

pub fn options() -> MessageServiceOptions {
    MessageServiceOptions {
        event_retry: EventRetryOptions { max_retries: 3, retry_delay_ms: 5000 },
        ..Default::default()
    }
}
```

A failed event then waits on `<Service>.Events.Retry`, comes back to this
service's queue only (through `<Service>.Events.Redelivery`, never the shared
events exchange), and lands on `<Service>.Events.DLQ` once the retries are
spent. A retried event runs every handler that matches it again, including
ones that succeeded the first time: make handlers idempotent. A handled error
(`Error::handled`) from an event handler is not retried; the event is dropped.
