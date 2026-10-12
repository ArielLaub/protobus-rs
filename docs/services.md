# Services

A service is a named queue of requests and the handlers that answer them. It
is named by its `.proto` `<package>.<Service>`, owns one durable queue of that
name bound to `REQUEST.<name>.*`, and every replica consumes from that queue:
the broker balances load between them and redelivers what a dead one held.

## Implementing one

The generator gives each proto service a handler trait with an async method
per rpc. Implement the ones you serve; the rest answer `PROTOCOL_ERROR`.

<!-- doc-check: compile -->
```rust
use std::sync::atomic::{AtomicU64, Ordering};
use protobus::{CallContext, Context, MessageServiceOptions, Result, RetryOptions};
use crate::proto::calculator::*;

#[derive(Default)]
pub struct Calculator {
    // Handlers run in parallel: shared state is atomic, or behind a lock.
    served: AtomicU64,
}

impl ServiceHandler for Calculator {
    async fn add(&self, request: AddRequest, call: CallContext) -> Result<AddResponse> {
        self.served.fetch_add(1, Ordering::Relaxed);
        println!("{} asked for {} (message {:?})", call.actor, call.method, call.message_id);
        Ok(AddResponse { result: request.a + request.b })
    }
}

pub async fn start(context: &Context) -> Result<ServiceService> {
    let options = MessageServiceOptions {
        max_concurrent: Some(8),
        retry: RetryOptions { max_retries: 5, retry_delay_ms: 2000, message_ttl_ms: None },
        processing_timeout_ms: Some(30_000),
        ..Default::default()
    };
    let service = ServiceService::with_options(context, ServiceService::NAME, Calculator::default(), options)?;
    service.init().await?;
    Ok(service)
}
```

`ServiceService` dereferences to `protobus::MessageService`, which is what
`init`, `subscribe_event`, `stop_consuming` and `close` are called on.
`from_arc` takes an `Arc` of the handler, for a handler you keep a handle to.

## What a handler receives

| `CallContext` field | |
|---|---|
| `actor` | free-text caller identity from the envelope; empty when none. Not authenticated |
| `correlation_id` | the call's id |
| `method` | the contract method, `<package>.<Service>.<method>` |
| `cancel` | a `CancelToken`: fires on the processing timeout and, for a stream, when the caller cancels |
| `routing_key` | the key the broker delivered on |
| `message_id` | stable across redeliveries and retries: deduplicate on it |
| `redelivered` | the broker has delivered this message before |
| `headers` | the delivery's AMQP headers (`x-retry-count`, ...) |

Delivery is at least once. A request can arrive twice: after a retry, after a
replica died holding it, after a reconnection. A handler whose effect must
happen once deduplicates on `message_id`.

A handler is never aborted. One doing long work watches `call.cancel`
(`call.cancel.cancelled().await` in a `tokio::select!`), or its work runs on
after the attempt has been written off.

## Options

| `MessageServiceOptions` | Default | |
|---|---|---|
| `max_concurrent` | 1 | requests handled at once by this process: the queue's prefetch |
| `retry.max_retries` | 3 | retry hops before the DLQ; 0 disables retry |
| `retry.retry_delay_ms` | 5000 | the wait before a retry. It is the retry queue's TTL, fixed when the queue is first declared: changing it needs the queue deleted, and `init` says so |
| `retry.message_ttl_ms` | none | a TTL for the service's own queue |
| `late_ack` | true | acknowledge after the handler rather than on delivery. `false` is at-most-once: no retry, no dead-lettering |
| `processing_timeout_ms` | `MESSAGE_PROCESSING_TIMEOUT` | one attempt's limit |
| `max_priority` | none | declare the queue with `x-max-priority`; see below |
| `event_retry` | off | retry for the service's event subscriptions; see [Events](events.md) |
| `serialize_handlers` | false | run the service's handlers one at a time; see [Threading](threading.md) |

## Priorities

A service declared with `max_priority: Some(2)` serves waiting requests highest
priority first; callers set `CallOptions::priority`. Priority only reorders
what is waiting in the queue, so it needs `late_ack` and a small
`max_concurrent`, and protobus refuses `max_priority` with `late_ack: false`.
`config::PRIORITY_NORMAL`, `PRIORITY_HIGH` and `PRIORITY_CONTROL` name the three
levels `max_priority: 2` gives. The queue's arguments are fixed when it is
first declared: adding a priority to an existing service means a new queue.

## Instances

Several services can serve one contract under names of their own:

<!-- doc-check: compile -->
```rust
use protobus::{CallContext, Context, Result};
use crate::proto::calculator::*;

struct Calculator;

impl ServiceHandler for Calculator {
    async fn add(&self, r: AddRequest, _: CallContext) -> Result<AddResponse> {
        Ok(AddResponse { result: r.a + r.b })
    }
}

pub async fn instances(context: &Context) -> Result<()> {
    let east = ServiceService::with_options(context, "Calculator.Service.east", Calculator, Default::default())?;
    east.init().await?;
    // Routed to REQUEST.Calculator.Service.east.add; the contract is Calculator.Service.
    let proxy = ServiceProxy::with_name(context, "Calculator.Service.east")?;
    proxy.add(AddRequest { a: 1, b: 2 }).await?;
    Ok(())
}
```

The contract is found by trimming trailing segments of the name until one
names a service the schema declares.

## Lifecycle

`RunnableService::start(&service)` initialises a service and registers it for
graceful shutdown on SIGINT, SIGTERM or `RunnableService::request_shutdown()`:

1. every started service stops taking new requests and events;
2. work in hand finishes, replies and retry publishes included, for up to
   `SHUTDOWN_DRAIN_TIMEOUT_MS`;
3. each service's cleanup (`start_with_cleanup`) runs, never under a running
   handler unless the drain deadline passed;
4. the contexts close.

`RunnableService::wait_for_shutdown()` returns the process's exit code once
that has happened.

By hand, the same steps are `service.stop_consuming()`,
`context.connection().drain_in_flight(ms)`, `service.close()` and
`context.close()`.

## Without generated code

`MessageService::new(&context, name, options)` with `register_method` and
`register_streaming_method` serves a contract loaded at runtime, over
[`DynamicMessage`](https://docs.rs/prost-reflect)s:

<!-- doc-check: compile -->
```rust
use protobus::prost_reflect::Value;
use protobus::{Context, MessageService, Result};

pub async fn dynamic(context: &Context) -> Result<MessageService> {
    // The schema was loaded with context.init(url, &["proto"]).
    let service = MessageService::new(context, "Calculator.Service", Default::default())?;
    let factory = context.factory().clone();
    service.register_method("add", move |request, _call| {
        let factory = factory.clone();
        async move {
            let field = |name| request.get_field_by_name(name).and_then(|v| v.as_i32()).unwrap_or(0);
            let mut reply = factory.new_message("Calculator.AddResponse")?;
            reply.set_field_by_name("result", Value::I32(field("a") + field("b")));
            Ok(reply)
        }
    });
    service.init().await?;
    Ok(service)
}
```
