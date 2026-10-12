# Testing

## Your services, without RabbitMQ

`protobus::testing::MemoryBroker` is an AMQP broker in memory. Give it to a
context as its transport and everything runs in-process:

<!-- doc-check: compile -->
```rust
use std::sync::Arc;
use std::time::Duration;
use protobus::testing::{ConfirmMode, MemoryBroker};
use protobus::{CallContext, Context, ContextOptions, ErrorKind, Result};
use crate::proto::calculator::*;

struct Calculator;

impl ServiceHandler for Calculator {
    async fn add(&self, r: AddRequest, _: CallContext) -> Result<AddResponse> {
        Ok(AddResponse { result: r.a + r.b })
    }
}

pub async fn in_memory() -> Result<()> {
    let broker = MemoryBroker::new();
    let context = Context::new(ContextOptions { transport: Some(Arc::new(broker.clone())), ..Default::default() });
    context.init("amqp://memory/", &[] as &[&str]).await?;
    let service = ServiceService::new(&context, Calculator)?;
    service.init().await?;
    let calculator = ServiceProxy::new(&context)?;
    assert_eq!(calculator.add(AddRequest { a: 1, b: 2 }).await?.result, 3);

    // Look inside the broker.
    assert!(broker.queue_exists("Calculator.Service.DLQ"));
    assert_eq!(broker.queue_depth("Calculator.Service.DLQ"), 0);

    // Inject faults.
    broker.set_confirm_mode(ConfirmMode::Nack); // the broker refuses publishes
    assert_eq!(calculator.add(AddRequest { a: 1, b: 2 }).await.unwrap_err().kind(), ErrorKind::PublishNacked);
    broker.set_confirm_mode(ConfirmMode::Ack);

    broker.kill_connections("connection reset by peer"); // a lost socket: reconnection runs
    let connection = context.connection().clone();
    assert!(MemoryBroker::wait_for(|| connection.is_ready(), Duration::from_secs(5)).await);
    assert_eq!(calculator.add(AddRequest { a: 2, b: 2 }).await?.result, 4);

    service.close().await;
    context.close().await;
    Ok(())
}
```

It implements what protobus relies on: direct, topic and fanout exchanges and
the default exchange; durable, exclusive, auto-delete and server-named queues;
per-queue TTL with dead-lettering; priority queues; per-consumer prefetch with
acknowledgement, rejection and redelivery; publisher confirms with mandatory
returns; and the channel errors RabbitMQ raises for a missing exchange or queue
(404), a redeclaration with different arguments (406) and an exclusive queue
owned elsewhere (405).

| Fault injection | |
|---|---|
| `kill_connections(reason)` | drop every connection: unacked deliveries requeue, exclusive queues go |
| `refuse_connections(true)` | make connecting fail |
| `set_confirm_mode(Nack / Drop)` | refuse publishes, or never confirm them |
| `release_held_confirms(outcome)` | deliver the confirms `Drop` held back |
| `close_channels_consuming(queue, reason)` | close a channel, leaving the connection up |
| `cancel_consumers(queue)` | cancel consumers from the broker side |
| `delete_queue(queue)` | delete a queue, as an operator would |
| `publish_raw(exchange, key, body, properties)` | publish as a foreign client would |

| Inspection | |
|---|---|
| `queue_exists`, `exchange_exists`, `queue_names` | topology |
| `queue_depth`, `unacked_count`, `consumer_count` | queue state |
| `peek(queue)` | the waiting messages, with their properties and headers |
| `bindings(queue, exchange)`, `queue_arguments(queue)` | declarations |
| `wait_for(predicate, timeout)` | poll until something holds |

Timeouts are environment-driven:
`protobus::config::set("RPC_CALL_TIMEOUT_MS", "500")` shortens one for a test,
and `config::reset()` undoes it. `config` is process-wide, so tests that
change it should not run in parallel with each other. Prefer the per-call
options (`CallOptions::timeout_ms`, `StreamOptions::idle_timeout_ms`,
`MessageServiceOptions::processing_timeout_ms`) where they exist.

## This repository's suites

| | Needs | Run |
|---|---|---|
| unit (each crate's own) | nothing | `cargo test -p protobus -p protobus-schema` |
| the suites over the in-memory broker | nothing | `cargo test -p protobus-tests` |
| integration | RabbitMQ | `cargo test -p protobus-tests --test integration` |
| cross-language | RabbitMQ and the other ports | `cargo test -p protobus-crosslang --test crosslang` |
| examples | RabbitMQ | `cargo run -p protobus-examples --bin calculator` |

`cargo test --workspace` runs all of them; the broker suites skip when no
broker is named. They never default to one. Point them at one:

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:29672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:29673
```

The in-memory suites cover every behaviour: the golden envelope bytes shared
with the other ports, retries and dead-lettering with their headers, streaming
with cancellation, backpressure, sequence gaps and duplicates, events and event
retry, reconnection, channel and consumer loss, publish confirms, graceful
shutdown, and the multi-threaded behaviour (`tests/tests/threading.rs`:
concurrency bounds, many tasks on one proxy, calls under dropping connections,
serialized handlers). The integration suite repeats the parts only a real
broker can prove: TTL and dead-lettering, returns, priorities, header
encodings, consumer cancellation and a connection the broker closes. The
[cross-language suite](../crosslang/README.md) runs Rust against the other
ports' real libraries.

`PROTOBUS_TEST_LOG=1` turns the library's logging on in every suite.
