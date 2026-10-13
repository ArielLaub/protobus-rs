<p align="center">
  <img src="https://raw.githubusercontent.com/ArielLaub/protobus-rs/main/docs/assets/banner.jpg" alt="ProtoBus: many languages, one message bus. A thin layer over Protocol Buffers and RabbitMQ." width="100%">
</p>

# ProtoBus for Rust

**RabbitMQ-native microservices for Rust, with Protocol Buffers on the wire.**

[![CI](https://github.com/ArielLaub/protobus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/ArielLaub/protobus-rs/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/Rust-1.89%2B-DEA584?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-%E2%89%A53.8-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com)

Define a service in a `.proto` file, implement the trait protobus generates for
it, and call it from anywhere on the bus as if it were local. ProtoBus turns
each service into **one durable RabbitMQ queue with N processes competing for
it**, so load balancing, failover, backpressure, retries and dead-lettering are
handled by the broker.

This is the Rust port of [protobus](https://github.com/ArielLaub/protobus)
(TypeScript). It follows the TypeScript reference's design, and the C++ port's
where a multi-threaded runtime has to decide something an event loop never
faced. The ports are **wire-compatible**: a Rust service serves callers written
for any of them and the other way round, with streaming, events, custom types
and error codes included. They are listed under
[Wire compatibility](#wire-compatibility).

**Status: new.** 2.0.0 is the first release of the Rust port. It is at feature
parity with the TypeScript port's runtime, and its
[cross-language suite](crosslang/README.md) runs it against the other ports'
real libraries, in both directions, on RabbitMQ.

---

## Install

You need Rust 1.89 or later and a [tokio](https://tokio.rs) runtime. Nothing
else: the schemas are compiled in pure Rust, so there is no `protoc` to
install.

```toml
[dependencies]
protobus = "2"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }

[build-dependencies]
protobus-build = "2"
```

`protobus` is the runtime. `protobus-build` generates the code at build time
for the schemas you point it at. For the CLI and custom types, see
[Code generation](docs/codegen.md).

You also need a RabbitMQ 3.8+ broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

---

## Quick start

Four steps to a working RPC. The code is the
[`examples`](examples/src/bin/calculator.rs) calculator, trimmed.

### 1. Describe the service

```protobuf
// proto/Calculator.proto
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
}

message AddResponse {
  int32 result = 1;
}

message DivideRequest {
  double dividend = 1;
  double divisor = 2;
}

message DivideResponse {
  double quotient = 1;
}
```

The package plus the service name is the service's name on the bus:
`Calculator.Service`. The file is an ordinary protobus schema, shared as-is
with the other languages. It needs no Rust options.

### 2. Generate the Rust code

`build.rs` compiles every `.proto` in the directory, in pure Rust:

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    protobus_build::configure().proto_dir("proto").compile()?;
    Ok(())
}
```

and the crate includes the result once:

```rust,ignore
pub mod proto {
    protobus::include_protos!();
}
```

Each proto package becomes a module (`package Calculator` becomes
`proto::calculator`), holding prost's message structs plus:

- `ServiceHandler`, the trait your implementation satisfies, with an async
  method per rpc;
- `ServiceService`, which serves an implementation on the bus;
- `ServiceProxy`, a typed client.

### 3. Implement and run the service

<!-- doc-check: compile -->
```rust
use protobus::{CallContext, Context, Error, MessageServiceOptions, Result, RunnableService};
use crate::proto::calculator::{AddRequest, AddResponse, DivideRequest, DivideResponse, ServiceHandler, ServiceService};

pub struct Calculator;

impl ServiceHandler for Calculator {
    async fn add(&self, request: AddRequest, _: CallContext) -> Result<AddResponse> {
        Ok(AddResponse { result: request.a + request.b })
    }

    async fn divide(&self, request: DivideRequest, _: CallContext) -> Result<DivideResponse> {
        if request.divisor == 0.0 {
            // A handled error is an answer, not a failure: never retried.
            return Err(Error::handled("cannot divide by zero").with_code("DIVISION_BY_ZERO"));
        }
        Ok(DivideResponse { quotient: request.dividend / request.divisor })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::default();
    context.init(&std::env::var("AMQP_URL").expect("AMQP_URL"), &[] as &[&str]).await?;
    let options = MessageServiceOptions { max_concurrent: Some(8), ..Default::default() };
    let service = ServiceService::with_options(&context, ServiceService::NAME, Calculator, options)?;
    RunnableService::start(&service).await?;
    // Serves until SIGINT/SIGTERM, then drains in-flight work and closes.
    std::process::exit(RunnableService::wait_for_shutdown().await)
}
```

Every handler receives a `CallContext` with the caller's actor, the message id
to deduplicate on, and a cancel token that fires when the processing timeout
expires. An rpc you do not implement answers `PROTOCOL_ERROR`.

### 4. Call it

<!-- doc-check: compile -->
```rust
use protobus::{CallOptions, Context, Result};
use crate::proto::calculator::{AddRequest, ServiceProxy};

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::default();
    context.init(&std::env::var("AMQP_URL").expect("AMQP_URL"), &[] as &[&str]).await?;
    let calculator = ServiceProxy::new(&context)?;
    let sum = calculator.add_with(AddRequest { a: 5, b: 3 }, CallOptions::new().timeout_ms(10_000)).await?;
    println!("5 + 3 = {}", sum.result);
    context.close().await;
    Ok(())
}
```

```
$ cargo run --bin calculator &
$ cargo run --bin client
5 + 3 = 8
```

Without `timeout_ms` the call is bounded by `RPC_CALL_TIMEOUT_MS` (10 minutes
by default, as in the other ports).

The full walkthrough adds events, error handling and a unit test:
**[Getting Started](docs/getting-started.md)**.

---

## Why ProtoBus

### RabbitMQ only, on purpose

ProtoBus is built for one broker, so the things a broker is good at stay in the
broker instead of being reimplemented above it:

| Concern | Where it lives |
|---|---|
| Load balancing | competing consumers on one queue |
| Routing | topic exchange bindings (`REQUEST.<Service>.*`) |
| Redelivery on consumer loss | late ack: an unacked delivery returns to the queue |
| Retry delay | the retry queue's `x-message-ttl`, drained by a dead-letter exchange |
| Persistence | durable queues, persistent messages |
| Dead letters | a real `<Service>.DLQ` |
| Priority | native queue priorities |

A request goes publisher → exchange → queue → consumer. Nothing tracks live
instances, so nothing holds a stale one, and a consumer that dies mid-request
leaves its delivery unacked for the next consumer to take.

If you may need to swap RabbitMQ for another broker, use a transport-agnostic
framework instead. That is a real feature and protobus does not have it.

### Protocol Buffers, not JSON

- **Contract-first.** The `.proto` file is the interface between teams and
  languages, and the generated Rust types fail the build when the two drift
  apart.
- **Versioning by field number.** Adding a field does not break an old peer.

### A small protocol, a small port

The protocol protobus adds on top of AMQP is small and documented: five
envelope messages, a routing-key convention, an error encoding and two
streaming headers. Because queueing, consumer distribution and retry delays
belong to the broker, a port adapts that protocol to another AMQP client
rather than reimplementing messaging. This port's runtime is
[lapin](https://github.com/amqp-rs/lapin) for AMQP,
[prost](https://github.com/tokio-rs/prost) for the wire and
[tokio](https://tokio.rs) for the runtime; its schemas are compiled by
[protox](https://github.com/andrewhickman/protox), in pure Rust. It brings no
framework: a handler is a trait implementation, and the generated code
registers each rpc's handler directly.

---

## Features

- **Retries and dead-lettering.** An unhandled error, a panic or a processing
  timeout parks the request on `<Service>.Retry` and redelivers it; after
  `max_retries` (3 by default, 5 seconds apart) it lands on `<Service>.DLQ`,
  and the caller is answered with the final error. A handled error
  (`Error::handled`) is an answer and is never retried. See
  [Errors](docs/errors.md).
- **Server streaming.** A method declared `returns (stream T)` is implemented
  by writing chunks to a `StreamWriter<T>`, and consumed as a `Stream<T>`, one
  `next().await` at a time. Dropping the stream, cancelling it or letting it go
  idle stops the producer on the server. See [Streaming](docs/streaming.md).
- **Events.** `publish_event` publishes on a topic exchange; `subscribe_event`
  receives events by message type, topic pattern (`*`, `#`) or both, typed or
  dynamic, with optional per-listener retries and a dead-letter queue. See
  [Events](docs/events.md).
- **Custom types.** `bigint` (unsigned 256-bit) and `timestamp` (milliseconds
  since the epoch) are built in and need no import in the schema; in Rust they
  are `protobus::Bigint` and `protobus::Timestamp`. Declare your own in
  `build.rs` with `custom_type`. See [Code generation](docs/codegen.md).
- **Priority.** `max_priority` makes a service queue a priority queue, and
  `CallOptions::priority` lets a control message overtake a bulk backlog.
- **Instance-named services.** Many instances can serve one contract under
  their own names (`Calculator.Service.east`), each addressed by its own proxy.
- **Processing timeout.** `processing_timeout_ms` caps one attempt at a unary
  request; the handler's cancel token fires and the attempt counts as failed.
- **Reconnection.** A lost connection is re-established with capped
  exponential backoff, and every queue, binding and consumer is restored
  before the connection carries traffic again. Calls made meanwhile wait for
  it; calls in flight when it dropped fail with `ErrorKind::Disconnected`.
- **Publisher confirms.** Every publish waits for the broker's confirm. A
  failure says whether the outcome is ambiguous, and `CallOptions::message_id`
  makes a republish safe to deduplicate.
- **Parallel by default, serialized on request.** Handlers run on tokio's
  workers, as many at once as `max_concurrent` allows; `serialize_handlers`
  runs one service's handlers one at a time, for work that must not
  interleave. See [Threading](docs/threading.md).
- **Dynamic API.** `MessageFactory` loads `.proto` files at runtime;
  `ServiceProxy::call`, `ServiceProxy::call_stream` and
  `MessageService::register_method` serve and call services over
  `DynamicMessage`s, with no generated code.
- **An in-memory broker.** `testing::MemoryBroker` runs services and clients
  in-process, with retries, dead-lettering, priorities and connection loss
  modelled. See [Testing](docs/testing.md).
- **Graceful shutdown.** `RunnableService` stops intake on SIGINT/SIGTERM, lets
  in-flight work finish within `SHUTDOWN_DRAIN_TIMEOUT_MS`, runs your cleanup
  (never beneath a handler still running), then closes.

### A short tour

<!-- doc-check: compile -->
```rust
use protobus::config::{PRIORITY_HIGH, RECOMMENDED_MAX_PRIORITY};
use protobus::{CallOptions, Context, MessageServiceOptions, Result, RetryOptions, Timestamp};
use crate::proto::calculator::{AddRequest, Calculated, ServiceProxy, ServiceService};

pub async fn tour(context: &Context) -> Result<()> {
    // Options on a service: concurrency, retries, a processing timeout and a
    // priority queue.
    let options = MessageServiceOptions {
        max_concurrent: Some(16),
        retry: RetryOptions { max_retries: 5, retry_delay_ms: 10_000, ..Default::default() },
        processing_timeout_ms: Some(30_000),
        max_priority: Some(RECOMMENDED_MAX_PRIORITY.into()),
        ..Default::default()
    };
    let service = ServiceService::with_options(context, ServiceService::NAME, Calculator, options)?;
    service.init().await?;

    // Options on a call.
    let calculator = ServiceProxy::new(context)?;
    let call = CallOptions::new().priority(PRIORITY_HIGH.into()).timeout_ms(5000).actor("billing-job");
    calculator.add_with(AddRequest { a: 1, b: 2 }, call).await?;

    // Events: subscribe by type, publish from anywhere on the bus.
    service
        .subscribe_event(|event: Calculated, _| async move {
            println!("{}", event.operation);
            Ok(())
        }, None)
        .await?;
    let event = Calculated { operation: "add".into(), at: Some(Timestamp::now()) };
    context.publish_event(&event, None).await
}
```

<!-- doc-check: compile -->
```rust
use protobus::{CallContext, Result, StreamWriter};
use crate::proto::chat::{AssistantHandler, AssistantProxy, GenerateRequest, Token};

// Streaming, server side: each send is one chunk.
pub struct Assistant;

impl AssistantHandler for Assistant {
    async fn generate(&self, _: GenerateRequest, call: CallContext, out: StreamWriter<Token>) -> Result<()> {
        for (index, word) in ["hello", "from", "rust"].into_iter().enumerate() {
            if call.cancel.is_cancelled() {
                return Ok(()); // the caller has gone: stop producing
            }
            out.send(Token { index: index as i32, text: word.into() }).await?;
        }
        Ok(())
    }
}

// Client side: read the stream one chunk at a time.
pub async fn read(assistant: &AssistantProxy) -> Result<()> {
    let mut tokens = assistant.generate(GenerateRequest { prompt: "hi".into(), ..Default::default() })?;
    while let Some(token) = tokens.next().await {
        let token = token?;
        print!("{} ", token.text);
        if token.index == 1 {
            break; // dropping the stream tells the server to stop
        }
    }
    Ok(())
}
```

---

## Concurrency

Each delivery is handled on a tokio task of its own, on the runtime's workers,
so handlers run in parallel and must be `Send + Sync`: the compiler holds the
state they share to that, and there is no data race to find at run time. The
number in flight is bounded by the consumer's prefetch: `max_concurrent` for a
service (default 1, one request at a time), `DEFAULT_PREFETCH` for event
handling (also 1). A streaming handler holds its slot for the life of its
stream. Replies are routed on the reply consumer's own task, never behind a
handler, so a handler that calls another service and awaits its reply cannot
starve the reply's delivery. Do not block a worker: long computation and
blocking calls belong in `tokio::task::spawn_blocking`.

A `Context`, its proxies and its services are handles, cheap to clone and safe
to use from any number of tasks at once: create one context per process and
share it. See [Threading](docs/threading.md) and
[Architecture](docs/architecture.md).

---

## Wire compatibility

protobus-rs speaks the protobus wire protocol exactly as TypeScript protobus
2.5 and the other ports do: the same exchanges, queues, envelopes (byte for
byte), headers and error codes, and the same environment variables for
configuration. Replicas of one service in different languages can share its
queue and climb one retry ladder together. The
[cross-language suite](crosslang/README.md) runs Rust against the other
ports' real libraries over a real broker, in both directions.

| Language | Repo | Status |
|---|---|---|
| TypeScript | [protobus](https://github.com/ArielLaub/protobus) | stable (reference) |
| Python | [protobus-py](https://github.com/ArielLaub/protobus-py) | stable |
| Go | [protobus-go](https://github.com/ArielLaub/protobus-go) | stable |
| C++ | [protobus-cpp](https://github.com/ArielLaub/protobus-cpp) | new |
| Java | [protobus-java](https://github.com/ArielLaub/protobus-java) | new |
| C# | [protobus-csharp](https://github.com/ArielLaub/protobus-csharp) | new |
| Rust | [protobus-rs](https://github.com/ArielLaub/protobus-rs) (this repository) | new |

The type mapping, the topology and the few deliberate behavioural differences
are in **[Compatibility](docs/compatibility.md)**.

---

## Documentation

Full index: **[docs/](docs/README.md)**

| Start | |
|---|---|
| [Getting Started](docs/getting-started.md) | From an empty directory to a service, a client, events and a test |
| [Services](docs/services.md) | Implementing services: options, retries, concurrency, lifecycle |
| [Clients](docs/clients.md) | Calling services: options, timeouts, errors, the untyped proxy |
| [Streaming](docs/streaming.md) | Server streaming and cancellation |
| [Events](docs/events.md) | Publishing and subscribing, topic patterns, event retry |
| [Errors](docs/errors.md) | The error model, retries and dead letters |

| Reference | |
|---|---|
| [Threading](docs/threading.md) | What runs where, and what that asks of your code |
| [Configuration](docs/configuration.md) | Environment variables, reconnection, logging |
| [Code generation](docs/codegen.md) | `build.rs`, the CLI, custom types |
| [Testing](docs/testing.md) | The in-memory broker and the suites |
| [Compatibility](docs/compatibility.md) | The wire contract and how the ports differ |
| [Architecture](docs/architecture.md) | Tasks, ownership and reconnection |
| [Security](docs/security.md) | Dispatch checks, error exposure, logging, TLS |

---

## Examples

| Example | Shows |
|---|---|
| [`calculator`](examples/src/bin/calculator.rs) | A service, a client, a handled error and an event |
| [`tokenstream`](examples/src/bin/tokenstream.rs) | Streaming tokens, cancelled three ways, with the server's own count |

```bash
docker compose up -d --wait   # RabbitMQ on 127.0.0.1:29672
export AMQP_URL=amqp://guest:guest@127.0.0.1:29672/
cargo run -p protobus-examples --bin calculator
cargo run -p protobus-examples --bin tokenstream
```

---

## License

MIT. See [LICENSE](LICENSE).
