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
faced. It is wire-compatible with every other port: a Rust service serves
callers in any of them and the other way round, with streaming, events, custom
types and error codes included. The ports are listed under
[Other languages](docs/README.md#other-languages); what they share and where
they differ is in [Compatibility](docs/compatibility.md).

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

You also need a RabbitMQ 3.8+ broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## A service and a client

The contract, `proto/Calculator.proto`, is shared verbatim with every other
language:

```protobuf
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
}

message AddRequest  { int32 a = 1; int32 b = 2; }
message AddResponse { int32 result = 1; }
```

`build.rs` generates the Rust code for it:

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    protobus_build::configure().proto_dir("proto").compile()?;
    Ok(())
}
```

The service implements the generated trait, one async method per rpc:

<!-- doc-check: compile -->
```rust
use protobus::{CallContext, Context, Result, RunnableService};
use crate::proto::calculator::{AddRequest, AddResponse, ServiceHandler, ServiceService};

struct Calculator;

impl ServiceHandler for Calculator {
    async fn add(&self, request: AddRequest, _: CallContext) -> Result<AddResponse> {
        Ok(AddResponse { result: request.a + request.b })
    }
}

pub async fn serve(url: &str) -> Result<i32> {
    let context = Context::default();
    context.init(url, &[] as &[&str]).await?;
    let service = ServiceService::new(&context, Calculator)?;
    RunnableService::start(&service).await?;
    // Serves until SIGINT or SIGTERM, then drains what is in hand.
    Ok(RunnableService::wait_for_shutdown().await)
}
```

where `crate::proto` is the generated code, included once in the crate:

```rust,ignore
pub mod proto {
    protobus::include_protos!();
}
```

Any process on the bus calls it through the generated proxy:

<!-- doc-check: compile -->
```rust
use protobus::{Context, Result};
use crate::proto::calculator::{AddRequest, ServiceProxy};

pub async fn call(context: &Context) -> Result<()> {
    let calculator = ServiceProxy::new(context)?;
    let reply = calculator.add(AddRequest { a: 20, b: 22 }).await?;
    println!("20 + 22 = {}", reply.result);
    Ok(())
}
```

Run more copies of the service and RabbitMQ balances requests between them;
stop one mid-request and its request is redelivered to another.

The step-by-step version, with events, streaming and a test, is
[Getting Started](docs/getting-started.md).

## What you get

- **Unary RPC** with deadlines, a caller identity and priorities.
- **Server streaming** with ordering, backpressure bounds and cancellation that
  reaches the producer. See [Streaming](docs/streaming.md).
- **Events** on topic patterns, with an opt-in retry ladder. See
  [Events](docs/events.md).
- **Retries and dead-lettering**: a handler that fails is retried through a
  retry queue and dead-lettered once the retries are spent; an expected failure
  (`Error::handled`) is answered at once. See [Errors](docs/errors.md).
- **Reconnection**: a lost connection is re-established with every queue,
  binding and consumer restored before it carries traffic again.
- **Confirmed publishing**: every publish waits for the broker's confirm, and
  the two ambiguous outcomes are named as such.
- **An in-memory broker** for tests, with fault injection. See
  [Testing](docs/testing.md).
- **Built for a multi-threaded runtime**: everything shared is `Send + Sync`,
  handlers run in parallel on tokio's workers, and no lock is held across an
  await. See [Threading](docs/threading.md).

## Examples

[`examples/`](examples) holds runnable programs: a calculator (a service, a
client and an event) and a token stream (server streaming cancelled three
ways).

```bash
docker compose up -d --wait
export AMQP_URL=amqp://guest:guest@127.0.0.1:29672/
cargo run -p protobus-examples --bin calculator
cargo run -p protobus-examples --bin tokenstream
```

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | From an empty directory to a service, a client, events and a test |
| [Services](docs/services.md) | Implementing services: options, retries, concurrency, lifecycle |
| [Clients](docs/clients.md) | Calling services: options, timeouts, errors, the untyped proxy |
| [Streaming](docs/streaming.md) | Server streaming and cancellation |
| [Events](docs/events.md) | Publishing and subscribing, topic patterns, event retry |
| [Errors](docs/errors.md) | The error model, retries and dead letters |
| [Threading](docs/threading.md) | What runs where, and what that asks of your code |
| [Configuration](docs/configuration.md) | Environment variables, reconnection, logging |
| [Code generation](docs/codegen.md) | `build.rs`, the CLI, custom types |
| [Testing](docs/testing.md) | The in-memory broker and the suites |
| [Compatibility](docs/compatibility.md) | The wire contract and how the ports differ |
| [Architecture](docs/architecture.md) | Tasks, ownership and reconnection |
| [Security](docs/security.md) | Dispatch checks, error exposure, logging, TLS |

## License

MIT. See [LICENSE](LICENSE).
