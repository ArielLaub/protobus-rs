# Getting started

From an empty directory to a service, a client, an event, a stream and a test.

## 1. A project

```bash
cargo new orders && cd orders
cargo add protobus@2 tokio --features tokio/rt-multi-thread,tokio/macros
cargo add --build protobus-build@2
mkdir proto
```

And a broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## 2. The contract

`proto/Calculator.proto`. It is an ordinary `.proto` file, and the same file
serves every other protobus port:

```protobuf
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest     { int32 a = 1; int32 b = 2; }
message AddResponse    { int32 result = 1; }
message DivideRequest  { double dividend = 1; double divisor = 2; }
message DivideResponse { double quotient = 1; }

// Published after every successful calculation.
message Calculated {
  string operation = 1;
  timestamp at = 2;
}
```

`timestamp` is one of protobus's [custom types](codegen.md#custom-types); it
needs no import.

## 3. Generate the code

`build.rs`:

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    protobus_build::configure().proto_dir("proto").compile()?;
    Ok(())
}
```

and, once, in `src/main.rs`:

```rust,ignore
pub mod proto {
    protobus::include_protos!();
}
```

The package `Calculator` becomes the module `proto::calculator`. For
`service Service` it holds a `ServiceHandler` trait, a `ServiceService` and a
`ServiceProxy`; see [Code generation](codegen.md).

## 4. The service

<!-- doc-check: compile -->
```rust
use protobus::{CallContext, Context, Error, Result, Timestamp};
use crate::proto::calculator::*;

pub struct Calculator {
    pub context: Context,
}

impl ServiceHandler for Calculator {
    async fn add(&self, request: AddRequest, _: CallContext) -> Result<AddResponse> {
        let event = Calculated { operation: "add".into(), at: Some(Timestamp::now()) };
        self.context.publish_event(&event, None).await?;
        Ok(AddResponse { result: request.a + request.b })
    }

    async fn divide(&self, request: DivideRequest, _: CallContext) -> Result<DivideResponse> {
        if request.divisor == 0.0 {
            // An answer, not a failure: the caller gets the message and the
            // code, and the request is never retried.
            return Err(Error::handled("cannot divide by zero").with_code("DIVISION_BY_ZERO"));
        }
        Ok(DivideResponse { quotient: request.dividend / request.divisor })
    }
}
```

Anything other than a handled error is an infrastructure failure: the request
is retried and, once the retries are spent, dead-lettered. See
[Errors](errors.md).

## 5. Run it

<!-- doc-check: compile -->
```rust
use protobus::{Context, Result, RunnableService};
use crate::proto::calculator::*;

pub async fn run() -> Result<()> {
    let context = Context::default();
    context.init("amqp://guest:guest@localhost:5672/", &[] as &[&str]).await?;

    let service = ServiceService::new(&context, Calculator { context: context.clone() })?;
    RunnableService::start(&service).await?;

    // Subscribe to the events this service cares about, after it has started.
    service
        .subscribe_event(
            |event: Calculated, _| async move {
                println!("calculated: {}", event.operation);
                Ok(())
            },
            None,
        )
        .await?;

    std::process::exit(RunnableService::wait_for_shutdown().await);
}
```

`RunnableService` shuts down gracefully on SIGINT or SIGTERM: the service
stops taking new requests, the ones in hand are answered, and the context
closes.

## 6. Call it

<!-- doc-check: compile -->
```rust
use protobus::{CallOptions, Context, ErrorKind, Result};
use crate::proto::calculator::*;

pub async fn call(context: &Context) -> Result<()> {
    let calculator = ServiceProxy::new(context)?;

    let sum = calculator.add(AddRequest { a: 20, b: 22 }).await?;
    println!("20 + 22 = {}", sum.result);

    // Options: a deadline, who is calling, and an identity for the message.
    let options = CallOptions::new().timeout_ms(2000).actor("user:42").message_id("div-1");
    match calculator.divide_with(DivideRequest { dividend: 1.0, divisor: 0.0 }, options).await {
        Ok(reply) => println!("{}", reply.quotient),
        // What the service answered with.
        Err(e) if e.kind() == ErrorKind::Remote => println!("refused: {} [{:?}]", e, e.code()),
        // Everything else: no reply in time, no service bound, a lost connection...
        Err(e) => return Err(e),
    }
    Ok(())
}
```

## 7. A stream

Add a server-streaming rpc to the contract:

```protobuf
package Chat;

service Assistant {
  rpc generate(GenerateRequest) returns (stream Token);
}

message GenerateRequest { string prompt = 1; int32 token_delay_ms = 2; }
message Token           { int32 index = 1; string text = 2; }
```

The handler writes chunks; the caller reads them:

<!-- doc-check: compile -->
```rust
use protobus::{CallContext, Context, Result, StreamWriter};
use crate::proto::chat::*;

pub struct Assistant;

impl AssistantHandler for Assistant {
    async fn generate(&self, request: GenerateRequest, _: CallContext, out: StreamWriter<Token>) -> Result<()> {
        for (index, word) in request.prompt.split_whitespace().enumerate() {
            // Fails once the caller has cancelled: `?` ends the stream quietly.
            out.send(Token { index: index as i32, text: word.to_string() }).await?;
        }
        Ok(())
    }
}

pub async fn read(context: &Context) -> Result<()> {
    let assistant = AssistantProxy::new(context)?;
    let mut tokens = assistant.generate(GenerateRequest { prompt: "one two three".into(), token_delay_ms: 0 })?;
    while let Some(token) = tokens.next().await {
        print!("{} ", token?.text);
    }
    Ok(())
}
```

Dropping `tokens` before the end cancels the call, and the producer is told to
stop. See [Streaming](streaming.md).

## 8. A test, without RabbitMQ

`protobus::testing::MemoryBroker` is an AMQP broker in memory:

<!-- doc-check: compile -->
```rust
use std::sync::Arc;
use protobus::testing::MemoryBroker;
use protobus::{Context, ContextOptions, ErrorKind};
use crate::proto::calculator::*;

pub async fn divides() {
    let broker = MemoryBroker::new();
    let context = Context::new(ContextOptions { transport: Some(Arc::new(broker.clone())), ..Default::default() });
    context.init("amqp://memory/", &[] as &[&str]).await.unwrap();
    let service = ServiceService::new(&context, Calculator { context: context.clone() }).unwrap();
    service.init().await.unwrap();

    let calculator = ServiceProxy::new(&context).unwrap();
    assert_eq!(calculator.divide(DivideRequest { dividend: 1.0, divisor: 4.0 }).await.unwrap().quotient, 0.25);
    let refused = calculator.divide(DivideRequest { dividend: 1.0, divisor: 0.0 }).await.unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::Remote);
    assert_eq!(refused.code(), Some("DIVISION_BY_ZERO"));

    service.close().await;
    context.close().await;
}
```

Put it under `#[tokio::test]`. The broker also injects faults (a lost
connection, a refused publish) and lets a test look inside queues; see
[Testing](testing.md).
