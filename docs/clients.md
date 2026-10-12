# Clients

A generated `<Service>Proxy` has a method per rpc. It is cheap to clone and
safe to call from any number of tasks at once: clone it into every task that
needs it.

<!-- doc-check: compile -->
```rust
use protobus::{CallOptions, Context, ErrorKind, Result};
use crate::proto::calculator::*;

pub async fn calls(context: &Context) -> Result<()> {
    let calculator = ServiceProxy::new(context)?;

    // Defaults: wait up to RPC_CALL_TIMEOUT_MS for the reply.
    let sum = calculator.add(AddRequest { a: 1, b: 2 }).await?;

    // Every rpc has a `_with` form taking options.
    let options = CallOptions::new().timeout_ms(1500).actor("billing").priority(1).message_id("add-7");
    let again = calculator.add_with(AddRequest { a: sum.result, b: 1 }, options).await?;

    // One-way: completes when the broker has confirmed the request. No reply is awaited.
    calculator.add_with(AddRequest { a: again.result, b: 1 }, CallOptions::new().one_way()).await?;

    match calculator.divide(DivideRequest { dividend: 1.0, divisor: 0.0 }).await {
        Ok(reply) => println!("{}", reply.quotient),
        Err(e) => match e.kind() {
            // The service answered with an error: its message and code.
            ErrorKind::Remote => println!("refused: {} [{}]", e.message(), e.code().unwrap_or("")),
            // No reply within the deadline. The request may still be processed.
            ErrorKind::RpcTimeout => println!("no answer"),
            // No service is bound for this method: nothing was delivered.
            ErrorKind::Unroutable => println!("nobody serves it"),
            // The connection dropped with the call pending. It may have been processed.
            ErrorKind::Disconnected => println!("connection lost"),
            _ => return Err(e),
        },
    }
    Ok(())
}
```

## Options

| `CallOptions` | Default | |
|---|---|---|
| `timeout_ms` | `RPC_CALL_TIMEOUT_MS` | how long the call may take. The deadline starts once the connection is ready and bounds the request's confirm as well as the reply |
| `actor` | none | free-text caller identity, for the handler's `CallContext::actor`. Not authenticated |
| `priority` | none | 0-255. Only a queue declared with a max priority honours it |
| `message_id` | a UUID | the message's identity as the handler sees it. Set it to make a retry of yours recognisable: derive it from the work, never from a clock. Refused when blank or over 255 bytes |
| `rpc` (`one_way()`) | true | `false` publishes without waiting for a reply |

## What a call does

1. Waits for the connection to be ready, through a reconnection if one is
   under way (up to `CONNECTION_READY_TIMEOUT_MS`).
2. Publishes the request, persistent and `mandatory`, and waits for the
   broker's confirm.
3. Waits for the reply on the context's reply queue.

A request that routes nowhere fails as `Unroutable` at once rather than after
the timeout. When the publish fails, its error wins over the deadline: "the
request never left" is the more specific answer.

Dropping the future of a call stops waiting for it. The request may already
have been delivered.

## Delivery failures

| `ErrorKind` | Code | Was it sent? |
|---|---|---|
| `Unroutable` | `UNROUTABLE` | returned by the broker: no queue took it |
| `PublishNacked` | `PUBLISH_NACKED` | refused by the broker: not stored |
| `PublishConfirmTimeout` | `PUBLISH_CONFIRM_TIMEOUT` | **unknown**: no confirm in time (or, when the message says so, never sent for want of a confirm slot) |
| `ChannelClosed` | `CHANNEL_CLOSED` | **unknown**: the channel closed before the confirm |
| `NotReady` | `NOT_READY` | no: the connection did not come back in time |
| `NotConnected` | | no: the context is not connected and is not reconnecting |

`Error::is_ambiguous()` is true for the two unknown outcomes. Republish with
the same `message_id`, and have the handler deduplicate on it.

## The untyped proxy

`protobus::ServiceProxy` calls a service by name, for a contract loaded at
runtime (`context.init(url, &["proto"])`) or over payloads you serialized
yourself. Generated proxies expose theirs as `raw()`.

<!-- doc-check: compile -->
```rust
use protobus::prost_reflect::Value;
use protobus::{Context, Result, ServiceProxy};

pub async fn untyped(context: &Context) -> Result<i32> {
    let calculator = ServiceProxy::new(context, "Calculator.Service");
    calculator.init()?;

    let mut request = context.factory().new_message("Calculator.AddRequest")?;
    request.set_field_by_name("a", Value::I32(20));
    request.set_field_by_name("b", Value::I32(22));
    let reply = calculator.call("add", &request, &Default::default()).await?;
    Ok(reply.get_field_by_name("result").and_then(|v| v.as_i32()).unwrap_or(0))
}
```

`call_raw` and `call_stream_raw` take and return serialized payloads;
`call_typed` and `call_stream_typed` are what generated proxies use.
