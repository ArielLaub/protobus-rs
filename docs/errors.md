# Errors

Every failure protobus reports is a `protobus::Error`: a `kind()`, a
`message()` and, where one applies, a `code()`, the stable identifier that
crosses the wire and is shared with every other port.

## In a handler

What a handler returns decides what happens to the request.

| The handler returns | The request | The caller gets |
|---|---|---|
| `Ok(reply)` | acknowledged | the reply |
| `Err(Error::handled(..))` | acknowledged, never retried | a `Remote` error with the message and the code (`HANDLED_ERROR` unless `with_code`) |
| any other error | retried, then dead-lettered | once the retries are spent, a `Remote` error |
| nothing, within `processing_timeout_ms` | retried, then dead-lettered | once the retries are spent, `PROCESSING_TIMEOUT` |

<!-- doc-check: compile -->
```rust
use protobus::{CallContext, Error, Result};
use crate::proto::calculator::*;

pub struct Calculator;

impl ServiceHandler for Calculator {
    async fn divide(&self, request: DivideRequest, _: CallContext) -> Result<DivideResponse> {
        if request.divisor == 0.0 {
            // An expected failure: an answer for the caller.
            return Err(Error::handled("cannot divide by zero").with_code("DIVISION_BY_ZERO"));
        }
        // An infrastructure failure: retried. `unhandled` keeps the source
        // error's type name for the x-last-error header.
        let rate: f64 = std::env::var("RATE").map_err(Error::unhandled)?.parse().map_err(Error::unhandled)?;
        Ok(DivideResponse { quotient: rate * request.dividend / request.divisor })
    }
}
```

`Error::failure("...")` is an unhandled error with a message of your own. A
panic in a handler is contained and treated as one. An error a handler got
from calling another service (a `Remote` error) is unhandled like any other:
it is retried unless you turn it into a handled one.

## The retry ladder

A failed request is republished to `<Service>.Retry`, waits there for
`retry_delay_ms`, and is dead-lettered by RabbitMQ back to the service's
queue. After `max_retries` hops it goes to `<Service>.DLQ` and the caller is
answered. Each copy carries:

| Header | |
|---|---|
| `x-retry-count` | hops so far |
| `x-original-routing-key` | the key it was delivered on |
| `x-first-failure-time` | milliseconds since the epoch |
| `x-last-error` | the error's name and code; its message only for a handled error |
| `x-original-queue`, `x-dlq-time` | on the DLQ |

The `messageId` is the same on every hop. The retry and dead-letter publishes
are confirmed before the original is acknowledged, so the only copy of a
message is never dropped on the strength of a publish that went nowhere; if
one fails, the message is requeued after a second.

## What crosses to the caller

A handled error always crosses, message and code. An unhandled error's message
crosses too by default: a protobus caller is another of your own services.
Set `PROTOBUS_EXPOSE_INTERNAL_ERRORS=false` for a service whose callers relay
errors to untrusted clients; they then get `INTERNAL_ERROR` and the
correlation id to look the real one up by.

## Codes

| Code | Raised when |
|---|---|
| `HANDLED_ERROR` | the default for `Error::handled` |
| `PROTOCOL_ERROR` | the service will not run the request: it did not decode, or names a method the service does not serve |
| `INTERNAL_ERROR` | an unhandled error, with exposure off |
| `PROCESSING_TIMEOUT` | the handler overran its processing timeout on every attempt |
| `RPC_TIMEOUT` | the caller's own deadline passed |
| `UNROUTABLE`, `PUBLISH_NACKED`, `PUBLISH_CONFIRM_TIMEOUT`, `CHANNEL_CLOSED` | the publish did not demonstrably reach a queue; see [Clients](clients.md#delivery-failures) |
| `NOT_READY` | the connection did not become ready in time |

Any other code is one a service chose.

## Kinds

`ErrorKind` names every failure, one per error class of the TypeScript
reference: `Handled`, `Protocol`, `InvalidMethod`, `Remote`, `RpcTimeout`,
`ProcessingTimeout`, the publish outcomes, `StreamTimeout`,
`StreamBackpressure`, `StreamSequence`, `Disconnected`, `NotConnected`,
`NotReady`, `Reconnection`, the validation errors (`InvalidPriority`,
`InvalidMessageId`, `InvalidRequest`, ...), the schema errors (`Schema`,
`MissingProto`, `InvalidServiceName`, `UnknownMethod`, `CustomTypeRange`), and
`Amqp` for a broker-reported failure, whose `reply_code()` is the AMQP code
(404, 406, ...). `Error::name()` is the class name the other ports give the
same failure (`HandledError`, `RpcTimeoutError`, ...): it is what appears in
`x-last-error`.

`Error` is one pointer wide, `Clone`, `Send + Sync`, and implements
`std::error::Error`, so it travels through `?`, `anyhow` and `Box<dyn Error>`.
