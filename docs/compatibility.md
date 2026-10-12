# Compatibility with the other ports

protobus-rs speaks the protobus wire protocol exactly as the TypeScript
reference (2.5) and every other port (2.0) do; they are listed under
[Other languages](README.md#other-languages). Services and clients in all of
them can share one broker, one schema and even one queue: replicas of a
service in different languages compete for its requests and climb one retry
ladder together.

This is tested: the [cross-language suite](../crosslang/README.md) runs Rust
against the other ports' real libraries over a real broker, in both
directions, and runs `interop.Flaky` in every language at once on one queue.

## The contract is the `.proto`

A schema is shared verbatim. It needs no Rust options and no import for the
built-in custom types: protobus adds it when it compiles the schema. A proto
package becomes a module, each segment snake_cased as prost does
(`package Shop.Orders` is `shop::orders`).

| Proto | TypeScript | Rust |
|---|---|---|
| `int64`, `uint64`, … | decimal `string` | `i64`, `u64` |
| `bigint` (custom) | `bigint` | `protobus::Bigint` (`to_u256()` gives a `U256`) |
| `timestamp` (custom) | `Date` | `protobus::Timestamp` (`to_system_time()`, `to_millis()`) |
| `bytes` | `Buffer` | `Vec<u8>` |
| enum | value name (`string`) | `i32`, with a generated enum to convert through |
| message field | object or `undefined` | `Option<T>` |
| unset scalar | its default | its default |

The wire bytes are identical in every case; only the in-language
representation differs. Each port's own compatibility page gives its column.

`bigint` is an unsigned integer up to 2^256-1 carried as exactly 32 big-endian
bytes; `timestamp` is signed milliseconds since the epoch. Both are one-field
messages (`message bigint { optional bytes value = 1; }`) declared at the root
of the type namespace. Every port refuses a negative or oversized `bigint` and
refuses to decode one wider than 32 bytes.

## Topology

Identical names, flags and arguments; the exchange names come from the same
environment variables.

| | Name | Type / flags |
|---|---|---|
| RPC exchange | `proto.bus` (`BUS_EXCHANGE_NAME`) | topic, durable |
| Reply exchange | `proto.bus.callback` (`CALLBACKS_EXCHANGE_NAME`) | direct, durable |
| Event exchange | `proto.bus.events` (`EVENTS_EXCHANGE_NAME`) | topic, durable |
| Stream-cancel exchange | `proto.bus.cancel` (`CANCEL_EXCHANGE_NAME`) | fanout, durable |
| Service queue | `<Service>` bound `REQUEST.<Service>.*` | durable; arguments only when configured (`x-message-ttl`, `x-max-priority`) |
| Retry | `<Service>.Retry` (TTL, DLX → `proto.bus`), `<Service>.Retry.Exchange` (topic, `#`), `<Service>.DLQ` | durable |
| Event queue | `<Service>.Events` | durable |
| Event retry (opt-in) | `<Service>.Events.Retry`, `.Events.Retry.Exchange`, `.Events.Redelivery`, `.Events.DLQ` | durable |
| Reply queue | server-named, bound to `proto.bus.callback` under its own name | exclusive, auto-delete |
| Cancel queue | server-named, bound to `proto.bus.cancel` | exclusive, auto-delete |

RabbitMQ compares integer queue arguments by value, so the different integer
widths the AMQP clients pick for the same TTL are equivalent; the
mixed-replica test declares one service's queues from every language.

## Messages

- Requests, replies and events travel in the protobus envelopes
  (`RequestContainer`, `ResponseContainer`, `EventContainer`). protobus-rs
  encodes them byte for byte as TypeScript does (the golden vectors in
  `protobus/src/wire.rs`, shared with the other ports), including the empty
  fields TypeScript writes explicitly.
- Every publish carries a `messageId` (a UUID unless the caller sets one),
  `contentType: application/octet-stream` and a `correlationId`; requests and
  events are persistent. Unary requests are published `mandatory`.
- Streaming replies carry `x-protobus-seq` (from 0) and `x-protobus-final`;
  the last frame is final, an empty stream is one empty final frame, and a
  failure is the final frame. Cancellation is an empty message on
  `proto.bus.cancel` carrying the stream's correlation id.
- Retry and dead-letter copies carry `x-retry-count`,
  `x-original-routing-key` (the delivered routing key; nothing routes by the
  header), `x-first-failure-time`, `x-last-error` (the error's name and code,
  never an unhandled error's message), and on the DLQ `x-original-queue` and
  `x-dlq-time`; they keep `contentType`, `contentEncoding`, `priority`,
  `timestamp`, `type` and `appId`, and drop `expiration` and `userId`.
- Readers accept every encoding peers produce: integer headers of any width or
  as text, `x-protobus-final` as a boolean, number or text. A header read from
  a delivery is written back with the encoding it came with.

## Errors

A service error crosses as `ResponseError{method, message, code}`. In Rust a
remote error is a `protobus::Error` of kind `Remote`; the codes are shared:
`HANDLED_ERROR` (the default for `Error::handled`), `PROTOCOL_ERROR` (a request
the service will not run), `INTERNAL_ERROR` (an unhandled error when
`PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`), `PROCESSING_TIMEOUT`, and any code a
service chooses. `x-last-error` names an error as the other ports name the
same failure (`HandledError`, `TimeoutError`), and an unhandled one by the
Rust type `Error::unhandled` wrapped (`ParseIntError`).

## Where the ports differ

These are deliberate, and none changes what is on the wire. Rust follows the
TypeScript reference except where the later ports' experience, or Rust itself,
argued otherwise; where a multi-threaded port had to decide something
TypeScript's event loop never faced, it follows protobus-cpp.

| | Rust | TypeScript 2.5 | C++ 2.0 |
|---|---|---|---|
| Who declares the core exchanges | every process, for what it publishes to | services only | every process, for what it publishes to |
| Processing timeout, after the last retry | caller answered: `PROCESSING_TIMEOUT` | caller waits for its own timeout | caller answered |
| Settlement publish fails | message requeued after 1 s | left unacknowledged | requeued after 1 s |
| Retry/DLQ copies | `mandatory` | not mandatory | `mandatory` |
| Channel lost on a live connection | listener rebuilt; dispatcher reopens on next publish | not recovered | listener rebuilt; dispatcher reopens on next publish |
| Reply queue replaced on a live connection | calls pending on the old one fail as `Disconnected` | not recovered | |
| A downstream remote error escaping a handler | retried, like any unhandled error | retried | retried |
| A handled error from an event handler with event retry on | dropped | dropped | dropped |
| Streaming handler | writes to a `StreamWriter`; an error at any point ends the stream with an error chunk | async generator; an eager throw before the iterable exists is answered like a unary error | coroutine generator |
| Streaming call | request published at call time; failures surface from the first `next()` | starts at call time | published at call time |
| A stream opened while reconnecting | waits for the connection, then publishes | | |
| Processing timeout on streams | not applied: the handler hands its stream over at once | covers obtaining the iterator | covers obtaining the generator |
| Explicit priority 0 | sent | sent | sent |
| Handlers run | on tokio tasks, in parallel up to the prefetch (or one at a time with `serialize_handlers`) | on the event loop | on worker threads, in parallel (or one at a time with `serializeHandlers`) |
| Early-ack (at-most-once) consumers | at most `max_concurrent` handlers at once | unbounded on the event loop | |
| A handler that panics | contained, and retried like an unhandled error | | |
| Closing the context | fails pending calls and streams at once, then waits for running handlers, up to `SHUTDOWN_DRAIN_TIMEOUT_MS` | closes at once | fails pending, then drains |
| Matching a returned publish to its confirm | done by the AMQP client; no `x-protobus-publish-tag` is written | by `messageId` | by `messageId`, with `x-protobus-publish-tag` when two share one |
| Service classes | a generated handler trait and a `MessageService` built around it; no `ProxiedService` (a handler holds its own proxy) | `MessageService`, `RunnableService`, `ProxiedService` base classes | the same base classes |
| Untyped API | `prost_reflect::DynamicMessage` | plain objects | dynamic messages |
| Schemas loaded at run time | `.proto` text, or compiled descriptor sets | `.proto` text | `.proto` text |
| Code generation | `build.rs` or the `protobus-rs` CLI; no `protoc` needed; no protoc plugin, no service scaffolding | CLI | CMake function, CLI, protoc plugin |
| Logging | the `log` crate, target `protobus` | `ILogger` / structured `Log` | `ILogger` / structured `Log` |

Blank cells are behaviours the other port's documentation does not state.
