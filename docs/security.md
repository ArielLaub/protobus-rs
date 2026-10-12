# Security

What protobus-rs checks, what it leaves to the broker and to you.

## The trust model

protobus is a bus between your own services. The broker is the authority on
who may connect, and on who may publish and consume what; protobus adds no
authentication of its own. `CallContext::actor` is free text a caller wrote,
for tracing: never an identity to authorise on.

Use RabbitMQ's users, vhosts and permissions to separate what must be
separate, and `amqps://` URLs for TLS (through lapin, with rustls).

## What a service checks before it runs anything

A request is checked against the service's contract before its payload is
read:

1. The routing key the broker delivered on belongs to this service.
2. The method the envelope names is the method the routing key names, so a
   client permitted to publish to one method cannot have another executed.
3. The envelope names a method of this service's contract, spelled in full,
   and the contract declares it.
4. Only then is the payload decoded, as that method's request type, and its
   custom-type values checked.

A request that fails any of these is answered `PROTOCOL_ERROR` and never
retried: the same bytes fail the same way on every redelivery.

Event handlers are matched on the routing key the broker delivered on, not on
the topic written in the event's body, which the publisher controls.

## What crosses back to a caller

A handled error crosses with its message and code: it is meant to be seen. An
unhandled error's message crosses too by default, on the assumption that the
caller is another of your services. For a service whose callers relay errors
to untrusted clients, set `PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`: callers then
get `INTERNAL_ERROR` and a correlation id.

The `x-last-error` header on retry and dead-letter copies never carries an
unhandled error's message, whatever that setting: only its type name and code.

## What is logged

Never a payload: log lines carry type names, sizes and correlation ids. Broker
URLs are logged with the password removed (`config::redact_url`).

## Bounds

- A decoded `bigint` is at most 32 bytes and a `timestamp` within ±8.64e15 ms,
  wherever it is nested; nesting deeper than 100 messages is refused.
- A caller buffers at most `STREAM_MAX_BUFFERED_CHUNKS` chunks and
  `STREAM_MAX_BUFFERED_BYTES` bytes per stream, and
  `STREAM_MAX_TOTAL_BUFFERED_BYTES` across its streams.
- A channel carries at most `MAX_OUTSTANDING_CONFIRMS` unanswered publishes.
- A late-ack consumer always has a prefetch, so a backlog stays in the broker
  rather than in the process's memory.
- A `messageId` is at most 255 bytes, and never blank.

## Dependencies

The runtime depends on tokio, lapin, prost and prost-reflect, and on protox
when `.proto` text is loaded at run time (the `proto-text` feature, on by
default). The crate contains no `unsafe` code.
