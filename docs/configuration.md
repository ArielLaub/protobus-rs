# Configuration

protobus-rs reads the same environment variables as every other port. Each is
read when it is used, so a value changed at run time is picked up. An integer
must be all digits and positive, or the default is kept; a boolean is one of
`1/true/yes/on` or `0/false/no/off`.

`protobus::config::set(name, value)` overrides a variable inside the process,
for tests and for programs that cannot set their environment; `unset` and
`reset` undo it.

| Variable | Default | |
|---|---|---|
| `BUS_EXCHANGE_NAME` | `proto.bus` | RPC requests (topic) |
| `CALLBACKS_EXCHANGE_NAME` | `proto.bus.callback` | RPC replies (direct) |
| `EVENTS_EXCHANGE_NAME` | `proto.bus.events` | events (topic) |
| `CANCEL_EXCHANGE_NAME` | `proto.bus.cancel` | stream cancellation (fanout) |
| `RPC_CALL_TIMEOUT_MS` | 600000 | how long a unary caller waits |
| `MESSAGE_PROCESSING_TIMEOUT` | 600000 | one attempt at a request, in ms |
| `STREAM_IDLE_TIMEOUT_MS` | 60000 | the longest gap between stream chunks |
| `STREAM_MAX_BUFFERED_CHUNKS` | 1024 | unread chunks per stream |
| `STREAM_MAX_BUFFERED_BYTES` | 67108864 | unread bytes per stream |
| `STREAM_MAX_TOTAL_BUFFERED_BYTES` | 268435456 | unread bytes across a context's streams |
| `DEFAULT_PREFETCH` | 1 | prefetch for late-ack consumers that set none |
| `PUBLISH_CONFIRM_TIMEOUT_MS` | 30000 | how long a publish waits for its confirm |
| `MAX_OUTSTANDING_CONFIRMS` | 256 | publishes the broker has not answered, per channel; further ones wait |
| `CONNECTION_READY_TIMEOUT_MS` | 30000 | how long a publish waits through a reconnection |
| `AMQP_HEARTBEAT_SECONDS` | 30 | heartbeat; a `heartbeat` in the broker URL wins |
| `SHUTDOWN_DRAIN_TIMEOUT_MS` | 30000 | how long shutdown waits for work in hand |
| `PROTOBUS_EXPOSE_INTERNAL_ERRORS` | true | send an unhandled error's message to the caller; see [Errors](errors.md) |

The exchange names are part of the wire protocol: every process on one bus,
whatever its language, must agree on them.

## The broker URL

`amqp://user:password@host:port/vhost`, or `amqps://` for TLS. `amqp://host/`
names the default vhost, `/`, as in every port. The URL's query takes the
RabbitMQ URI parameters lapin understands: `heartbeat`, `connection_timeout`,
`channel_max`, `frame_max`.

## Reconnection

<!-- doc-check: compile -->
```rust
use protobus::{Context, ContextOptions, ReconnectionOptions};

pub fn context() -> Context {
    let context = Context::new(ContextOptions {
        reconnection: Some(ReconnectionOptions {
            max_retries: 0, // forever; the default gives up after 10 attempts
            initial_delay_ms: 500,
            max_delay_ms: 15_000,
            backoff_multiplier: 2.0,
        }),
        ..Default::default()
    });
    let connection = context.connection();
    connection.on_reconnecting(|attempt, delay_ms| eprintln!("reconnecting: attempt {attempt} in {delay_ms}ms"));
    connection.on_reconnected(|| eprintln!("back"));
    // Giving up is the one connection failure nothing recovers from: exit, and let a supervisor restart.
    connection.on_error(|error| eprintln!("the bus is gone: {error}"));
    context
}
```

Each delay is the previous one multiplied, up to the maximum, with up to 30%
jitter. While reconnecting, publishes wait (up to
`CONNECTION_READY_TIMEOUT_MS`); calls that were pending fail as
`Disconnected`. The connection is ready again only once every queue, binding
and consumer has been restored. The listeners are synchronous: keep them
short.

## Logging

protobus-rs logs through the [`log`](https://docs.rs/log) crate, under the
target `protobus`: install any logger (`env_logger`, `tracing-log`, ...) and
filter on that target. It logs lifecycle at `info`, per-message traces at
`debug`, recoverable trouble at `warn` and failures at `error`.

Message payloads are never logged: a line carries a type name, a size and a
correlation id. Broker URLs are logged with the password removed.
