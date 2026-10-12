# Architecture

The types follow the TypeScript reference one to one.

| Type | Role |
|---|---|
| `Context` | one process's place on the bus: owns the connection, the factory and the two dispatchers |
| `Connection` | the AMQP connection: reconnection, restoration, confirmed publishing, the consume and settle loop |
| `MessageFactory` | schemas, and the request, reply and event envelopes |
| `MessageDispatcher` | the caller's side of RPC: publishes requests, routes replies and stream chunks |
| `EventDispatcher` | publishes events |
| `BaseListener` | a queue and its consumer, restored after a reconnection |
| `MessageListener` | a service's request queue, with its retry queue, retry exchange and DLQ |
| `EventListener` | an event queue and its topic router |
| `CallbackListener` | the context's reply queue |
| `CancelListener` | hears stream cancellations |
| `MessageService`, `RunnableService` | a service, and its process lifecycle |
| `ServiceProxy` | calls a service by name |

Where TypeScript has a class to extend, Rust has a trait to implement: the
generator emits a handler trait per service and builds a `MessageService`
around an implementation. Every type above is a handle over shared state:
`Clone`, `Send + Sync`.

`amqp::Transport` is the seam to the AMQP client: `amqp::LapinTransport` over
[lapin](https://docs.rs/lapin) in production, `testing::MemoryBroker` in
tests. lapin's own automatic recovery is off: protobus restores its topology
itself, the same way in every port.

## Tasks

Everything is asynchronous; nothing in the library blocks a thread, and no
lock is held across an await. See [Threading](threading.md) for what that asks
of application code.

| Where | Runs |
|---|---|
| a consumer's task | its deliveries, one at a time: a service's delivery is handed to a task of its own at once, a reply or a cancellation is routed right there |
| a task per delivery | the handler, raced against its processing timeout, then its settlement (reply, ack, retry or dead-letter) |
| a task per publish | the write and its confirm, holding one of the channel's confirm slots until the broker answers |
| a task per stream | the producer on the service's side; an idle watchdog on the caller's |
| the reconnection task | backoff, connecting, and the restorers, one after another |

The handler itself runs on a task inside the delivery's, so a panic is
contained and a handler that outlives its deadline keeps running without
holding up the settlement.

## Settling a delivery

A late-ack consumer (every service, by default) settles after its handler:

1. On success, publish the reply (or the stream's chunks), then ack. The reply
   goes first, so the worst case is a redelivered request rather than a
   settled one whose reply was lost.
2. On a handled error (or a protocol error, which is handled), reply with it,
   then settle.
3. On any other failure, publish the message to the retry exchange with
   `x-retry-count` incremented, then ack; or, after the last retry, reply with
   the error, publish it to the DLQ, then ack. The reply is best effort: the
   dead-letter copy is the durable record, and a failed reply never stops it.
4. If a settlement publish itself fails, the delivery is requeued after a
   second.

Retry and dead-letter copies are published `mandatory`, and the ack waits for
their confirm, so the only copy of a message is never acknowledged on the
strength of a publish that went nowhere.

## Reconnection

Every listener and dispatcher registers a restorer with the connection. When
the socket drops: the generation number advances, pending calls fail, and
reconnection is scheduled with backoff. After connecting, the restorers run in
registration order (each reopens its channel and redeclares its topology), and
only when all have succeeded does the connection report itself ready and
release the publishes waiting on it. A restorer that fails, or a socket that
drops during restoration, fails the whole attempt; a stale attempt, whose
generation was superseded meanwhile, discards itself. Restorers run one after
another because one component's exchange is what another binds to.

From the moment a connection is lost until it is ready again or gives up,
`is_reconnecting()` is true: there is no instant at which a lost connection
looks merely absent to a caller deciding whether to wait.

## Publishing

Every channel is a confirm channel. A publish takes one of its channel's
`MAX_OUTSTANDING_CONFIRMS` slots, is written, and waits for the broker's ack or
nack; a mandatory publish that comes back as a `basic.return` is reported
`UNROUTABLE` when its ack arrives. A publish whose confirm timed out keeps its
slot until the broker answers or the channel closes, so a stalled broker
cannot be handed an unbounded backlog; a publish still waiting for a slot when
its deadline passes fails without being sent.

A publish that finds its channel gone because the connection was just lost
waits for the reconnection, as one arriving a moment later would: nothing has
been sent, so nothing is ambiguous.
