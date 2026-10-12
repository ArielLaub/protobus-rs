# Threading

protobus-rs is built for a multi-threaded tokio runtime. A `Context`, its
proxies and its services are used from any number of tasks and threads at
once, and the library's own state is safe under that. This page says what runs
where, what the compiler already holds your code to, and the few rules it
cannot check.

## What the compiler enforces

- **Everything a program shares is `Send + Sync`**: `Context`, `Connection`,
  `MessageFactory`, `MessageService`, `ServiceProxy` and the generated proxies,
  `EventListener`, `Error`, `CancelToken`, `CallContext`, `StreamWriter`,
  `MemoryBroker`. They are handles: cloning one is cheap and gives the same
  object.
- **A handler is `Send + Sync + 'static`**, and its methods take `&self`. State
  it shares between requests does not compile unless it is safe to share: an
  atomic, a `Mutex`, a channel. There is no data race to find at run time.
- **Nothing can be destroyed under a running handler.** Services and proxies
  hold the context they were built on, a running handler holds its service,
  and the components register with the connection through weak references. A
  handle dropped mid-request frees the object when that request is done.

A `Stream` is `Send` but read by one task at a time (`next` takes `&mut
self`), and may be moved between tasks.

## What runs where

| Code | Runs | |
|---|---|---|
| Service handlers, event handlers | a tokio task each, on the runtime's workers | in parallel, up to the service's `max_concurrent` |
| A stream's producer | a task of its own | `StreamWriter::send` waits for the previous chunk's confirm |
| Reply routing | the reply consumer's task | one reply at a time, in arrival order, never waiting on a handler |
| `on_disconnected`, `on_reconnecting`, `on_reconnected`, `on_error` listeners | the task that noticed | synchronous: keep them short, and do not block |
| Proxy calls, `publish_event`, a stream's `next()` | the calling task | asynchronous: they wait without holding a thread |

A request handler awaiting a call to another service cannot starve the reply
it waits for: replies are routed on the reply consumer's own task.

## The rules the compiler cannot check

1. **Do not block a worker thread.** A handler that computes for long, or
   calls blocking code, holds a runtime worker for that long. Move it to
   `tokio::task::spawn_blocking`. The same goes for holding a `std::sync::Mutex`
   across an `.await` (use a `tokio::sync::Mutex` there, or do not hold it).
2. **Your handlers run in parallel.** With `max_concurrent` above 1 several
   requests run at once; even at the default of 1, a service's event handlers
   run beside its request handlers. Each one's shared state is safe (see
   above), but an invariant that spans several steps (read, await, write)
   needs one lock held across them, or `serialize_handlers`.
3. **Watch the cancel token in long work.** A handler is never aborted. After
   a processing timeout it keeps running until it returns; `call.cancel` is how
   it learns its attempt was written off.
4. **Close the context when the process is done.** `context.close().await`
   fails what is pending, closes the connection and waits for handlers still
   running. Dropping the last handle without closing leaves the connection to
   end with the runtime.

## Serialized handlers

`MessageServiceOptions::serialize_handlers` (off by default) runs a service's
handlers one at a time, as the TypeScript port's event loop would: requests,
stream producers and events never overlap. Use it when the work itself must
not interleave, not to make state safe, which the compiler has already done.

<!-- doc-check: compile -->
```rust
use std::sync::atomic::{AtomicI64, Ordering};
use protobus::{CallContext, Context, MessageServiceOptions, Result};
use crate::proto::calculator::*;

#[derive(Default)]
pub struct Running {
    total: AtomicI64,
}

impl ServiceHandler for Running {
    async fn add(&self, request: AddRequest, _: CallContext) -> Result<AddResponse> {
        // Read, await, write: correct only because no other handler of this
        // service runs meanwhile.
        let before = self.total.load(Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.total.store(before + i64::from(request.a), Ordering::SeqCst);
        Ok(AddResponse { result: (before + i64::from(request.a)) as i32 })
    }
}

pub fn start(context: &Context) -> Result<ServiceService> {
    let options = MessageServiceOptions { serialize_handlers: true, max_concurrent: Some(8), ..Default::default() };
    ServiceService::with_options(context, ServiceService::NAME, Running::default(), options)
}
```

What it costs:

- One handler at a time per service, in this process. Other replicas, and
  other services in the process, still run in parallel.
- A request waiting for its turn is already counting down its processing
  timeout.
- A handler must not call its own service and wait for the answer: that
  request could only run after the handler returns. protobus refuses the call
  with an error rather than let it deadlock. Calling another service is fine,
  and so is publishing an event, even to yourself. A cycle across two
  serialized services (A waits on B, which waits on A) is not detected; it
  ends at the callers' timeouts.

## Runtimes

A multi-threaded runtime is the expected one. A current-thread runtime works
too: everything then runs on that thread, interleaved at awaits. The
connection remembers the runtime it was opened on and runs its own tasks
(reconnection, restoration, stream watchdogs) there.
