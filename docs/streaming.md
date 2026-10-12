# Streaming

An rpc declared `returns (stream T)` is server streaming: one request, any
number of replies. Client streaming is not part of the protobus protocol.

## Producing

A streaming handler receives a `StreamWriter<T>` and writes chunks to it:

<!-- doc-check: compile -->
```rust
use std::time::Duration;
use protobus::{CallContext, Error, Result, StreamWriter};
use crate::proto::chat::*;

pub struct Assistant;

impl AssistantHandler for Assistant {
    async fn generate(&self, request: GenerateRequest, call: CallContext, out: StreamWriter<Token>) -> Result<()> {
        if request.prompt.is_empty() {
            // Ends the stream with this error; the caller's read fails with it.
            return Err(Error::handled("an empty prompt").with_code("EMPTY_PROMPT"));
        }
        for (index, word) in request.prompt.split_whitespace().enumerate() {
            // Stop the expensive part as soon as the caller has gone.
            tokio::select! {
                _ = call.cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(request.token_delay_ms as u64)) => {}
            }
            // Fails once the caller has cancelled: `?` ends the stream quietly.
            out.send(Token { index: index as i32, text: word.to_string() }).await?;
        }
        Ok(())
    }
}
```

- `send` completes once the previous chunk has been published and confirmed,
  so a producer cannot outrun the broker.
- Returning `Ok` ends the stream. Returning an error ends it with that error,
  after the chunks already sent; a handled error crosses as it is, anything
  else as an unhandled one. Streams are never retried: a stream that failed
  half way cannot be replayed from the start for a caller that has read half.
- A cancelled stream ends without an error. The handler learns of it from
  `call.cancel`, from `out.is_cancelled()`, or from `send` failing.
- One chunk is always held back, so the last one can be marked final. A slow
  producer's first chunk reaches the caller when its second exists.

## Consuming

<!-- doc-check: compile -->
```rust
use protobus::{Context, ErrorKind, Result, StreamOptions};
use crate::proto::chat::*;

pub async fn read(context: &Context) -> Result<usize> {
    let assistant = AssistantProxy::new(context)?;
    let request = GenerateRequest { prompt: "to be or not to be".into(), token_delay_ms: 20 };
    let mut tokens = assistant.generate_with(request, StreamOptions::new().idle_timeout_ms(5000))?;
    let mut count = 0;
    while let Some(token) = tokens.next().await {
        match token {
            Ok(token) => {
                count += 1;
                if token.text == "not" {
                    break; // enough: dropping the stream cancels it
                }
            }
            // The service ended the stream with an error.
            Err(e) if e.kind() == ErrorKind::Remote => return Err(e),
            // StreamTimeout, StreamBackpressure, StreamSequence, Disconnected, a publish failure...
            Err(e) => return Err(e),
        }
    }
    Ok(count)
}
```

The request is published when the call is made. `next()` returns the chunks in
order, then `None`; a failure is returned once, and the stream is over.
`collect()` reads to the end; `into_stream()` adapts it to
`futures::Stream`.

## Cancelling

Dropping a `Stream` before its end, or calling `cancel()`, releases its buffer
and publishes a cancel notice. Every replica hears it; the one running that
stream fires the handler's `call.cancel` and stops publishing its chunks. It is
cooperative and best effort: a producer that ignores its token runs to the
end, and what it writes is discarded.

## Limits on the caller's side

| | Default | When exceeded |
|---|---|---|
| `StreamOptions::idle_timeout_ms` (`STREAM_IDLE_TIMEOUT_MS`) | 60 s | `StreamTimeout`: no chunk arrived, and none was read, for that long |
| `STREAM_MAX_BUFFERED_CHUNKS` | 1024 | `StreamBackpressure`: the reader fell that far behind |
| `STREAM_MAX_BUFFERED_BYTES` | 64 MiB | `StreamBackpressure` |
| `STREAM_MAX_TOTAL_BUFFERED_BYTES` | 256 MiB | `StreamBackpressure`: across every stream of the context |

In each case the stream fails and the producer is cancelled. A gap in the
chunks' sequence numbers fails the stream as `StreamSequence`; a duplicate (a
redelivery) is dropped.
