//! Server streaming, as each side sees it: the caller's [`Stream`] of decoded
//! chunks and the producer's [`StreamWriter`].

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::connection::CancelToken;
use crate::errors::{Error, ErrorKind, Result};
use crate::message_dispatcher::ChunkStream;
use crate::message_factory::MessageFactory;
use crate::wire;

pub(crate) type Decoder<T> = Arc<dyn Fn(&[u8]) -> Result<T> + Send + Sync>;
pub(crate) type Encoder<T> = Arc<dyn Fn(&T) -> Result<Vec<u8>> + Send + Sync>;

/// The reply of a server-streaming call.
///
/// ```no_run
/// # async fn run(mut ticks: protobus::Stream<String>) -> protobus::Result<()> {
/// while let Some(tick) = ticks.next().await {
///     println!("{}", tick?);
/// }
/// # Ok(()) }
/// ```
///
/// The request is published when the call is made. Dropping the stream before
/// its end cancels the call: its buffer is released and the producer is told
/// to stop. A service error ends the stream as a remote error; a failure to
/// publish the request surfaces from the first [`next`](Self::next).
pub struct Stream<T> {
    chunks: ChunkStream,
    method: String,
    decode: Decoder<T>,
    done: bool,
}

impl<T> Stream<T> {
    pub(crate) fn new(chunks: ChunkStream, method: String, decode: Decoder<T>) -> Self {
        Stream {
            chunks,
            method,
            decode,
            done: false,
        }
    }

    /// The next chunk; `None` once the stream has ended. After an error the
    /// stream is over.
    pub async fn next(&mut self) -> Option<Result<T>> {
        if self.done {
            return None;
        }
        let item = match self.chunks.next().await {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(chunk)) => Some(self.decode_chunk(&chunk)),
        };
        if !matches!(item, Some(Ok(_))) {
            self.done = true;
            // An error mid-stream leaves the producer running for nobody.
            self.chunks.cancel();
        }
        item
    }

    fn decode_chunk(&self, chunk: &[u8]) -> Result<T> {
        let invalid = || {
            Error::new(
                ErrorKind::InvalidResponse,
                format!("failed parsing streaming chunk for {}", self.method),
            )
        };
        match wire::decode_response(chunk).map_err(|_| invalid())? {
            // A terminal chunk may carry an error instead of a result.
            wire::Response::Error(e) => Err(Error::remote(e.message, &e.code, e.method)),
            wire::Response::Result(r) => (self.decode)(&r.data).map_err(|e| match e.kind() {
                ErrorKind::CustomTypeRange => Error::new(
                    ErrorKind::InvalidResponse,
                    format!("failed parsing streaming chunk for {}: {e}", self.method),
                ),
                _ => invalid(),
            }),
        }
    }

    /// Read the stream to its end.
    pub async fn collect(mut self) -> Result<Vec<T>> {
        let mut all = Vec::new();
        while let Some(item) = self.next().await {
            all.push(item?);
        }
        Ok(all)
    }

    /// Stop the call: the producer's cancel token fires and its chunks stop
    /// being published. The stream then ends without an error. Idempotent.
    pub fn cancel(&mut self) {
        self.done = true;
        self.chunks.cancel();
    }

    /// The stream has ended and every chunk has been read.
    pub fn is_finished(&self) -> bool {
        self.done || self.chunks.is_finished()
    }

    /// The stream as a [`futures_util::Stream`], for the combinators.
    pub fn into_stream(self) -> impl futures_util::Stream<Item = Result<T>> + Send
    where
        T: Send + 'static,
    {
        futures_util::stream::unfold(self, |mut stream| async move {
            stream.next().await.map(|item| (item, stream))
        })
    }
}

/// The producer's end of a streaming reply, over encoded payloads.
#[derive(Clone)]
pub(crate) struct RawStreamWriter {
    pub(crate) chunks: mpsc::Sender<Result<Vec<u8>>>,
    pub(crate) cancel: CancelToken,
    pub(crate) method: Arc<str>,
}

impl RawStreamWriter {
    fn cancelled() -> Error {
        Error::new(ErrorKind::StreamCancelled, "the stream was cancelled by its caller")
    }

    async fn send(&self, payload: Vec<u8>) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(Self::cancelled());
        }
        let chunk = MessageFactory::build_encoded_response(&self.method, payload);
        // The reader going away is how a cancellation reaches a producer that
        // was waiting to hand over its next chunk.
        self.chunks.send(Ok(chunk)).await.map_err(|_| Self::cancelled())
    }
}

/// Where a streaming handler writes its chunks.
///
/// ```ignore
/// async fn tick(&self, request: TickRequest, call: CallContext, out: StreamWriter<Tick>) -> protobus::Result<()> {
///     for seq in 0..request.count {
///         out.send(Tick { seq, ..Default::default() }).await?;
///     }
///     Ok(())
/// }
/// ```
///
/// [`send`](Self::send) completes once the previous chunk has been published
/// and confirmed, so a producer cannot outrun the broker. It fails once the
/// caller has cancelled: return that error (`?`) and the stream ends quietly.
/// Returning `Ok` ends the stream; returning any other error ends it with that
/// error, which the caller's iteration raises. Streams are never retried.
pub struct StreamWriter<T> {
    raw: RawStreamWriter,
    encode: Encoder<T>,
}

impl<T> Clone for StreamWriter<T> {
    fn clone(&self) -> Self {
        StreamWriter {
            raw: self.raw.clone(),
            encode: self.encode.clone(),
        }
    }
}

impl<T> StreamWriter<T> {
    pub(crate) fn new(raw: RawStreamWriter, encode: Encoder<T>) -> Self {
        StreamWriter { raw, encode }
    }

    /// Send one chunk.
    pub async fn send(&self, chunk: T) -> Result<()> {
        self.send_ref(&chunk).await
    }

    /// Send one chunk by reference.
    pub async fn send_ref(&self, chunk: &T) -> Result<()> {
        let payload = (self.encode)(chunk)?;
        self.raw.send(payload).await
    }

    /// Whether the caller has cancelled the stream.
    pub fn is_cancelled(&self) -> bool {
        self.raw.cancel.is_cancelled() || self.raw.chunks.is_closed()
    }
}
