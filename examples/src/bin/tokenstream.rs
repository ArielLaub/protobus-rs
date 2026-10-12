//! Server-streaming with cancellation, in the shape a chat UI needs: a service
//! streams tokens like a language model, and the caller stops it three ways.
//!
//! ```text
//! docker compose up -d --wait
//! AMQP_URL=amqp://guest:guest@127.0.0.1:29672/ cargo run -p protobus-examples --bin tokenstream
//! ```
//!
//! After each run the demo asks the SERVER how much it generated. Cancellation
//! that only stopped the reader would show the full count; here the producer
//! stops, because the caller's cancel reaches it as its token firing.

use std::sync::Mutex;
use std::time::Duration;

use protobus::{CallContext, Context, ErrorKind, Result, StreamOptions, StreamWriter};
use protobus_examples::proto::chat::*;

fn completion(prompt: &str) -> Vec<String> {
    let mut body = format!("Answering \"{prompt}\". ");
    for _ in 0..3 {
        body.push_str(
            "Streaming responses arrive one token at a time, which is what lets a chat interface render text as it is \
             produced rather than waiting for a whole reply. That same property is what makes stopping useful: when \
             the reader has seen enough, every token after that point is wasted work on the server. ",
        );
    }
    body.split_whitespace().map(|word| format!("{word} ")).collect()
}

/// Streams a long canned completion, one word at a time.
#[derive(Default)]
struct Assistant {
    stats: Mutex<StatsResponse>,
}

impl AssistantHandler for Assistant {
    async fn generate(&self, request: GenerateRequest, call: CallContext, out: StreamWriter<Token>) -> Result<()> {
        let delay = Duration::from_millis(if request.token_delay_ms > 0 {
            request.token_delay_ms as u64
        } else {
            60
        });
        *self.stats.lock().unwrap() = StatsResponse::default();
        for (index, text) in completion(&request.prompt).into_iter().enumerate() {
            // The token fires when the caller cancels: it dropped the stream,
            // cancelled it, or went idle. This is where a real service would
            // abort its upstream model call, and the point of the demo:
            // stopping saves the work, not just the reading.
            let cancelled = tokio::select! {
                _ = call.cancel.cancelled() => true,
                _ = tokio::time::sleep(delay) => false,
            };
            if cancelled
                || out
                    .send(Token {
                        index: index as i32,
                        text,
                    })
                    .await
                    .is_err()
            {
                self.stats.lock().unwrap().stopped_early = true;
                return Ok(());
            }
            self.stats.lock().unwrap().tokens_generated += 1;
        }
        Ok(())
    }

    async fn stats(&self, _: StatsRequest, _: CallContext) -> Result<StatsResponse> {
        Ok(*self.stats.lock().unwrap())
    }
}

async fn report(assistant: &AssistantProxy, read: usize) -> Result<()> {
    // Give the cancel a moment to cross the broker.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let stats = assistant.stats(StatsRequest {}).await?;
    println!(
        "  the caller read {read} tokens; the server generated {}{}\n",
        stats.tokens_generated,
        if stats.stopped_early {
            " and stopped early"
        } else {
            " and ran to the end"
        }
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::default();
    context.init(&protobus_examples::amqp_url(), &[] as &[&str]).await?;
    let service = AssistantService::new(&context, Assistant::default())?;
    service.init().await?;
    let assistant = AssistantProxy::new(&context)?;
    let request = |prompt: &str| GenerateRequest {
        prompt: prompt.into(),
        token_delay_ms: 15,
    };

    println!("1. Read the whole stream.");
    let tokens = assistant.generate(request("the whole thing"))?.collect().await?;
    report(&assistant, tokens.len()).await?;

    println!("2. Drop the stream after ten tokens, as a reader who has seen enough.");
    let mut stream = assistant.generate(request("just the start"))?;
    let mut read = 0;
    while let Some(token) = stream.next().await {
        token?;
        read += 1;
        if read == 10 {
            break;
        }
    }
    drop(stream);
    report(&assistant, read).await?;

    println!("3. Stop reading: the idle timeout ends the stream and stops the producer.");
    let slow = GenerateRequest {
        prompt: "too slow".into(),
        token_delay_ms: 400,
    };
    let mut stream = assistant.generate_with(slow, StreamOptions::new().idle_timeout_ms(150))?;
    let mut read = 0;
    loop {
        match stream.next().await {
            Some(Ok(_)) => read += 1,
            Some(Err(e)) if e.kind() == ErrorKind::StreamTimeout => {
                println!("  {e}");
                break;
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    report(&assistant, read).await?;

    service.close().await;
    context.close().await;
    Ok(())
}
