//! A service, a client and an event, in one file.
//!
//! ```text
//! docker compose up -d --wait
//! export AMQP_URL=amqp://guest:guest@127.0.0.1:29672/
//! cargo run -p protobus-examples --bin calculator            # both, in one process
//! cargo run -p protobus-examples --bin calculator -- server  # serve until Ctrl-C
//! cargo run -p protobus-examples --bin calculator -- client  # call a running server
//! ```

use protobus::{CallContext, Context, Error, ErrorKind, Result, RunnableService, Timestamp};
use protobus_examples::proto::calculator::*;

/// Implements `Calculator.Service`: a method per rpc.
struct Calculator {
    context: Context,
}

impl ServiceHandler for Calculator {
    async fn add(&self, request: AddRequest, _: CallContext) -> Result<AddResponse> {
        let calculated = Calculated {
            operation: "add".into(),
            at: Some(Timestamp::now()),
        };
        self.context.publish_event(&calculated, None).await?;
        Ok(AddResponse {
            result: request.a + request.b,
        })
    }

    async fn divide(&self, request: DivideRequest, _: CallContext) -> Result<DivideResponse> {
        if request.divisor == 0.0 {
            // A handled error is an answer: the caller gets the message and
            // the code, and the request is never retried.
            return Err(Error::handled("cannot divide by zero").with_code("DIVISION_BY_ZERO"));
        }
        Ok(DivideResponse {
            quotient: request.dividend / request.divisor,
        })
    }
}

async fn serve(context: &Context) -> Result<ServiceService> {
    let service = ServiceService::new(
        context,
        Calculator {
            context: context.clone(),
        },
    )?;
    RunnableService::start(&service).await?;
    service
        .subscribe_event(
            |event: Calculated, _| async move {
                println!(
                    "[event] {} at {} ms",
                    event.operation,
                    event.at.unwrap_or_default().to_millis()?
                );
                Ok(())
            },
            None,
        )
        .await?;
    Ok(service)
}

async fn call(context: &Context) -> Result<()> {
    let calculator = ServiceProxy::new(context)?;
    println!(
        "20 + 22 = {}",
        calculator.add(AddRequest { a: 20, b: 22 }).await?.result
    );
    println!(
        "1 / 4 = {}",
        calculator
            .divide(DivideRequest {
                dividend: 1.0,
                divisor: 4.0
            })
            .await?
            .quotient
    );
    match calculator
        .divide(DivideRequest {
            dividend: 1.0,
            divisor: 0.0,
        })
        .await
    {
        Err(e) if e.kind() == ErrorKind::Remote => println!("1 / 0: {} [{}]", e, e.code().unwrap_or("")),
        other => println!("1 / 0: unexpected {other:?}"),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "both".into());
    let context = Context::default();
    context.init(&protobus_examples::amqp_url(), &[] as &[&str]).await?;
    match mode.as_str() {
        "server" => {
            serve(&context).await?;
            println!("serving Calculator.Service; Ctrl-C to stop");
            std::process::exit(RunnableService::wait_for_shutdown().await);
        }
        "client" => call(&context).await?,
        _ => {
            serve(&context).await?;
            call(&context).await?;
            // Let the last event print, then shut down as a signal would.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            RunnableService::request_shutdown();
            RunnableService::wait_for_shutdown().await;
            return Ok(());
        }
    }
    context.close().await;
    Ok(())
}
