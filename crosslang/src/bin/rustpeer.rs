//! The Rust participant of the cross-language suite.
//!
//! ```text
//! rustpeer server   serve the interop services; prints READY
//! rustpeer client   run the client scenario against PEER_TARGET's services;
//!                   prints PASS/FAIL lines, then DONE
//! ```
//!
//! The broker is `PROTOBUS_TEST_AMQP`.

use std::io::Write;
use std::process::ExitCode;

use protobus::Context;
use protobus_crosslang::{client, serve, LANG};

fn env(name: &str, fallback: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

struct StderrLog;

impl log::Log for StderrLog {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        eprintln!("[{}] {}", record.level(), record.args());
    }

    fn flush(&self) {}
}

async fn run(mode: &str) -> protobus::Result<u8> {
    let ctx = Context::default();
    ctx.init(&env("PROTOBUS_TEST_AMQP", ""), &[] as &[&str]).await?;
    if mode == "server" {
        let services = serve(&ctx).await?;
        say("READY");
        wait_for_signal().await;
        for service in &services {
            service.stop_consuming().await;
        }
        ctx.connection().drain_in_flight(5000).await;
        for service in &services {
            service.close().await;
        }
        ctx.close().await;
        return Ok(0);
    }
    let failures = client(&ctx, &env("PEER_TARGET", LANG), say).await?;
    ctx.close().await;
    Ok(u8::from(failures > 0))
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM can be handled");
        let mut interrupt = signal(SignalKind::interrupt()).expect("SIGINT can be handled");
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> ExitCode {
    static LOG: StderrLog = StderrLog;
    let _ = log::set_logger(&LOG);
    log::set_max_level(if std::env::var_os("PROTOBUS_TEST_LOG").is_some() {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Error
    });
    let mode = std::env::args().nth(1).unwrap_or_else(|| "client".into());
    match run(&mode).await {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("{e}");
            ExitCode::from(2)
        }
    }
}
