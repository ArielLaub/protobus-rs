//! Process lifecycle for services: start, signals, graceful shutdown.

use std::future::Future;
use std::sync::{Mutex, OnceLock};

use tokio::sync::watch;

use crate::amqp::BoxFuture;
use crate::config;
use crate::context::Context;
use crate::errors::{Error, ErrorKind, Result};
use crate::message_service::MessageService;

type Cleanup = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

struct Started {
    service: MessageService,
    cleanup: Option<Cleanup>,
}

struct Registry {
    started: Vec<Started>,
    shutting_down: bool,
    signals_installed: bool,
    exit_code: i32,
    /// Counts completed shutdowns, so a waiter that arrives late still returns.
    done: watch::Sender<u64>,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        Mutex::new(Registry {
            started: Vec::new(),
            shutting_down: false,
            signals_installed: false,
            exit_code: 0,
            done: watch::channel(0).0,
        })
    })
}

/// Services with process lifecycle.
///
/// ```no_run
/// # async fn run(ctx: protobus::Context, calculator: protobus::MessageService) -> protobus::Result<()> {
/// protobus::RunnableService::start(&calculator).await?;
/// std::process::exit(protobus::RunnableService::wait_for_shutdown().await);
/// # }
/// ```
///
/// [`start`](Self::start) initialises a service and registers it for graceful
/// shutdown, which runs on SIGINT or SIGTERM or on
/// [`request_shutdown`](Self::request_shutdown): every started service stops
/// taking new work, in-flight work drains (up to `SHUTDOWN_DRAIN_TIMEOUT_MS`),
/// each service's cleanup runs, and the contexts close. A cleanup never runs
/// while a delivery is still being handled, unless the drain deadline passed.
pub struct RunnableService;

impl RunnableService {
    /// Initialise a service and register it for graceful shutdown. On a
    /// startup failure the service is stopped, its context closed, and the
    /// error returned.
    pub async fn start(service: &MessageService) -> Result<()> {
        Self::start_inner(service, None).await
    }

    /// [`start`](Self::start), with a cleanup to run at shutdown once the
    /// service's work has drained: release the service's own resources there.
    pub async fn start_with_cleanup<F, Fut>(service: &MessageService, cleanup: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self::start_inner(service, Some(Box::new(move || Box::pin(cleanup())))).await
    }

    async fn start_inner(service: &MessageService, cleanup: Option<Cleanup>) -> Result<()> {
        install_signal_handlers();
        log::info!(target: "protobus", "Starting service: {}", service.service_name());
        let started = async {
            service.init().await?;
            let mut r = registry().lock().unwrap();
            if r.shutting_down {
                return Err(Error::new(ErrorKind::NotReady, "a shutdown is in progress"));
            }
            r.started.push(Started {
                service: service.clone(),
                cleanup,
            });
            Ok(())
        }
        .await;
        match &started {
            Ok(()) => log::info!(target: "protobus", "Service ready: {}", service.service_name()),
            Err(e) => {
                log::error!(target: "protobus", "Service startup failed: {e}");
                service.close().await;
                service.context().close().await;
                registry().lock().unwrap().exit_code = 1;
            }
        }
        started
    }

    /// Shut every started service down, as a signal would. Returns at once.
    pub fn request_shutdown() {
        tokio::spawn(shutdown("requested"));
    }

    /// Wait until a shutdown has completed; returns the exit code (0, or 1
    /// after a startup failure).
    pub async fn wait_for_shutdown() -> i32 {
        let mut done = registry().lock().unwrap().done.subscribe();
        let seen = *done.borrow_and_update();
        let _ = done.wait_for(|n| *n > seen).await;
        registry().lock().unwrap().exit_code
    }

    /// Allow another start/shutdown cycle in the same process, for tests that
    /// start services in-process.
    pub fn reset() {
        let mut r = registry().lock().unwrap();
        r.started.clear();
        r.shutting_down = false;
        r.exit_code = 0;
    }
}

fn install_signal_handlers() {
    {
        let mut r = registry().lock().unwrap();
        if r.signals_installed {
            return;
        }
        r.signals_installed = true;
    }
    #[cfg(unix)]
    tokio::spawn(async {
        use tokio::signal::unix::{signal, SignalKind};
        let (Ok(mut interrupt), Ok(mut terminate)) = (signal(SignalKind::interrupt()), signal(SignalKind::terminate()))
        else {
            log::warn!(target: "protobus", "could not install signal handlers; shut down with request_shutdown()");
            return;
        };
        loop {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
            // The process stays up until the shutdown completes and the caller
            // of wait_for_shutdown returns.
            tokio::spawn(shutdown("signal"));
        }
    });
    #[cfg(not(unix))]
    tokio::spawn(async {
        while tokio::signal::ctrl_c().await.is_ok() {
            tokio::spawn(shutdown("signal"));
        }
    });
}

async fn shutdown(reason: &'static str) {
    let started = {
        let mut r = registry().lock().unwrap();
        if r.shutting_down {
            return;
        }
        r.shutting_down = true;
        std::mem::take(&mut r.started)
    };
    let mut services = Vec::new();
    let mut cleanups = Vec::new();
    for s in started {
        services.push(s.service);
        cleanups.extend(s.cleanup);
    }
    log::info!(target: "protobus", "Shutdown initiated ({reason})");
    // 1. Stop taking new work, keeping channels open: cleanup must not run
    //    while consumers still deliver.
    for service in &services {
        service.stop_consuming().await;
    }
    log::info!(target: "protobus", "Stopped accepting new messages");
    // 2. Let work in hand finish, including the reply, retry or DLQ publish
    //    that settles it.
    let budget = config::shutdown_drain_timeout_ms();
    let mut contexts: Vec<Context> = Vec::new();
    for service in &services {
        let context = service.context();
        if contexts.iter().any(|c| c.same(context)) {
            continue;
        }
        contexts.push(context.clone());
        let connection = context.connection();
        let in_flight = connection.in_flight_deliveries();
        if in_flight == 0 {
            continue;
        }
        log::info!(target: "protobus", "Draining {in_flight} in-flight message(s), up to {budget}ms");
        if connection.drain_in_flight(budget).await {
            log::info!(target: "protobus", "In-flight messages drained");
        } else {
            log::info!(
                target: "protobus",
                "Drain deadline reached with {} still running; they stay unacknowledged and will be redelivered",
                connection.in_flight_deliveries()
            );
        }
    }
    // 3. Only now is it safe to release the services' own resources.
    for cleanup in cleanups {
        cleanup().await;
    }
    for service in &services {
        service.close().await;
    }
    for context in contexts {
        context.close().await;
    }
    log::info!(target: "protobus", "Connection closed");
    registry().lock().unwrap().done.send_modify(|n| *n += 1);
}
