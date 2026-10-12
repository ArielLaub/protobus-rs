//! The Rust participant of the cross-language suite.
//!
//! Behaviour mirrors the other ports' peers exactly: the same services, the
//! same canonical values, the same fifteen client checks. [`serve`] and
//! [`client`] are what the `rustpeer` binary runs; the in-memory tests run
//! them against each other without a broker.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use protobus::{
    Bigint, CallContext, CallOptions, Context, Error, ErrorKind, EventListener, MessageService, MessageServiceOptions,
    Result, RetryOptions, StreamWriter, Timestamp, U256,
};

protobus::include_protos!();

use interop::*;

pub const LANG: &str = "rust";

fn pow(base: u64, exp: u32) -> U256 {
    U256::from(base).checked_pow(exp).expect("fits in 256 bits")
}

/// The Balance every peer returns for an ordinary account.
pub fn canonical() -> Balance {
    Balance {
        amount: Some(Bigint::from(pow(10, 30))),
        as_of: Some(Timestamp::from_millis(1_577_836_800_000).unwrap()), // 2020-01-01T00:00:00Z
        big: 9_007_199_254_740_993,
        tags: vec!["a".into(), "b".into()],
        counts: [("x".to_string(), 1), ("y".to_string(), 2)].into(),
        balances: [("k".to_string(), Bigint::from(pow(2, 200)))].into(),
        parts: (1..=3u64).map(Bigint::from).collect(),
        kind: Kind::Future as i32,
        inner: Some(Inner {
            name: "root".into(),
            value: Some(Bigint::from(7u64)),
            children: vec![Inner {
                name: "leaf".into(),
                value: Some(Bigint::from(8u64)),
                children: vec![],
            }],
        }),
        ubig: u64::MAX,
        blob: vec![0x00, 0x01, 0xff],
        ratio: 0.5,
        flag: true,
        neg: -5,
        before_epoch: Some(Timestamp::from_millis(-14_182_940_000).unwrap()), // 1969-07-20T20:17:40Z
        zero: 0,
    }
}

// ---- server ----------------------------------------------------------------------

/// What the last tick stream's producer did, shared by every Counter instance.
#[derive(Clone, Default)]
pub struct ProducedState(Arc<Mutex<Produced>>);

impl ProducedState {
    fn record(&self, f: impl FnOnce(&mut Produced)) {
        f(&mut self.0.lock().unwrap());
    }
}

struct Counter {
    produced: ProducedState,
}

impl CounterHandler for Counter {
    async fn add(&self, r: AddRequest, _: CallContext) -> Result<AddResponse> {
        Ok(AddResponse { sum: r.a + r.b })
    }

    async fn tick(&self, r: TickRequest, call: CallContext, out: StreamWriter<Tick>) -> Result<()> {
        if r.emit_nothing {
            return Ok(());
        }
        let produced = &self.produced;
        produced.record(|p| *p = Produced::default());
        for i in 0..r.count {
            if r.fail_at > 0 && i >= r.fail_at {
                return Err(if r.unhandled {
                    Error::failure("stream broke")
                } else {
                    Error::handled(format!("deliberate failure at chunk {i}")).with_code("TEST_FAIL")
                });
            }
            let stopped = call.cancel.is_cancelled()
                || out
                    .send(Tick {
                        seq: i,
                        payload: format!("chunk-{i}"),
                    })
                    .await
                    .is_err();
            if stopped {
                produced.record(|p| p.stopped_early = true);
                return Ok(());
            }
            produced.record(|p| p.yielded += 1);
            if r.delay_ms > 0 {
                tokio::select! {
                    _ = call.cancel.cancelled() => {
                        produced.record(|p| p.stopped_early = true);
                        return Ok(());
                    }
                    _ = tokio::time::sleep(Duration::from_millis(r.delay_ms as u64)) => {}
                }
            }
        }
        produced.record(|p| p.finished = true);
        Ok(())
    }

    async fn produced(&self, _: Nothing, _: CallContext) -> Result<Produced> {
        Ok(*self.produced.0.lock().unwrap())
    }

    async fn whoami(&self, _: Nothing, call: CallContext) -> Result<Who> {
        Ok(Who {
            actor: call.actor,
            message_id: call.message_id.unwrap_or_default(),
            routing_key: call.routing_key,
            lang: LANG.into(),
        })
    }
}

struct Wallet;

impl WalletHandler for Wallet {
    async fn balance(&self, q: Query, _: CallContext) -> Result<Balance> {
        match q.account.as_str() {
            "boom" => Err(Error::handled("no such account").with_code("NOT_FOUND")),
            "crash" => Err(Error::failure("kaboom")),
            _ => Ok(canonical()),
        }
    }

    async fn echo(&self, b: Balance, _: CallContext) -> Result<Balance> {
        Ok(b)
    }
}

struct Flaky {
    context: Context,
}

impl FlakyHandler for Flaky {
    async fn fail(&self, _: FailRequest, call: CallContext) -> Result<Nothing> {
        let attempted = Attempted {
            lang: LANG.into(),
            message_id: call.message_id.unwrap_or_default(),
        };
        self.context.publish_event(&attempted, Some("EVENT.attempted")).await?;
        Err(Error::failure(format!("flaky {LANG}")))
    }
}

struct Listener;

impl ListenerHandler for Listener {}

/// Start the interop services on `ctx`; returns them, for stopping.
pub async fn serve(ctx: &Context) -> Result<Vec<MessageService>> {
    let no_retry = MessageServiceOptions {
        retry: RetryOptions {
            max_retries: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    // The queues are shared with the other languages' replicas, so the retry
    // arguments must be equivalent to theirs.
    let flaky_options = MessageServiceOptions {
        retry: RetryOptions {
            max_retries: 3,
            retry_delay_ms: 100,
            ..Default::default()
        },
        max_concurrent: Some(4),
        ..Default::default()
    };
    let produced = ProducedState::default();
    let listener = ListenerService::with_options(ctx, &format!("interop.Listener.{LANG}"), Listener, no_retry.clone())?;
    let services: Vec<MessageService> = vec![
        CounterService::new(
            ctx,
            Counter {
                produced: produced.clone(),
            },
        )?
        .into(),
        CounterService::with_options(ctx, "interop.Counter.inst1", Counter { produced }, Default::default())?.into(),
        WalletService::with_options(ctx, WalletService::NAME, Wallet, no_retry)?.into(),
        FlakyService::with_options(ctx, FlakyService::NAME, Flaky { context: ctx.clone() }, flaky_options)?.into(),
        listener.clone().into(),
    ];
    for service in &services {
        service.init().await?;
    }
    let publisher = ctx.clone();
    listener
        .subscribe_event(
            move |ping: Ping, _| {
                let publisher = publisher.clone();
                async move {
                    let n = ping.n.unwrap_or_default().to_u256()?.checked_add(U256::ONE);
                    let pong = Ping {
                        id: format!("pong:{}", ping.id),
                        n: Some(Bigint::from(n.ok_or_else(|| Error::failure("n overflows"))?)),
                        from: LANG.into(),
                    };
                    publisher
                        .publish_event(&pong, Some(&format!("EVENT.pong.{LANG}")))
                        .await
                }
            },
            Some(&format!("EVENT.ping.{LANG}")),
        )
        .await?;
    Ok(services)
}

// ---- client ----------------------------------------------------------------------

type Check = std::result::Result<(), String>;

struct Checks {
    failures: u32,
    report: Box<dyn Fn(&str) + Send>,
}

impl Checks {
    async fn run(&mut self, name: &str, check: impl Future<Output = Check>) {
        match check.await {
            Ok(()) => (self.report)(&format!("PASS {name}")),
            Err(why) => {
                self.failures += 1;
                (self.report)(&format!("FAIL {name}: {}", why.replace('\n', " ")));
            }
        }
    }
}

fn eq<T: PartialEq + std::fmt::Debug>(got: T, want: T, what: &str) -> Check {
    if got == want {
        Ok(())
    } else {
        Err(format!("{what}: got {got:?}, want {want:?}"))
    }
}

fn require(ok: bool, what: impl Into<String>) -> Check {
    if ok {
        Ok(())
    } else {
        Err(what.into())
    }
}

fn why(e: Error) -> String {
    format!("{} ({})", e, e.name())
}

/// The remote error a call was expected to fail with.
fn remote<T: std::fmt::Debug>(result: Result<T>) -> std::result::Result<Error, String> {
    match result {
        Err(e) if e.kind() == ErrorKind::Remote => Ok(e),
        Err(e) => Err(format!("expected a remote error, got {}", why(e))),
        Ok(v) => Err(format!("expected a remote error, got {v:?}")),
    }
}

/// Run the client scenario against `target`'s services, reporting each check
/// through `report`. Returns how many failed.
pub async fn client(ctx: &Context, target: &str, report: impl Fn(&str) + Send + 'static) -> Result<u32> {
    let counter = CounterProxy::new(ctx)?;
    let inst = CounterProxy::with_name(ctx, "interop.Counter.inst1")?;
    let wallet = WalletProxy::new(ctx)?;
    let mut checks = Checks {
        failures: 0,
        report: Box::new(report),
    };

    checks
        .run("unary", async {
            eq(counter.add(AddRequest { a: 2, b: 3 }).await.map_err(why)?.sum, 5, "sum")
        })
        .await;
    checks
        .run("priority on a plain queue", async {
            let reply = counter
                .add_with(AddRequest { a: 1, b: 1 }, CallOptions::new().priority(2))
                .await;
            eq(reply.map_err(why)?.sum, 2, "sum")
        })
        .await;
    checks
        .run("stream in order", async {
            let mut ticks = counter
                .tick(TickRequest {
                    count: 5,
                    ..Default::default()
                })
                .map_err(why)?;
            let mut i = 0;
            while let Some(tick) = ticks.next().await {
                let tick = tick.map_err(why)?;
                eq(tick.seq, i, "seq")?;
                eq(tick.payload, format!("chunk-{i}"), "payload")?;
                i += 1;
            }
            eq(i, 5, "chunks")
        })
        .await;
    checks
        .run("empty stream", async {
            let ticks = counter
                .tick(TickRequest {
                    emit_nothing: true,
                    ..Default::default()
                })
                .map_err(why)?;
            eq(ticks.collect().await.map_err(why)?.len(), 0, "chunks")
        })
        .await;
    checks
        .run("mid-stream handled error", async {
            let mut ticks = counter
                .tick(TickRequest {
                    count: 10,
                    fail_at: 2,
                    ..Default::default()
                })
                .map_err(why)?;
            let mut n = 0;
            let error = loop {
                match ticks.next().await {
                    Some(Ok(_)) => n += 1,
                    Some(Err(e)) => break remote::<()>(Err(e))?,
                    None => return Err("the stream ended without an error".into()),
                }
            };
            eq(error.code(), Some("TEST_FAIL"), "code")?;
            require(
                error.message().contains("deliberate failure at chunk 2"),
                error.message(),
            )?;
            eq(n, 2, "chunks before the error")
        })
        .await;
    checks
        .run("mid-stream unhandled error", async {
            let request = TickRequest {
                count: 10,
                fail_at: 1,
                unhandled: true,
                ..Default::default()
            };
            let error = remote(counter.tick(request).map_err(why)?.collect().await)?;
            eq(error.message(), "stream broke", "message")
        })
        .await;
    checks
        .run("cancellation reaches the producer", async {
            let mut ticks = counter
                .tick(TickRequest {
                    count: 500,
                    delay_ms: 10,
                    ..Default::default()
                })
                .map_err(why)?;
            for _ in 0..3 {
                ticks.next().await.ok_or("the stream ended early")?.map_err(why)?;
            }
            drop(ticks);
            let mut produced = Produced::default();
            for _ in 0..100 {
                produced = counter.produced(Nothing {}).await.map_err(why)?;
                if produced.stopped_early {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            require(
                produced.stopped_early && !produced.finished && produced.yielded < 500,
                format!("the {target} producer never saw the cancellation: {produced:?}"),
            )
        })
        .await;
    checks
        .run("custom types, defaults and maps", async {
            let balance = wallet.balance(Query { account: "acc".into() }).await.map_err(why)?;
            eq(balance, canonical(), &format!("balance from {target}"))
        })
        .await;
    checks
        .run("echo round trip", async {
            eq(
                wallet.echo(canonical()).await.map_err(why)?,
                canonical(),
                &format!("echo through {target}"),
            )
        })
        .await;
    checks
        .run("handled error", async {
            let error = remote(wallet.balance(Query { account: "boom".into() }).await)?;
            eq(error.code(), Some("NOT_FOUND"), "code")?;
            eq(error.message(), "no such account", "message")
        })
        .await;
    checks
        .run("unhandled error", async {
            let error = remote(
                wallet
                    .balance(Query {
                        account: "crash".into(),
                    })
                    .await,
            )?;
            eq(error.message(), "kaboom", "message")
        })
        .await;
    checks
        .run("unimplemented method", async {
            eq(
                remote(counter.unimplemented(Nothing {}).await)?.code(),
                Some("PROTOCOL_ERROR"),
                "code",
            )
        })
        .await;
    checks
        .run("call metadata", async {
            let options = CallOptions::new().actor("client-rust").message_id("mid-rust-1");
            let want = Who {
                actor: "client-rust".into(),
                message_id: "mid-rust-1".into(),
                routing_key: "REQUEST.interop.Counter.whoami".into(),
                lang: target.into(),
            };
            eq(
                counter.whoami_with(Nothing {}, options).await.map_err(why)?,
                want,
                "who",
            )
        })
        .await;
    checks
        .run("instance routing", async {
            let who = inst.whoami(Nothing {}).await.map_err(why)?;
            eq(
                who.routing_key.as_str(),
                "REQUEST.interop.Counter.inst1.whoami",
                "routing key",
            )
        })
        .await;
    checks
        .run("events both ways", async {
            let two_to_70 = pow(2, 70);
            let listener = EventListener::new(ctx.connection(), ctx.factory(), None);
            listener.init(None).await.map_err(why)?;
            let (got, mut pongs) = tokio::sync::mpsc::unbounded_channel();
            listener
                .subscribe(
                    move |pong: Ping, _| {
                        let _ = got.send(pong);
                        async { Ok(()) }
                    },
                    Some(&format!("EVENT.pong.{target}")),
                )
                .await
                .map_err(why)?;
            listener.start().await.map_err(why)?;
            let ping = Ping {
                id: "rust-1".into(),
                n: Some(Bigint::from(two_to_70)),
                from: LANG.into(),
            };
            ctx.publish_event(&ping, Some(&format!("EVENT.ping.{target}")))
                .await
                .map_err(why)?;
            let pong = tokio::time::timeout(Duration::from_secs(10), pongs.recv()).await;
            listener.close().await;
            let pong = pong.map_err(|_| "no pong")?.ok_or("no pong")?;
            eq(pong.id.as_str(), "pong:rust-1", "id")?;
            eq(pong.from.as_str(), target, "from")?;
            let n = pong.n.unwrap_or_default().to_u256().map_err(why)?;
            eq(
                n.to_string(),
                two_to_70.checked_add(U256::ONE).unwrap().to_string(),
                "n",
            )
        })
        .await;

    (checks.report)("DONE");
    Ok(checks.failures)
}
