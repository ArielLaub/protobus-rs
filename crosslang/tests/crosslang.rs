//! The cross-language suite: protobus-rs against the TypeScript, Python, Go,
//! C++, Java and C# ports' real libraries over a real broker, in both
//! directions.
//!
//!   - the Rust client against a Rust, TypeScript, Python, Go, C++, Java and
//!     C# server;
//!   - each of those languages' clients against a Rust server;
//!   - replicas of one service in all seven languages sharing a queue and its
//!     retry ladder.
//!
//! It needs a broker (`PROTOBUS_TEST_AMQP_URL` and `PROTOBUS_TEST_MGMT_URL`)
//! and the sibling checkouts: `PROTOBUS_TS` (default `../protobus`, built with
//! `npm run build-ts`), `PROTOBUS_PY` (default `../protobus-py`, with a
//! `venv/`), `PROTOBUS_GO` (default `../protobus-go`, with Go on the PATH),
//! `PROTOBUS_CPP` (default `../protobus-cpp`, with `build/crosslang/cpppeer`),
//! `PROTOBUS_JAVA` (default `../protobus-java`, with `JAVA_HOME` set) and
//! `PROTOBUS_CSHARP` (default `../protobus-csharp`, with its `CSharpPeer`
//! built and `dotnet` on the PATH or under `DOTNET_ROOT`). A missing peer
//! skips its tests, unless its variable is set explicitly, as CI does: then
//! its absence is a failure, so a misconfigured run cannot pass by skipping.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use protobus::{CallOptions, Context, ErrorKind, EventListener};
use protobus_crosslang::interop::{Attempted, FailRequest, FlakyProxy};
use protobus_tests::real_broker::VHost;

const OTHERS: [&str; 6] = ["ts", "py", "go", "cpp", "java", "csharp"];

fn here() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn sibling(variable: &str, name: &str) -> PathBuf {
    match var(variable) {
        Some(path) => PathBuf::from(path),
        None => here().join("../..").join(name),
    }
}

// ---- peer processes --------------------------------------------------------------

/// A peer process, its stdout read line by line.
struct Process {
    child: Child,
    lines: Receiver<String>,
    stderr: Arc<Mutex<String>>,
}

impl Process {
    fn start(command: &PeerCommand, env: &BTreeMap<String, String>) -> Process {
        let mut child = Command::new(&command.program)
            .args(&command.args)
            .envs(&command.env)
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("cannot start {}: {e}", command.program));
        let (tx, lines) = channel();
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line.trim_end().to_string()).is_err() {
                    break;
                }
            }
        });
        let stderr = Arc::new(Mutex::new(String::new()));
        let (sink, mut pipe) = (stderr.clone(), child.stderr.take().unwrap());
        std::thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            while let Ok(n) = pipe.read(&mut buffer) {
                if n == 0 {
                    break;
                }
                sink.lock().unwrap().push_str(&String::from_utf8_lossy(&buffer[..n]));
            }
        });
        Process { child, lines, stderr }
    }

    /// The next stdout line, or `None` at end of output or after `timeout`.
    fn line(&self, timeout: Duration) -> Option<String> {
        self.lines.recv_timeout(timeout).ok()
    }

    fn stderr(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone)]
struct PeerCommand {
    program: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
}

fn command(program: impl AsRef<Path>, args: &[&str]) -> PeerCommand {
    PeerCommand {
        program: program.as_ref().to_string_lossy().into_owned(),
        args: args.iter().map(|a| a.to_string()).collect(),
        env: BTreeMap::new(),
    }
}

fn run(program: &Path, args: &[&str], dir: &Path, env: &[(&str, &str)]) -> (bool, String) {
    match Command::new(program)
        .args(args)
        .current_dir(dir)
        .envs(env.iter().copied())
        .output()
    {
        Ok(out) => (
            out.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        ),
        Err(e) => (false, e.to_string()),
    }
}

/// The Go peer, built once in a scratch copy whose go.mod points at the checkout under test.
fn go_peer() -> Result<PathBuf, String> {
    static BUILT: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let go = sibling("PROTOBUS_GO", "protobus-go");
            if !go.join("crosslang/gopeer/gopeer.go").exists() {
                return Err(format!("protobus-go not found at {} (set PROTOBUS_GO)", go.display()));
            }
            let dir = std::env::temp_dir().join(format!("pbrs-gopeer-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            std::fs::copy(here().join("peers/go/main.go"), dir.join("main.go")).map_err(|e| e.to_string())?;
            std::fs::copy(go.join("go.sum"), dir.join("go.sum")).map_err(|e| e.to_string())?;
            let absolute = go.canonicalize().map_err(|e| e.to_string())?;
            std::fs::write(
                dir.join("go.mod"),
                format!(
                    "module protobus-rs/crosslang/gopeer\n\ngo 1.25\n\nrequire github.com/ArielLaub/protobus-go/v2 v2.0.0\n\n\
                     replace github.com/ArielLaub/protobus-go/v2 => {}\n",
                    absolute.display()
                ),
            )
            .map_err(|e| e.to_string())?;
            let (ok, output) = run(Path::new("go"), &["build", "-o", "gopeer", "."], &dir, &[("GOFLAGS", "-mod=mod")]);
            if !ok {
                return Err(format!("could not build the Go peer (is Go on the PATH?): {output}"));
            }
            Ok(dir.join("gopeer"))
        })
        .clone()
}

fn java_exe() -> Option<PathBuf> {
    let java = PathBuf::from(var("JAVA_HOME")?).join("bin/java");
    java.exists().then_some(java)
}

fn java_classpath() -> Result<String, String> {
    static BUILT: OnceLock<Result<String, String>> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let java = sibling("PROTOBUS_JAVA", "protobus-java");
            if !java.join("gradlew").exists() {
                return Err(format!(
                    "protobus-java not found at {} (set PROTOBUS_JAVA)",
                    java.display()
                ));
            }
            if java_exe().is_none() {
                return Err("JAVA_HOME is not set to a JDK (needed for the Java peer)".into());
            }
            // The runtime is on the classpath as its jar, which compiling the peer does not build.
            let tasks = ["-q", ":protobus:jar", ":crosslang:classes", ":crosslang:printClasspath"];
            let (ok, output) = run(&java.join("gradlew"), &tasks, &java, &[]);
            // Gradle may print notices around it: the classpath is the line holding the module's classes.
            let classpath = output
                .lines()
                .map(str::trim)
                .find(|l| l.contains("crosslang/build/classes") && l.contains(':'));
            match (ok, classpath) {
                (true, Some(cp)) => Ok(cp.to_string()),
                _ => Err(format!("could not build the Java peer: {output}")),
            }
        })
        .clone()
}

fn csharp_peer() -> Result<(PathBuf, PathBuf), String> {
    let csharp = sibling("PROTOBUS_CSHARP", "protobus-csharp");
    let dll = ["Release", "Debug"]
        .iter()
        .map(|c| csharp.join(format!("crosslang/CSharpPeer/bin/{c}/net8.0/CSharpPeer.dll")))
        .find(|p| p.exists())
        .ok_or_else(|| {
            format!(
                "protobus-csharp's CSharpPeer not built under {} (set PROTOBUS_CSHARP)",
                csharp.display()
            )
        })?;
    let dotnet = var("DOTNET_ROOT")
        .map(|root| PathBuf::from(root).join("dotnet"))
        .filter(|p| p.exists())
        .or_else(|| {
            let home = PathBuf::from(var("HOME")?).join(".dotnet/dotnet");
            home.exists().then_some(home)
        })
        .unwrap_or_else(|| PathBuf::from("dotnet"));
    if Command::new(&dotnet)
        .arg("--version")
        .output()
        .map(|o| !o.status.success())
        .unwrap_or(true)
    {
        return Err("dotnet is not on the PATH or under DOTNET_ROOT (needed for the C# peer)".into());
    }
    Ok((dotnet, dll))
}

/// The command for a peer, and the variable that configures it, or why it is unavailable.
fn peer(lang: &str, mode: &str) -> (Option<&'static str>, Result<PeerCommand, String>) {
    match lang {
        "rust" => (None, Ok(command(env!("CARGO_BIN_EXE_rustpeer"), &[mode]))),
        "ts" => {
            let ts = sibling("PROTOBUS_TS", "protobus");
            let found = if ts.join("dist/lib/context.js").exists() {
                let mut c = command("node", &[&here().join("peers/ts/peer.js").to_string_lossy(), mode]);
                c.env.insert("PROTOBUS_TS".into(), ts.to_string_lossy().into_owned());
                Ok(c)
            } else {
                Err(format!(
                    "TypeScript protobus not built at {} (set PROTOBUS_TS)",
                    ts.display()
                ))
            };
            (Some("PROTOBUS_TS"), found)
        }
        "py" => {
            let py = sibling("PROTOBUS_PY", "protobus-py");
            let interpreter = py.join("venv/bin/python");
            let found = if interpreter.exists() {
                let mut c = command(
                    &interpreter,
                    &[&here().join("peers/py/peer.py").to_string_lossy(), mode],
                );
                c.env.insert("PYTHONPATH".into(), py.to_string_lossy().into_owned());
                Ok(c)
            } else {
                Err(format!(
                    "protobus-py venv not found at {} (set PROTOBUS_PY)",
                    py.display()
                ))
            };
            (Some("PROTOBUS_PY"), found)
        }
        "go" => (Some("PROTOBUS_GO"), go_peer().map(|bin| command(bin, &[mode]))),
        "cpp" => {
            let bin = sibling("PROTOBUS_CPP", "protobus-cpp").join("build/crosslang/cpppeer");
            let found = if bin.exists() {
                Ok(command(&bin, &[mode]))
            } else {
                Err(format!(
                    "protobus-cpp's cpppeer not built at {} (set PROTOBUS_CPP)",
                    bin.display()
                ))
            };
            (Some("PROTOBUS_CPP"), found)
        }
        "java" => {
            let found = java_classpath().map(|cp| {
                command(
                    java_exe().unwrap(),
                    &["-cp", &cp, "io.github.ariellaub.protobus.crosslang.JavaPeer", mode],
                )
            });
            (Some("PROTOBUS_JAVA"), found)
        }
        "csharp" => {
            let found = csharp_peer().map(|(dotnet, dll)| command(dotnet, &[&dll.to_string_lossy(), mode]));
            (Some("PROTOBUS_CSHARP"), found)
        }
        other => panic!("no peer {other}"),
    }
}

/// One test's broker and the peers it started.
struct Suite {
    vhost: VHost,
    servers: Vec<Process>,
}

/// A peer that is not available: the test skips, unless the peer was configured explicitly.
struct Unavailable(String);

impl Suite {
    /// `None` skips the test: the broker variables are not both set.
    fn new() -> Option<Suite> {
        match VHost::create() {
            Some(vhost) => Some(Suite {
                vhost,
                servers: Vec::new(),
            }),
            None => {
                eprintln!("skipped: PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL are not both set");
                None
            }
        }
    }

    fn command(&self, lang: &str, mode: &str) -> Result<PeerCommand, Unavailable> {
        let (variable, found) = peer(lang, mode);
        found.map_err(|why| {
            if variable.is_some_and(|v| var(v).is_some()) {
                panic!("{why}");
            }
            Unavailable(why)
        })
    }

    fn env(&self, target: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("PROTOBUS_TEST_AMQP".to_string(), self.vhost.url().to_string()),
            (
                "PROTOBUS_TEST_PROTO_DIR".to_string(),
                here().join("proto").to_string_lossy().into_owned(),
            ),
            ("PEER_TARGET".to_string(), target.to_string()),
        ])
    }

    /// Start a peer's server, returning once it has printed READY.
    fn start_server(&mut self, lang: &str) -> Result<(), Unavailable> {
        let process = Process::start(&self.command(lang, "server")?, &self.env(lang));
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match process.line(deadline.saturating_duration_since(Instant::now())) {
                Some(line) if line == "READY" => break,
                Some(_) => {}
                None => panic!("{lang} server did not become ready\n{}", process.stderr()),
            }
        }
        self.servers.push(process);
        Ok(())
    }

    /// Run a peer's client scenario against `target`, requiring every check to pass.
    fn run_client(&self, lang: &str, target: &str) -> Result<(), Unavailable> {
        let process = Process::start(&self.command(lang, "client")?, &self.env(target));
        let (mut passed, mut done, mut failed) = (0, false, Vec::new());
        while let Some(line) = process.line(Duration::from_secs(180)) {
            if line.starts_with("PASS ") {
                passed += 1;
            } else if let Some(failure) = line.strip_prefix("FAIL ") {
                failed.push(failure.to_string());
            } else if line == "DONE" {
                done = true;
                break;
            }
        }
        assert!(
            failed.is_empty(),
            "{lang} client against {target}: {failed:#?}\n{}",
            process.stderr()
        );
        assert!(done, "{lang} client did not finish\n{}", process.stderr());
        assert!(
            passed >= 15,
            "{lang} client against {target} passed only {passed} checks"
        );
        Ok(())
    }
}

fn skip(unavailable: Unavailable) {
    eprintln!("skipped: {}", unavailable.0);
}

// ---- the tests -------------------------------------------------------------------

fn rust_client_against(target: &str) {
    let Some(mut suite) = Suite::new() else {
        return;
    };
    if let Err(unavailable) = suite
        .start_server(target)
        .and_then(|()| suite.run_client("rust", target))
    {
        skip(unavailable);
    }
}

fn client_against_a_rust_server(lang: &str) {
    let Some(mut suite) = Suite::new() else {
        return;
    };
    suite.start_server("rust").unwrap_or_else(|u| panic!("{}", u.0));
    if let Err(unavailable) = suite.run_client(lang, "rust") {
        skip(unavailable);
    }
}

macro_rules! both_directions {
    ($($lang:ident),*) => {
        mod the_rust_client_against_a_server_in {
            $(#[test] fn $lang() { super::rust_client_against(stringify!($lang)); })*
        }
        mod against_a_rust_server_the_client_in {
            $(#[test] fn $lang() { super::client_against_a_rust_server(stringify!($lang)); })*
        }
    };
}

both_directions!(ts, py, go, cpp, java, csharp);

#[test]
fn the_rust_client_against_a_rust_server_process() {
    rust_client_against("rust");
}

/// `interop.Flaky` in all seven languages at once, competing on one queue.
/// Every attempt fails, so each message climbs the retry ladder across
/// replicas of different languages: the `x-retry-count` one writes is read by
/// the others, the queue arguments each declares must be equivalent to the
/// rest's, and the message must end in the dead-letter queue exactly once,
/// after exactly three retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_replicas_share_one_retry_ladder() {
    const MESSAGES: usize = 9;
    const RETRIES: usize = 3;
    let Some(mut suite) = Suite::new() else {
        return;
    };
    for lang in std::iter::once("rust").chain(OTHERS) {
        if let Err(unavailable) = suite.start_server(lang) {
            return skip(unavailable);
        }
    }

    let ctx = Context::default();
    ctx.init(suite.vhost.url(), &[] as &[&str]).await.unwrap();
    let listener = EventListener::new(ctx.connection(), ctx.factory(), None);
    listener.init(None).await.unwrap();
    let attempts: Arc<Mutex<Vec<Attempted>>> = Arc::default();
    let seen = attempts.clone();
    listener
        .subscribe(
            move |attempted: Attempted, _| {
                seen.lock().unwrap().push(attempted);
                async { Ok(()) }
            },
            Some("EVENT.attempted"),
        )
        .await
        .unwrap();
    listener.start().await.unwrap();

    let flaky = FlakyProxy::new(&ctx).unwrap();
    let mut calls = Vec::new();
    for i in 0..MESSAGES {
        let flaky = flaky.clone();
        calls.push(tokio::spawn(async move {
            let options = CallOptions::new().message_id(format!("flaky-{i}")).timeout_ms(60_000);
            // The caller is answered with the final failure.
            let error = flaky
                .fail_with(FailRequest { id: i.to_string() }, options)
                .await
                .expect_err("fail succeeded");
            assert_eq!(error.kind(), ErrorKind::Remote, "{error}");
            assert!(error.message().starts_with("flaky "), "{error}");
        }));
    }
    for call in calls {
        call.await.unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    while attempts.lock().unwrap().len() < MESSAGES * (RETRIES + 1) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let attempts = attempts.lock().unwrap().clone();
    let mut per_message: BTreeMap<String, usize> = BTreeMap::new();
    let mut langs: BTreeMap<String, usize> = BTreeMap::new();
    for a in &attempts {
        *per_message.entry(a.message_id.clone()).or_default() += 1;
        *langs.entry(a.lang.clone()).or_default() += 1;
    }
    eprintln!("attempts by language: {langs:?}");
    assert_eq!(attempts.len(), MESSAGES * (RETRIES + 1), "{per_message:?}");
    assert!(per_message.values().all(|n| *n == RETRIES + 1), "{per_message:?}");
    assert!(
        langs.len() >= 2,
        "the ladder never crossed languages; the test proves nothing"
    );

    let channel = ctx.connection().open_channel().await.unwrap();
    let (dead, mut letters) = tokio::sync::mpsc::unbounded_channel();
    channel
        .raw()
        .consume(
            "interop.Flaky.DLQ",
            "dlq",
            true,
            false,
            Arc::new(move |delivery| {
                let _ = dead.send(delivery);
                Box::pin(async {})
            }),
            None,
        )
        .await
        .unwrap();
    let mut ids = BTreeSet::new();
    for _ in 0..MESSAGES {
        let delivery = tokio::time::timeout(Duration::from_secs(30), letters.recv())
            .await
            .expect("a dead letter")
            .unwrap();
        let headers = delivery.properties.headers.clone().unwrap_or_default();
        assert_eq!(headers["x-retry-count"].as_int(), Some(RETRIES as i64));
        assert_eq!(headers["x-original-queue"].as_text().as_deref(), Some("interop.Flaky"));
        assert_eq!(
            headers["x-original-routing-key"].as_text().as_deref(),
            Some("REQUEST.interop.Flaky.fail")
        );
        let last = headers["x-last-error"].as_text().unwrap_or_default();
        assert!(!last.is_empty());
        assert!(
            !last.contains("flaky"),
            "x-last-error must name the class, never the message: {last}"
        );
        ids.insert(delivery.properties.message_id.unwrap_or_default());
    }
    assert_eq!(ids.len(), MESSAGES, "each message must be dead-lettered exactly once");
    // Nothing more follows: no message was dead-lettered twice.
    assert!(tokio::time::timeout(Duration::from_millis(500), letters.recv())
        .await
        .is_err());

    listener.close().await;
    ctx.close().await;
}
