//! The Rust client scenario against the Rust services, over the in-memory
//! broker: the cross-language checks, with no broker and no other language.

use std::sync::{Arc, Mutex};

use protobus::testing::MemoryBroker;
use protobus::{Context, ContextOptions};
use protobus_crosslang::{client, serve, LANG};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rust_client_against_the_rust_server() {
    let broker = MemoryBroker::new();
    let options = || ContextOptions {
        transport: Some(Arc::new(broker.clone())),
        ..Default::default()
    };
    let server = Context::new(options());
    server.init("amqp://memory/", &[] as &[&str]).await.unwrap();
    let services = serve(&server).await.unwrap();

    let caller = Context::new(options());
    caller.init("amqp://memory/", &[] as &[&str]).await.unwrap();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = lines.clone();
    let failures = client(&caller, LANG, move |line| sink.lock().unwrap().push(line.to_string()))
        .await
        .unwrap();
    let lines = lines.lock().unwrap().clone();
    assert_eq!(failures, 0, "{lines:#?}");
    assert_eq!(
        lines.iter().filter(|l| l.starts_with("PASS ")).count(),
        15,
        "{lines:#?}"
    );
    assert_eq!(lines.last().map(String::as_str), Some("DONE"));

    caller.close().await;
    for service in &services {
        service.close().await;
    }
    server.close().await;
}
