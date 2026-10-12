//! Schemas at runtime, the untyped API, and the code generator.

use std::path::{Path, PathBuf};

use protobus::prost_reflect::{DynamicMessage, Value};
use protobus::testing::MemoryBroker;
use protobus::{CallContext, ErrorKind, MessageFactory, MessageService, ServiceProxy, StreamWriter};
use protobus_tests::proto::deep::pkg::v1::{thing, NamingHandler, NamingProxy, NamingService, Thing};
use protobus_tests::proto::{RootPing, RootlessHandler, RootlessProxy, RootlessService, Uuid};
use protobus_tests::*;

fn proto_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("proto")
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pbrs-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn method_names_split_from_the_right() {
    assert_eq!(
        MessageFactory::split_method_name("com.example.Calc.add").unwrap(),
        ("com.example.Calc", "add")
    );
    for bad in ["add", ".add", "Calc.", ""] {
        assert_eq!(
            MessageFactory::split_method_name(bad).unwrap_err().kind(),
            ErrorKind::InvalidMethodName,
            "{bad}"
        );
    }
}

#[test]
fn the_builtin_custom_types_are_always_known() {
    let factory = MessageFactory::new();
    assert!(factory.has_type("bigint") && factory.has_type("timestamp"));
    assert!(!factory.has_service("suite.Calc"));
    assert_eq!(
        factory.method("suite.Calc.add").unwrap_err().kind(),
        ErrorKind::UnknownMethod
    );
    assert!(
        !factory.is_streaming_method("suite.Calc.count"),
        "an unknown method is treated as unary"
    );
}

#[test]
fn schemas_load_as_proto_text() {
    let factory = MessageFactory::new();
    // The schema uses bigint, timestamp without importing them: the import is added.
    let text = std::fs::read_to_string(proto_dir().join("suite.proto"))
        .unwrap()
        .replace("uuid id = 5;", "");
    factory.parse(&text, "suite.proto").unwrap();
    factory.parse(&text, "suite.proto").unwrap(); // idempotent per name
    assert!(factory.has_service("suite.Calc"));
    assert_eq!(
        factory.service_method_names("suite.Calc").unwrap(),
        ["add", "slow", "fail", "count", "wallet", "who", "missing"]
    );
    assert!(factory.is_streaming_method("suite.Calc.count"));
    assert_eq!(
        factory.method("suite.Calc.wallet").unwrap().input().full_name(),
        "suite.Money"
    );
    assert_eq!(
        factory.method("suite.Calc.nope").unwrap_err().kind(),
        ErrorKind::UnknownMethod
    );

    let e = factory
        .parse("syntax = \"proto3\"; message {", "bad.proto")
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Schema);
    assert!(e.message().contains("bad.proto"), "{e}");
    let e = MessageFactory::new().load(&["/no/such/dir"]).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Schema);
}

#[test]
fn schemas_load_as_descriptor_sets() {
    let dir = scratch("descriptors");
    std::fs::write(dir.join("schemas.binpb"), protobus_tests::proto::FILE_DESCRIPTOR_SET).unwrap();
    std::fs::write(dir.join("notes.txt"), "not a schema").unwrap();
    let factory = MessageFactory::new();
    factory.load(&[&dir]).unwrap();
    assert!(
        factory.has_service("suite.Calc")
            && factory.has_service("deep.pkg.v1.Naming")
            && factory.has_service("Rootless")
    );
    assert!(factory.has_type("uuid"), "a custom type of our own is a root message");
    // A single file, too; and loading twice changes nothing.
    factory.load(&[dir.join("schemas.binpb")]).unwrap();
    assert_eq!(
        factory.register_descriptor_set(b"\xff\xff").unwrap_err().kind(),
        ErrorKind::Schema
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_context_loads_proto_directories_for_the_dynamic_api() {
    // A copy of the schemas without the custom type only the generator was told about.
    let dir = scratch("protos");
    std::fs::write(
        dir.join("suite.proto"),
        std::fs::read_to_string(proto_dir().join("suite.proto"))
            .unwrap()
            .replace("uuid id = 5;", ""),
    )
    .unwrap();
    let broker = MemoryBroker::new();
    let ctx = protobus::Context::new(protobus::ContextOptions {
        transport: Some(std::sync::Arc::new(broker.clone())),
        reconnection: None,
    });
    ctx.init("amqp://memory/", &[&dir]).await.unwrap();

    // A service and a client with no generated code at all.
    let service = MessageService::new(&ctx, "suite.Calc", fast_retry(0)).unwrap();
    let factory = ctx.factory().clone();
    service.register_method("add", move |request: DynamicMessage, call: CallContext| {
        let factory = factory.clone();
        async move {
            assert_eq!(call.method, "suite.Calc.add");
            let field = |name| request.get_field_by_name(name).and_then(|v| v.as_i32()).unwrap_or(0);
            let mut reply = factory.new_message("suite.AddResponse")?;
            reply.set_field_by_name("result", Value::I32(field("a") + field("b")));
            Ok(reply)
        }
    });
    let factory = ctx.factory().clone();
    service.register_streaming_method(
        "count",
        move |request: DynamicMessage, _, out: StreamWriter<DynamicMessage>| {
            let factory = factory.clone();
            async move {
                let count = request.get_field_by_name("count").and_then(|v| v.as_i32()).unwrap_or(0);
                for value in 0..count {
                    let mut number = factory.new_message("suite.Number")?;
                    number.set_field_by_name("value", Value::I32(value));
                    out.send(number).await?;
                }
                Ok(())
            }
        },
    );
    // A handler returning the wrong type is an error of the service's, not the caller's.
    let factory = ctx.factory().clone();
    service.register_method("who", move |_, _| {
        let factory = factory.clone();
        async move { factory.new_message("suite.Empty") }
    });
    service.init().await.unwrap();

    let calc = ServiceProxy::new(&ctx, "suite.Calc");
    calc.init().unwrap();
    let mut request = ctx.factory().new_message("suite.AddRequest").unwrap();
    request.set_field_by_name("a", Value::I32(20));
    request.set_field_by_name("b", Value::I32(22));
    let reply = calc.call("add", &request, &Default::default()).await.unwrap();
    assert_eq!(reply.get_field_by_name("result").unwrap().as_i32(), Some(42));

    let mut request = ctx.factory().new_message("suite.CountRequest").unwrap();
    request.set_field_by_name("count", Value::I32(3));
    let numbers = calc
        .call_stream("count", &request, &Default::default())
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values: Vec<i32> = numbers
        .iter()
        .map(|n| n.get_field_by_name("value").unwrap().as_i32().unwrap())
        .collect();
    assert_eq!(values, [0, 1, 2]);

    // The request must be of the method's request type.
    let wrong = ctx.factory().new_message("suite.Empty").unwrap();
    assert_eq!(
        calc.call("add", &wrong, &Default::default()).await.unwrap_err().kind(),
        ErrorKind::InvalidRequest
    );
    let e = calc.call("who", &wrong, &Default::default()).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Remote);
    assert!(e.message().contains("must return suite.Who, not suite.Empty"), "{e}");

    // Raw payloads work against the same service, typed or not.
    let payload = prost::Message::encode_to_vec(&AddRequest { a: 1, b: 2 });
    let reply = calc.call_raw("add", payload, &Default::default()).await.unwrap();
    assert_eq!(
        <AddResponse as prost::Message>::decode(reply.as_slice())
            .unwrap()
            .result,
        3
    );

    service.close().await;
    ctx.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

struct Named;

impl NamingHandler for Named {
    async fn new(&self, r: Thing, _: CallContext) -> protobus::Result<Thing> {
        Ok(Thing {
            r#type: format!("new:{}", r.r#type),
            ..r
        })
    }

    async fn r#type(&self, r: Thing, _: CallContext) -> protobus::Result<Thing> {
        Ok(r)
    }

    async fn get_user(&self, r: Thing, _: CallContext, out: StreamWriter<Thing>) -> protobus::Result<()> {
        out.send(r.clone()).await?;
        out.send(r).await
    }

    async fn pay(&self, r: Money, _: CallContext) -> protobus::Result<Money> {
        Ok(r)
    }
}

struct Root;

impl RootlessHandler for Root {
    async fn ping(&self, r: RootPing, _: CallContext) -> protobus::Result<RootPing> {
        Ok(r)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_names_follow_the_schema_wherever_rust_allows() {
    let broker = MemoryBroker::new();
    let ctx = connect(&broker).await;
    assert_eq!(NamingService::NAME, "deep.pkg.v1.Naming");
    assert_eq!(RootlessProxy::NAME, "Rootless");
    let naming = NamingService::new(&ctx, Named).unwrap();
    naming.init().await.unwrap();
    let rootless = RootlessService::new(&ctx, Root).unwrap();
    rootless.init().await.unwrap();
    assert!(broker.queue_exists("deep.pkg.v1.Naming") && broker.queue_exists("Rootless"));

    let proxy = NamingProxy::new(&ctx).unwrap();
    let thing = Thing {
        r#type: "t".into(),
        inner: Some(thing::Inner { n: Some(9u64.into()) }),
        choice: Some(thing::Choice::At(protobus::Timestamp::from_millis(5).unwrap())),
    };
    // rpc new / raw: the proxy's own `new` and `raw` keep their names.
    assert_eq!(proxy.new_(thing.clone()).await.unwrap().r#type, "new:t");
    assert_eq!(proxy.raw().service_name(), "deep.pkg.v1.Naming");
    // rpc raw is declared and not implemented.
    assert_eq!(
        proxy.raw_(thing.clone()).await.unwrap_err().code(),
        Some("PROTOCOL_ERROR")
    );
    // A keyword, a camelCase rpc, and one whose name collides with another's `_with`.
    assert_eq!(proxy.r#type(thing.clone()).await.unwrap(), thing);
    assert_eq!(proxy.get_user(thing.clone()).unwrap().collect().await.unwrap().len(), 2);
    assert_eq!(
        proxy.get_user_with(thing.clone()).await.unwrap_err().code(),
        Some("PROTOCOL_ERROR")
    );
    // The wire name is the schema's, whatever Rust calls the method.
    assert!(proxy.raw().methods().contains(&"getUser".to_string()));
    assert!(!proxy.raw().methods().is_empty());
    // A type from another package, and a custom type nested in a oneof and a submessage.
    assert_eq!(
        proxy
            .pay(Money {
                amount: Some(3u64.into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .amount,
        Some(3u64.into())
    );
    let wide = Thing {
        inner: Some(thing::Inner {
            n: Some(protobus::Bigint {
                value: Some(vec![1; 33]),
            }),
        }),
        ..Default::default()
    };
    assert_eq!(proxy.new_(wide).await.unwrap_err().kind(), ErrorKind::CustomTypeRange);

    let ping = RootPing {
        text: "hi".into(),
        id: Some(Uuid {
            value: Some("u-1".into()),
        }),
    };
    assert_eq!(
        RootlessProxy::new(&ctx).unwrap().ping(ping.clone()).await.unwrap(),
        ping
    );

    naming.close().await;
    rootless.close().await;
    ctx.close().await;
}

#[test]
fn the_generator_runs_outside_a_build_script() {
    let out = scratch("gen");
    let descriptors = out.join("sets/all.binpb");
    protobus_build::configure()
        .proto_dir(proto_dir())
        .custom_type("uuid", protobus_build::WireType::String)
        .out_dir(&out)
        .descriptor_out(&descriptors)
        .quiet()
        .compile()
        .unwrap();
    for file in [
        "protobus_gen.rs",
        "protobus_descriptor.bin",
        "suite.rs",
        "deep.pkg.v1.rs",
        "_.rs",
    ] {
        assert!(out.join(file).exists(), "{file}");
    }
    let suite = std::fs::read_to_string(out.join("suite.rs")).unwrap();
    for expected in [
        "pub trait CalcHandler",
        "pub struct CalcService",
        "pub struct CalcProxy",
        "/// Adds two numbers.",
    ] {
        assert!(suite.contains(expected), "{expected}");
    }
    // Client streaming is not part of the protocol: no trace of the rpc.
    assert!(!std::fs::read_to_string(out.join("deep.pkg.v1.rs"))
        .unwrap()
        .contains("upload"));
    // The descriptor set is what the ports that load descriptor sets need.
    let factory = MessageFactory::new();
    factory.load(&[&descriptors]).unwrap();
    assert!(factory.has_service("suite.Calc"));

    let e = protobus_build::configure()
        .proto_dir(out.join("nowhere"))
        .out_dir(&out)
        .quiet()
        .compile()
        .unwrap_err();
    assert!(e.to_string().contains("does not exist"), "{e}");
    // A custom type the generator was not told about is an unknown type.
    let e = protobus_build::configure()
        .proto_dir(proto_dir())
        .out_dir(&out)
        .quiet()
        .compile()
        .unwrap_err();
    assert!(e.to_string().contains("uuid"), "{e}");
    std::fs::remove_dir_all(out).unwrap();
}
