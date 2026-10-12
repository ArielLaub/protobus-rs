# Code generation

protobus schemas are ordinary `.proto` files shared by every language. They
need no Rust options and no import for the custom types. `protobus-build`
compiles them in pure Rust (with [protox](https://docs.rs/protox)), so no
`protoc` is needed, and generates messages with
[prost](https://docs.rs/prost).

## In `build.rs`

```toml
[build-dependencies]
protobus-build = "2"
```

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    protobus_build::configure()
        .proto_dir("proto")
        .proto_dir("../shared/protos")
        .custom_type("uuid", protobus_build::WireType::String)
        .compile()?;
    Ok(())
}
```

Every `.proto` under the directories is compiled; paths inside a directory are
the import paths (`proto/billing/invoice.proto` is `billing/invoice.proto`).
The build reruns when a schema changes. Include the result once:

```rust,ignore
pub mod proto {
    protobus::include_protos!();
}
```

| `Builder` | Default | |
|---|---|---|
| `proto_dir(dir)` | | a directory of schemas; repeatable |
| `custom_type(name, wire)` | none | a custom type of your own; see below |
| `out_dir(dir)` | `OUT_DIR` | where the generated code goes |
| `descriptor_out(file)` | none | also write the compiled `FileDescriptorSet`, for the ports that load schemas that way |
| `protobus_path(path)` | `::protobus` | how generated code names the runtime crate |

## What is generated

Each proto package becomes a module: `package Calculator` is
`proto::calculator`, `package shop.orders.v1` is `proto::shop::orders::v1`, and
a schema with no package lands at the root. Messages and enums are prost's:
`message AddRequest` is the struct `AddRequest`, its fields snake_case.

For `service Service` in `package Calculator`:

```rust,ignore
/// An async method per rpc. Each defaults to answering PROTOCOL_ERROR.
pub trait ServiceHandler: Send + Sync + 'static {
    fn add(&self, request: AddRequest, call: CallContext) -> impl Future<Output = Result<AddResponse>> + Send;
    // a streaming rpc also receives `out: StreamWriter<T>` and returns Result<()>
}

/// A MessageService built around a handler. Dereferences to it.
pub struct ServiceService { /* ... */ }
impl ServiceService {
    pub const NAME: &'static str = "Calculator.Service";
    pub fn new<H: ServiceHandler>(context: &Context, handler: H) -> Result<Self>;
    pub fn with_options<H: ServiceHandler>(context: &Context, service_name: &str, handler: H, options: MessageServiceOptions) -> Result<Self>;
    pub fn from_arc<H: ServiceHandler>(context: &Context, service_name: &str, handler: Arc<H>, options: MessageServiceOptions) -> Result<Self>;
}

/// A typed client.
#[derive(Clone)]
pub struct ServiceProxy { /* ... */ }
impl ServiceProxy {
    pub const NAME: &'static str = "Calculator.Service";
    pub fn new(context: &Context) -> Result<Self>;
    pub fn with_name(context: &Context, service_name: &str) -> Result<Self>;
    pub fn raw(&self) -> &protobus::ServiceProxy;
    pub async fn add(&self, request: AddRequest) -> Result<AddResponse>;
    pub async fn add_with(&self, request: AddRequest, options: CallOptions) -> Result<AddResponse>;
    // a streaming rpc returns Result<Stream<T>>, and is not async
}
```

Implement a handler with `async fn`. Method names are the rpc's name in
snake_case (`getUser` is `get_user`); the name on the wire stays the schema's.
An rpc named like a Rust keyword is a raw identifier (`r#type`), and one that
collides with a proxy's own `new`, `with_name` or `raw`, or with another rpc's
`_with` form, gets a trailing underscore on the proxy. Client streaming is not
part of the protobus protocol; such an rpc is skipped. The schemas' comments
become doc comments.

The generated code embeds the compiled schemas
(`proto::FILE_DESCRIPTOR_SET`), and every service and proxy registers them
with its context, so a program built this way loads nothing at run time.

## The CLI

Outside Cargo's build:

```bash
cargo install protobus-build
protobus-rs generate --proto-dir proto --out src/gen \
    [--custom-type uuid:string] [--descriptor-out schemas.binpb]
```

then `include!("gen/protobus_gen.rs");` inside a module. There is no protoc
plugin and no `generate:service` scaffolding in this port.

## Custom types

`bigint` (unsigned, up to 2^256-1, exactly 32 big-endian bytes) and
`timestamp` (signed milliseconds since the epoch) are built in: use them as
field types without an import. In Rust they are `protobus::Bigint` and
`protobus::Timestamp`:

<!-- doc-check: compile -->
```rust
use std::time::SystemTime;
use protobus::{Bigint, Result, Timestamp, U256};

pub fn convert() -> Result<()> {
    let amount = Bigint::from(U256::from(10u64).checked_pow(30).unwrap());
    let parsed = Bigint::parse("0xde0b6b3a7640000")?; // decimal, or 0x-hex
    let value: U256 = amount.to_u256()?;
    let total = value.checked_add(parsed.to_u256()?).expect("fits in 256 bits");
    println!("{total} = {}", total.to_hex());

    let now = Timestamp::now();
    let then = Timestamp::from_millis(-14_182_940_000)?; // before the epoch is fine
    let at: SystemTime = then.to_system_time()?;
    println!("{} ms, {at:?}", now.to_millis()?);
    Ok(())
}
```

`U256` parses and prints decimal and hex, and its arithmetic is checked
(`checked_add`, `checked_sub`, `checked_mul`, `checked_pow`): nothing wraps.
A value no port can carry (a `bigint` wider than 32 bytes, a `timestamp`
beyond ±8.64e15 ms) is refused on the way out, wherever it is nested, and
answered `PROTOCOL_ERROR` on the way in.

### Custom types of your own

`custom_type("uuid", WireType::String)` declares
`message uuid { optional string value = 1; }` at the root, like the built-ins,
so schemas use `uuid` without an import; in Rust it is `proto::Uuid`. The wire
type is one of `Bytes`, `Int64`, `Uint64`, `String`, `Int32`, `Uint32` or
`Double`. Every port that shares the schema must declare it the same way.

## Loading schemas at run time

`context.init(url, &["proto"])` loads schemas into the context's
`MessageFactory` for the untyped API: a directory of `.proto` files, or files
of compiled descriptor sets (`.binpb`, `.desc`, `.pb`, `.protoset`).
`factory.parse(text, name)` adds one schema from its text. Loading `.proto`
text is the `proto-text` feature, on by default; without it only descriptor
sets load.
