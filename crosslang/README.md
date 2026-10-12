# Cross-language suite

protobus-rs against the other ports' real libraries (TypeScript, Python, Go,
C++, Java and C#), over a real broker, in both directions:

| Test | What runs |
|---|---|
| `the_rust_client_against_a_server_in::{ts,py,go,cpp,java,csharp}` | the Rust client scenario against each language's server |
| `the_rust_client_against_a_rust_server_process` | the same, against the Rust server in a process of its own |
| `against_a_rust_server_the_client_in::{ts,py,go,cpp,java,csharp}` | each language's client scenario against the Rust server |
| `mixed_replicas_share_one_retry_ladder` | `interop.Flaky` served in all seven languages at once, sharing one queue and one retry ladder |

Every client runs the same fifteen checks (unary calls, priority, streams in
order, empty streams, mid-stream errors, cancellation reaching the producer,
custom types with maps and defaults, an echo round trip, handled, unhandled and
protocol errors, call metadata, instance routing, and events both ways). The
mixed-replica test requires every message to be attempted exactly four times,
in more than one language, and to reach `interop.Flaky.DLQ` exactly once with
the shared metadata headers. Each test runs in a fresh vhost.

`tests/in_memory.rs` runs the Rust client against the Rust services over the
in-memory broker, with no broker and no other language.

## Layout

- `proto/interop.proto`: the contract, identical to the other ports'.
- `src/lib.rs`, `src/bin/rustpeer.rs`: the Rust participant (`server` / `client`).
- `peers/ts/peer.js`, `peers/py/peer.py`, `peers/go/main.go`: the TypeScript,
  Python and Go participants, the same files the other ports' suites run.
- The C++ participant is protobus-cpp's own `cpppeer`, from its build tree;
  the Java participant is protobus-java's `JavaPeer`, from its `crosslang`
  module; the C# participant is protobus-csharp's `CSharpPeer`.
- `tests/crosslang.rs`: the harness.

## Running it

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:29672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:29673
(cd ../protobus && npm ci && npm run build-ts)        # the TypeScript peer runs the built library
(cd ../protobus-py && python -m venv venv && venv/bin/pip install -e .)
(cmake -S ../protobus-cpp -B ../protobus-cpp/build && cmake --build ../protobus-cpp/build --target cpppeer)
(cd ../protobus-csharp && dotnet build crosslang/CSharpPeer -c Release)
export JAVA_HOME=...                                  # a JDK 17+, for the Java peer
cargo test -p protobus-crosslang --test crosslang
```

The peers are found in sibling checkouts: `PROTOBUS_TS` (default
`../protobus`), `PROTOBUS_PY` (default `../protobus-py`, with a `venv/`),
`PROTOBUS_GO` (default `../protobus-go`, with Go on the PATH), `PROTOBUS_CPP`
(default `../protobus-cpp`, with `build/crosslang/cpppeer`), `PROTOBUS_JAVA`
(default `../protobus-java`, with `JAVA_HOME` set) and `PROTOBUS_CSHARP`
(default `../protobus-csharp`, with its `CSharpPeer` built, and `dotnet` on the
PATH or under `DOTNET_ROOT`). A missing peer skips its tests, unless its
variable is set explicitly, as CI does: then its absence is a failure, so a
misconfigured run cannot pass by skipping.

Any peer can be run by hand against any server:

```bash
PROTOBUS_TEST_AMQP=$PROTOBUS_TEST_AMQP_URL PEER_TARGET=rust node crosslang/peers/ts/peer.js client
PROTOBUS_TEST_AMQP=$PROTOBUS_TEST_AMQP_URL PEER_TARGET=ts cargo run -p protobus-crosslang --bin rustpeer -- client
```
