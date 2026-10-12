# Changelog

## 2.0.0

The first release of the Rust port. The version matches the protobus wire
protocol it speaks (TypeScript protobus 2.5 and the 2.0 ports).

- `protobus`: the runtime. Contexts, services, proxies, server streaming with
  cancellation, events with opt-in retry, retries and dead-lettering,
  reconnection with coordinated restoration, confirmed publishing, the custom
  types `bigint` and `timestamp`, and an in-memory broker for tests.
- `protobus-build`: code generation in `build.rs` or from the `protobus-rs`
  CLI, in pure Rust.
- `protobus-schema`: the schema compiler the two share.
