//! The code generated from `proto/`, shared by the examples.

pub mod proto {
    protobus::include_protos!();
}

/// The broker the examples talk to: `AMQP_URL`.
pub fn amqp_url() -> String {
    std::env::var("AMQP_URL").unwrap_or_else(|_| {
        eprintln!("set AMQP_URL, e.g. amqp://guest:guest@127.0.0.1:29672/ (docker compose up -d --wait)");
        std::process::exit(2);
    })
}
