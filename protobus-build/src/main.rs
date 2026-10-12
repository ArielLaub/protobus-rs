//! protobus-rs: generate Rust code from protobus schemas, outside a build script.
//!
//! ```text
//! protobus-rs generate --proto-dir DIR... --out DIR [--custom-type NAME:WIRE]... [--descriptor-out FILE]
//! ```

use std::process::ExitCode;

const USAGE: &str = "\
protobus-rs: generate Rust code from protobus schemas

USAGE:
    protobus-rs generate --proto-dir DIR... --out DIR [OPTIONS]
    protobus-rs version
    protobus-rs help

OPTIONS:
    --proto-dir DIR          a directory of .proto files, read recursively (repeatable)
    --out DIR                where the generated code goes: protobus_gen.rs and its modules
    --custom-type NAME:WIRE  a custom type of your own; WIRE is bytes, int64, uint64,
                             string, int32, uint32 or double (repeatable)
    --descriptor-out FILE    also write the compiled FileDescriptorSet there
    --protobus-path PATH     how generated code names the protobus crate (default ::protobus)

Include the result with:  include!(\"<out>/protobus_gen.rs\");
In a Cargo project, prefer build.rs: protobus_build::configure().proto_dir(\"proto\").compile()
";

fn run(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str) {
        None | Some("help" | "--help" | "-h") => {
            print!("{USAGE}");
            Ok(())
        }
        Some("version" | "--version" | "-V") => {
            println!("protobus-rs {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("generate") => generate(&args[1..]),
        Some(other) => Err(format!("unknown command '{other}'\n\n{USAGE}")),
    }
}

fn generate(args: &[String]) -> Result<(), String> {
    let mut builder = protobus_build::configure().quiet();
    let (mut dirs, mut out) = (0, false);
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--proto-dir" => {
                builder = builder.proto_dir(value()?);
                dirs += 1;
            }
            "--out" => {
                builder = builder.out_dir(value()?);
                out = true;
            }
            "--descriptor-out" => builder = builder.descriptor_out(value()?),
            "--protobus-path" => builder = builder.protobus_path(value()?),
            "--custom-type" => {
                let t = protobus_build::CustomType::parse(value()?).map_err(|e| e.to_string())?;
                builder = builder.custom_type(&t.name, t.wire);
            }
            other => return Err(format!("unknown option '{other}'\n\n{USAGE}")),
        }
    }
    if dirs == 0 {
        return Err("generate needs at least one --proto-dir".into());
    }
    if !out {
        return Err("generate needs --out".into());
    }
    builder.compile().map_err(|e| e.to_string())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("protobus-rs: {message}");
            ExitCode::FAILURE
        }
    }
}
