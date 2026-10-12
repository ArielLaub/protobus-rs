fn main() -> Result<(), Box<dyn std::error::Error>> {
    protobus_build::configure().proto_dir("proto").compile()?;
    Ok(())
}
