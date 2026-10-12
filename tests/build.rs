fn main() -> Result<(), Box<dyn std::error::Error>> {
    protobus_build::configure()
        .proto_dir("proto")
        .custom_type("uuid", protobus_build::WireType::String)
        .compile()?;
    Ok(())
}
