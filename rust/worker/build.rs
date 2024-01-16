fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &[
            "../../idl/chromadb/proto/chroma.proto",
            "../../idl/chromadb/proto/coordinator.proto",
        ],
        &["../../idl/"],
    )?;
    Ok(())
}
