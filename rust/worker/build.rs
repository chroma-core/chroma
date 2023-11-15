fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../idl/chromadb/proto/chroma.proto")?;
    Ok(())
}
