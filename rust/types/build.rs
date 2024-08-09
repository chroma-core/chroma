fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the protobuf files in the chromadb proto directory.
    let mut proto_paths = vec![
        "../../idl/chromadb/proto/chroma.proto",
        "../../idl/chromadb/proto/coordinator.proto",
        "../../idl/chromadb/proto/logservice.proto",
        "../../idl/chromadb/proto/debug.proto",
    ];

    tonic_build::configure()
        .emit_rerun_if_changed(true)
        .compile(&proto_paths, &["../../idl/"])?;

    Ok(())
}
