fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the protobuf files in the chromadb proto directory.
    let proto_paths = vec![
        "../../idl/chromadb/proto/chroma.proto",
        "../../idl/chromadb/proto/coordinator.proto",
        "../../idl/chromadb/proto/logservice.proto",
    ];

    // Can't use #[cfg(test)] here because a build for tests is technically a regular debug build, meaning that #[cfg(test)] is useless in build.rs.
    // See https://github.com/rust-lang/cargo/issues/1581
    #[cfg(debug_assertions)]
    proto_paths.push("../../idl/chromadb/proto/debug.proto");

    tonic_build::configure()
        .emit_rerun_if_changed(true)
        .compile(&proto_paths, &["../../idl/"])?;

    Ok(())
}
