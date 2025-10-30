fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the protobuf files in the chromadb proto directory.
    let mut proto_paths = vec![
        "idl/chromadb/proto/chroma.proto",
        "idl/chromadb/proto/compactor.proto",
        "idl/chromadb/proto/coordinator.proto",
        "idl/chromadb/proto/heapservice.proto",
        "idl/chromadb/proto/logservice.proto",
        "idl/chromadb/proto/query_executor.proto",
        "idl/chromadb/proto/garbage_collector.proto",
    ];

    // Can't use #[cfg(test)] here because a build for tests is technically a regular debug build, meaning that #[cfg(test)] is useless in build.rs.
    // See https://github.com/rust-lang/cargo/issues/1581
    #[cfg(debug_assertions)]
    let debug_assertions = true;
    #[cfg(not(debug_assertions))]
    let debug_assertions = false;

    if debug_assertions {
        proto_paths.push("idl/chromadb/proto/debug.proto");
    }

    tonic_build::configure()
        .emit_rerun_if_changed(true)
        .compile(&proto_paths, &["idl/"])?;

    // Note: Operator constants are now generated via bin/generate_operator_constants.sh
    // and committed to git as rust/types/src/functions.rs.
    // This avoids needing Go source files in Docker builds.

    Ok(())
}
