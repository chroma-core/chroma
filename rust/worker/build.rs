fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the protobuf files in the chromadb proto directory.
    tonic_build::configure().compile(
        &[
            "../../idl/chromadb/proto/chroma.proto",
            "../../idl/chromadb/proto/coordinator.proto",
        ],
        &["../../idl/"],
    )?;

    // Compile the hnswlib bindings.
    cc::Build::new()
        .cpp(true)
        .file("bindings.cpp")
        .flag("-std=c++11")
        .flag("-Ofast")
        .flag("-DHAVE_CXX0X")
        .flag("-fpic")
        .flag("-ftree-vectorize")
        .compile("bindings");

    Ok(())
}
