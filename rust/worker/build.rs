fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the protobuf files in the chromadb proto directory.
    tonic_build::configure()
        .emit_rerun_if_changed(true)
        .compile(
            &[
                "../../idl/chromadb/proto/chroma.proto",
                "../../idl/chromadb/proto/coordinator.proto",
                "../../idl/chromadb/proto/logservice.proto",
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
        .flag("-w")
        .compile("bindings");

    // Set a compile flag based on an environment variable that tells us if we should
    // run the cluster tests
    let run_cluster_tests_env_var = std::env::var("CHROMA_KUBERNETES_INTEGRATION");
    match run_cluster_tests_env_var {
        Ok(val) => {
            let lowered = val.to_lowercase();
            if lowered == "true" || lowered == "1" {
                println!("cargo:rustc-cfg=CHROMA_KUBERNETES_INTEGRATION");
            }
        }
        Err(_) => {}
    }

    Ok(())
}
