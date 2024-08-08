fn main() -> Result<(), Box<dyn std::error::Error>> {
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
